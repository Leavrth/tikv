use std::{
    collections::{BTreeSet, HashMap},
    marker::PhantomData,
    pin::Pin,
    process::Output,
    sync::Arc,
    task::ready,
    time::Duration,
};

use async_compression::futures::write::ZstdDecoder;
use engine_traits::{
    CfName, ExternalSstFileInfo, SstCompressionType, SstExt, SstMetaInfo, SstWriter,
    SstWriterBuilder,
};
use external_storage::ExternalStorage;
use futures::io::{AllowStdIo, AsyncReadExt, AsyncWriteExt, Cursor};
use tikv_util::{
    codec::{
        self,
        stream_event::{self, Iterator as KvStreamIter},
    },
    config::ReadableSize,
    stream::block_on_external_io,
    time::Instant,
};
use tokio::{io::AsyncRead, sync::mpsc::Receiver};
use tokio_stream::Stream;

use super::{
    errors::Result,
    storage::{LogFile, LogFileId},
    util::{Cooperate, ExecuteAllExt},
};

#[derive(Debug)]
pub struct Compaction {
    pub source: Vec<LogFileId>,
    pub size: u64,
    pub region_id: u64,
    pub cf: &'static str,
    pub max_ts: u64,
    pub min_ts: u64,
    pub min_key: Arc<[u8]>,
    pub max_key: Arc<[u8]>,
}

struct UnformedCompaction {
    size: u64,
    files: Vec<LogFileId>,
    min_ts: u64,
    max_ts: u64,
    min_key: Arc<[u8]>,
    max_key: Arc<[u8]>,
}

#[pin_project::pin_project]
pub struct CollectCompaction<S: Stream<Item = Result<LogFile>>> {
    #[pin]
    inner: S,
    last_compactions: Option<Vec<Compaction>>,

    collector: CompactionCollector,
}

impl<S: Stream<Item = Result<LogFile>>> CollectCompaction<S> {
    pub fn new(s: S) -> Self {
        CollectCompaction {
            inner: s,
            last_compactions: None,
            collector: CompactionCollector {
                items: HashMap::new(),
                compaction_size_threshold: ReadableSize::mb(128).0,
            },
        }
    }
}

#[derive(Hash, Debug, PartialEq, Eq, Clone, Copy)]
struct CompactionCollectKey {
    cf: &'static str,
    region_id: u64,
}

struct CompactionCollector {
    items: HashMap<CompactionCollectKey, UnformedCompaction>,
    compaction_size_threshold: u64,
}

impl CompactionCollector {
    fn add_new_file(&mut self, file: LogFile) -> Option<Compaction> {
        use std::collections::hash_map::Entry;
        let key = CompactionCollectKey {
            region_id: file.region_id,
            cf: file.cf,
        };
        match self.items.entry(key) {
            Entry::Occupied(mut o) => {
                let key = *o.key();
                let u = o.get_mut();
                u.files.push(file.id);
                u.size += file.real_size;
                u.min_ts = u.min_ts.min(file.min_ts);
                u.max_ts = u.max_ts.max(file.max_ts);
                if u.max_key < file.max_key {
                    u.max_key = file.max_key;
                }
                if u.min_key > file.min_key {
                    u.min_key = file.min_key;
                }

                if u.size > self.compaction_size_threshold {
                    let c = Compaction {
                        source: std::mem::take(&mut u.files),
                        region_id: key.region_id,
                        cf: key.cf,
                        size: u.size,
                        min_ts: u.min_ts,
                        max_ts: u.max_ts,
                        min_key: u.min_key.clone(),
                        max_key: u.max_key.clone(),
                    };
                    o.remove();
                    return Some(c);
                }
            }
            Entry::Vacant(v) => {
                let u = UnformedCompaction {
                    size: file.real_size,
                    files: vec![file.id],
                    min_ts: file.min_ts,
                    max_ts: file.max_ts,
                    min_key: file.min_key.clone(),
                    max_key: file.max_key.clone(),
                };
                v.insert(u);
            }
        }
        None
    }

    fn take_pending_compactions(&mut self) -> impl Iterator<Item = Compaction> + '_ {
        self.items.drain().map(|(key, c)| Compaction {
            source: c.files,
            region_id: key.region_id,
            size: c.size,
            cf: key.cf,
            max_ts: c.max_ts,
            min_ts: c.min_ts,
            min_key: c.min_key,
            max_key: c.max_key,
        })
    }
}

impl<S: Stream<Item = Result<LogFile>>> Stream for CollectCompaction<S> {
    type Item = Result<Compaction>;

    fn poll_next(
        self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Option<Self::Item>> {
        let mut this = self.project();
        loop {
            if let Some(finalize) = this.last_compactions {
                return finalize.pop().map(Ok).into();
            }

            let item = ready!(this.inner.as_mut().poll_next(cx));
            match item {
                None => {
                    *this.last_compactions =
                        Some(this.collector.take_pending_compactions().collect())
                }
                Some(Err(err)) => return Some(Err(err.attach_current_frame())).into(),
                Some(Ok(item)) => {
                    if let Some(comp) = this.collector.add_new_file(item) {
                        return Some(Ok(comp)).into();
                    }
                }
            }
        }
    }
}

#[derive(Clone)]
struct Source {
    inner: Arc<dyn ExternalStorage>,
}

#[derive(PartialEq, Eq, PartialOrd, Ord)]
struct Record {
    prefix: Arc<[u8]>,
    key: Vec<u8>,
    value: Vec<u8>,
}

impl Record {
    fn cmp_key(&self, other: &Self) -> std::cmp::Ordering {
        if core::intrinsics::likely(Arc::ptr_eq(&self.prefix, &other.prefix)) {
            self.key.cmp(&other.key)
        } else {
            self.prefix
                .cmp(&other.prefix)
                .then(self.key.cmp(&other.key))
        }
    }
}

impl Source {
    async fn load(
        &self,
        id: LogFileId,
        mut stat: Option<&mut LoadStatistic>,
        mut on_key_value: impl FnMut(&[u8], &[u8]),
    ) -> Result<()> {
        let mut content = vec![];
        let mut decompress = ZstdDecoder::new(Cursor::new(&mut content));
        let source = self.inner.read_part(&id.name, id.offset, id.length);
        let n = futures::io::copy(source, &mut decompress).await?;
        stat.as_mut().map(|stat| stat.physical_bytes_in += n);
        decompress.flush().await?;
        drop(decompress);

        let mut co = Cooperate::new(4096);
        let mut iter = stream_event::EventIterator::new(&content);
        iter.next()?;
        while iter.valid() {
            co.step().await;
            on_key_value(iter.key(), iter.value());
            stat.as_mut().map(|stat| {
                stat.keys_in += 1;
                stat.logical_key_bytes_in += iter.key().len() as u64;
                stat.logical_value_bytes_in += iter.value().len() as u64;
            });
            iter.next()?;
        }
        stat.as_mut().map(|stat| stat.files_in += 1);
        Ok(())
    }
}

pub struct CompactWorker<DB> {
    source: Source,
    output: Arc<dyn ExternalStorage>,
    max_load_concurrency: usize,
    co: Cooperate,

    // Note: maybe use the TiKV config to construct a DB?
    _great_phantom: PhantomData<DB>,
}

#[derive(Default)]
pub struct CompactLogExt<'a> {
    pub load_statistic: Option<&'a mut LoadStatistic>,
    pub compact_statistic: Option<&'a mut CompactStatistic>,
    pub max_load_concurrency: usize,
}

impl<'a> CompactLogExt<'a> {
    fn with_compact_stat(&mut self, f: impl FnOnce(&mut CompactStatistic)) {
        if let Some(stat) = &mut self.compact_statistic {
            f(stat)
        }
    }

    fn with_load_stat(&mut self, f: impl FnOnce(&mut LoadStatistic)) {
        if let Some(stat) = &mut self.load_statistic {
            f(stat)
        }
    }
}

impl<DB> CompactWorker<DB> {
    pub fn inplace(storage: Arc<dyn ExternalStorage>) -> Self {
        Self {
            source: Source {
                inner: storage.clone(),
            },
            output: storage,
            max_load_concurrency: 16,
            co: Cooperate::new(4096),
            _great_phantom: PhantomData,
        }
    }
}

#[derive(Default, Debug)]
pub struct LoadStatistic {
    pub files_in: u64,
    pub keys_in: u64,
    pub physical_bytes_in: u64,
    pub logical_key_bytes_in: u64,
    pub logical_value_bytes_in: u64,
}

impl LoadStatistic {
    pub fn merge_with(&mut self, other: &Self) {
        self.files_in += other.files_in;
        self.keys_in += other.keys_in;
        self.physical_bytes_in += other.physical_bytes_in;
        self.logical_key_bytes_in += other.logical_key_bytes_in;
        self.logical_value_bytes_in += other.logical_value_bytes_in;
    }
}

#[derive(Default, Debug)]
pub struct CompactStatistic {
    pub keys_out: u64,
    pub physical_bytes_out: u64,
    pub logical_key_bytes_out: u64,
    pub logical_value_bytes_out: u64,

    pub write_sst_duration: Duration,
    pub load_duration: Duration,
    pub sort_duration: Duration,
    pub save_duration: Duration,
}

impl CompactStatistic {
    pub fn merge_with(&mut self, other: &Self) {
        self.keys_out += other.keys_out;
        self.physical_bytes_out += other.physical_bytes_out;
        self.logical_key_bytes_out += other.logical_key_bytes_out;
        self.logical_value_bytes_out += other.logical_value_bytes_out;
        self.write_sst_duration += other.write_sst_duration;
        self.load_duration += other.load_duration;
        self.sort_duration += other.sort_duration;
        self.save_duration += other.save_duration;
    }
}

impl<DB: SstExt> CompactWorker<DB>
where
    <<DB as SstExt>::SstWriter as SstWriter>::ExternalSstFileReader: 'static,
{
    const COMPRESSION: Option<SstCompressionType> = Some(SstCompressionType::Lz4);

    async fn merge_and_sort(&mut self, items: impl Iterator<Item = Vec<Record>>) -> Vec<Record> {
        let mut flatten_items = items
            .into_iter()
            .flat_map(|v| v.into_iter())
            .collect::<Vec<_>>();
        flatten_items.sort_unstable_by(|k1, k2| k1.cmp_key(&k2));
        tokio::task::yield_now().await;
        flatten_items.dedup_by(|k1, k2| k1.cmp_key(&k2) == std::cmp::Ordering::Equal);
        flatten_items
    }

    async fn load(
        &mut self,
        c: &Compaction,
        ext: &mut CompactLogExt<'_>,
    ) -> Result<impl Iterator<Item = Vec<Record>>> {
        let mut eext = ExecuteAllExt::default();
        let load_stat = ext.load_statistic.is_some();
        eext.max_concurrency = ext.max_load_concurrency;

        let common_prefix_len = common_prefix_len(&c.min_key, &c.max_key);
        let common_prefix =
            Arc::<[u8]>::from(c.min_key[..common_prefix_len].to_vec().into_boxed_slice());

        let items = super::util::execute_all_ext(
            c.source
                .iter()
                .cloned()
                .map(|f| {
                    let source = &self.source;
                    let common_prefix = common_prefix.clone();
                    Box::pin(async move {
                        let mut out = vec![];
                        let mut stat = LoadStatistic::default();
                        source
                            .load(f, load_stat.then_some(&mut stat), |k, v| {
                                out.push(Record {
                                    prefix: common_prefix.clone(),
                                    key: k.strip_prefix(common_prefix.as_ref()).unwrap().to_owned(),
                                    value: v.to_owned(),
                                })
                            })
                            .await?;
                        Result::Ok((out, stat))
                    })
                })
                .collect(),
            eext,
        )
        .await?;

        let mut result = Vec::with_capacity(items.len());
        for (item, stat) in items {
            ext.with_load_stat(|s| s.merge_with(&stat));
            result.push(item);
        }
        Ok(result.into_iter())
    }

    async fn write_sst(
        &mut self,
        cf: CfName,
        sorted_items: impl Iterator<Item = Record>,
        ext: &mut CompactLogExt<'_>,
    ) -> Result<(impl ExternalSstFileInfo, impl std::io::Read + 'static)> {
        let mut w = <DB as SstExt>::SstWriterBuilder::new()
            .set_cf(cf)
            .set_compression_type(Self::COMPRESSION)
            .set_in_memory(true)
            .build(&"in-mem.sst")?;

        let mut key_buf = vec![];
        let mut last_prefix = None;
        for mut item in sorted_items {
            self.co.step().await;
            if last_prefix == Some(Arc::as_ptr(&item.prefix)) {
                key_buf.truncate(item.prefix.len());
            } else {
                last_prefix = Some(Arc::as_ptr(&item.prefix));
                key_buf = item.prefix.to_vec();
            }
            key_buf.append(&mut item.key);
            w.put(&key_buf, &item.value)?;
            ext.with_compact_stat(|stat| {
                stat.logical_key_bytes_out += key_buf.len() as u64;
                stat.logical_value_bytes_out += item.value.len() as u64;
            })
        }
        let (info, out) = w.finish_read()?;
        ext.with_compact_stat(|stat| {
            stat.keys_out += info.num_entries();
            stat.physical_bytes_out += info.file_size();
        });

        Ok((info, out))
    }

    pub async fn compact_ext(&mut self, c: Compaction, mut ext: CompactLogExt<'_>) -> Result<()> {
        let mut eext = ExecuteAllExt::default();
        eext.max_concurrency = ext.max_load_concurrency;

        let begin = Instant::now();
        let items = self.load(&c, &mut ext).await?;
        ext.with_compact_stat(|stat| stat.load_duration += begin.saturating_elapsed());

        let begin = Instant::now();
        let sorted_items = self.merge_and_sort(items).await;
        ext.with_compact_stat(|stat| stat.sort_duration += begin.saturating_elapsed());

        let begin = Instant::now();
        let (info, out) = self
            .write_sst(c.cf, sorted_items.into_iter(), &mut ext)
            .await?;
        ext.with_compact_stat(|stat| stat.write_sst_duration += begin.saturating_elapsed());

        let begin = Instant::now();
        let out_name = format!("{}-{}-{}.sst", c.region_id, c.min_ts, c.max_ts);
        self.output
            .write(
                &out_name,
                external_storage::UnpinReader(Box::new(AllowStdIo::new(out))),
                info.file_size(),
            )
            .await?;
        ext.with_compact_stat(|stat| stat.save_duration += begin.saturating_elapsed());
        Ok(())
    }
}

fn common_prefix_len(k1: &[u8], k2: &[u8]) -> usize {
    let mut n = 0;
    while n < k1.len() && n < k2.len() && k1[n] == k2[n] {
        n += 1;
    }
    n
}