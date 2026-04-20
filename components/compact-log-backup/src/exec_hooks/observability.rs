// Copyright 2024 TiKV Project Authors. Licensed under Apache-2.0.

pub use engine_traits::SstCompressionType;
use tikv_util::{error, info, warn};
use tokio::{io::AsyncWriteExt, signal::unix::SignalKind};

use super::CollectStatistic;
use crate::{
    ErrorKind,
    errors::Result,
    execute::hooking::{
        AbortedCtx, AfterFinishCtx, BeforeStartCtx, CId, ExecHooks, SubcompactionFinishCtx,
        SubcompactionStartCtx,
    },
    statistic::prom,
    storage::StreamMetaStorage,
    util::storage_url,
};

fn ratio_of(part: std::time::Duration, total: std::time::Duration) -> f64 {
    if total.is_zero() {
        0.0
    } else {
        part.as_secs_f64() / total.as_secs_f64()
    }
}

fn rate_per_sec(bytes: u64, dur: std::time::Duration) -> f64 {
    if dur.is_zero() {
        0.0
    } else {
        bytes as f64 / dur.as_secs_f64()
    }
}

fn classify_bottleneck(
    load_ratio: f64,
    download_ratio_in_load: f64,
    decode_ratio_in_load: f64,
    sort_ratio: f64,
    write_ratio: f64,
    save_ratio: f64,
) -> &'static str {
    if load_ratio >= 0.55 && download_ratio_in_load >= 0.6 {
        "download-bound (small-file or remote read overhead likely dominates)"
    } else if load_ratio >= 0.55 && decode_ratio_in_load >= 0.45 {
        "decode-bound during load (CPU work while parsing downloaded files)"
    } else if sort_ratio >= 0.3 {
        "sort-bound (CPU and memory reordering/dedup dominates)"
    } else if write_ratio >= 0.25 {
        "sst-write-bound (CPU/compression while generating SST dominates)"
    } else if save_ratio >= 0.25 {
        "upload-bound (saving SST/meta back to storage dominates)"
    } else {
        "mixed/unclear"
    }
}

/// The hooks that used for an execution from a TTY. Providing the basic
/// observability related to the progress of the comapction.
///
/// This prints the log when events happens, and prints statistics after
/// compaction finished.
///
/// This also enables async-backtrace, you can send `SIGUSR1` to the executing
/// compaction task and the running async tasks will be dumped to a file.
#[derive(Default)]
pub struct Observability {
    stats: CollectStatistic,
    meta_len: u64,
}

impl ExecHooks for Observability {
    fn before_a_subcompaction_start(&mut self, cid: CId, cx: SubcompactionStartCtx<'_>) {
        let c = cx.subc;
        self.stats
            .update_collect_compaction_stat(cx.collect_compaction_stat_diff);
        self.stats.update_load_meta_stat(cx.load_stat_diff);

        info!("Spawning compaction."; "cid" => cid.0, 
            "cf" => c.cf, 
            "input_min_ts" => c.input_min_ts, 
            "input_max_ts" => c.input_max_ts, 
            "source" => c.inputs.len(), 
            "size" => c.size, 
            "region_id" => c.region_id);
    }

    async fn after_a_subcompaction_end(
        &mut self,
        cid: CId,
        cx: SubcompactionFinishCtx<'_>,
    ) -> Result<()> {
        let lst = &cx.result.load_stat;
        let cst = &cx.result.compact_stat;
        let logical_input_size = lst.logical_key_bytes_in + lst.logical_value_bytes_in;
        let total_take =
            cst.load_duration + cst.sort_duration + cst.save_duration + cst.write_sst_duration;
        let speed = logical_input_size as f64 / total_take.as_millis() as f64;
        let load_ratio = ratio_of(cst.load_duration, total_take);
        let sort_ratio = ratio_of(cst.sort_duration, total_take);
        let write_ratio = ratio_of(cst.write_sst_duration, total_take);
        let save_ratio = ratio_of(cst.save_duration, total_take);
        let download_ratio_in_load = ratio_of(lst.download_duration, cst.load_duration);
        let decode_ratio_in_load = ratio_of(lst.decode_duration, cst.load_duration);
        let input_files = lst.files_in.max(1);
        let likely_bottleneck = classify_bottleneck(
            load_ratio,
            download_ratio_in_load,
            decode_ratio_in_load,
            sort_ratio,
            write_ratio,
            save_ratio,
        );

        self.stats.update_subcompaction(cx.result);

        prom::COMPACT_LOG_BACKUP_LOAD_DURATION.observe(cst.load_duration.as_secs_f64());
        prom::COMPACT_LOG_BACKUP_SORT_DURATION.observe(cst.sort_duration.as_secs_f64());
        prom::COMPACT_LOG_BACKUP_SAVE_DURATION.observe(cst.save_duration.as_secs_f64());
        prom::COMPACT_LOG_BACKUP_WRITE_SST_DURATION.observe(cst.write_sst_duration.as_secs_f64());

        info!("Finishing compaction."; 
            "meta_completed" => self.stats.load_meta_stat.meta_files_in, 
            "meta_total" => self.meta_len, 
            "bytes_to_compact" => self.stats.collect_stat.bytes_in,
            "bytes_compacted" => self.stats.collect_stat.bytes_out, 
            "cid" => cid.0, 
            "load_stat" => ?lst, 
            "compact_stat" => ?cst, 
            "speed(KiB/s)" => speed, 
            "total_take" => ?total_take, 
            "stage_ratio" => format_args!(
                "load={:.0}%, sort={:.0}%, write={:.0}%, save={:.0}%",
                load_ratio * 100.0,
                sort_ratio * 100.0,
                write_ratio * 100.0,
                save_ratio * 100.0,
            ),
            "load_breakdown" => format_args!(
                "download={:.0}%, decode={:.0}%",
                download_ratio_in_load * 100.0,
                decode_ratio_in_load * 100.0,
            ),
            "avg_file_time" => format_args!(
                "download={:?}, decode={:?}, total={:?}",
                lst.download_duration / input_files as u32,
                lst.decode_duration / input_files as u32,
                cst.load_duration / input_files as u32,
            ),
            "max_file_time" => format_args!(
                "download={:?}, decode={:?}, total={:?}",
                lst.max_file_download_duration,
                lst.max_file_decode_duration,
                lst.max_file_total_duration,
            ),
            "load_rate" => format_args!(
                "physical={:.1} MiB/s, logical={:.1} MiB/s",
                rate_per_sec(lst.physical_bytes_in, cst.load_duration) / (1024.0 * 1024.0),
                rate_per_sec(logical_input_size, cst.load_duration) / (1024.0 * 1024.0),
            ),
            "suspect_bottleneck" => likely_bottleneck,
            "global_load_meta_stat" => ?self.stats.load_meta_stat);
        Ok(())
    }

    async fn on_aborted(&mut self, cx: AbortedCtx<'_>) {
        error!("Compaction aborted."; "err" => %cx.err);
    }

    async fn after_execution_finished(&mut self, cx: AfterFinishCtx<'_>) -> Result<()> {
        if self.stats.load_meta_stat.meta_files_in == 0 {
            let url = storage_url(cx.storage);
            warn!("No meta files loaded, maybe wrong storage used?"; "url" => %url);
            return Err(ErrorKind::Other(format!("Nothing loaded from {}", url)).into());
        }
        info!("All compactions done.";
            "meta_hol_block_count" => self.stats.load_meta_stat.prefetch_head_of_line_block_count,
            "meta_hol_blocked_ready_max" => self.stats.load_meta_stat.max_ready_but_blocked_prefetch_tasks,
            "meta_prefetch_emitted" => self.stats.load_meta_stat.prefetch_task_emitted,
            "meta_prefetch_finished" => self.stats.load_meta_stat.prefetch_task_finished);
        Ok(())
    }

    async fn before_execution_started(&mut self, cx: BeforeStartCtx<'_>) -> Result<()> {
        tracing_active_tree::init();

        let sigusr1_handler = async {
            let mut signal = tokio::signal::unix::signal(SignalKind::user_defined1()).unwrap();
            while signal.recv().await.is_some() {
                let file_name = "/tmp/compact-sst.dump".to_owned();
                let res = async {
                    let mut file = tokio::fs::File::create(&file_name).await?;
                    file.write_all(&tracing_active_tree::layer::global().fmt_bytes())
                        .await
                }
                .await;
                match res {
                    Ok(_) => warn!("dumped async backtrace."; "to" => file_name),
                    Err(err) => warn!("failed to dump async backtrace."; "err" => %err),
                }
            }
        };

        cx.async_rt.spawn(sigusr1_handler);
        let (meta_len, shift_ts) = StreamMetaStorage::count_objects(
            cx.storage,
            cx.this.cfg.shard,
            cx.this.cfg.from_ts,
            cx.this.cfg.until_ts,
        )
        .await?;
        self.meta_len = meta_len;
        cx.shift_ts.set(shift_ts);

        info!("About to start compaction."; &cx.this.cfg,
            "url" => cx.storage.url().map(|v| v.to_string()).unwrap_or_else(|err| format!("<err: {err}>")),
            "meta_count_scan" => self.meta_len,
            "shift_ts" => shift_ts);
        Ok(())
    }
}
