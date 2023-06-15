// Copyright 2022 TiKV Project Authors. Licensed under Apache-2.0.

use std::{path::Path, collections::BTreeMap, sync::Arc};

use engine_traits::{Checkpointable, Checkpointer, Result, SstFileInfo, CfName, ColumnFamilyMetadata};
use rocksdb::DB;

use crate::{r2e, RocksEngine, util};

impl Checkpointable for RocksEngine {
    type Checkpointer = RocksEngineCheckpointer;

    fn new_checkpointer(&self) -> Result<Self::Checkpointer> {
        match self.as_inner().new_checkpointer() {
            Ok(pointer) => Ok(RocksEngineCheckpointer{
                db: self.as_inner().clone(),
                pointer
            }),
            Err(e) => Err(r2e(e)),
        }
    }

    fn merge(&self, dbs: &[&Self]) -> Result<()> {
        let mut mopts = rocksdb::MergeInstanceOptions::default();
        mopts.merge_memtable = false;
        mopts.allow_source_write = true;
        let inner: Vec<_> = dbs.iter().map(|e| e.as_inner().as_ref()).collect();
        self.as_inner().merge_instances(&mopts, &inner).map_err(r2e)
    }
}

pub struct RocksEngineCheckpointer {
    db: Arc<DB>,
    pointer: rocksdb::Checkpointer
}

impl Checkpointer for RocksEngineCheckpointer {
    fn create_at(
        &mut self,
        db_out_dir: &Path,
        titan_out_dir: Option<&Path>,
        log_size_for_flush: u64,
    ) -> Result<()> {
        self.pointer
            .create_at(db_out_dir, titan_out_dir, log_size_for_flush)
            .map_err(|e| r2e(e))
    }

    fn column_family_meta_data(&self, cf: CfName) -> Result<ColumnFamilyMetadata> {
        let db = &self.db;
        let handle = util::get_cf_handle(db, cf)?;
        let metadata = self.db.get_column_family_meta_data(handle);
        let levels_metadata = metadata.get_levels();

        let mut file_count: usize = 0;
        let mut file_size: usize = 0;
        let mut lssts = Vec::new();
        for level_metadata in levels_metadata {
            let mut ssts = BTreeMap::new();
            let files = level_metadata.get_files();
            file_count += files.len();
            for file in files {
                file_size += file.get_size();
                let start_key = file.get_smallestkey().to_vec();
                ssts.insert(start_key, SstFileInfo{
                    file_name: file.get_name(),
                    end_key: file.get_largestkey().to_vec(),
                    idx: 0,
                });
            };
            lssts.push(ssts);
        }

        Ok(ColumnFamilyMetadata {
            file_count,
            file_size,
            ssts: lssts
        })
    }
}

#[cfg(test)]
mod tests {
    use engine_traits::{Checkpointable, Checkpointer, Peekable, SyncMutable, ALL_CFS, CF_DEFAULT, MiscExt};
    use tempfile::tempdir;

    use crate::util::new_engine;

    #[test]
    fn test_checkpoint() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("origin");
        let engine = new_engine(path.as_path().to_str().unwrap(), ALL_CFS).unwrap();
        engine.put(b"key", b"value").unwrap();

        let mut check_pointer = engine.new_checkpointer().unwrap();
        let path2 = dir.path().join("checkpoint");
        check_pointer.create_at(path2.as_path(), None, 0).unwrap();
        let engine2 = new_engine(path2.as_path().to_str().unwrap(), ALL_CFS).unwrap();
        assert_eq!(engine2.get_value(b"key").unwrap().unwrap(), b"value");
    }

    #[test]
    fn test_column_family_meta_data() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("origin");
        let engine = new_engine(path.as_path().to_str().unwrap(), ALL_CFS).unwrap();
        engine.put_cf(CF_DEFAULT, b"key", b"value").unwrap();
        engine.flush_cf(CF_DEFAULT, true).unwrap();

        let check_pointer = engine.new_checkpointer().unwrap();
        let t = check_pointer.column_family_meta_data(CF_DEFAULT).unwrap();
        println!("{:?}", t);
    }
}
