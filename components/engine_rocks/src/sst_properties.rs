// Copyright 2023 TiKV Project Authors. Licensed under Apache-2.0.
use engine_traits::SSTPropertiesExt;

use crate::RocksEngine;

impl SSTPropertiesExt for RocksEngine {
    fn test(&self) {
        let _live_files = self.as_inner().get_live_files();
    }
}
