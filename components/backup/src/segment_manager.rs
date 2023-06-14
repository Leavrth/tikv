// Copyright 2023 TiKV Project Authors. Licensed under Apache-2.0.

use engine_traits::SstFileInfo;
use std::collections::{HashMap, BTreeMap};
use txn_types::Key;

type SegmentMap = Vec<BTreeMap<Key, SstFileInfo>>;

pub struct SegmentMapManager (HashMap<String, (SegmentMap, SegmentMap)>);

impl SegmentMapManager {
    pub fn new() -> Self {
        Self (HashMap::new())
    }

    pub fn register(&mut self, d: SegmentMap, w: SegmentMap) -> String {
        let id = uuid::Uuid::new_v4().to_string();
        self.0.insert(id.clone(), (d, w));
        id
    }

}