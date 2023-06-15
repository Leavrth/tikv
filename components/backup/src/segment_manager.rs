// Copyright 2023 TiKV Project Authors. Licensed under Apache-2.0.

use engine_traits::SstFileInfo;
use std::{collections::{HashMap, BTreeMap}, sync::{Arc, RwLock, Mutex}};

enum SstStatus {
    NotUpload,
    Uploading,
    Uploaded
}

type SegmentMap = Vec<BTreeMap<Vec<u8>, SstFileInfo>>;

#[derive(Clone)]
pub struct SegmentMapManager {
    map: Arc<RwLock<HashMap<String, (SegmentMap, SegmentMap)>>>,
    // TODO: directly update the uploaded flag in hashmap
    index_d: Arc<Mutex<Vec<Vec<SstStatus>>>>,
    index_w: Arc<Mutex<Vec<Vec<SstStatus>>>>,
}

impl SegmentMapManager {
    pub fn new() -> Self {
        Self {
            map: Arc::new(RwLock::new(HashMap::new())),
            index_d: Arc::new(Mutex::new(Vec::new())),
            index_w: Arc::new(Mutex::new(Vec::new())),
        }
    }

    pub fn register(&mut self, d: SegmentMap, w: SegmentMap) -> String {
        let id = uuid::Uuid::new_v4().to_string();
        {
            let mut map = self.map.write().unwrap();
            map.insert(id.clone(), (d, w));
        }
        self.generate_index(&id);
        id
    }

    fn generate_index(&mut self, id: &str) {
        let mut map = self.map.write().unwrap();
        let map = map.get_mut(id).unwrap();
        {
            let mut index_d = self.index_d.lock().unwrap();
            generate_index_internal(&mut map.0, &mut index_d);
        }
        {
            let mut index_w = self.index_w.lock().unwrap();
            generate_index_internal(&mut map.1, &mut index_w);
        }
    }

    pub fn find_ssts(&mut self, id: &str, start_key: &Vec<u8>, end_key: &Vec<u8>) -> (Vec<Vec<(String, usize)>>, Vec<Vec<(String, usize)>>) {
        let m = self.map.read().unwrap();
        let map = m.get(id).unwrap();
        let d = {
            let mut index_d = self.index_d.lock().unwrap();
            find_ssts_internal(&mut index_d, &map.0, start_key, end_key)
        };
        let w = {
            let mut index_w = self.index_w.lock().unwrap();
            find_ssts_internal(&mut index_w, &map.1, start_key, end_key)
        };
        (d, w)
    }

    pub fn release_index(
        &mut self,
        d: Vec<Vec<(String, usize)>>, d_progress_l: usize, d_progress_f: usize,
        w: Vec<Vec<(String, usize)>>, w_progress_l: usize, w_progress_f: usize,
    ) {
        {
            let mut index_d = self.index_d.lock().unwrap();
            release_index_internal(&mut index_d, d, d_progress_l, d_progress_f);
        }
        {
            let mut index_w = self.index_w.lock().unwrap();
            release_index_internal(&mut index_w, w, w_progress_l, w_progress_f);
        }
    }
}

fn generate_index_internal(map: &mut SegmentMap, index: &mut Vec<Vec<SstStatus>>) {
    for tree in map {
        let mut lvl_idx = Vec::new();
        for (idx, (_, info)) in tree.iter_mut().enumerate() {
            info.idx = idx;
            lvl_idx.push(SstStatus::NotUpload);
        }

        index.push(lvl_idx);
    }
}

fn find_ssts_internal(index: &mut [Vec<SstStatus>], map: &SegmentMap, start_key: &Vec<u8>, end_key: &Vec<u8>) -> Vec<Vec<(String, usize)>> {
    let mut res = Vec::new();
    for (level, tree) in map.iter().enumerate() {
        let lvl_index = &mut index[level];
        let mut fs = Vec::new();
        for f in tree.iter().filter(|info| {
            let idx = info.1.idx;
            if matches!(lvl_index[idx], SstStatus::NotUpload) {
                return false
            }
            

            let sk = info.0;
            let ek = &info.1.end_key;
            if (end_key.is_empty() || sk < end_key) && (ek.is_empty() || ek > start_key) {
                lvl_index[idx] = SstStatus::Uploading;
                return true
            }
            false
        }) {
            fs.push((f.1.file_name.clone(), f.1.idx));
        }
        res.push(fs);
    }
    res
}

fn release_index_internal(index: &mut [Vec<SstStatus>], findex: Vec<Vec<(String, usize)>>, progress_l: usize, progress_f: usize) {
    for (level, sst_index) in findex.iter().enumerate() {
        for (_, idx) in sst_index {
            index[level][*idx] = if level < progress_l || (level == progress_l && *idx <= progress_f) {
                SstStatus::Uploaded
            } else {
                SstStatus::NotUpload
            };
        }
    }
}
