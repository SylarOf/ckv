use crossbeam_skiplist::SkipMap;
use std::sync::Arc;
use crate::utils::slice::Slice;
pub struct MemTable {
    skiplist: SkipMap<Slice, Slice>,
}

impl MemTable {
    pub fn new() -> Self {
        MemTable {
            skiplist: SkipMap::new(),
        }
    }

    pub fn insert(&self, key: &[u8], val: &[u8]) {
        self.skiplist.insert(Slice::from(key), Slice::from(val));
    }

    pub fn seek(&self, key: &[u8]) -> Option<Slice> {
        self.skiplist.get(key).map(|v|{
            v.value().clone()
        })
    }
}

