use crate::table::table::TableIterator;
use crate::utils::slice::Slice;
use std::cmp::Ordering;
use std::collections::binary_heap::BinaryHeap;

pub trait DBIterator {
    fn seek_to_first(&mut self);
    fn seek(&mut self, key: &Slice) -> Option<&Slice>;
    fn next(&mut self) -> Option<()>;
    fn key(&self) -> &Slice;
    fn val(&self) -> &Slice;
}

#[derive(PartialEq, Eq)]
struct Item {
    key: Slice,
    val: Slice,
    idx: usize,
}

// if Item try to own &key of iters, but move iters in MergeIterator, it will
// conflict
pub struct MergeIterator<'a> {
    heap: BinaryHeap<Item>,
    iters: Vec<TableIterator<'a>>,
}

impl Ord for Item {
    fn cmp(&self, other: &Self) -> Ordering {
        other
            .key
            .cmp(&self.key)
            .then_with(|| other.idx.cmp(&self.idx))
    }
}

impl PartialOrd for Item {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(other.cmp(self))
    }
}

impl<'a> MergeIterator<'a> {
    pub fn new(mut iters: Vec<TableIterator<'a>>) -> Self {
        let mut heap = BinaryHeap::new();

        for (idx, iter) in iters.iter_mut().enumerate() {
            iter.seek_to_first();

            let key = iter.key();
            let val = iter.val();
            heap.push(Item {
                key: key.clone(),
                val: val.clone(),
                idx,
            });
        }

        MergeIterator { heap, iters }
    }

    pub fn seek(&mut self, key: &Slice) -> Option<()> {
        self.heap.clear();
        for (idx, iter) in self.iters.iter_mut().enumerate() {
            // try to seek each iterator to the target key
            if iter.seek(key).is_some() {
                let key = iter.key();
                let val = iter.val();
                self.heap.push(Item {
                    key: key.clone(),
                    val: val.clone(),
                    idx,
                });

                return Some(());
            }
        }
        None
    }
}

impl<'a> Iterator for MergeIterator<'a> {
    type Item = (Slice, Slice);

    fn next(&mut self) -> Option<Self::Item> {
        if let Some(Item { key, val, idx }) = self.heap.pop() {
            if let Some(()) = self.iters[idx].next() {
                self.heap.push(Item {
                    key: self.iters[idx].key().clone(),
                    val: self.iters[idx].val().clone(),
                    idx,
                });
            }
            Some((key, val))
        } else {
            None
        }
    }
}
