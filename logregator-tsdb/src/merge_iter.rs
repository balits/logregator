use std::{
    borrow::Cow,
    cmp::Reverse,
    collections::{btree_set::Range, BinaryHeap, VecDeque},
    ops::Bound,
};

use crate::{
    codec::RecordCodecExt,
    memtable::{FrozenMemtable, MutMemtable},
    record::{Key, Record},
    sst::{SstCursor, SstError, SstHandle},
};

/// ['m]: lifetime of the memtables
/// ['s']: lifetime of the sstables
/// ['b']: lifetime the bound key
#[derive(Debug)]
pub struct MergeIter<'m, 's, C> {
    memtables: Vec<Range<'m, Record>>,
    cursors: Vec<SstCursor<'s, C>>,
    minheap: BinaryHeap<Reverse<HeapItem<'s>>>,
}

impl<'m, 's, C> MergeIter<'m, 's, C>
where
    C: RecordCodecExt,
    'm: 's,
{
    pub fn new(
        active: &'m MutMemtable,
        frozen: &'m VecDeque<FrozenMemtable>,
        handles: &'s [SstHandle<C>],
        start_bound: Bound<Key>,
        end_bound: Bound<Key>,
    ) -> Result<Self, SstError> {
        let frozen_len = frozen.len();
        let frozen_memtables = frozen
            .iter()
            .map(|m| m.range(start_bound.as_ref(), end_bound.as_ref()));

        let start_cloned = start_bound.as_ref().cloned();
        let end_cloned = end_bound.as_ref().cloned();
        let mut cursors = Vec::with_capacity(handles.len());

        for h in handles {
            if let Bound::Included(k) | Bound::Excluded(k) = start_bound.as_ref()
                && !h.may_contain_stream_id(k.stream_id) {
                continue
            }

            // ugh "hidden" cloning of keys again...
            match h.cursor(Some((start_bound.clone(), end_bound.clone()))) {
                Ok(Some(c)) => cursors.push(c),
                Ok(None) => {}
                Err(e) => return Err(e),
            };
        }

        let mut memtables = Vec::with_capacity(1 + frozen_len);
        memtables.push(active.range(start_cloned, end_cloned));
        memtables.extend(frozen_memtables);

        let mut this = Self {
            memtables,
            cursors,
            minheap: BinaryHeap::new(),
        };
        this.fill_from_all();
        Ok(this)
    }

    fn fill_from_all(&mut self) {
        for i in 0..self.memtables.len() {
            self.fill_from_single(Source::Memtable(i));
        }
        for i in 0..self.cursors.len() {
            self.fill_from_single(Source::Sst(i));
        }
    }

    #[inline]
    fn fill_from_single(&mut self, source: Source) {
        match source {
            Source::Memtable(idx) => {
                if let Some(rec) = self.memtables[idx].next() {
                    let item = Reverse(HeapItem {
                        inner: Cow::Borrowed(rec),
                        source,
                    });
                    self.minheap.push(item);
                }
            }
            Source::Sst(idx) => {
                self.cursors[idx].next();
                if let Some(rec) = self.cursors[idx].take_current() {
                    let item = Reverse(HeapItem {
                        inner: Cow::Owned(rec),
                        source,
                    });
                    self.minheap.push(item);
                }
            }
        }
    }
}

// TODO: which one?
// 1) make Item into Result<&'a Record, SstReadError>
// 2) swallow errors from ssts silently, maybe logging it
impl<'m, 's, C> Iterator for MergeIter<'m, 's, C>
where
    C: RecordCodecExt,
    'm: 's,
{
    type Item = Cow<'s, Record>;

    fn next(&mut self) -> Option<Self::Item> {
        match self.minheap.pop() {
            Some(pop) => {
                self.fill_from_single(pop.0.source.clone());
                Some(pop.0.inner)
            }
            None => None,
        }
    }
}

#[derive(Debug, Clone)]
enum Source {
    Memtable(usize),
    Sst(usize),
}

#[derive(Debug)]
struct HeapItem<'a> {
    /// actual record
    inner: Cow<'a, Record>,
    /// which iterator this item came from
    source: Source,
}

impl<'a> PartialEq for HeapItem<'a> {
    fn eq(&self, other: &Self) -> bool {
        self.inner.as_ref().eq(other.inner.as_ref())
    }
}

impl<'a> Eq for HeapItem<'a> {}

impl<'a> PartialOrd for HeapItem<'a> {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl<'a> Ord for HeapItem<'a> {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.inner.as_ref().cmp(other.inner.as_ref())
    }
}
