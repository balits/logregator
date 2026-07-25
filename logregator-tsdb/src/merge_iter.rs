use std::{
    borrow::Cow,
    cmp::Reverse,
    collections::{BinaryHeap, VecDeque, btree_set::Range},
    ops::Bound,
    rc::Rc,
    sync::Arc,
};

use crate::{
    codec::RecordCodecExt,
    memtable::{FrozenMemtable, Memtable},
    record::{Key, Record},
    sst::{SstCursor, SstHandle, SstReadError},
};

pub struct MergeIter<'a, C> {
    memtables: Vec<Range<'a, Record>>,
    ssts: Vec<SstCursor<C>>,
    minheap: BinaryHeap<Reverse<HeapItem<'a>>>,
}

impl<'a, C> MergeIter<'a, C>
where
    C: RecordCodecExt,
{
    pub fn new(
        active: &'a Memtable,
        frozen: &'a VecDeque<Arc<FrozenMemtable>>,
        handles: &'a [Rc<SstHandle<C>>],
        start: Bound<&Key>,
        end: Bound<&Key>,
    ) -> Result<Self, SstReadError> {
        let minheap = BinaryHeap::new();

        let mut memtables = Vec::with_capacity(1 + frozen.len());
        memtables.push(active.range(start, end));
        memtables.extend(frozen.iter().map(|m| m.range(start, end)));

        let mut ssts = Vec::with_capacity(handles.len());
        for h in handles {
            ssts.push(h.cursor()?);
        }

        let mut this = Self {
            memtables,
            ssts,
            minheap,
        };
        this.fill_from_all();
        Ok(this)
    }

    fn fill_from_all(&mut self) {
        for i in 0..self.memtables.len() {
            self.fill_from_single(Source::Memtable(i));
        }
    }

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
                self.ssts[idx].next();
                if let Some(rec) = self.ssts[idx].take_current() {
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
impl<'a, C> Iterator for MergeIter<'a, C>
where
    C: RecordCodecExt,
{
    type Item = Cow<'a, Record>;

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
