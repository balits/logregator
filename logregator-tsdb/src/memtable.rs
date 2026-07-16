use std::{
    cmp::Reverse,
    collections::{BTreeSet, BinaryHeap},
    ops::Bound,
    sync::Arc,
};

use tracing::trace;

use crate::record::{Key, Record};

#[derive(Debug)]
pub struct Memtable {
    set: BTreeSet<Record>,
    limit: usize,
    size_bytes: usize,
}

impl Memtable {
    pub fn new(limit: usize) -> Self {
        let set = BTreeSet::new();
        Self {
            set,
            limit,
            size_bytes: 0,
        }
    }

    pub fn append(&mut self, r: Record) -> bool {
        if self.size_bytes + r.size_of() > self.limit {
            trace!("append: memtable full");
            return true;
        }
        trace!(
            "current_size: {}, limit: {}, new_record_size: {}",
            self.size_bytes,
            self.limit,
            r.size_of()
        );
        self.size_bytes += r.size_of();
        self.set.insert(r);
        self.size_bytes >= self.limit
    }

    pub fn range(&self, start: Bound<&Key>, end: Bound<&Key>) -> BTreeSetRange<'_> {
        self.set
            .range::<Key, (Bound<&Key>, Bound<&Key>)>((start, end))
    }

    pub fn full_range(&self) -> BTreeSetRange<'_> {
        let start = Key::default();
        let end = Key::new(u64::MAX, u64::MAX, u64::MAX, u64::MAX);
        self.set.range::<Key, (Bound<&Key>, Bound<&Key>)>((
            Bound::Included(&start),
            Bound::Included(&end),
        ))
    }

    pub fn freeze(&mut self) -> Arc<FrozenMemtable> {
        let frozen = std::mem::replace(self, Self::new(self.limit));
        Arc::new(FrozenMemtable(frozen))
    }

    #[inline]
    pub fn count(&self) -> usize {
        self.set.len()
    }

    #[inline]
    pub fn size_bytes(&self) -> usize {
        self.size_bytes
    }

    #[inline]
    pub fn limit(&self) -> usize {
        self.limit
    }
}

#[derive(Debug)]
pub struct FrozenMemtable(Memtable);

impl FrozenMemtable {
    pub fn range(&self, start: Bound<&Key>, end: Bound<&Key>) -> BTreeSetRange<'_> {
        self.0.range(start, end)
    }

    #[inline]
    pub fn count(&self) -> usize {
        self.0.count()
    }

    #[inline]
    pub fn size_bytes(&self) -> usize {
        self.0.size_bytes()
    }

    #[inline]
    pub fn limit(&self) -> usize {
        self.0.limit()
    }
}

pub type BTreeSetRange<'a> = std::collections::btree_set::Range<'a, Record>;

pub struct MergeIter<'a> {
    // TODO: use tinyvec / stack based vec to avoid heap allocations on
    // (frequent) ranging + small amounts of btrees??
    ranges: Vec<BTreeSetRange<'a>>,
    minheap: BinaryHeap<Reverse<HeapItem<'a>>>,
}

impl<'a> MergeIter<'a> {
    pub fn new(
        active: &'a Memtable,
        frozen: &'a [Arc<FrozenMemtable>],
        start: Bound<&Key>,
        end: Bound<&Key>,
    ) -> Self {
        let minheap = BinaryHeap::new();
        let mut ranges = Vec::with_capacity(1 + frozen.len());
        ranges.push(active.range(start, end));
        ranges.extend(frozen.iter().map(|m| m.range(start, end)));

        let mut this = Self { ranges, minheap };
        this.fill_from_all();
        this
    }

    fn fill_from_all(&mut self) {
        for i in 0..self.ranges.len() {
            self.fill_from_single(i);
        }
    }

    fn fill_from_single(&mut self, iter_idx: usize) {
        if let Some(rec) = self.ranges[iter_idx].next() {
            self.minheap.push(Reverse(HeapItem {
                inner: rec,
                iter_idx,
            }));
        }
    }
}

impl<'a> Iterator for MergeIter<'a> {
    type Item = &'a Record;

    fn next(&mut self) -> Option<Self::Item> {
        self.minheap
            .pop()
            .inspect(|item| {
                self.fill_from_single(item.0.iter_idx);
            })
            .map(|item| item.0.inner)
    }
}

struct HeapItem<'a> {
    /// actual record
    inner: &'a Record,
    /// which iterator this item came from
    iter_idx: usize,
}

impl<'a> PartialEq for HeapItem<'a> {
    fn eq(&self, other: &Self) -> bool {
        self.inner.eq(other.inner)
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
        self.inner.cmp(other.inner)
    }
}

#[cfg(test)]
mod test {
    use std::ops::Bound;

    use crate::{
        memtable::Memtable,
        record::{Key, Record},
    };
    use pretty_assertions::assert_eq;

    #[test]
    fn lifecylce() {
        let base = Record {
            key: Key::new(1, 2, 3, 4),
            payload: vec![0x01, 0x02, 0x03, 0x04].into_boxed_slice(),
        };
        let max_count = 4;
        let max_size = max_count * base.size_of();

        let mut m = Memtable::new(max_size);
        for i in 0..(max_count - 1) {
            let k = Key {
                source_id: i as u64,
                ..Default::default()
            };
            let mut rec = Record {
                key: k,
                payload: base.payload.clone(),
            };
            rec.key.source_id = i as u64;
            let rec_sz = rec.size_of();

            assert_eq!(false, m.append(rec));
            assert_eq!(m.size_bytes, rec_sz * (i + 1))
        }

        // memtable just filled up
        assert_eq!(
            true,
            m.append(Record {
                key: Key {
                    source_id: (max_count - 1) as u64,
                    ..Default::default()
                },
                payload: base.payload.clone(),
            })
        );
        assert_eq!(max_count, m.count());
        assert_eq!(max_size, m.size_bytes());

        // memtable cant hold more records
        assert_eq!(true, m.append(base.clone()));
        dbg!(max_count, m.count());
        dbg!(max_size, m.size_bytes());

        for (i, rec) in m
            .range(
                Bound::Included(&Key::default()),
                Bound::Excluded(&Key {
                    source_id: u64::MAX,
                    ..Default::default()
                }),
            )
            .enumerate()
        {
            assert_eq!(i as u64, rec.key.source_id);
        }

        let f = m.freeze();
        assert_eq!(max_size, f.size_bytes());
        assert_eq!(max_count, f.count());
        // .freeze leaves behind an empty memtable
        assert_eq!(0, m.size_bytes());
        assert_eq!(0, m.count());

        for (i, rec) in f
            .range(
                Bound::Included(&Key::default()),
                Bound::Excluded(&Key {
                    source_id: u64::MAX,
                    ..Default::default()
                }),
            )
            .enumerate()
        {
            assert_eq!(i as u64, rec.key.source_id);
        }

        dbg!(m);
        dbg!(f);
    }
}
