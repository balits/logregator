use std::{
    cmp::Reverse,
    collections::{BTreeMap, BinaryHeap},
    ops::Bound,
    sync::Arc,
};

use crate::record::{Key, Value};

#[derive(Debug)]
pub struct Memtable {
    map: BTreeMap<Key, Value>,
    limit: usize,
    size_bytes: usize,
}

impl Memtable {
    pub fn new(limit: usize) -> Self {
        let map = BTreeMap::new();
        Self {
            map,
            limit,
            size_bytes: 0,
        }
    }

    pub fn append(&mut self, key: Key, value: Value) -> bool {
        if self.size_bytes >= self.limit {
            return true;
        }
        self.size_bytes += std::mem::size_of::<Key>() + value.sizeof();
        self.map.insert(key, value);
        self.size_bytes >= self.limit
    }

    pub fn range(&self, start: Bound<&Key>, end: Bound<&Key>) -> Range<'_> {
        self.map.range((start, end))
    }

    pub fn freeze(&mut self) -> Arc<FrozenMemtable> {
        let frozen = std::mem::replace(self, Self::new(self.limit));
        Arc::new(FrozenMemtable(frozen))
    }

    #[inline]
    pub fn count(&self) -> usize {
        self.map.len()
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

pub struct FrozenMemtable(Memtable);

impl FrozenMemtable {
    pub fn range(&self, start: Bound<&Key>, end: Bound<&Key>) -> Range<'_> {
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

pub type Range<'a> = std::collections::btree_map::Range<'a, Key, Value>;

pub struct MergeIter<'a> {
    // TODO: use tinyvec / stack based vec to avoid heap allocations on (frequent) ranging?
    ranges: Vec<Range<'a>>,
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
        for (i, r) in self.ranges.iter_mut().enumerate() {
            if let Some((key, value)) = r.next() {
                self.minheap.push(Reverse(HeapItem {
                    source_idx: i,
                    key,
                    value,
                }));
            }
        }
    }

    fn fill_from_source(&mut self, source_id: usize) {
        if let Some((key, value)) = self.ranges[source_id].next() {
            self.minheap.push(Reverse(HeapItem {
                source_idx: source_id,
                key,
                value,
            }));
        }
    }
}

impl<'a> Iterator for MergeIter<'a> {
    type Item = (&'a Key, &'a Value);

    fn next(&mut self) -> Option<Self::Item> {
        self.minheap
            .pop()
            .inspect(|item| {
                self.fill_from_source(item.0.source_idx);
            })
            .map(|item| (item.0.key, item.0.value))
    }
}

struct HeapItem<'a> {
    source_idx: usize,
    key: &'a Key,
    value: &'a Value,
}

impl<'a> PartialEq for HeapItem<'a> {
    fn eq(&self, other: &Self) -> bool {
        self.key.eq(&other.key)
    }
}

impl<'a> Eq for HeapItem<'a> {}

impl<'a> PartialOrd for HeapItem<'a> {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        self.key.partial_cmp(&other.key)
    }
}

impl<'a> Ord for HeapItem<'a> {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.key.cmp(&other.key)
    }
}

#[cfg(test)]
mod test {
    use std::ops::Bound;

    use crate::{
        memtable::Memtable,
        record::{Key, Value},
    };
    use pretty_assertions::assert_eq;

    #[test]
    fn lifecylce() {
        let v = Value::default();
        let kv_size = std::mem::size_of::<Key>() + v.sizeof();
        let max_count = 4;
        let max_size = max_count * kv_size;

        let mut m = Memtable::new(max_size);
        for i in 0..(max_count - 1) {
            let k = Key {
                source_id: i as u64,
                ..Default::default()
            };
            assert_eq!(false, m.append(k, v.clone()));
            assert_eq!(m.size_bytes, kv_size * (i + 1))
        }

        assert_eq!(
            true,
            m.append(
                Key {
                    source_id: (max_count - 1) as u64,
                    ..Default::default()
                },
                v.clone()
            )
        );
        assert_eq!(max_count, m.count());
        assert_eq!(max_size, m.size_bytes());

        assert_eq!(true, m.append(Key::default(), v.clone()));

        for (i, (k, _)) in m
            .range(
                Bound::Included(&Key::default()),
                Bound::Excluded(&Key {
                    source_id: u64::MAX,
                    ..Default::default()
                }),
            )
            .enumerate()
        {
            assert_eq!(i as u64, k.source_id);
        }

        let f = m.freeze();
        assert_eq!(max_size, f.size_bytes());
        assert_eq!(max_count, f.count());
        assert_eq!(0, m.size_bytes());
        assert_eq!(0, m.count());

        for (i, (k, _)) in f
            .range(
                Bound::Included(&Key::default()),
                Bound::Excluded(&Key {
                    source_id: u64::MAX,
                    ..Default::default()
                }),
            )
            .enumerate()
        {
            assert_eq!(i as u64, k.source_id);
        }
    }
}
