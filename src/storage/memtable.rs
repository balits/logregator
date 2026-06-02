use std::collections::{BTreeSet, btree_set};

use crate::storage::record::Record;

/// MemTable holds an in memory buffer of the latest
/// log records. Inserting a record fills up the capacity
/// of the memtable, returning a true if the table has exceeded
/// its capacity and is in need of flushing.
#[derive(Debug, Clone)]
pub struct MemTable {
    sz: usize,
    limit: usize,
    bset: BTreeSet<Record>,
}

impl MemTable {
    pub fn new(limit: usize) -> Self {
        // let cap = limit.div_euclid(Record::MIN_RECORD_SIZE);
        Self {
            sz: 0,
            limit,
            bset: BTreeSet::new(),
        }
    }

    pub fn len(&self) -> usize {
        return self.bset.len();
    }
    pub fn is_empty(&self) -> bool {
        return self.bset.is_empty();
    }

    pub fn capacity(&self) -> usize {
        self.limit
    }

    pub fn current_size(&self) -> usize {
        self.sz
    }

    pub fn insert(&mut self, rec: Record) -> bool {
        let rec_sz = rec.len();
        self.bset.insert(rec);
        self.sz += rec_sz;
        self.sz > self.limit
    }

    /// Clones only records within [source_id, start_ts) .. (source_id, end_ts),
    /// reducing allocations vs cloning the entire memtable.
    pub fn clone_range(&self, source_id: i64, start_ts: i64, end_ts: i64) -> Vec<Record> {
        let range_start = Record::from_raw_parts(source_id, start_ts, 0, "", "");
        let range_end = Record::from_raw_parts(source_id, end_ts, 0, "", "");
        self.bset.range(range_start..range_end).cloned().collect()
    }

    pub fn clear(&mut self) {
        self.bset.clear();
        self.sz = 0;
    }

    pub fn freeze(&mut self) -> Self {
        let current_sz = self.sz;
        let limit = self.limit;
        let bset = std::mem::take(&mut self.bset);
        self.clear();
        Self {
            limit,
            sz: current_sz,
            bset,
        }
    }

    pub fn iter(&self) -> Iter<'_> {
        let inner = self.bset.iter();
        Iter { inner }
    }

    // IntoIter implemened below

    #[cfg(test)]
    pub fn get(&self, source_id: i64, timestamp: i64, key: &str) -> Option<&Record> {
        let start = Record::from_raw_parts(source_id, timestamp, 0, key, "");
        self.bset
            .range(start..)
            .take_while(|r| {
                r.extract_source_id().ok() == Some(source_id)
                    && r.extract_timestamp().ok() == Some(timestamp)
            })
            .filter(|r| r.extract_key().ok() == Some(key.as_bytes()))
            .last()
    }
}

pub struct Iter<'a> {
    inner: btree_set::Iter<'a, Record>,
}

impl<'a> Iterator for Iter<'a> {
    type Item = &'a Record;

    fn next(&mut self) -> Option<Self::Item> {
        self.inner.next()
    }
}

#[derive(Debug)]
pub struct IntoIter {
    inner: btree_set::IntoIter<Record>,
}

impl Iterator for IntoIter {
    type Item = Record;

    fn next(&mut self) -> Option<Self::Item> {
        self.inner.next()
    }
}

impl IntoIterator for MemTable {
    type Item = Record;
    type IntoIter = IntoIter;

    fn into_iter(self) -> Self::IntoIter {
        IntoIter {
            inner: self.bset.into_iter(),
        }
    }
}

/// Iterates over a `Vec<Record>`, yielding only records matching
/// the given source_id, key, and time range.
#[derive(Debug)]
pub struct IntoIterFiltered {
    inner: std::vec::IntoIter<Record>,
    source_id: i64,
    key: Vec<u8>,
    start_ts: i64,
    end_ts: i64,
    filter: String,
}

impl IntoIterFiltered {
    pub(crate) fn new(
        records: Vec<Record>,
        source_id: i64,
        key: &[u8],
        start_ts: i64,
        end_ts: i64,
        filter: &str,
    ) -> Self {
        Self {
            inner: records.into_iter(),
            source_id,
            key: key.to_vec(),
            start_ts,
            end_ts,
            filter: filter.to_string(),
        }
    }
}

impl Iterator for IntoIterFiltered {
    type Item = Record;

    fn next(&mut self) -> Option<Self::Item> {
        for r in self.inner.by_ref() {
            let Ok((sid, ts, _seq, key, val)) = r.extract_all_fields_ref() else {
                continue;
            };
            if sid == self.source_id
                && ts >= self.start_ts
                && ts < self.end_ts
                && key == self.key.as_slice()
                && (self.filter.is_empty()
                    || unsafe { std::str::from_utf8_unchecked(val) }.contains(&self.filter))
            {
                return Some(r);
            }
        }
        None
    }
}

mod tests {
    #[allow(unused_imports)]
    use super::*;

    #[test]
    fn test_memtable_put_and_get() {
        let mut mt = MemTable::new(1024);

        mt.insert(Record::from_raw_parts(1, 1, 0, "log_1", "error on line 42"));
        mt.insert(Record::from_raw_parts(1, 2, 0, "log_2", "user logged in"));

        let retrieved = mt.get(1, 1, "log_1").expect("record should exist");
        assert_eq!(retrieved.extract_value().unwrap(), b"error on line 42");

        assert!(mt.get(1, 3, "log_3").is_none());
    }

    #[test]
    fn test_memtable_maintains_strict_ordering() {
        let mut mt = MemTable::new(1024);

        mt.insert(Record::from_raw_parts(1, 102, 0, "c_log", "third"));
        mt.insert(Record::from_raw_parts(1, 100, 0, "a_log", "first"));
        mt.insert(Record::from_raw_parts(1, 101, 0, "b_log", "second"));

        let mut iter = mt.iter();

        assert_eq!(iter.next().unwrap().extract_timestamp().unwrap(), 100);
        assert_eq!(iter.next().unwrap().extract_key().unwrap(), b"b_log");
        assert_eq!(iter.next().unwrap().extract_key().unwrap(), b"c_log");
        assert!(iter.next().is_none());
    }

    #[test]
    fn test_memtable_size_tracking_and_overwrite() {
        let mut mt = MemTable::new(1024);
        assert_eq!(mt.current_size(), 0);

        let rec1 = Record::from_raw_parts(1, 100, 0, "sys_metric", "cpu: 45%");
        let size_1 = rec1.len();

        mt.insert(rec1);
        assert_eq!(mt.current_size(), size_1);

        let rec2 = Record::from_raw_parts(1, 100, 1, "sys_metric", "cpu: 45%, mem: 80%");
        let size_2 = rec2.len();

        mt.insert(rec2);

        assert_eq!(mt.current_size(), size_1 + size_2);
        assert_eq!(
            mt.get(1, 100, "sys_metric")
                .unwrap()
                .extract_value()
                .unwrap(),
            b"cpu: 45%, mem: 80%"
        );
    }

    #[test]
    fn test_memtable_capacity_trigger() {
        let mut mt = MemTable::new(100);

        assert!(
            !mt.insert(Record::from_raw_parts(1, 1, 0, "a", "x")),
            "should not trigger yet"
        );

        assert!(
            !mt.insert(Record::from_raw_parts(1, 2, 0, "b", "y")),
            "should still be under 100 bytes"
        );

        assert!(
            mt.insert(Record::from_raw_parts(
                1,
                3,
                0,
                "c",
                "this_is_a_massive_string_that_will_overflow_the_limit"
            )),
            "should return true, signaling it is full"
        );
    }

    #[test]
    fn test_memtable_clear() {
        let mut mt = MemTable::new(1024);
        mt.insert(Record::from_raw_parts(1, 1, 0, "k1", "v1"));
        assert!(mt.current_size() > 0);

        mt.clear();

        assert_eq!(mt.current_size(), 0);
        assert!(mt.get(1, 1, "k1").is_none());
        assert!(mt.iter().next().is_none());
    }
}
