use std::{collections::{BTreeSet, btree_set}};

use crate::storage::record::Record;

/// MemTable holds an in memory buffer of the latest
/// log records. Inserting a record fills up the capacity
/// of the memtable, returning a true if the table has exceeded
/// its capacity and is in need of flushing.
#[derive(Clone)]
pub(crate) struct MemTable {
    current_sz: usize,
    limit: usize,
    bset: BTreeSet<Record>,
}

impl MemTable {
    pub(crate) fn new(limit: usize) -> Self {
        Self { current_sz: 0, limit, bset: BTreeSet::new(), }
    }

    pub(crate) fn insert(&mut self, rec: Record) -> bool {
        let rec_sz = rec.len();
        self.bset.insert(rec);
        self.current_sz += rec_sz;
        self.current_sz > self.limit
    }

    pub(crate) fn get(&self, source_id: i64, timestamp: i64, key: &str) -> Option<&Record> {
        let start = Record::from_raw_parts(source_id, timestamp, 0, key, "");
        self.bset.range(start..)
            .take_while(|r| {
                r.extract_source_id().ok() == Some(source_id)
                    && r.extract_timestamp().ok() == Some(timestamp)
            })
            .filter(|r| r.extract_key().ok() == Some(key.as_bytes()))
            .last()
    }

    pub(crate) fn len(&self) -> usize {
        self.bset.len()
    }

    pub(crate) fn size_hint(&self) -> usize {
        self.current_sz
    }

    pub(crate) fn iter(&self) -> btree_set::Iter<'_, Record> {
        self.bset.iter()
    }

    pub(crate) fn clear(&mut self) {
        self.bset.clear();
        self.current_sz = 0;
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
        assert_eq!(mt.size_hint(), 0);

        let rec1 = Record::from_raw_parts(1, 100, 0, "sys_metric", "cpu: 45%");
        let size_1 = rec1.len();

        mt.insert(rec1);
        assert_eq!(mt.size_hint(), size_1);

        let rec2 = Record::from_raw_parts(1, 100, 1, "sys_metric", "cpu: 45%, mem: 80%");
        let size_2 = rec2.len();

        mt.insert(rec2);

        assert_eq!(mt.size_hint(), size_1 + size_2);
        assert_eq!(
            mt.get(1, 100, "sys_metric").unwrap().extract_value().unwrap(),
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
        assert!(mt.size_hint() > 0);

        mt.clear();

        assert_eq!(mt.size_hint(), 0);
        assert!(mt.get(1, 1, "k1").is_none());
        assert!(mt.iter().next().is_none());
    }
}