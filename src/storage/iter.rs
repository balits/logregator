use std::{
    cmp::{Ordering, Reverse}, collections::{BinaryHeap, btree_set}, fs, io::{self, Read, Seek}, path,
};

use anyhow::{Context, bail};

use crate::storage::{MemTable, record::Record};

pub struct SSTableIter {
    r: io::BufReader<fs::File>,
    start: Record,
    end: Record, // exclusive
    key: Vec<u8>,
    buffer: Vec<u8>,
    end_pos: u64, // byte offset where records end (tfooter start)
}

impl SSTableIter {
    pub(crate) fn new(
        path: &path::PathBuf,
        source_id: i64,
        key: &str,
        start_ts: i64,
        end_ts: i64,
        end_pos: u64,
    ) -> anyhow::Result<Self> {
        if start_ts > end_ts {
            bail!("SSTableIter::new: end_ts cannot be smaller than start_ts")
        }
        if start_ts < 0 {
            bail!("SSTableIter::new: start_ts cannot be negative")
        }

        let start = Record::from_raw_parts(source_id, start_ts, 0, key, "");
        let end = Record::from_raw_parts(source_id, end_ts, 0, key, "");

        let f = std::fs::OpenOptions::new()
            .read(true)
            .open(path)
            .context("SSTableIter::new: failed to open sstable file")?;
        let r: io::BufReader<fs::File> = io::BufReader::new(f);

        Ok(Self {
            r,
            start,
            end,
            key: key.as_bytes().to_vec(),
            buffer: vec![],
            end_pos,
        })
    }
}

impl Iterator for SSTableIter {
    type Item = Record;
    fn next(&mut self) -> Option<Self::Item> {
        loop {
            if self.r.stream_position().ok()? >= self.end_pos {
                return None;
            }
            self.buffer.resize(8, 0);
            match self.r.read_exact(&mut self.buffer[..8]) {
                Err(e) if e.kind() == io::ErrorKind::UnexpectedEof => return None,
                Err(e) => {
                    tracing::error!(error = %e, "SSTableIter::next: failed to read record length");
                    return None;
                }
                Ok(_) => {}
            }

            let rec_len = u64::from_le_bytes([
                self.buffer[0], self.buffer[1], self.buffer[2], self.buffer[3],
                self.buffer[4], self.buffer[5], self.buffer[6], self.buffer[7],
            ]) as usize;
            self.buffer.resize(8 + rec_len, 0);
            if let Err(e) = self.r.read_exact(&mut self.buffer[8..8 + rec_len]) {
                tracing::error!(error = %e, "SSTableIter::next: failed to read record");
                return None;
            }

            let rec = Record::from_vec(self.buffer[8..].to_vec());
            if rec.cmp(&self.start) == Ordering::Less {
                continue;
            }
            if rec.cmp(&self.end) != Ordering::Less {
                return None;
            }
            if rec.extract_key().ok() != Some(&self.key) {
                continue;
            }
            return Some(rec);
        }
    }
}

#[cfg(test)]
mod sstable_iter_tests {
    use super::*;
    use std::{io::Write, path::PathBuf};
    use tempfile::tempdir;

    fn write_sstable(path: &PathBuf, records: &[Record]) -> u64 {
        let f = std::fs::OpenOptions::new()
            .write(true)
            .create(true)
            .open(path)
            .unwrap();
        let mut w = std::io::BufWriter::new(f);
        for rec in records {
            let len_buf = (rec.len() as u64).to_le_bytes();
            w.write_all(&len_buf).unwrap();
            w.write_all(rec.as_bytes()).unwrap();
        }
        w.flush().unwrap();
        std::fs::metadata(path).unwrap().len()
    }

    #[test]
    fn test_iter_all_in_range() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("test.sst");
        let records = vec![
            Record::from_raw_parts(1, 10, 0, "sys", "a"),
            Record::from_raw_parts(1, 20, 0, "sys", "b"),
            Record::from_raw_parts(1, 30, 0, "sys", "c"),
        ];
        let len = write_sstable(&path, &records);
        let iter = SSTableIter::new(&path, 1, "sys", 10, 40, len).unwrap();
        let collected: Vec<Record> = iter.collect();
        assert_eq!(collected.len(), 3);
        assert_eq!(collected[0].extract_timestamp().unwrap(), 10);
        assert_eq!(collected[1].extract_timestamp().unwrap(), 20);
        assert_eq!(collected[2].extract_timestamp().unwrap(), 30);
    }

    #[test]
    fn test_iter_skip_before_start() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("test.sst");
        let records = vec![
            Record::from_raw_parts(1, 5, 0, "sys", "too_early"),
            Record::from_raw_parts(1, 10, 0, "sys", "a"),
            Record::from_raw_parts(1, 20, 0, "sys", "b"),
        ];
        let len = write_sstable(&path, &records);
        let iter = SSTableIter::new(&path, 1, "sys", 10, 30, len).unwrap();
        let collected: Vec<Record> = iter.collect();
        assert_eq!(collected.len(), 2);
        assert_eq!(collected[0].extract_timestamp().unwrap(), 10);
        assert_eq!(collected[1].extract_timestamp().unwrap(), 20);
    }

    #[test]
    fn test_iter_stop_at_end() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("test.sst");
        let records = vec![
            Record::from_raw_parts(1, 10, 0, "sys", "a"),
            Record::from_raw_parts(1, 20, 0, "sys", "b"),
            Record::from_raw_parts(1, 30, 0, "sys", "past_end"),
        ];
        let len = write_sstable(&path, &records);
        let iter = SSTableIter::new(&path, 1, "sys", 10, 20, len).unwrap();
        let collected: Vec<Record> = iter.collect();
        assert_eq!(collected.len(), 1);
        assert_eq!(collected[0].extract_timestamp().unwrap(), 10);
    }

    #[test]
    fn test_iter_empty_range() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("test.sst");
        let records = vec![Record::from_raw_parts(1, 10, 0, "sys", "a")];
        let len = write_sstable(&path, &records);
        let iter = SSTableIter::new(&path, 1, "sys", 10, 10, len).unwrap();
        assert!(iter.collect::<Vec<_>>().is_empty());
    }

    #[test]
    fn test_iter_empty_file() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("empty.sst");
        let f = std::fs::OpenOptions::new()
            .write(true)
            .create(true)
            .open(&path)
            .unwrap();
        drop(f);

        let len = std::fs::metadata(&path).unwrap().len();
        let iter = SSTableIter::new(&path, 1, "sys", 0, 100, len).unwrap();
        assert!(iter.collect::<Vec<_>>().is_empty());
    }

    #[test]
    fn test_iter_single_record() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("test.sst");
        let records = vec![Record::from_raw_parts(1, 50, 0, "sys", "loner")];
        let len = write_sstable(&path, &records);
        let iter = SSTableIter::new(&path, 1, "sys", 0, 100, len).unwrap();
        let collected: Vec<Record> = iter.collect();
        assert_eq!(collected.len(), 1);
        assert_eq!(collected[0].extract_value().unwrap(), b"loner");
    }

    #[test]
    fn test_iter_range_before_first_record() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("test.sst");
        let records = vec![Record::from_raw_parts(1, 20, 0, "sys", "a")];
        let len = write_sstable(&path, &records);
        let iter = SSTableIter::new(&path, 1, "sys", 0, 10, len).unwrap();
        assert!(iter.collect::<Vec<_>>().is_empty());
    }

    #[test]
    fn test_iter_validation() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("test.sst");
        std::fs::OpenOptions::new()
            .write(true)
            .create(true)
            .open(&path)
            .unwrap();

        assert!(SSTableIter::new(&path, 1, "sys", 10, 5, 0).is_err());
        assert!(SSTableIter::new(&path, 1, "sys", -1, 10, 0).is_err());
    }

    #[test]
    fn test_iter_source_id_boundary() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("test.sst");
        let records = vec![
            Record::from_raw_parts(1, 10, 0, "sys", "a"),
            Record::from_raw_parts(1, 20, 0, "sys", "b"),
            Record::from_raw_parts(2, 10, 0, "sys", "other_source"),
        ];
        let len = write_sstable(&path, &records);
        let iter = SSTableIter::new(&path, 1, "sys", 0, 100, len).unwrap();
        let collected: Vec<Record> = iter.collect();
        assert_eq!(collected.len(), 2);
        assert!(
            collected
                .iter()
                .all(|r| r.extract_source_id().unwrap() == 1)
        );
    }
}


/// Iterates over the MemTable's BTreeSet, yielding only records matching
/// the given source_id, key, and time range.
pub(crate) struct MemTableIter<'a> {
    inner: btree_set::Iter<'a, Record>,
    source_id: i64,
    key: Vec<u8>,
    start_ts: i64,
    end_ts: i64,
}

impl<'a> MemTableIter<'a> {
    pub(crate) fn filtered(
        memtable: &'a MemTable,
        source_id: i64,
        key: &'a [u8],
        start_ts: i64,
        end_ts: i64,
    ) -> Self {
        MemTableIter {
            inner: memtable.iter(),
            source_id,
            key: key.to_vec(),
            start_ts,
            end_ts,
        }
    }
}

impl<'a> Iterator for MemTableIter<'a> {
    type Item = Record;

    fn next(&mut self) -> Option<Self::Item> {
        while let Some(r) = self.inner.next() {
            let Ok(sid) = r.extract_source_id() else { continue };
            let Ok(ts) = r.extract_timestamp() else { continue };
            let Ok(key) = r.extract_key() else { continue };
            if sid == self.source_id
                && ts >= self.start_ts
                && ts < self.end_ts
                && key == self.key.as_slice()
            {
                let rec = r.clone();
                return Some(rec);
            }
        }
        None
    }
}

/// HeapItem is a wrapper around a Record with a source
/// index indicating which iterator it came from.
/// If the source is the memtable, the source_idx is 0.
/// If the source is an sstable, the actaul index of the sstable is source_idx - 1.
///
/// HeapItems implement <code>Ord</code> by delegating it to the underlying record.
#[derive(Clone)]
struct HeapItem {
    record: Record,
    /// 0 if memtable, > 0 if sstable iter
    source_idx: usize,
}

impl PartialEq for HeapItem {
    fn eq(&self, other: &Self) -> bool {
        self.record.eq(&other.record)
    }
}

impl Eq for HeapItem {}

impl PartialOrd for HeapItem {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        self.record.partial_cmp(&other.record)
    }
}

impl Ord for HeapItem {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.record.cmp(&other.record)
    }
}

pub(crate) struct MergeIter<'a> {
    heap: BinaryHeap<Reverse<HeapItem>>,
    memtable_iter: MemTableIter<'a>,
    sstable_iters: Vec<SSTableIter>,
    last_item: Option<HeapItem>,
}

impl<'a> MergeIter<'a> {
    pub(crate) fn new(
        mut memtable_iter: MemTableIter<'a>,
        mut sstable_iters: Vec<SSTableIter>,
    ) -> Self {
        let mut heap = BinaryHeap::new();

        // insert first items of memtable + sstables
        if let Some(r) = memtable_iter.next() {
            heap.push(Reverse(HeapItem {
                record: r,
                source_idx: 0,
            }));
        }
        for (i, iter) in sstable_iters.iter_mut().enumerate() {
            if let Some(r) = iter.next() {
                heap.push(Reverse(HeapItem {
                    record: r,
                    source_idx: i + 1,
                }));
            }
        }

        Self {
            heap: heap,
            memtable_iter: memtable_iter,
            sstable_iters: sstable_iters,
            last_item: None,
        }
    }

    fn refil(&mut self, source_idx: usize) {
        match source_idx {
            0 => {
                if let Some(r) = self.memtable_iter.next() {
                    self.heap.push(Reverse(HeapItem {
                        record: r,
                        source_idx: 0,
                    }));
                }
            },
            n => {
                if let Some(iter) = self.sstable_iters.get_mut(n-1) {
                    if let Some(r) = iter.next() {
                        self.heap.push(Reverse(HeapItem {
                            record: r,
                            source_idx: n,
                        }));
                    }
                }
            }
        }
    }
}

impl<'a> Iterator for MergeIter<'a> {
    type Item = Record;

    fn next(&mut self) -> Option<Self::Item> {
        while let Some(item) = self.heap.pop() {
            self.refil(item.0.source_idx);
            if let Some(ref prev) = self.last_item && prev.eq(&item.0) {
                continue
            }
            self.last_item = Some(item.0.clone());
            return Some(item.0.record)
        }

        None
    }
}