use std::{
    cmp::{Ordering, Reverse},
    collections::BinaryHeap,
    fs,
    io::{self, Read, Seek},
    vec,
};

use anyhow::{Context, bail};

use crate::storage::{SSTableMeta, record::Record};

/// SSTableIter reads key-values from an SSTable file
/// within a [start, end) range, using the index block to
/// binary-seek to the nearest offset and then scanning linearly.
#[derive(Debug)]
pub struct SSTableIter {
    r: io::BufReader<fs::File>,
    start: Record,
    end: Record, // exclusive
    key: Vec<u8>,
    buffer: Vec<u8>,
    end_pos: u64, // byte offset where records end (index block start)
    filter: String,
}

impl SSTableIter {
    pub(crate) fn new(
        meta: &SSTableMeta,
        source_id: i64,
        key: &str,
        start_ts: i64,
        end_ts: i64,
        filter: &str,
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
            .open(&meta.path)
            .context("SSTableIter::new: failed to open sstable file")?;
        let mut r: io::BufReader<fs::File> = io::BufReader::new(f);

        let seek_offset = meta.index.binary_seek(&start);
        r.seek(io::SeekFrom::Start(seek_offset))
            .context("SSTableIter::new: failed to seek to index offset")?;

        Ok(Self {
            r,
            start,
            end,
            key: key.as_bytes().to_vec(),
            buffer: vec![],
            end_pos: meta.index_offset,
            filter: filter.to_string(),
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

            #[rustfmt::skip]
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
            let Ok((_sid, _ts, _seq, key, val)) = rec.extract_all_fields_ref() else {
                continue;
            };
            if key != self.key.as_slice() {
                continue;
            }
            if !self.filter.is_empty()
                && !unsafe { std::str::from_utf8_unchecked(val) }.contains(&self.filter)
            {
                continue;
            }
            return Some(rec);
        }
    }
}

/// Iterates over the MemTable's BTreeSet, yielding only records matching
/// the given source_id, key, and time range.
#[derive(Debug)]
pub(crate) struct MemTableIterOwned {
    inner: vec::IntoIter<Record>,
    source_id: i64,
    key: Vec<u8>,
    start_ts: i64,
    end_ts: i64,
    filter: String,
}

impl MemTableIterOwned {
    pub(crate) fn filtered(
        memtable_records: Vec<Record>,
        source_id: i64,
        key: &[u8],
        start_ts: i64,
        end_ts: i64,
        filter: &str,
    ) -> Self {
        MemTableIterOwned {
            inner: memtable_records.into_iter(),
            source_id,
            key: key.to_vec(),
            start_ts,
            end_ts,
            filter: filter.to_string(),
        }
    }
}

impl Iterator for MemTableIterOwned {
    type Item = Record;

    fn next(&mut self) -> Option<Self::Item> {
        while let Some(r) = self.inner.next() {
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

//
/// SSTableScan allows unfiltered scanning of an SSTable file with the help of its metadata.
#[derive(Debug)]
pub struct SSTableScaner {
    r: io::BufReader<fs::File>,
    buffer: Vec<u8>,
    meta: SSTableMeta,
}

impl SSTableScaner {
    pub(crate) fn new(meta: SSTableMeta) -> anyhow::Result<Self> {
        let f = std::fs::OpenOptions::new()
            .read(true)
            .open(&meta.path)
            .context("SSTableIter::new: failed to open sstable file")?;
        let r: io::BufReader<fs::File> = io::BufReader::new(f);

        Ok(Self {
            r,
            buffer: Vec::with_capacity(1024),
            meta,
        })
    }
}

impl Iterator for SSTableScaner {
    type Item = Record;
    fn next(&mut self) -> Option<Self::Item> {
        if self.r.stream_position().ok()? >= self.meta.index_offset {
            return None;
        }

        self.buffer.resize(8, 0);
        match self.r.read_exact(&mut self.buffer[..8]) {
            Err(e) if e.kind() == io::ErrorKind::UnexpectedEof => return None,
            Err(e) => {
                tracing::error!(error = %e, "SSTableScan.next: failed to read record length");
                return None;
            }
            Ok(_) => {}
        }

        #[rustfmt::skip]
        let rec_len = u64::from_le_bytes([
            self.buffer[0], self.buffer[1], self.buffer[2], self.buffer[3],
            self.buffer[4], self.buffer[5], self.buffer[6], self.buffer[7],
        ]) as usize;

        self.buffer.resize(8 + rec_len, 0);
        if let Err(e) = self.r.read_exact(&mut self.buffer[8..8 + rec_len]) {
            tracing::error!(error = %e, "SSTableScan.next: failed to read record");
            return None;
        }

        let rec = Record::from_vec(self.buffer[8..].to_vec());
        return Some(rec);
    }
}

/// HeapItem is a wrapper around a Record with a source
/// index indicating which iterator it came from.
/// If the source is the memtable, the source_idx is 0.
/// If the source is an sstable, the actaul index of the sstable is source_idx - 1.
///
/// HeapItems implement <code>Ord</code> by delegating it to the underlying record.
#[derive(Clone, Debug)]
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

/// A type-erased record iterator — either backed by a file (SSTableIter / SSTableScaner)
/// or by an in-memory vec (into_iter). Reading SSTables into memory upfront
/// avoids scattered file I/O during the merge.
#[derive(Debug)]
pub(crate) enum RecordIter {
    File(SSTableIter),
    Scan(SSTableScaner),
    Mem(std::vec::IntoIter<Record>),
}

impl Iterator for RecordIter {
    type Item = Record;

    fn next(&mut self) -> Option<Self::Item> {
        match self {
            RecordIter::File(iter) => iter.next(),
            RecordIter::Scan(iter) => iter.next(),
            RecordIter::Mem(iter) => iter.next(),
        }
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        match self {
            RecordIter::File(iter) => iter.size_hint(),
            RecordIter::Scan(iter) => iter.size_hint(),
            RecordIter::Mem(iter) => iter.size_hint(),
        }
    }
}

#[derive(Debug)]
pub struct MergeIter {
    heap: BinaryHeap<Reverse<HeapItem>>,

    memtable_iter: Option<MemTableIterOwned>,

    sstable_iters: Vec<RecordIter>,

    last_item_idx: Option<Record>,
}

impl MergeIter {
    pub fn new(
        mut memtable_iter: Option<MemTableIterOwned>,
        mut sstable_iters: Vec<RecordIter>,
    ) -> Self {
        let mut heap = BinaryHeap::new();

        // insert first items of memtable + sstables
        if let Some(iter) = memtable_iter.as_mut()
            && let Some(r) = iter.next()
        {
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
            heap,
            memtable_iter,
            sstable_iters,
            last_item_idx: None,
        }
    }

    fn refil(&mut self, source_idx: usize) {
        match source_idx {
            0 if self.memtable_iter.is_some() => {
                if let Some(r) = self.memtable_iter.as_mut().unwrap().next() {
                    self.heap.push(Reverse(HeapItem {
                        record: r,
                        source_idx: 0,
                    }));
                }
            }
            n => {
                if let Some(iter) = self.sstable_iters.get_mut(n - 1)
                    && let Some(r) = iter.next()
                {
                    self.heap.push(Reverse(HeapItem {
                        record: r,
                        source_idx: n,
                    }));
                }
            }
        }
    }
}

impl Iterator for MergeIter {
    type Item = Record;

    fn next(&mut self) -> Option<Self::Item> {
        while let Some(item) = self.heap.pop() {
            self.refil(item.0.source_idx);
            let record = item.0.record;
            if let Some(ref prev) = self.last_item_idx {
                if prev.cmp(&record) == std::cmp::Ordering::Equal {
                    continue;
                }
            }
            self.last_item_idx = Some(record.clone());
            return Some(record);
        }

        None
    }
}

#[cfg(test)]
mod sstable_iter_tests {
    use super::*;
    use crate::storage::{IndexBlock, SSTableMeta};
    use std::{io::Write, path::PathBuf};
    use tempfile::tempdir;

    fn make_meta(path: PathBuf, end_pos: u64) -> SSTableMeta {
        SSTableMeta {
            id: 0,
            path,
            bloom: crate::storage::BloomFilter::new(0, 0.01),
            bloom_offset: end_pos,
            index: crate::storage::IndexBlock::default(),
            index_offset: end_pos,
            file_size: end_pos,
            num_records: 0,
        }
    }

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
        let meta = make_meta(path, len);
        let iter = SSTableIter::new(&meta, 1, "sys", 10, 40, "").unwrap();
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
        let meta = make_meta(path, len);
        let iter = SSTableIter::new(&meta, 1, "sys", 10, 30, "").unwrap();
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
        let meta = make_meta(path, len);
        let iter = SSTableIter::new(&meta, 1, "sys", 10, 20, "").unwrap();
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
        let meta = make_meta(path, len);
        let iter = SSTableIter::new(&meta, 1, "sys", 10, 10, "").unwrap();
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
        let meta = make_meta(path, len);
        let iter = SSTableIter::new(&meta, 1, "sys", 0, 100, "").unwrap();
        assert!(iter.collect::<Vec<_>>().is_empty());
    }

    #[test]
    fn test_iter_single_record() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("test.sst");
        let records = vec![Record::from_raw_parts(1, 50, 0, "sys", "loner")];
        let len = write_sstable(&path, &records);
        let meta = make_meta(path, len);
        let iter = SSTableIter::new(&meta, 1, "sys", 0, 100, "").unwrap();
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
        let meta = make_meta(path, len);
        let iter = SSTableIter::new(&meta, 1, "sys", 0, 10, "").unwrap();
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

        let meta = make_meta(path, 0);
        assert!(SSTableIter::new(&meta, 1, "sys", 10, 5, "").is_err());
        assert!(SSTableIter::new(&meta, 1, "sys", -1, 10, "").is_err());
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
        let meta = make_meta(path, len);
        let iter = SSTableIter::new(&meta, 1, "sys", 0, 100, "").unwrap();
        let collected: Vec<Record> = iter.collect();
        assert_eq!(collected.len(), 2);
        assert!(
            collected
                .iter()
                .all(|r| r.extract_source_id().unwrap() == 1)
        );
    }

    #[test]
    fn test_index_binary_seek_empty() {
        let index = IndexBlock::default();
        let target = Record::from_raw_parts(1, 100, 0, "sys", "");
        assert_eq!(index.binary_seek(&target), 0);
    }

    #[test]
    fn test_index_binary_seek_exact_match() {
        let mut index = IndexBlock::with_capacity(4);
        index.try_insert(0, &Record::from_raw_parts(1, 100, 5, "sys", "v"), 42).unwrap();
        index.try_insert(16, &Record::from_raw_parts(1, 200, 10, "sys", "v"), 128).unwrap();

        let target = Record::from_raw_parts(1, 100, 0, "sys", "");
        assert_eq!(index.binary_seek(&target), 42);
    }

    #[test]
    fn test_index_binary_seek_between_entries() {
        let mut index = IndexBlock::with_capacity(4);
        index.try_insert(0, &Record::from_raw_parts(1, 100, 5, "sys", "v"), 42).unwrap();
        index.try_insert(16, &Record::from_raw_parts(1, 200, 10, "sys", "v"), 128).unwrap();

        let target = Record::from_raw_parts(1, 150, 0, "sys", "");
        assert_eq!(index.binary_seek(&target), 42);
    }

    #[test]
    fn test_index_binary_seek_before_all() {
        let mut index = IndexBlock::with_capacity(4);
        index.try_insert(0, &Record::from_raw_parts(1, 100, 5, "sys", "v"), 42).unwrap();

        let target = Record::from_raw_parts(1, 50, 0, "sys", "");
        assert_eq!(index.binary_seek(&target), 0);
    }

    #[test]
    fn test_index_binary_seek_after_all() {
        let mut index = IndexBlock::with_capacity(4);
        index.try_insert(0, &Record::from_raw_parts(1, 100, 5, "sys", "v"), 42).unwrap();

        let target = Record::from_raw_parts(1, 200, 0, "sys", "");
        assert_eq!(index.binary_seek(&target), 42);
    }

    #[test]
    fn test_index_binary_seek_ignores_seq_num() {
        // Index entry has seq=25, target has seq=0 — binary_seek should still match on ts
        let mut index = IndexBlock::with_capacity(4);
        index.try_insert(16, &Record::from_raw_parts(1, 116, 25, "sys", "v"), 200).unwrap();

        let target = Record::from_raw_parts(1, 116, 0, "sys", "");
        assert_eq!(index.binary_seek(&target), 200);
    }

    #[test]
    fn test_index_binary_seek_different_source_id() {
        let mut index = IndexBlock::with_capacity(4);
        index.try_insert(0, &Record::from_raw_parts(1, 100, 5, "sys", "v"), 42).unwrap();

        let target = Record::from_raw_parts(2, 100, 0, "sys", "");
        assert_eq!(index.binary_seek(&target), 42); // falls back to previous (or 0 since no entry for sid=2)
    }


    fn write_indexed_sstable(path: &PathBuf, records: &[Record]) -> SSTableMeta {
        let len = records.len();
        SSTableMeta::write_to_file(path.clone(), 1, records.iter().cloned(), len).unwrap()
    }

    #[test]
    fn test_iter_real_index_returns_all_records() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("indexed.sst");
        let records: Vec<Record> = (0..33)
            .map(|i| Record::from_raw_parts(1, i, i as u64, "sys", &format!("val{}", i)))
            .collect();
        let meta = write_indexed_sstable(&path, &records);

        assert!(!meta.index.inner.is_empty(), "index should have entries for 33 records");

        let iter = SSTableIter::new(&meta, 1, "sys", 0, 100, "").unwrap();
        let collected: Vec<Record> = iter.collect();
        assert_eq!(collected.len(), 33);
    }

    #[test]
    fn test_iter_real_index_skips_early_records() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("indexed.sst");
        let records: Vec<Record> = (0..33)
            .map(|i| Record::from_raw_parts(1, 100 + i, i as u64, "sys", &format!("val{}", i)))
            .collect();
        let meta = write_indexed_sstable(&path, &records);

        assert!(meta.index.inner.len() >= 2, "33 records should produce >=2 index entries");

        // Range [116, 120) — binary_seek should land on entry at ts=116
        let iter = SSTableIter::new(&meta, 1, "sys", 116, 120, "").unwrap();
        let collected: Vec<Record> = iter.collect();
        assert_eq!(collected.len(), 4);
        for rec in &collected {
            let ts = rec.extract_timestamp().unwrap();
            assert!(ts >= 116 && ts < 120, "timestamp {} should be in [116, 120)", ts);
        }
    }

    #[test]
    fn test_iter_real_index_skips_early_records_different_source() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("indexed.sst");
        let mut records = Vec::with_capacity(48);
        for i in 0..48 {
            let sid = if i < 16 { 1 } else { 2 };
            records.push(Record::from_raw_parts(sid, 100 + i, i as u64, "sys", &format!("val{}", i)));
        }
        let meta = write_indexed_sstable(&path, &records);

        // Source_id=2 records start at index 16 (ts=116)
        let iter = SSTableIter::new(&meta, 2, "sys", 116, 120, "").unwrap();
        let collected: Vec<Record> = iter.collect();
        assert_eq!(collected.len(), 4);
        for rec in &collected {
            assert_eq!(rec.extract_source_id().unwrap(), 2);
            let ts = rec.extract_timestamp().unwrap();
            assert!(ts >= 116 && ts < 120);
        }
    }

    #[test]
    fn test_iter_real_index_range_before_start() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("indexed.sst");
        let records: Vec<Record> = (0..33)
            .map(|i| Record::from_raw_parts(1, 100 + i, i as u64, "sys", &format!("val{}", i)))
            .collect();
        let meta = write_indexed_sstable(&path, &records);

        // Range before all records
        let iter = SSTableIter::new(&meta, 1, "sys", 0, 50, "").unwrap();
        assert!(iter.collect::<Vec<_>>().is_empty());
    }

    #[test]
    fn test_iter_real_index_range_after_end() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("indexed.sst");
        let records: Vec<Record> = (0..33)
            .map(|i| Record::from_raw_parts(1, 100 + i, i as u64, "sys", &format!("val{}", i)))
            .collect();
        let meta = write_indexed_sstable(&path, &records);

        let iter = SSTableIter::new(&meta, 1, "sys", 200, 300, "").unwrap();
        assert!(iter.collect::<Vec<_>>().is_empty());
    }
}

#[cfg(test)]
mod sstable_scanner_tests {
    use super::*;
    use crate::storage::{BloomFilter, IndexBlock, SSTableMeta};
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

    fn make_meta(path: PathBuf, end_pos: u64, file_size: u64) -> SSTableMeta {
        SSTableMeta {
            id: 0,
            path,
            bloom: BloomFilter::new(0, 0.01),
            bloom_offset: end_pos,
            index: IndexBlock::default(),
            index_offset: end_pos,
            num_records: 0,
            file_size,
        }
    }

    #[test]
    fn test_scanner_all_records() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("test.sst");
        let records = vec![
            Record::from_raw_parts(1, 10, 0, "sys", "a"),
            Record::from_raw_parts(1, 20, 0, "sys", "b"),
            Record::from_raw_parts(2, 30, 0, "db", "c"),
        ];
        let len = write_sstable(&path, &records);
        let meta = make_meta(path.clone(), len, len);
        let scanner = SSTableScaner::new(meta.clone()).unwrap();
        let collected: Vec<Record> = scanner.collect();
        assert_eq!(collected.len(), 3);
    }

    #[test]
    fn test_scanner_empty_file() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("empty.sst");
        let f = std::fs::OpenOptions::new()
            .write(true)
            .create(true)
            .open(&path)
            .unwrap();
        drop(f);
        let meta = make_meta(path.clone(), 0, 0);
        let scanner = SSTableScaner::new(meta.clone()).unwrap();
        assert!(scanner.collect::<Vec<_>>().is_empty());
    }

    #[test]
    fn test_scanner_single_record() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("test.sst");
        let records = vec![Record::from_raw_parts(1, 50, 0, "sys", "loner")];
        let len = write_sstable(&path, &records);
        let meta = make_meta(path.clone(), len, len);
        let scanner = SSTableScaner::new(meta.clone()).unwrap();
        let collected: Vec<Record> = scanner.collect();
        assert_eq!(collected.len(), 1);
        assert_eq!(collected[0].extract_value().unwrap(), b"loner");
    }

    #[test]
    fn test_scanner_stops_at_bloom_offset() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("test.sst");
        let records = vec![
            Record::from_raw_parts(1, 10, 0, "sys", "a"),
            Record::from_raw_parts(1, 20, 0, "sys", "b"),
        ];
        let len = write_sstable(&path, &records);
        // bloom_offset < file_len — scanner should stop before extra garbage
        let meta = make_meta(path.clone(), len, len);
        let scanner = SSTableScaner::new(meta.clone()).unwrap();
        let collected: Vec<Record> = scanner.collect();
        assert_eq!(collected.len(), 2);
    }

    #[test]
    fn test_scanner_multiple_source_ids() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("test.sst");
        let records = vec![
            Record::from_raw_parts(1, 10, 0, "sys", "a"),
            Record::from_raw_parts(2, 20, 0, "db", "b"),
            Record::from_raw_parts(1, 30, 0, "web", "c"),
            Record::from_raw_parts(3, 40, 0, "cache", "d"),
        ];
        let len = write_sstable(&path, &records);
        let meta = make_meta(path.clone(), len, len);
        let scanner = SSTableScaner::new(meta.clone()).unwrap();
        let collected: Vec<Record> = scanner.collect();
        assert_eq!(collected.len(), 4);
    }
}

#[cfg(test)]
mod merge_iter_tests {
    use super::*;

    #[test]
    fn test_merge_memtable_only() {
        let records = vec![
            Record::from_raw_parts(1, 10, 0, "sys", "a"),
            Record::from_raw_parts(1, 20, 0, "sys", "b"),
        ];
        let mem_iter = MemTableIterOwned::filtered(records, 1, b"sys", 0, 100, "");
        let empty: Vec<RecordIter> = vec![];
        let merge = MergeIter::new(Some(mem_iter), empty);
        let collected: Vec<Record> = merge.collect();
        assert_eq!(collected.len(), 2);
    }

    #[test]
    fn test_merge_sstable_only() {
        let sstable = vec![
            Record::from_raw_parts(1, 10, 0, "sys", "a"),
            Record::from_raw_parts(1, 20, 0, "sys", "b"),
        ];
        let merge = MergeIter::new(None, vec![RecordIter::Mem(sstable.into_iter())]);
        let collected: Vec<Record> = merge.collect();
        assert_eq!(collected.len(), 2);
    }

    #[test]
    fn test_merge_memtable_and_sstable() {
        let mem_records = vec![
            Record::from_raw_parts(1, 5, 0, "sys", "early"),
            Record::from_raw_parts(1, 25, 0, "sys", "late"),
        ];
        let mem_iter = MemTableIterOwned::filtered(mem_records, 1, b"sys", 0, 100, "");
        let sstable = vec![
            Record::from_raw_parts(1, 10, 0, "sys", "mid"),
            Record::from_raw_parts(1, 20, 0, "sys", "mid2"),
        ];
        let merge = MergeIter::new(Some(mem_iter), vec![RecordIter::Mem(sstable.into_iter())]);
        let collected: Vec<Record> = merge.collect();
        assert_eq!(collected.len(), 4);
        let ts: Vec<i64> = collected
            .iter()
            .map(|r| r.extract_timestamp().unwrap())
            .collect();
        assert_eq!(ts, vec![5, 10, 20, 25]);
    }

    #[test]
    fn test_merge_multiple_sstables() {
        let sst1 = vec![
            Record::from_raw_parts(1, 10, 0, "sys", "a"),
            Record::from_raw_parts(1, 30, 0, "sys", "c"),
        ];
        let sst2 = vec![
            Record::from_raw_parts(1, 20, 0, "sys", "b"),
            Record::from_raw_parts(1, 40, 0, "sys", "d"),
        ];
        let merge = MergeIter::new(None, vec![RecordIter::Mem(sst1.into_iter()), RecordIter::Mem(sst2.into_iter())]);
        let collected: Vec<Record> = merge.collect();
        assert_eq!(collected.len(), 4);
        let ts: Vec<i64> = collected
            .iter()
            .map(|r| r.extract_timestamp().unwrap())
            .collect();
        assert_eq!(ts, vec![10, 20, 30, 40]);
    }

    #[test]
    fn test_merge_dedup() {
        let sst1 = vec![
            Record::from_raw_parts(1, 10, 0, "sys", "dup"),
            Record::from_raw_parts(1, 30, 0, "sys", "c"),
        ];
        let sst2 = vec![
            Record::from_raw_parts(1, 10, 0, "sys", "dup"),
            Record::from_raw_parts(1, 20, 0, "sys", "b"),
        ];
        let merge = MergeIter::new(None, vec![RecordIter::Mem(sst1.into_iter()), RecordIter::Mem(sst2.into_iter())]);
        let collected: Vec<Record> = merge.collect();
        assert_eq!(collected.len(), 3);
        let ts: Vec<i64> = collected
            .iter()
            .map(|r| r.extract_timestamp().unwrap())
            .collect();
        assert_eq!(ts, vec![10, 20, 30]);
    }

    #[test]
    fn test_merge_empty_sstables() {
        let merge = MergeIter::new(None, vec![]);
        assert!(merge.collect::<Vec<_>>().is_empty());
    }

    #[test]
    fn test_merge_memtable_only_no_matches() {
        let records = vec![
            Record::from_raw_parts(1, 10, 0, "sys", "a"),
            Record::from_raw_parts(1, 20, 0, "sys", "b"),
        ];
        let mem_iter = MemTableIterOwned::filtered(records, 2, b"other", 0, 100, "");
        let empty: Vec<RecordIter> = vec![];
        let merge = MergeIter::new(Some(mem_iter), empty);
        assert!(merge.collect::<Vec<_>>().is_empty());
    }

    #[test]
    fn test_merge_memtable_filter_matches_subset() {
        let records = vec![
            Record::from_raw_parts(1, 10, 0, "sys", "cpu normal"),
            Record::from_raw_parts(1, 20, 0, "sys", "mem high"),
            Record::from_raw_parts(1, 30, 0, "sys", "disk full"),
            Record::from_raw_parts(1, 40, 0, "sys", "memory leak"),
        ];
        let mem_iter = MemTableIterOwned::filtered(records, 1, b"sys", 0, 100, "mem");
        let empty: Vec<RecordIter> = vec![];
        let merge = MergeIter::new(Some(mem_iter), empty);
        let collected: Vec<Record> = merge.collect();
        assert_eq!(collected.len(), 2);
        for rec in &collected {
            let val = rec.extract_value().unwrap();
            assert!(val == b"mem high" || val == b"memory leak");
        }
    }

    #[test]
    fn test_merge_memtable_filter_no_match() {
        let records = vec![
            Record::from_raw_parts(1, 10, 0, "sys", "cpu normal"),
            Record::from_raw_parts(1, 20, 0, "sys", "mem high"),
        ];
        let mem_iter = MemTableIterOwned::filtered(records, 1, b"sys", 0, 100, "nonexistent");
        let empty: Vec<RecordIter> = vec![];
        let merge = MergeIter::new(Some(mem_iter), empty);
        assert!(merge.collect::<Vec<_>>().is_empty());
    }

    #[test]
    fn test_merge_memtable_filter_with_sstable() {
        // memtable records with filter
        let mem_records = vec![
            Record::from_raw_parts(1, 5, 0, "sys", "early"),
            Record::from_raw_parts(1, 25, 0, "sys", "late"),
        ];
        let mem_iter = MemTableIterOwned::filtered(mem_records, 1, b"sys", 0, 100, "late");
        // sstable vec iterator (no filter applied)
        let sstable = vec![
            Record::from_raw_parts(1, 10, 0, "sys", "mid"),
            Record::from_raw_parts(1, 20, 0, "sys", "mid2"),
        ];
        let merge = MergeIter::new(Some(mem_iter), vec![RecordIter::Mem(sstable.into_iter())]);
        let collected: Vec<Record> = merge.collect();
        // sstable records pass through unfiltered, memtable only yields "late"
        assert_eq!(collected.len(), 3);
        let ts: Vec<i64> = collected
            .iter()
            .map(|r| r.extract_timestamp().unwrap())
            .collect();
        assert_eq!(ts, vec![10, 20, 25]);
    }
}
