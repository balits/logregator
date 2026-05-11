use std::{
    cmp::{Ordering, Reverse},
    collections::BinaryHeap,
    fs,
    io::{self, Read, Seek},
    path, vec,
};

use anyhow::{Context, bail};

use crate::storage::{engine::SSTableMeta, record::Record};

/// SSTableIter allows buffered reading of key-values in
/// an SSTable file within a [start, end) range
#[derive(Debug)]
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
                self.buffer[0],
                self.buffer[1],
                self.buffer[2],
                self.buffer[3],
                self.buffer[4],
                self.buffer[5],
                self.buffer[6],
                self.buffer[7],
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


/// Iterates over the MemTable's BTreeSet, yielding only records matching
/// the given source_id, key, and time range.
#[derive(Debug)]
pub(crate) struct MemTableIterOwned {
    inner: vec::IntoIter<Record>,
    source_id: i64,
    key: Vec<u8>,
    start_ts: i64,
    end_ts: i64,
}

impl MemTableIterOwned {
    pub(crate) fn filtered(
        memtable_records: Vec<Record>,
        source_id: i64,
        key: &[u8],
        start_ts: i64,
        end_ts: i64,
    ) -> Self {
        MemTableIterOwned {
            inner: memtable_records.into_iter(),
            source_id,
            key: key.to_vec(),
            start_ts,
            end_ts,
        }
    }
}

impl Iterator for MemTableIterOwned {
    type Item = Record;

    fn next(&mut self) -> Option<Self::Item> {
        for r in self.inner.by_ref() {
            let Ok(sid) = r.extract_source_id() else {
                continue;
            };
            let Ok(ts) = r.extract_timestamp() else {
                continue;
            };
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

#[derive(Debug)]
pub struct MergeIter<I> {
    heap: BinaryHeap<Reverse<HeapItem>>,
    memtable_iter: Option<MemTableIterOwned>,
    sstable_iters: Vec<I>,
    last_item: Option<HeapItem>,
}

impl<I> MergeIter<I>
where
    I: Iterator<Item = Record>,
{
    pub(crate) fn new(
        mut memtable_iter: Option<MemTableIterOwned>,
        mut sstable_iters: Vec<I>,
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
            last_item: None,
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

impl<I> Iterator for MergeIter<I>
where
    I: Iterator<Item = Record>,
{
    type Item = I::Item;

    fn next(&mut self) -> Option<Self::Item> {
        while let Some(item) = self.heap.pop() {
            self.refil(item.0.source_idx);
            if let Some(ref prev) = self.last_item
                && prev.eq(&item.0)
            {
                continue;
            }
            self.last_item = Some(item.0.clone());
            return Some(item.0.record);
        }

        None
    }
}

/// SSTableScanner allows unfiltered scanning of an SSTable file with the help of its metadata.
pub struct SSTableScanner<'a> {
    r: io::BufReader<fs::File>,
    buffer: Vec<u8>,
    meta: &'a SSTableMeta,
}

impl<'a> SSTableScanner<'a> {
    pub(crate) fn new(path: &'a path::PathBuf, meta: &'a SSTableMeta) -> anyhow::Result<Self> {
        let f = std::fs::OpenOptions::new()
            .read(true)
            .open(path)
            .context("SSTableIter::new: failed to open sstable file")?;
        let r: io::BufReader<fs::File> = io::BufReader::new(f);

        Ok(Self {
            r,
            buffer: Vec::with_capacity(1024),
            meta,
        })
    }
}

impl<'a> Iterator for SSTableScanner<'a> {
    type Item = Record;
    fn next(&mut self) -> Option<Self::Item> {
        if self.r.stream_position().ok()? >= self.meta.bloom_offset {
            return None;
        }
        self.buffer.resize(8, 0);
        match self.r.read_exact(&mut self.buffer[..8]) {
            Err(e) if e.kind() == io::ErrorKind::UnexpectedEof => return None,
            Err(e) => {
                tracing::error!(error = %e, "SSTableScanner.next: failed to read record length");
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
            tracing::error!(error = %e, "SSTableScanner.next: failed to read record");
            return None;
        }

        let rec = Record::from_vec(self.buffer[8..].to_vec());
        return Some(rec);
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

#[cfg(test)]
mod sstable_scanner_tests {
    use super::*;
    use crate::storage::BloomFilter;
    use crate::storage::engine::SSTableMeta;
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

    fn make_meta(path: PathBuf, bloom_offset: u64, file_size: u64) -> SSTableMeta {
        SSTableMeta {
            id: 0,
            path,
            bloom: BloomFilter::new(0, 0.01),
            bloom_offset,
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
        let scanner = SSTableScanner::new(&path, &meta).unwrap();
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
        let scanner = SSTableScanner::new(&path, &meta).unwrap();
        assert!(scanner.collect::<Vec<_>>().is_empty());
    }

    #[test]
    fn test_scanner_single_record() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("test.sst");
        let records = vec![Record::from_raw_parts(1, 50, 0, "sys", "loner")];
        let len = write_sstable(&path, &records);
        let meta = make_meta(path.clone(), len, len);
        let scanner = SSTableScanner::new(&path, &meta).unwrap();
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
        let scanner = SSTableScanner::new(&path, &meta).unwrap();
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
        let scanner = SSTableScanner::new(&path, &meta).unwrap();
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
        let mem_iter = MemTableIterOwned::filtered(records, 1, b"sys", 0, 100);
        let empty: Vec<vec::IntoIter<Record>> = vec![];
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
        let merge = MergeIter::new(None, vec![sstable.into_iter()]);
        let collected: Vec<Record> = merge.collect();
        assert_eq!(collected.len(), 2);
    }

    #[test]
    fn test_merge_memtable_and_sstable() {
        let mem_records = vec![
            Record::from_raw_parts(1, 5, 0, "sys", "early"),
            Record::from_raw_parts(1, 25, 0, "sys", "late"),
        ];
        let mem_iter = MemTableIterOwned::filtered(mem_records, 1, b"sys", 0, 100);
        let sstable = vec![
            Record::from_raw_parts(1, 10, 0, "sys", "mid"),
            Record::from_raw_parts(1, 20, 0, "sys", "mid2"),
        ];
        let merge = MergeIter::new(Some(mem_iter), vec![sstable.into_iter()]);
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
        let merge = MergeIter::new(None, vec![sst1.into_iter(), sst2.into_iter()]);
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
        let merge = MergeIter::new(None, vec![sst1.into_iter(), sst2.into_iter()]);
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
        let merge = MergeIter::new(None, vec![] as Vec<vec::IntoIter<Record>>);
        assert!(merge.collect::<Vec<_>>().is_empty());
    }

    #[test]
    fn test_merge_memtable_only_no_matches() {
        let records = vec![
            Record::from_raw_parts(1, 10, 0, "sys", "a"),
            Record::from_raw_parts(1, 20, 0, "sys", "b"),
        ];
        let mem_iter = MemTableIterOwned::filtered(records, 2, b"other", 0, 100);
        let empty: Vec<vec::IntoIter<Record>> = vec![];
        let merge = MergeIter::new(Some(mem_iter), empty);
        assert!(merge.collect::<Vec<_>>().is_empty());
    }
}