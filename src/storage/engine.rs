use std::{
    collections::HashMap, fs::{self}, io::{self, Read, Seek, Write}, path::{Path, PathBuf}, sync::atomic::{AtomicU64, AtomicUsize, Ordering}
};

use anyhow::{Context, bail};
use tracing::trace;

use crate::storage::{BloomFilter, iter::{MemTableIter, MergeIter, SSTableIter}, record::Record};
use crate::storage::{MemTable, Wal};

fn read_sorted_sstables(dir: &PathBuf) -> anyhow::Result<Vec<PathBuf>> {
    let mut sstables: Vec<PathBuf> = std::fs::read_dir(dir)
        .context("failed to read sstable directory contents")?
        .filter_map(Result::ok)
        .map(|e| e.path())
        .filter(|p| p.is_file() && p.extension().and_then(|s| s.to_str()) == Some("sst"))
        .collect();
    sstables.sort();
    Ok(sstables)
}

pub(crate) struct SSTableMeta {
    pub(crate) bloom: BloomFilter,
    pub(crate) bloom_offset: u64,
    pub(crate) file_size: u64,
    pub(crate) num_records: usize,
}

pub struct Engine {
    pub(crate) dir: PathBuf,
    pub(crate) wal: Wal,
    pub(crate) memtable: MemTable,
    pub(crate) sst_counter: AtomicUsize,
    pub(crate) seq_counter: AtomicU64,
    pub(crate) sstable_map: HashMap<PathBuf, SSTableMeta>,
}

impl Engine {
    pub fn open(dir: PathBuf, limit: usize) -> anyhow::Result<Self> {
        if !dir.exists() {
            fs::create_dir_all(&dir).context("engine.open: directory does not exist")?;
        }

        let wal_path = dir.join(Wal::WAL_PATH_FMT);
        let mut memtable = MemTable::new(limit);
        let mut wal = Wal::new(&wal_path).context("engine.open: failed to create wal")?;

        let recovered = wal.recover().context("engine.open: failed recover WAL")?;
        let mut max_seq_num = 0u64;
        let mut needs_flush = false;

        for rec in recovered {
            if let Ok(seq) = rec.extract_seq_num() {
                max_seq_num = max_seq_num.max(seq);
            }
            needs_flush = memtable.insert(rec);
        }

        let sstables = read_sorted_sstables(&dir).context("engine.open: failed to read sstable directory entries")?;
        let mut max_sst_id = 0;
        for path in &sstables {
            if let Some(stem) = path.file_stem().and_then(|s| s.to_str()) {
                if let Ok(id) = stem.parse::<usize>() {
                    max_sst_id = max_sst_id.max(id);
                }
            }
        }

        let mut sstable_map = HashMap::new();

        // scan existing sstables for max seq_num
        for path in &sstables {
            let f = std::fs::OpenOptions::new()
                .read(true)
                .open(path)
                .context("engine.open: failed to open sstable for seq scan")?;

            let mut r = io::BufReader::new(f);
            let mut num_records = 0usize;
            
            loop {
                let mut len_buf = [0u8; 8];
                match r.read_exact(&mut len_buf) {
                    Err(e) if e.kind() == io::ErrorKind::UnexpectedEof => break,
                    Err(e) => return Err(e).context("engine.open: failed to read sstable record length"),
                    Ok(_) => {}
                }
                let rec_len = u64::from_le_bytes(len_buf) as usize;
                let mut rec_buf = vec![0u8; rec_len];
                r.read_exact(&mut rec_buf)
                    .context("engine.open: failed to read sstable record")?;
                if let Ok(seq) = Record::from_vec(rec_buf).extract_seq_num() {
                    max_seq_num = max_seq_num.max(seq);
                    num_records += 1
                }
            }

            let entry = load_bloom(r)?;
            let meta = SSTableMeta {
                bloom: entry.0,
                bloom_offset: entry.1,
                num_records,
                file_size: fs::metadata(path)
                    .context("engine.open: failed to read sstable file metadata")?
                    .len(),
            };
            sstable_map.insert(path.to_owned(), meta);
        }


        let mut e = Self {
            dir,
            wal,
            memtable,
            sst_counter: AtomicUsize::new(max_sst_id + 1),
            seq_counter: AtomicU64::new(max_seq_num + 1),
            sstable_map,
        };

        if needs_flush {
            e.flush().context("engine.open: failed to flush to SSTable after recovering logs")?;
        }

        Ok(e)
    }

    pub fn insert(&mut self, source_id: i64, ts: i64, key: &str, value: &str) -> anyhow::Result<()> {
        let seq = self.seq_counter.fetch_add(1, Ordering::Relaxed);
        let rec = Record::from_raw_parts(source_id, ts, seq, key, value);

        self.wal
            .append(&rec)
            .context("engine.insert: failed to append to WAL")?;
        if self.memtable.insert(rec) {
            self.flush().context("engine.insert: failed to flush to SSTable")?;
        }
        Ok(())
    }

    fn flush(&mut self) -> anyhow::Result<()> {
        let mut path = self.dir.clone();
        path.push(format!(
            "{:010}.sst",
            self.sst_counter.load(Ordering::Relaxed)
        ));

        let f = std::fs::OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(&path)
            .context("engine.flush: failed to open sstable file")?;
        let mut w = io::BufWriter::new(f);
        let mut bloom = BloomFilter::new(self.memtable.len(), 0.01);

        let mut num_records = 0usize;
        for rec in self.memtable.iter() {
            let len_buf = (rec.len() as u64).to_le_bytes();
            w.write_all(&len_buf)
                .context("engine.flush: failed to write record length")?;
            w.write_all(rec.as_bytes())
                .context("engine.flush: failed to write record")?;
            bloom.insert(rec.extract_source_id()?, rec.extract_key()?);
            num_records += 1
        }

        let bloom_offset = w.stream_position()
            .context("engine.flush: failed to seek to current position")?;
        bloom.encode(&mut w)
            .context("engine.flush: failed to write bloom filter footer")?;
        w.write_all(&bloom_offset.to_le_bytes())
            .context("engine.flush: failed to write bloom filter offset")?;

        w.flush().context("engine.flush: failed to flush write buffer")?;
        w.get_ref()
            .sync_all()
            .context("engine.flush: failed to sync_all file")?;

        self.sstable_map.insert(path.clone(), SSTableMeta {
            bloom,
            bloom_offset,
            num_records,
            file_size: fs::metadata(&path)
                .context("engine.flush: failed to read sstable file metadata")?
                .len(),
        });

        self.memtable.clear();
        self.wal
            .clear()
            .context("engine.flush: failed to clear WAL")?;
        self.sst_counter.fetch_add(1, Ordering::Relaxed);

        Ok(())
    }

    pub fn range<'a>(&'a self, source_id: i64, key: &'a str, start_ts: i64, end_ts: i64) -> anyhow::Result<MergeIter<'a, SSTableIter>> {
        if start_ts > end_ts {
            bail!("engine.range: end_ts cannot be smaller than start_ts")
        }
        if start_ts < 0 {
            bail!("engine.range: start_ts cannot be negative")
        }

        let memtable_iter = MemTableIter::filtered(&self.memtable, source_id, key.as_bytes(), start_ts, end_ts);
        let sstable_iters = read_sorted_sstables(&self.dir)
            .context("engine.range: failed to read sstable directory entries")?
            .iter().filter_map(|p| {
                let meta_opt = self.sstable_map.get(p);
                if meta_opt.is_none()  {
                    tracing::warn!(sstable_path = %p.display(), "metadata not found for sstabl");
                    return None
                }
                let meta = self.sstable_map.get(p).unwrap();
                if !meta.bloom.contains(source_id, key.as_bytes()) {
                    return None
                }

                SSTableIter::new(p, source_id, key, start_ts, end_ts, meta.bloom_offset)
                    .inspect_err(|e| {
                        tracing::error!(error = %e, "engine.range: failed to turn sstable path to iterator");
                    })
                    .ok()
            })
            .collect();

        Ok(MergeIter::new(Some(memtable_iter), sstable_iters))
    }
}

fn load_bloom<R: io::Read + io::Seek>(mut w: R) -> anyhow::Result<(BloomFilter, u64)> {
    // footer is 8 bytes yet, in the future when we introduce block indecies it needs to be revisited
    w.seek(io::SeekFrom::End(-8)).context("failed to seek to sstable file end")?;
    let mut bloom_offset_buf = [0u8; 8];
    w.read_exact(&mut bloom_offset_buf).context("failed to read bloom filter offset")?;
    let bloom_offset = u64::from_le_bytes(bloom_offset_buf);

        w.seek(io::SeekFrom::Start(bloom_offset)).context("failed to seek to bloom filter start offset")?;
        let bloom = BloomFilter::decode(&mut w).context("failed to decode bloom filter")?;

        Ok((bloom, bloom_offset))
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::tempdir;

    #[test]
    fn test_engine_initialization() {
        let dir = tempdir().unwrap();
        let engine_dir = dir.path().join("db_data");

        let engine = Engine::open(engine_dir.clone(), 1024).unwrap();

        assert!(
            engine_dir.exists(),
            "engine should create the data directory"
        );
        assert!(
            engine_dir.join(Wal::WAL_PATH_FMT).exists(),
            "engine should create the WAL file"
        );

        assert_eq!(engine.sst_counter.load(Ordering::Relaxed), 1);
    }

    #[test]
    fn test_engine_range_validation() {
        let dir = tempdir().unwrap();
        let engine = Engine::open(dir.path().to_path_buf(), 1024).unwrap();

        assert!(engine.range(1, "sys", 10, 5).is_err());
        assert!(engine.range(1, "sys", -1, 10).is_err());
    }

    #[test]
    fn test_engine_range_no_matches() {
        let dir = tempdir().unwrap();
        let mut engine = Engine::open(dir.path().to_path_buf(), 1024).unwrap();

        engine.insert(1, 10, "sys", "cpu normal").unwrap();

        assert!(engine.range(1, "other", 5, 15).unwrap().collect::<Vec<_>>().is_empty());
        assert!(engine.range(1, "sys", 20, 30).unwrap().collect::<Vec<_>>().is_empty());
        assert!(engine.range(1, "sys", 10, 10).unwrap().collect::<Vec<_>>().is_empty());
    }

    #[test]
    fn test_engine_range_in_memtable_only() {
        let dir = tempdir().unwrap();
        let mut engine = Engine::open(dir.path().to_path_buf(), 1024).unwrap();

        engine.insert(1, 10, "sys", "cpu normal").unwrap();
        engine.insert(1, 20, "sys", "mem high").unwrap();
        engine.insert(1, 30, "sys", "disk full").unwrap();

        let result: Vec<_> = engine.range(1, "sys", 15, 35).unwrap().collect();
        assert_eq!(result.len(), 2);
        assert!(result.iter().any(|r| {
            r.extract_timestamp().unwrap() == 20 && r.extract_value().unwrap() == b"mem high"
        }));
        assert!(result.iter().any(|r| {
            r.extract_timestamp().unwrap() == 30 && r.extract_value().unwrap() == b"disk full"
        }));
    }

    #[test]
    fn test_engine_range_start_inclusive_end_exclusive() {
        let dir = tempdir().unwrap();
        let mut engine = Engine::open(dir.path().to_path_buf(), 1024).unwrap();

        engine.insert(1, 10, "sys", "cpu normal").unwrap();
        engine.insert(1, 20, "sys", "mem high").unwrap();

        let result: Vec<_> = engine.range(1, "sys", 10, 20).unwrap().collect();
        assert_eq!(result.len(), 1);
        assert_eq!(result[0].extract_timestamp().unwrap(), 10);

        let result: Vec<_> = engine.range(1, "sys", 10, 30).unwrap().collect();
        assert_eq!(result.len(), 2);
    }

    #[test]
    fn test_engine_range_key_filtering() {
        let dir = tempdir().unwrap();
        let mut engine = Engine::open(dir.path().to_path_buf(), 50).unwrap();

        engine.insert(1, 10, "sys", "cpu normal").unwrap();
        engine.insert(1, 20, "db", "query slow").unwrap();
        engine.insert(1, 30, "sys", "flush_trigger_01").unwrap();

        engine.insert(1, 40, "db", "connection lost").unwrap();

        let result: Vec<_> = engine.range(1, "sys", 0, 100).unwrap().collect();
        assert_eq!(result.len(), 2);
        assert!(result.iter().all(|r| r.extract_key().unwrap() == b"sys"));

        let result: Vec<_> = engine.range(1, "db", 0, 100).unwrap().collect();
        assert_eq!(result.len(), 2);
        assert!(result.iter().all(|r| r.extract_key().unwrap() == b"db"));
    }

    #[test]
    fn test_engine_range_across_sstables() {
        let dir = tempdir().unwrap();
        let mut engine = Engine::open(dir.path().to_path_buf(), 50).unwrap();

        engine.insert(1, 10, "sys", "cpu normal").unwrap();
        engine.insert(1, 20, "sys", "mem high").unwrap();
        engine.insert(1, 30, "sys", "flush_trigger_01").unwrap();

        engine.insert(1, 40, "sys", "disk full").unwrap();
        engine.insert(1, 50, "sys", "all good").unwrap();

        let result: Vec<_> = engine.range(1, "sys", 20, 45).unwrap().collect();
        assert_eq!(result.len(), 3);
        assert!(result.iter().any(|r| r.extract_timestamp().unwrap() == 20));
        assert!(result.iter().any(|r| r.extract_timestamp().unwrap() == 30));
        assert!(result.iter().any(|r| r.extract_timestamp().unwrap() == 40));
    }

    #[test]
    fn test_engine_range_overlapping_sstables() {
        let dir = tempdir().unwrap();
        let mut engine = Engine::open(dir.path().to_path_buf(), 50).unwrap();

        engine.insert(1, 15, "sys", "first flush").unwrap();
        engine.insert(1, 25, "sys", "flush_trigger_01").unwrap();

        engine.insert(1, 20, "sys", "second flush entry").unwrap();
        engine.insert(1, 45, "sys", "flush_trigger_02").unwrap();

        let result: Vec<_> = engine.range(1, "sys", 20, 30).unwrap().collect();
        assert_eq!(result.len(), 2);
        let mut ts: Vec<i64> = result.iter().map(|r| r.extract_timestamp().unwrap()).collect();
        ts.sort();
        assert_eq!(ts, vec![20, 25]);
    }

    #[test]
    fn test_engine_range_memtable_and_sstables() {
        let dir = tempdir().unwrap();
        let mut engine = Engine::open(dir.path().to_path_buf(), 50).unwrap();

        engine.insert(1, 10, "sys", "flushed").unwrap();
        engine.insert(1, 20, "sys", "flush_trigger_01").unwrap();

        engine.insert(1, 30, "sys", "in memtable").unwrap();

        let result: Vec<_> = engine.range(1, "sys", 5, 35).unwrap().collect();
        assert_eq!(result.len(), 3);
        assert!(result.iter().any(|r| {
            r.extract_timestamp().unwrap() == 10 && r.extract_value().unwrap() == b"flushed"
        }));
        assert!(result.iter().any(|r| r.extract_timestamp().unwrap() == 20));
        assert!(result.iter().any(|r| {
            r.extract_timestamp().unwrap() == 30 && r.extract_value().unwrap() == b"in memtable"
        }));
    }

    #[test]
    fn test_engine_flush_trigger() {
        let dir = tempdir().unwrap();
        let mut engine = Engine::open(dir.path().to_path_buf(), 50).unwrap();

        let sst_file_path = dir.path().join("0000000001.sst");

        engine.insert(1, 1, "k1", "small").unwrap();
        assert!(!sst_file_path.exists());

        engine
            .insert(1, 2, "k2", "this_is_a_massive_payload_to_force_a_flush")
            .unwrap();

        assert!(
            sst_file_path.exists(),
            "Engine should have created 0000000001.sst"
        );

        assert_eq!(engine.memtable.size_hint(), 0);

        let wal_metadata = std::fs::metadata(dir.path().join(Wal::WAL_PATH_FMT)).unwrap();
        assert_eq!(wal_metadata.len(), 0);

        assert_eq!(engine.sst_counter.load(Ordering::Relaxed), 2);
    }

}
