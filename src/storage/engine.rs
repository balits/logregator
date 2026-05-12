use std::{
    collections::BTreeMap,
    fs::{self},
    io::{self, Read, Seek, SeekFrom, Write},
    path::PathBuf,
};

use anyhow::{Context, bail};
use tokio::sync::mpsc;

use crate::storage::{MemTable, Wal};
use crate::{
    proto,
    storage::{
        BloomFilter,
        compaction::{self, CompactionCommand, CompactionResult},
        iter::{MemTableIterOwned, MergeIter, SSTableIter},
        record::Record,
    },
};

#[derive(Debug, Clone)]
pub(crate) struct SSTableMeta {
    pub(crate) id: usize,
    pub(crate) path: PathBuf,
    pub(crate) bloom: BloomFilter,
    pub(crate) bloom_offset: u64,
    pub(crate) file_size: u64,
    pub(crate) num_records: usize,
}

pub struct Engine {
    pub(crate) dir: PathBuf,
    pub(crate) wal: Wal,
    pub(crate) compaction_tx: mpsc::Sender<CompactionCommand>,
    pub(crate) compaction_rx: mpsc::Receiver<CompactionResult>,

    pub(crate) memtable: MemTable,
    pub(crate) sstable_map: BTreeMap<usize, SSTableMeta>,
    pub(crate) sst_counter: usize,
    pub(crate) seq_counter: u64,
}

impl Engine {
    pub async fn run_main_loop(
        &mut self,
        mut network_rx: mpsc::Receiver<proto::Command>,
    ) -> anyhow::Result<()> {
        loop {
            tokio::select! {
                Some(cmd) = network_rx.recv() => {
                    match cmd {
                        proto::Command::Insert(i, sender) => {
                            let res = self.insert(i.source_id, i.ts, &i.key, &i.value);
                            if sender.send(res).is_err() {
                                tracing::warn!("engine.loop(cmd=INSERT): failed to send result through the channel");
                            }
                            tracing::debug!("engine.loop(cmd=INSERT): replyed successfuly");
                        }
                        proto::Command::BatchInsert(b, sender) => {
                            let res = self.batch_insert(b.records);
                            if sender.send(res).is_err() {
                                tracing::warn!("engine.loop(cmd=BATCH_INSERT): failed to send result through the channel");
                            }
                            tracing::debug!("engine.loop(cmd=BATCH_INSERT): replyed successfuly");
                        }
                        proto::Command::Range(r, end_sender, record_sender) => {
                            let merge_iter = self.range(r.source_id, &r.key, r.start_ts, r.end_ts, &r.filter);
                            tokio::task::spawn_blocking(move || {
                                match merge_iter {
                                    Ok(iter) => {
                                        let mut loop_err: anyhow::Result<()> = Ok(());
                                        for raw_rec in iter {
                                            match proto::Record::try_from(raw_rec) {
                                                Ok(rec) => {
                                                    if let Err(err) = record_sender.blocking_send(rec) {
                                                        tracing::warn!(error = %err, "engine.loop(cmd=RANGE): failed to send record");
                                                        loop_err = Err(anyhow::Error::new(err));
                                                        break;
                                                    }
                                                }
                                                Err(err) => {
                                                    tracing::warn!(error = %err, "engine.loop(cmd=RANGE): failed parse record bytes");
                                                    loop_err = Err(err);
                                                    break;
                                                }
                                            }
                                        }

                                        if let Err(err) = loop_err {
                                            if end_sender.send(Err(err)).is_err() {
                                                tracing::warn!("engine.loop(cmd=RANGE): failed to send end=Error");
                                            }
                                        } else {
                                            if end_sender.send(Ok(())).is_err() {
                                                tracing::warn!("engine.loop(cmd=RANGE): failed to send end=RangeEnd");
                                            }
                                        }
                                        drop(record_sender);
                                    },
                                    Err(err) => {
                                        if end_sender.send(Err(err)).is_err() {
                                            tracing::warn!("engine.loop(cmd=RANGE): failed to send end=Error");
                                        }
                                    }
                                }
                            });
                        }
                    }
                },

                Some(CompactionResult { new_meta, ids_to_remove }) = self.compaction_rx.recv() => {
                    for file_id in ids_to_remove {
                        self.sstable_map.remove(&file_id);
                        let file_path = make_sstable_path(&self.dir, file_id);
                        if let Err(e) = fs::remove_file(&file_path) {
                            tracing::error!(error = %e, "engine.loop: compaction finished, but failed to remove file {}", file_path.display())
                        }
                    }
                    self.sstable_map.insert(new_meta.id, new_meta);
                    tracing::info!("engine.loop: compaction finished, applying result")
                }
            }
        }
    }

    pub fn open(
        dir: PathBuf,
        memtable_limit: usize,
        compaction_tx: mpsc::Sender<CompactionCommand>,
        compaction_rx: mpsc::Receiver<CompactionResult>,
    ) -> anyhow::Result<Self> {
        if !dir.exists() {
            fs::create_dir_all(&dir).context("engine.open: directory does not exist")?;
        }

        let wal_path = dir.join(Wal::WAL_PATH_FMT);
        let mut memtable = MemTable::new(memtable_limit);
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

        let mut sstables: Vec<PathBuf> = std::fs::read_dir(&dir)
            .context("failed to read sstable directory contents")?
            .filter_map(Result::ok)
            .map(|e| e.path())
            .filter(|p| p.is_file() && p.extension().and_then(|s| s.to_str()) == Some("sst"))
            .collect();
        sstables.sort();
        let mut max_sst_id = 0;

        let mut sstable_map = BTreeMap::new();

        // scan existing sstables for max seq_num
        for path in &sstables {
            let file_id = path
                .file_stem()
                .and_then(|stem| stem.to_str())
                .context("engine.open: failed to get valid UTF-8 file stem")?
                .parse::<usize>()
                .context("engine.open: failed to parse sstable id from file stem")?;
            max_sst_id = max_sst_id.max(file_id);

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
                    Err(e) => {
                        return Err(e).context("engine.open: failed to read sstable record length");
                    }
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
                id: file_id,
                path: path.clone(),
                bloom: entry.0,
                bloom_offset: entry.1,
                num_records,
                file_size: fs::metadata(path)
                    .context("engine.open: failed to read sstable file metadata")?
                    .len(),
            };
            sstable_map.insert(file_id, meta);
        }

        let mut e = Self {
            dir,
            wal,
            memtable,
            sstable_map,
            sst_counter: max_sst_id + 1,
            seq_counter: max_seq_num + 1,
            compaction_rx,
            compaction_tx,
        };

        if needs_flush {
            e.flush()
                .context("engine.open: failed to flush to SSTable after recovering logs")?;
        }

        Ok(e)
    }

    pub fn insert(
        &mut self,
        source_id: i64,
        ts: i64,
        key: &str,
        value: &str,
    ) -> anyhow::Result<()> {
        let current_seq = self.seq_counter;
        let rec = Record::from_raw_parts(source_id, ts, current_seq, key, value);
        self.wal
            .append(&rec)
            .context("engine.insert: failed to append to WAL")?;

        let needs_flush = self.memtable.insert(rec);
        self.seq_counter += 1;

        if needs_flush {
            self.flush()
                .context("engine.insert: failed to flush to SSTable")?;
        }
        Ok(())
    }

    pub fn batch_insert(&mut self, inserts: Vec<proto::Insert>) -> anyhow::Result<()> {
        let count = inserts.len();
        let mut records: Vec<Record> = Vec::with_capacity(count);

        for ins in &inserts {
            records.push(Record::from_raw_parts(
                ins.source_id,
                ins.ts,
                self.seq_counter,
                &ins.key,
                &ins.value,
            ));
            self.seq_counter += 1;
        }

        self.wal
            .batch_append(&records)
            .context("engine.batch_insert: failed to append to WAL")?;

        let mut needs_flush = false;
        for rec in records {
            if self.memtable.insert(rec) {
                needs_flush = true;
            }
        }

        if needs_flush {
            self.flush()
                .context("engine.batch_insert: failed to flush to SSTable")?;
        }

        tracing::debug!(count, "engine.batch_insert: inserted batch");
        Ok(())
    }

    fn flush(&mut self) -> anyhow::Result<()> {
        let file_id = self.sst_counter;
        let memtable_records: Vec<Record> = self.memtable.iter().cloned().collect();
        self.memtable.clear();

        if memtable_records.is_empty() {
            return Ok(());
        }

        let path = make_sstable_path(&self.dir, file_id);

        let f = std::fs::OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(&path)
            .context("engine.flush: failed to open sstable file")?;
        let mut w = io::BufWriter::new(f);
        let mut bloom = BloomFilter::new(memtable_records.len(), 0.01);

        let mut num_records = 0usize;
        for rec in memtable_records.iter() {
            let len_buf = (rec.len() as u64).to_le_bytes();
            w.write_all(&len_buf)
                .context("engine.flush: failed to write record length")?;
            w.write_all(rec.as_bytes())
                .context("engine.flush: failed to write record")?;
            bloom.insert(rec.extract_source_id()?, rec.extract_key()?);
            num_records += 1
        }

        let bloom_offset = w
            .stream_position()
            .context("engine.flush: failed to seek to current position")?;
        bloom
            .encode(&mut w)
            .context("engine.flush: failed to write bloom filter footer")?;
        w.write_all(&bloom_offset.to_le_bytes())
            .context("engine.flush: failed to write bloom filter offset")?;

        w.flush()
            .context("engine.flush: failed to flush write buffer")?;
        w.get_ref()
            .sync_all()
            .context("engine.flush: failed to sync_all file")?;

        let file_size = fs::metadata(&path)
            .context("engine.flush: failed to read sstable file metadata")?
            .len();

        let new_meta = SSTableMeta {
            id: file_id,
            path,
            bloom,
            bloom_offset,
            num_records,
            file_size,
        };
        self.sstable_map.insert(file_id, new_meta);
        self.sst_counter += 1;
        let need_compaction = self.sstable_map.len() >= 8;

        self.wal
            .clear()
            .context("engine.flush: failed to clear WAL")?;

        if need_compaction {
            let tables: Vec<SSTableMeta> = self
                .sstable_map
                .iter()
                .take(4)
                .map(|(_, m)| m.clone())
                .collect();

            let new_file_id = self.sst_counter;
            let new_file_path = self.dir.join(format!("{:010}.sst", new_file_id));
            let cmd = compaction::CompactionCommand {
                new_file_id,
                new_file_path,
                tables,
            };

            if let Err(e) = self.compaction_tx.try_send(cmd) {
                tracing::warn!(error = %e, "failed to issue compaction, compactor queue is full")
            }
        }

        Ok(())
    }

    pub fn range(
        &self,
        source_id: i64,
        key: &str,
        start_ts: i64,
        end_ts: i64,
        filter: &str,
    ) -> anyhow::Result<MergeIter<SSTableIter>> {
        if start_ts > end_ts {
            bail!("engine.range: end_ts cannot be smaller than start_ts")
        }
        if start_ts < 0 {
            bail!("engine.range: start_ts cannot be negative")
        }

        let memtable_records: Vec<Record> = self.memtable.iter().cloned().collect();

        let memtable_iter = MemTableIterOwned::filtered(
            memtable_records,
            source_id,
            key.as_bytes(),
            start_ts,
            end_ts,
            filter,
        );
        let sstable_iters = self.sstable_map
            .values()
            .filter_map(|meta| {
                if !meta.bloom.contains(source_id, key.as_bytes()) {
                    return None
                }

                SSTableIter::new(&meta.path, source_id, key, start_ts, end_ts, meta.bloom_offset, filter)
                    .inspect_err(|e| {
                        tracing::error!(error = %e, "engine.range: failed to turn sstable path to iterator");
                    })
                    .ok()
            })
            .collect();

        Ok(MergeIter::new(Some(memtable_iter), sstable_iters))
    }
}

fn make_sstable_path(basedir: &PathBuf, file_id: usize) -> PathBuf {
    basedir.join(format!("{:010}.sst", file_id))
}

fn load_bloom<R: Read + Seek>(mut w: R) -> anyhow::Result<(BloomFilter, u64)> {
    // footer is 8 bytes yet, in the future when we introduce block indecies it needs to be revisited
    w.seek(SeekFrom::End(-8))
        .context("failed to seek to sstable file end")?;
    let mut bloom_offset_buf = [0u8; 8];
    w.read_exact(&mut bloom_offset_buf)
        .context("failed to read bloom filter offset")?;
    let bloom_offset = u64::from_le_bytes(bloom_offset_buf);

    w.seek(io::SeekFrom::Start(bloom_offset))
        .context("failed to seek to bloom filter start offset")?;
    let bloom = BloomFilter::decode(&mut w).context("failed to decode bloom filter")?;
    Ok((bloom, bloom_offset))
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::tempdir;

    fn open_engine(dir: PathBuf, limit: usize) -> Engine {
        let (cmd_tx, _cmd_rx) = tokio::sync::mpsc::channel::<CompactionCommand>(1);
        let (_res_tx, res_rx) = tokio::sync::mpsc::channel::<CompactionResult>(1);
        Engine::open(dir, limit, cmd_tx, res_rx).unwrap()
    }

    #[test]
    fn test_engine_initialization() {
        let dir = tempdir().unwrap();
        let engine_dir = dir.path().join("db_data");

        let engine = open_engine(engine_dir.clone(), 1024);

        assert!(
            engine_dir.exists(),
            "engine should create the data directory"
        );
        assert!(
            engine_dir.join(Wal::WAL_PATH_FMT).exists(),
            "engine should create the WAL file"
        );

        assert_eq!(engine.sst_counter, 1);
    }

    #[test]
    fn test_engine_range_validation() {
        let dir = tempdir().unwrap();
        let engine = open_engine(dir.path().to_path_buf(), 1024);

        assert!(engine.range(1, "sys", 10, 5, "").is_err());
        assert!(engine.range(1, "sys", -1, 10, "").is_err());
    }

    #[test]
    fn test_engine_range_no_matches() {
        let dir = tempdir().unwrap();
        let mut engine = open_engine(dir.path().to_path_buf(), 1024);

        engine.insert(1, 10, "sys", "cpu normal").unwrap();

        assert!(
            engine
                .range(1, "other", 5, 15, "")
                .unwrap()
                .collect::<Vec<_>>()
                .is_empty()
        );
        assert!(
            engine
                .range(1, "sys", 20, 30, "")
                .unwrap()
                .collect::<Vec<_>>()
                .is_empty()
        );
        assert!(
            engine
                .range(1, "sys", 10, 10, "")
                .unwrap()
                .collect::<Vec<_>>()
                .is_empty()
        );
    }

    #[test]
    fn test_engine_range_in_memtable_only() {
        let dir = tempdir().unwrap();
        let mut engine = open_engine(dir.path().to_path_buf(), 1024);

        engine.insert(1, 10, "sys", "cpu normal").unwrap();
        engine.insert(1, 20, "sys", "mem high").unwrap();
        engine.insert(1, 30, "sys", "disk full").unwrap();

        let result: Vec<_> = engine.range(1, "sys", 15, 35, "").unwrap().collect();
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
        let mut engine = open_engine(dir.path().to_path_buf(), 1024);

        engine.insert(1, 10, "sys", "cpu normal").unwrap();
        engine.insert(1, 20, "sys", "mem high").unwrap();

        let result: Vec<_> = engine.range(1, "sys", 10, 20, "").unwrap().collect();
        assert_eq!(result.len(), 1);
        assert_eq!(result[0].extract_timestamp().unwrap(), 10);

        let result: Vec<_> = engine.range(1, "sys", 10, 30, "").unwrap().collect();
        assert_eq!(result.len(), 2);
    }

    #[test]
    fn test_engine_range_key_filtering() {
        let dir = tempdir().unwrap();
        let mut engine = open_engine(dir.path().to_path_buf(), 50);

        engine.insert(1, 10, "sys", "cpu normal").unwrap();
        engine.insert(1, 20, "db", "query slow").unwrap();
        engine.insert(1, 30, "sys", "flush_trigger_01").unwrap();

        engine.insert(1, 40, "db", "connection lost").unwrap();

        let result: Vec<_> = engine.range(1, "sys", 0, 100, "").unwrap().collect();
        assert_eq!(result.len(), 2);
        assert!(result.iter().all(|r| r.extract_key().unwrap() == b"sys"));

        let result: Vec<_> = engine.range(1, "db", 0, 100, "").unwrap().collect();
        assert_eq!(result.len(), 2);
        assert!(result.iter().all(|r| r.extract_key().unwrap() == b"db"));
    }

    #[test]
    fn test_engine_range_across_sstables() {
        let dir = tempdir().unwrap();
        let mut engine = open_engine(dir.path().to_path_buf(), 50);

        engine.insert(1, 10, "sys", "cpu normal").unwrap();
        engine.insert(1, 20, "sys", "mem high").unwrap();
        engine.insert(1, 30, "sys", "flush_trigger_01").unwrap();

        engine.insert(1, 40, "sys", "disk full").unwrap();
        engine.insert(1, 50, "sys", "all good").unwrap();

        let result: Vec<_> = engine.range(1, "sys", 20, 45, "").unwrap().collect();
        assert_eq!(result.len(), 3);
        assert!(result.iter().any(|r| r.extract_timestamp().unwrap() == 20));
        assert!(result.iter().any(|r| r.extract_timestamp().unwrap() == 30));
        assert!(result.iter().any(|r| r.extract_timestamp().unwrap() == 40));
    }

    #[test]
    fn test_engine_range_overlapping_sstables() {
        let dir = tempdir().unwrap();
        let mut engine = open_engine(dir.path().to_path_buf(), 50);

        engine.insert(1, 15, "sys", "first flush").unwrap();
        engine.insert(1, 25, "sys", "flush_trigger_01").unwrap();

        engine.insert(1, 20, "sys", "second flush entry").unwrap();
        engine.insert(1, 45, "sys", "flush_trigger_02").unwrap();

        let result: Vec<_> = engine.range(1, "sys", 20, 30, "").unwrap().collect();
        assert_eq!(result.len(), 2);
        let mut ts: Vec<i64> = result
            .iter()
            .map(|r| r.extract_timestamp().unwrap())
            .collect();
        ts.sort();
        assert_eq!(ts, vec![20, 25]);
    }

    #[test]
    fn test_engine_range_memtable_and_sstables() {
        let dir = tempdir().unwrap();
        let mut engine = open_engine(dir.path().to_path_buf(), 50);

        engine.insert(1, 10, "sys", "flushed").unwrap();
        engine.insert(1, 20, "sys", "flush_trigger_01").unwrap();

        engine.insert(1, 30, "sys", "in memtable").unwrap();

        let result: Vec<_> = engine.range(1, "sys", 5, 35, "").unwrap().collect();
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
        let mut engine = open_engine(dir.path().to_path_buf(), 50);

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

        assert_eq!(engine.sst_counter, 2);
    }

    fn make_insert(source_id: i64, ts: i64, key: &str, value: &str) -> proto::Insert {
        proto::Insert {
            source_id,
            ts,
            key: key.to_string(),
            value: value.to_string(),
        }
    }

    #[test]
    fn test_engine_batch_insert_basic() {
        let dir = tempdir().unwrap();
        let mut engine = open_engine(dir.path().to_path_buf(), 1024);

        let batch = vec![
            make_insert(1, 10, "sys", "cpu normal"),
            make_insert(1, 20, "sys", "mem high"),
            make_insert(1, 30, "sys", "disk full"),
        ];

        engine.batch_insert(batch).unwrap();

        let result: Vec<_> = engine.range(1, "sys", 0, 100, "").unwrap().collect();
        assert_eq!(result.len(), 3);
        assert_eq!(result[0].extract_timestamp().unwrap(), 10);
        assert_eq!(result[1].extract_timestamp().unwrap(), 20);
        assert_eq!(result[2].extract_timestamp().unwrap(), 30);
    }

    #[test]
    fn test_engine_batch_insert_seq_nums() {
        let dir = tempdir().unwrap();
        let mut engine = open_engine(dir.path().to_path_buf(), 1024);

        // seq_counter starts at 1 after open
        let start_seq = engine.seq_counter;

        let batch = vec![
            make_insert(1, 10, "sys", "a"),
            make_insert(1, 20, "sys", "b"),
            make_insert(1, 30, "sys", "c"),
        ];

        engine.batch_insert(batch).unwrap();

        assert_eq!(engine.seq_counter, start_seq + 3);

        let result: Vec<_> = engine.range(1, "sys", 0, 100, "").unwrap().collect();
        assert_eq!(result.len(), 3);
        for (i, rec) in result.iter().enumerate() {
            assert_eq!(rec.extract_seq_num().unwrap(), start_seq + i as u64);
        }
    }

    #[test]
    fn test_engine_batch_insert_flush_trigger() {
        let dir = tempdir().unwrap();
        let mut engine = open_engine(dir.path().to_path_buf(), 100);

        let batch = vec![
            make_insert(1, 1, "sys", "x"),
            make_insert(1, 2, "sys", "this_is_a_massive_payload_to_force_a_flush"),
        ];

        engine.batch_insert(batch).unwrap();

        // should have flushed to SSTable
        let sst_file_path = dir.path().join("0000000001.sst");
        assert!(sst_file_path.exists(), "batch insert should trigger flush");

        assert_eq!(engine.memtable.size_hint(), 0);

        let wal_metadata = std::fs::metadata(dir.path().join(Wal::WAL_PATH_FMT)).unwrap();
        assert_eq!(wal_metadata.len(), 0);
    }

    #[test]
    fn test_engine_batch_insert_with_range() {
        let dir = tempdir().unwrap();
        let mut engine = open_engine(dir.path().to_path_buf(), 50);

        let batch = vec![
            make_insert(1, 5, "sys", "first"),
            make_insert(1, 10, "sys", "second"),
        ];
        engine.batch_insert(batch).unwrap();

        // standalone insert after batch
        engine.insert(1, 15, "sys", "third").unwrap();

        let result: Vec<_> = engine.range(1, "sys", 0, 20, "").unwrap().collect();
        assert_eq!(result.len(), 3);
        assert_eq!(result[0].extract_timestamp().unwrap(), 5);
        assert_eq!(result[1].extract_timestamp().unwrap(), 10);
        assert_eq!(result[2].extract_timestamp().unwrap(), 15);
    }

    #[test]
    fn test_engine_batch_insert_multiple_batches() {
        let dir = tempdir().unwrap();
        let mut engine = open_engine(dir.path().to_path_buf(), 1024);

        let batch1 = vec![
            make_insert(1, 10, "sys", "batch1_a"),
            make_insert(1, 20, "sys", "batch1_b"),
        ];
        engine.batch_insert(batch1).unwrap();

        let batch2 = vec![
            make_insert(1, 30, "sys", "batch2_a"),
            make_insert(1, 40, "sys", "batch2_b"),
        ];
        engine.batch_insert(batch2).unwrap();

        let result: Vec<_> = engine.range(1, "sys", 0, 100, "").unwrap().collect();
        assert_eq!(result.len(), 4);
        assert!(result.iter().any(|r| r.extract_value().unwrap() == b"batch1_a"));
        assert!(result.iter().any(|r| r.extract_value().unwrap() == b"batch2_b"));
    }

    #[test]
    fn test_engine_range_filter_empty_returns_all() {
        let dir = tempdir().unwrap();
        let mut engine = open_engine(dir.path().to_path_buf(), 1024);

        engine.insert(1, 10, "sys", "cpu normal").unwrap();
        engine.insert(1, 20, "sys", "mem high").unwrap();
        engine.insert(1, 30, "sys", "disk full").unwrap();

        let result: Vec<_> = engine.range(1, "sys", 0, 100, "").unwrap().collect();
        assert_eq!(result.len(), 3);
    }

    #[test]
    fn test_engine_range_filter_partial_match() {
        let dir = tempdir().unwrap();
        let mut engine = open_engine(dir.path().to_path_buf(), 1024);

        engine.insert(1, 10, "sys", "cpu normal").unwrap();
        engine.insert(1, 20, "sys", "mem high pressure").unwrap();
        engine.insert(1, 30, "sys", "disk full").unwrap();
        engine.insert(1, 40, "sys", "memory leak detected").unwrap();

        let result: Vec<_> = engine.range(1, "sys", 0, 100, "mem").unwrap().collect();
        assert_eq!(result.len(), 2);
        assert!(result.iter().all(|r| {
            let val = r.extract_value().unwrap();
            val == b"mem high pressure" || val == b"memory leak detected"
        }));
    }

    #[test]
    fn test_engine_range_filter_no_match() {
        let dir = tempdir().unwrap();
        let mut engine = open_engine(dir.path().to_path_buf(), 1024);

        engine.insert(1, 10, "sys", "cpu normal").unwrap();
        engine.insert(1, 20, "sys", "mem high").unwrap();

        let result: Vec<_> = engine.range(1, "sys", 0, 100, "nonexistent").unwrap().collect();
        assert!(result.is_empty());
    }

    #[test]
    fn test_engine_range_filter_works_across_tiers() {
        let dir = tempdir().unwrap();
        let mut engine = open_engine(dir.path().to_path_buf(), 50);

        engine.insert(1, 10, "sys", "cpu normal").unwrap();
        engine.insert(1, 20, "sys", "first flush trigger").unwrap();
        engine.insert(1, 30, "sys", "disk full").unwrap();

        // all records flushed to SSTables, empty filter yields all
        let result: Vec<_> = engine.range(1, "sys", 0, 100, "").unwrap().collect();
        assert_eq!(result.len(), 3);

        // filter matches only one SSTable record
        let result: Vec<_> = engine.range(1, "sys", 0, 100, "cpu").unwrap().collect();
        assert_eq!(result.len(), 1);

        // filter matches no records
        let result: Vec<_> = engine.range(1, "sys", 0, 100, "nonexistent").unwrap().collect();
        assert!(result.is_empty());
    }
}
