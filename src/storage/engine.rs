use std::{
    collections::{BTreeMap, BTreeSet},
    fs::{self},
    io::{self, Read, Seek, SeekFrom},
    path::{Path, PathBuf},
    sync::Arc
};

use anyhow::{Context, bail};
use tokio::sync::mpsc::UnboundedSender;
use tracing::instrument;

use crate::storage::{
    BloomFilter, IndexBlock, SSTableMeta,
    iter::{MergeIter, RecordIter, SSTableIterFiltered},
    memtable::IntoIterFiltered,
    record::Record,
};
use crate::storage::{MemTable, Wal};
use crate::{metrics::Metrics, proto};

pub struct Engine {
    /// path of the base storage directory
    base_dir: PathBuf,

    /// Write ahead log file for durability. Using group commit,
    /// after N commands an fsync message is passed to the backgroud
    /// IoWorker, which syncs the file asynchronously, after the client
    /// got the result.
    wal: Wal,

    /// Main memtable used to hold records. When full, its moved
    /// to a frozen memtable and cleared
    memtable: MemTable,

    /// A map of frozen, inmutable memtables who are waiting to be flushed in the background.
    /// They are still readable until the flush completes and are removed from the map.
    /// They are shared with the IoWorker which does the actual flushing:
    /// both operations are read-only, so an Arc without mutexes is fine.
    frozen_memtables: BTreeMap<u64, Arc<MemTable>>,

    /// ordered collection of SSTableMetas by their file_ids
    sstable_map: BTreeMap<u64, SSTableMeta>,

    /// set of SSTable IDs currently being compacted (in-flight).
    /// Prevents duplicate compaction commands for the same files.
    compacting: BTreeSet<u64>,

    /// counter of sstable file ids
    sst_counter: u64,
    /// counter of log record sequnce numbers
    seq_counter: u64,
    /// optional channel to send information about
    /// engine internals (needs refinement)
    stats_tx: Option<UnboundedSender<(usize, usize)>>,

    /// memtable capacity in bytes, used as a compaction trigger threshold
    memtable_limit_bytes: usize,
}

impl Engine {
    #[instrument(fields(base_dir = %base_dir.display()))]
    pub fn open(base_dir: PathBuf, memtable_limit: usize) -> anyhow::Result<(Self, bool)> {
        if !base_dir.exists() {
            fs::create_dir_all(&base_dir).context("engine.open: failed to create base directory")?;
        }
        fs::create_dir_all(base_dir.join(Wal::WAL_DIR)).context("engine.open: failed to create WAL directory")?;

        let wal = Wal::open_active(&base_dir).context("engine.open: failed to create wal")?;

        let mut max_seq_num = 0u64;
        let mut needs_flush = false;
        let mut memtable = MemTable::new(memtable_limit);

        let recovered =
            Wal::recover_all(&base_dir).context("engine.open: failed to recover all WAL files")?;
        for rec in recovered {
            if let Ok(seq) = rec.extract_seq_num() {
                max_seq_num = max_seq_num.max(seq);
            }
            needs_flush = memtable.insert(rec);
        }

        let mut sstables: Vec<PathBuf> = std::fs::read_dir(&base_dir)
            .context("engine.open: failed to read sstable directory contents")?
            .filter_map(Result::ok)
            .map(|e| e.path())
            .filter(|p| p.is_file() && p.extension().and_then(|s| s.to_str()) == Some("sst"))
            .collect();
        sstables.sort();
        let mut max_sst_id = 0;

        let mut sstable_map = BTreeMap::new();

        // scan existing sstables for max seq_num
        for path in sstables.into_iter() {
            let file_id = path
                .file_stem()
                .and_then(|stem| stem.to_str())
                .context("engine.open: failed to get valid UTF-8 file stem")?
                .parse::<u64>()
                .context("engine.open: failed to parse sstable id from file stem")?;
            max_sst_id = max_sst_id.max(file_id);

            let f = std::fs::OpenOptions::new()
                .read(true)
                .open(&path)
                .with_context(|| {
                    format!(
                        "engine.open: failed to open {} for seq scan",
                        path.as_path().display()
                    )
                })?;

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

            r.seek(SeekFrom::End(-16))
                .context("engine.open: failed to seek to sstable footer")?;
            let (index, index_offset) = IndexBlock::read_from_unchecked(&mut r)
                .context("engine.open: failed to read index block from sstable")?;
            let bloom_offset = r
                .stream_position()
                .context("engine.open: failed to get bloom filter offset")?;
            let bloom = BloomFilter::decode(&mut r)
                .context("engine.open: failed to decode bloom filter")?;

            let meta = SSTableMeta {
                id: file_id,
                path: path.clone(),
                bloom,
                bloom_offset,
                index,
                index_offset,
                num_records,
                file_size: fs::metadata(path)
                    .context("engine.open: failed to read sstable file metadata")?
                    .len(),
            };
            sstable_map.insert(file_id, meta);
        }

        let e = Self {
            base_dir,
            wal,
            memtable,
            frozen_memtables: BTreeMap::new(),
            sstable_map,
            compacting: BTreeSet::new(),
            sst_counter: max_sst_id + 1,
            seq_counter: max_seq_num + 1,
            stats_tx: None,
            memtable_limit_bytes: memtable_limit,
        };

        Ok((e, needs_flush))
    }

    #[instrument(skip(self, value), fields(source_id, ts, key))]
    pub fn insert_record(
        &mut self,
        source_id: i64,
        ts: i64,
        key: &str,
        value: &str,
    ) -> anyhow::Result<bool> {
        let current_seq = self.seq_counter;
        let rec = Record::from_raw_parts(source_id, ts, current_seq, key, value);
        self.wal
            .write_one(&rec)
            .context("engine.insert: failed to write to WAL")?;

        let needs_flush = self.memtable.insert(rec);
        self.seq_counter += 1;

        Ok(needs_flush)
    }

    #[instrument(skip(self, inserts))]
    pub fn batch_insert_records(&mut self, inserts: Vec<proto::Insert>) -> anyhow::Result<bool> {
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
            .write_many(&records)
            .context("engine.batch_insert: failed to write to WAL")?;

        let mut needs_flush = false;
        for rec in records {
            if self.memtable.insert(rec) {
                needs_flush = true;
            }
        }
        Ok(needs_flush)
    }

    #[instrument(skip(self, filter), fields(source_id, key, start_ts, end_ts))]
    pub fn range(
        &self,
        source_id: i64,
        key: &str,
        start_ts: i64,
        end_ts: i64,
        filter: &str,
    ) -> anyhow::Result<MergeIter> {
        if start_ts > end_ts {
            bail!("engine.range: end_ts cannot be smaller than start_ts")
        }
        if start_ts < 0 {
            bail!("engine.range: start_ts cannot be negative")
        }

        let memtable_iter = if self.frozen_memtables.is_empty() {
            IntoIterFiltered::new(
                self.memtable.clone_range(source_id, start_ts, end_ts),
                source_id,
                key.as_bytes(),
                start_ts,
                end_ts,
                filter,
            )
        } else {
            let mut all_mem_records = BTreeSet::new();
            for rec in self.memtable.clone_range(source_id, start_ts, end_ts) {
                all_mem_records.insert(rec);
            }
            for (_, frozen) in self.frozen_memtables.iter() {
                for rec in frozen.clone_range(source_id, start_ts, end_ts) {
                    all_mem_records.insert(rec);
                }
            }
            IntoIterFiltered::new(
                all_mem_records.into_iter().collect(),
                source_id,
                key.as_bytes(),
                start_ts,
                end_ts,
                filter,
            )
        };
        let sstable_iters: Vec<RecordIter> = self.sstable_map
            .values()
            .filter_map(|meta| {
                if !meta.bloom.contains(source_id, key.as_bytes()) {
                    return None
                }

                let iter = SSTableIterFiltered::new(meta, source_id, key, start_ts, end_ts, filter)
                    .inspect_err(|e| {
                        tracing::error!(error = %e, "engine.range: failed to turn sstable path to iterator");
                    })
                    .ok()?;
                Some(RecordIter::Filtered(iter))
            })
            .collect();

        Ok(MergeIter::new(Some(memtable_iter), sstable_iters))
    }

    /// prepares all resources that the IoWorker would need for issuing
    /// a flush command.
    #[instrument(skip_all)]
    pub fn prepare_flush(&mut self) -> (Arc<MemTable>, u64) {
        let new_sst_id = self.sst_counter;
        self.sst_counter += 1;

        let shared = Arc::new(self.memtable.freeze());
        self.frozen_memtables.insert(new_sst_id, shared.clone());

        return (shared, new_sst_id);
    }

    pub fn remove_frozen_memtable(&mut self, id: u64) {
        self.frozen_memtables.remove(&id);
    }

    pub fn insert_meta(&mut self, meta: SSTableMeta) {
        self.sstable_map.insert(meta.id, meta);
    }

    pub fn remove_meta(&mut self, id: &u64) {
        self.sstable_map.remove(id);
    }

    pub fn sstable_count(&self) -> usize {
        self.sstable_map.len()
    }

    pub fn total_sstable_size(&self) -> u64 {
        self.sstable_map.values().map(|m| m.file_size).sum()
    }

    pub fn memtable_capacity(&self) -> usize {
        self.memtable_limit_bytes
    }

    pub fn set_stats_tx(&mut self, tx: UnboundedSender<(usize, usize)>) {
        self.stats_tx = Some(tx);
    }

    pub fn set_metrics(&mut self, metrics: &Arc<Metrics>) {
        let m = metrics.clone();
        m.engine.memtable_limit.set(self.memtable.capacity() as i64);
        self.wal.set_metrics(&m);
    }

    pub fn clone_base_dir(&self) -> PathBuf {
        self.base_dir.clone()
    }

    pub fn is_memtable_empty(&self) -> bool {
        self.memtable.is_empty()
    }

    pub fn memtable_size(&self) -> usize {
        self.memtable.current_size()
    }

    pub fn base_dir(&self) -> &Path {
        &self.base_dir
    }

    pub fn send_stats(&self) {
        if let Some(ref tx) = self.stats_tx {
            let _ = tx.send((self.memtable.current_size(), self.sstable_map.len()));
        }
    }

    #[instrument(skip_all, err)]
    pub fn flush_wal_buffer(&mut self) -> anyhow::Result<()> {
        self.wal.flush_buffer()
    }

    #[instrument(skip_all, err)]
    pub fn rotate_wal(&mut self, sst_id: u64) -> anyhow::Result<PathBuf> {
        let stale_path = self.wal.rotate(&self.base_dir, sst_id)?;
        Ok(stale_path)
    }

    pub fn mark_compacting(&mut self, ids: &[u64]) {
        for id in ids {
            self.compacting.insert(*id);
        }
    }

    pub fn unmark_compacting(&mut self, ids: &[u64]) {
        for id in ids {
            self.compacting.remove(id);
        }
    }

    pub fn get_oldest_metas(&self, n: usize) -> Vec<SSTableMeta> {
        self.sstable_map
            .values()
            .filter(|meta| !self.compacting.contains(&meta.id))
            .take(n)
            .cloned()
            .collect()
    }

    pub fn allocate_sstable_id(&mut self) -> u64 {
        let id = self.sst_counter;
        self.sst_counter += 1;
        id
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::tempdir;

    fn open_engine(dir: PathBuf, limit: usize) -> Engine {
        Engine::open(dir, limit).unwrap().0
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
            engine_dir
                .join(Wal::WAL_DIR)
                .join(Wal::ACTIVE_WAL_NAME)
                .exists(),
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

        engine.insert_record(1, 10, "sys", "cpu normal").unwrap();

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

        engine.insert_record(1, 10, "sys", "cpu normal").unwrap();
        engine.insert_record(1, 20, "sys", "mem high").unwrap();
        engine.insert_record(1, 30, "sys", "disk full").unwrap();

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

        engine.insert_record(1, 10, "sys", "cpu normal").unwrap();
        engine.insert_record(1, 20, "sys", "mem high").unwrap();

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

        engine.insert_record(1, 10, "sys", "cpu normal").unwrap();
        engine.insert_record(1, 20, "db", "query slow").unwrap();
        engine
            .insert_record(1, 30, "sys", "flush_trigger_01")
            .unwrap();

        engine
            .insert_record(1, 40, "db", "connection lost")
            .unwrap();

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

        engine.insert_record(1, 10, "sys", "cpu normal").unwrap();
        engine.insert_record(1, 20, "sys", "mem high").unwrap();
        engine
            .insert_record(1, 30, "sys", "flush_trigger_01")
            .unwrap();

        engine.insert_record(1, 40, "sys", "disk full").unwrap();
        engine.insert_record(1, 50, "sys", "all good").unwrap();

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

        engine.insert_record(1, 15, "sys", "first flush").unwrap();
        engine
            .insert_record(1, 25, "sys", "flush_trigger_01")
            .unwrap();

        engine
            .insert_record(1, 20, "sys", "second flush entry")
            .unwrap();
        engine
            .insert_record(1, 45, "sys", "flush_trigger_02")
            .unwrap();

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

        engine.insert_record(1, 10, "sys", "flushed").unwrap();
        engine
            .insert_record(1, 20, "sys", "flush_trigger_01")
            .unwrap();

        engine.insert_record(1, 30, "sys", "in memtable").unwrap();

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

        engine.insert_record(1, 1, "k1", "small").unwrap();

        let needs_flush = engine
            .insert_record(1, 2, "k2", "this_is_a_massive_payload_to_force_a_flush")
            .unwrap();
        assert!(needs_flush, "insert should indicate flush is needed");
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

        engine.batch_insert_records(batch).unwrap();

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

        engine.batch_insert_records(batch).unwrap();

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

        assert!(engine.batch_insert_records(batch).unwrap());
    }

    #[test]
    fn test_engine_batch_insert_with_range() {
        let dir = tempdir().unwrap();
        let mut engine = open_engine(dir.path().to_path_buf(), 50);

        let batch = vec![
            make_insert(1, 5, "sys", "first"),
            make_insert(1, 10, "sys", "second"),
        ];
        engine.batch_insert_records(batch).unwrap();

        // standalone insert after batch
        engine.insert_record(1, 15, "sys", "third").unwrap();

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
        engine.batch_insert_records(batch1).unwrap();

        let batch2 = vec![
            make_insert(1, 30, "sys", "batch2_a"),
            make_insert(1, 40, "sys", "batch2_b"),
        ];
        engine.batch_insert_records(batch2).unwrap();

        let result: Vec<_> = engine.range(1, "sys", 0, 100, "").unwrap().collect();
        assert_eq!(result.len(), 4);
        assert!(
            result
                .iter()
                .any(|r| r.extract_value().unwrap() == b"batch1_a")
        );
        assert!(
            result
                .iter()
                .any(|r| r.extract_value().unwrap() == b"batch2_b")
        );
    }

    #[test]
    fn test_engine_range_filter_empty_returns_all() {
        let dir = tempdir().unwrap();
        let mut engine = open_engine(dir.path().to_path_buf(), 1024);

        engine.insert_record(1, 10, "sys", "cpu normal").unwrap();
        engine.insert_record(1, 20, "sys", "mem high").unwrap();
        engine.insert_record(1, 30, "sys", "disk full").unwrap();

        let result: Vec<_> = engine.range(1, "sys", 0, 100, "").unwrap().collect();
        assert_eq!(result.len(), 3);
    }

    #[test]
    fn test_engine_range_filter_partial_match() {
        let dir = tempdir().unwrap();
        let mut engine = open_engine(dir.path().to_path_buf(), 1024);

        engine.insert_record(1, 10, "sys", "cpu normal").unwrap();
        engine
            .insert_record(1, 20, "sys", "mem high pressure")
            .unwrap();
        engine.insert_record(1, 30, "sys", "disk full").unwrap();
        engine
            .insert_record(1, 40, "sys", "memory leak detected")
            .unwrap();

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

        engine.insert_record(1, 10, "sys", "cpu normal").unwrap();
        engine.insert_record(1, 20, "sys", "mem high").unwrap();

        let result: Vec<_> = engine
            .range(1, "sys", 0, 100, "nonexistent")
            .unwrap()
            .collect();
        assert!(result.is_empty());
    }

    #[test]
    fn test_engine_range_filter_works_across_tiers() {
        let dir = tempdir().unwrap();
        let mut engine = open_engine(dir.path().to_path_buf(), 50);

        engine.insert_record(1, 10, "sys", "cpu normal").unwrap();
        engine
            .insert_record(1, 20, "sys", "first flush trigger")
            .unwrap();
        engine.insert_record(1, 30, "sys", "disk full").unwrap();

        // all records flushed to SSTables, empty filter yields all
        let result: Vec<_> = engine.range(1, "sys", 0, 100, "").unwrap().collect();
        assert_eq!(result.len(), 3);

        // filter matches only one SSTable record
        let result: Vec<_> = engine.range(1, "sys", 0, 100, "cpu").unwrap().collect();
        assert_eq!(result.len(), 1);

        // filter matches no records
        let result: Vec<_> = engine
            .range(1, "sys", 0, 100, "nonexistent")
            .unwrap()
            .collect();
        assert!(result.is_empty());
    }
}
