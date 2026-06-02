use std::{collections::{BTreeMap, BTreeSet}, fs, path::PathBuf, sync::Arc, time::{Duration, Instant}};

use crate::{
    metrics::Metrics,
    proto,
    storage::{
        Engine, SSTableMeta, Wal, io_worker::{self, IoCommand, IoResult}
    },
};

use anyhow::Context;
use tokio::sync::{Notify, mpsc, oneshot};
use tracing::instrument;

pub struct Backend {
    io_result_recv: mpsc::Receiver<IoResult>,
    io_cmd_sender: mpsc::UnboundedSender<IoCommand>,
    wal_sync_notify: Arc<Notify>,

    engine: Engine,
    data_dir: PathBuf,
    stale_wals: BTreeMap<u64, PathBuf>,
    applied_flushes: BTreeSet<u64>,
    next_wal_to_del: u64,

    metrics: Option<Arc<Metrics>>,

    fsync_interval: Duration,
}

impl Backend {
    /// size of batches to be sent over the network channel during range operations
    const RANGE_BATCH_SIZE: usize = 1024;
    /// how many commands to pull from the network channel in recv_many()
    const COMMAND_BATCH_SIZE: usize = 512;
    /// max number of allowed sstable files, if its exceeded, we issue compaction(s)
    pub const MAX_SSTABLE_COUNT: usize = 8;

    pub fn new(
        engine: Engine,
        io_cmd_sender: mpsc::UnboundedSender<IoCommand>,
        io_result_recv: mpsc::Receiver<IoResult>,
        wal_sync_notify: Arc<Notify>,
        fsync_interval: Duration,
    ) -> Self {
        let data_dir = engine.clone_base_dir();
        Backend {
            engine,
            data_dir: data_dir,
            io_cmd_sender,
            io_result_recv,
            wal_sync_notify,
            stale_wals: BTreeMap::new(),
            applied_flushes: BTreeSet::new(),
            next_wal_to_del: 1,
            metrics: None,
            fsync_interval,
        }
    }

    pub fn set_metrics(&mut self, m: Arc<Metrics>) {
        self.metrics = Some(m);
    }

    #[instrument(skip_all)]
    pub async fn run_engine_loop(&mut self, mut cmd_recv: mpsc::Receiver<proto::Command>) {
        let mut cmd_buf = Vec::with_capacity(Self::COMMAND_BATCH_SIZE);

        if !self.engine.is_memtable_empty() {
            self.send_flush_cmd().ok();
        }

        loop {
            let mut needs_flush = false;

            tokio::select! {
                Some(result) = self.io_result_recv.recv() => {
                    match result {
                       IoResult::Flush { new_meta, start_time } => {
                            if self.apply_flush_result(new_meta, start_time).ok() == Some(true) {
                                self.send_compaction_cmd().ok();
                            }
                            // self.engine.wal.clear().ok();
                       },
                       IoResult::Compaction { start_time, new_meta, ids_to_remove } => {
                            if self.apply_compaction_result(start_time, new_meta, ids_to_remove).ok() == Some(true) {
                                self.send_compaction_cmd().ok();
                            }
                       },
                    }
                }

                n = cmd_recv.recv_many(&mut cmd_buf, Self::COMMAND_BATCH_SIZE) => {
                    if n == 0 { continue }

                    for cmd in cmd_buf.drain(0..n) {
                        match cmd {
                            proto::Command::Insert(i, sender) => {
                                needs_flush = self.handle_insert(i, sender);
                            }
                            proto::Command::BatchInsert(batch, sender) => {
                                needs_flush = self.handle_batch_insert(batch, sender);
                            }
                            proto::Command::Range(r, end_sender, record_sender) => {
                                self.handle_range(r, end_sender, record_sender);
                            }
                            proto::Command::Metrics(sender) => {
                                let json = self
                                    .metrics
                                    .as_ref()
                                    .map(|m| serde_json::to_string(&m.snapshot()).unwrap_or_default())
                                    .unwrap_or_default();
                                let _ = sender.send(Ok(json));
                            }
                        }
                    }

                    let _ = self.send_wal_sync_cmd();
                }
            };

            if needs_flush {
                let _ = self.send_flush_cmd();
            }

            if let Some(ref m) = self.metrics {
                m.engine
                    .memtable_bytes
                    .set(self.engine.memtable_size() as i64);
                m.engine
                    .sstable_count
                    .set(self.engine.sstable_count() as i64);
            }
            self.engine.send_stats();
        }
    }

    #[instrument(skip_all)]
    fn handle_insert(
        &mut self,
        i: proto::Insert,
        sender: oneshot::Sender<anyhow::Result<()>>,
    ) -> bool {
        let _t0 = Instant::now();
        let mut needs_flush = false;

        match self.engine.insert_record(i.source_id, i.ts, &i.key, &i.value) {
            Ok(true) => needs_flush = true,
            Ok(false) => {}
            Err(e) => {
                if let Some(ref m) = self.metrics {
                    m.engine.insert_failures.inc(1);
                }
                let _ = sender.send(Err(e));
                return false;
            }
        }

        if let Some(ref m) = self.metrics {
            m.engine.insert_count.inc(1);
            m.engine.records_inserted.inc(1);
            m.engine.insert_latency.record_instant(_t0);
        }
        let _ = sender.send(Ok(()));
        needs_flush
    }

    #[instrument(skip_all)]
    fn handle_batch_insert(
        &mut self,
        batch: proto::BatchInsert,
        sender: oneshot::Sender<anyhow::Result<()>>,
    ) -> bool {
        let _t0 = Instant::now();
        let mut needs_flush = false;
        let count = batch.records.len() as u64;

        match self.engine.batch_insert_records(batch.records) {
            Ok(true) => needs_flush = true,
            Ok(false) => {}
            Err(e) => {
                if let Some(ref m) = self.metrics {
                    m.engine.insert_failures.inc(count);
                }
                let _ = sender.send(Err(e));
                return false;
            }
        }

        if let Some(ref m) = self.metrics {
            m.engine.batch_insert_count.inc(1);
            m.engine.records_inserted.inc(count);
            m.engine.batch_insert_latency.record_instant(_t0);
        }
        let _ = sender.send(Ok(()));
        needs_flush
    }

    #[instrument(skip_all)]
    fn handle_range(
        &mut self,
        r: proto::Range,
        end_sender: oneshot::Sender<anyhow::Result<()>>,
        record_sender: mpsc::Sender<Vec<proto::Record>>,
    ) {
        let metrics = self.metrics.clone();
        let _t0 = Instant::now();
        let mut batch = Vec::with_capacity(Self::RANGE_BATCH_SIZE);
        let mut scanned = 0u64;
        let iter = match self
            .engine
            .range(r.source_id, &r.key, r.start_ts, r.end_ts, &r.filter)
        {
            Ok(iter) => iter,
            Err(err) => {
                if let Some(ref m) = metrics {
                    m.engine.range_failures.inc(1);
                }
                let _ = end_sender.send(Err(err));
                return;
            }
        };

        tokio::task::spawn_blocking(move || {
            let mut loop_err: anyhow::Result<()> = Ok(());
            for storage_rec in iter {
                scanned += 1;
                match proto::Record::try_from(storage_rec) {
                    Ok(proto_rec) => {
                        batch.push(proto_rec);
                        if batch.len() >= Backend::RANGE_BATCH_SIZE
                            && record_sender
                                .blocking_send(std::mem::take(&mut batch))
                                .is_err()
                        {
                            break;
                        }
                    }
                    Err(err) => {
                        if let Some(ref m) = metrics {
                            m.engine.range_failures.inc(1);
                        }
                        loop_err = Err(err);
                        break;
                    }
                }
            }

            if !batch.is_empty() {
                let _ = record_sender.blocking_send(batch);
            }
            if let Some(ref m) = metrics {
                m.engine.records_scanned.inc(scanned);
                m.engine.range_count.inc(1);
                m.engine.range_latency.record_instant(_t0);
            }
            let _ = end_sender.send(loop_err);
        });
    }

    // message passing handlers

    #[instrument(skip_all, err)]
    fn send_flush_cmd(&mut self) -> anyhow::Result<()> {
        if self.engine.is_memtable_empty() {
            return Ok(());
        }

        let (frozen_memtable, new_sst_id) = self.engine.prepare_flush();
        let stale_wal_path = self.engine.rotate_wal(new_sst_id)?;
        self.stale_wals.insert(new_sst_id, stale_wal_path.clone());

        let resync = io_worker::IoCommand::ResyncWal {
            new_syncer_path: Wal::format_active_wal_path(self.data_dir.as_path()),
        };
        if let Err(e) = self.io_cmd_sender.send(resync) {
            tracing::warn!(error = %e, "failed to send resync command, IoWorker is dead")
        }

        let flush = io_worker::IoCommand::FlushMemtable {
            memtable: frozen_memtable,
            base_path: self.data_dir.clone(),
            sst_id: new_sst_id,
        };

        if let Err(e) = self.io_cmd_sender.send(flush) {
            tracing::warn!(error = %e, "failed to send flush command, IoWorker is dead")
        }
        Ok(())
    }

    #[instrument(skip_all, err)]
    fn apply_flush_result(
        &mut self,
        new_meta: SSTableMeta,
        start_time: Instant,
    ) -> anyhow::Result<bool> {
        self.engine.remove_frozen_memtable(new_meta.id);
        self.applied_flushes.insert(new_meta.id);

        while self.applied_flushes.contains(&self.next_wal_to_del) && self.applied_flushes.contains(&(self.next_wal_to_del + 1))
        {
            if let Some(stale) = self.stale_wals.remove(&self.next_wal_to_del) {
                fs::remove_file(&stale).ok();
            }
            self.next_wal_to_del += 1;
        }
        self.engine.insert_meta(new_meta);
        if let Some(ref m) = self.metrics {
            m.engine.flush_count.inc(1);
            m.engine.flush_duration.record_instant(start_time);
        }
        let needs_compaction =
            self.engine.total_sstable_size() >= self.engine.memtable_capacity() as u64 * 8;
        Ok(needs_compaction)
    }

    #[instrument(skip_all, err)]
    fn send_compaction_cmd(&mut self) -> anyhow::Result<()> {
        if self.engine.total_sstable_size() < self.engine.memtable_capacity() as u64 * 8 {
            return Ok(());
        }

        let count = (Self::MAX_SSTABLE_COUNT / 2).max(2);
        let metas = self.engine.get_oldest_metas(count);
        if metas.len() < 2 {
            return Ok(());
        }

        let ids: Vec<u64> = metas.iter().map(|m| m.id).collect();
        self.engine.mark_compacting(&ids);

        let file_id = self.engine.allocate_sstable_id();

        let cmd = IoCommand::CompactSSTables {
            tables: metas,
            base_path: self.data_dir.clone(),
            file_id,
        };

        if let Err(e) = self.io_cmd_sender.send(cmd) {
            self.engine.unmark_compacting(&ids);
            tracing::warn!(error = %e, "failed to send compaction command, IoWorker is dead")
        }
        Ok(())
    }

    #[instrument(skip_all, err)]
    fn apply_compaction_result(
        &mut self,
        start_time: Instant,
        new_meta: SSTableMeta,
        ids_to_remove: Vec<u64>,
    ) -> anyhow::Result<bool> {
        for file_id in &ids_to_remove {
            self.engine.remove_meta(file_id);
            let file_path = SSTableMeta::format_file_path(self.engine.base_dir(), *file_id);
            if let Err(e) = fs::remove_file(&file_path) {
                tracing::error!(error = %e, "engine.loop: failed to remove file {}", file_path.display())
                // TODO: maybe handle this better?
            }
        }
        self.engine.unmark_compacting(&ids_to_remove);
        self.engine.insert_meta(new_meta);
        if let Some(ref m) = self.metrics {
            m.engine.compaction_count.inc(1);
            m.engine.compaction_duration.record_instant(start_time);
        }

        let needs_compaction =
            self.engine.total_sstable_size() >= self.engine.memtable_capacity() as u64 * 8;
        Ok(needs_compaction)
    }

    #[instrument(skip_all, err)]
    fn send_wal_sync_cmd(&mut self) -> anyhow::Result<()> {
        self.engine
            .flush_wal_buffer()
            .context("failed to flush WAL buffer before sending sync message")?;
        if self.fsync_interval.is_zero() {
            self.wal_sync_notify.notify_one();
        }
        Ok(())
    }

}
