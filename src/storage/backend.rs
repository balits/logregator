use std::time::Instant;

use crate::{
    proto,
    storage::{
        Engine,
        compaction::{CompactionCommand, CompactionResult},
    },
};

use tokio::{
    select,
    sync::{mpsc, oneshot},
};
use tracing::instrument;

pub struct Backend {
    engine: Engine,
    comp_cmd_sender: mpsc::Sender<CompactionCommand>,
    comp_result_recv: mpsc::Receiver<CompactionResult>,
}

impl Backend {
    /// size of batches to be sent over the network channel during range operations
    const RANGE_BATCH_SIZE: usize = 1024;
    /// how many commands to pull from the network channel in recv_many()
    const COMMAND_BATCH_SIZE: usize = 1024;

    pub fn new(
        engine: Engine,
        comp_cmd_sender: mpsc::Sender<CompactionCommand>,
        comp_result_recv: mpsc::Receiver<CompactionResult>,
    ) -> Self {
        Backend {
            engine,
            comp_cmd_sender,
            comp_result_recv,
        }
    }

    #[instrument(skip_all)]
    pub async fn engine_loop(
        &mut self,
        mut cmd_rx: mpsc::Receiver<proto::Command>,
    ) -> anyhow::Result<()> {
        let mut cmd_buf = Vec::with_capacity(Self::COMMAND_BATCH_SIZE);

        loop {
            let mut needs_flush = false;

            tokio::select! {
                Some(CompactionResult { new_meta, ids_to_remove }) = self.comp_result_recv.recv() => {
                    self.engine.handle_compaction(new_meta, ids_to_remove);
                }

                n = cmd_rx.recv_many(&mut cmd_buf, Self::COMMAND_BATCH_SIZE) => {
                    if n == 0 { return Ok(()); }

                    for cmd in cmd_buf.drain(0..n) {
                        match cmd {
                            proto::Command::Insert(i, sender) => {
                                needs_flush = self.handle_insert(i, sender);
                            }
                            proto::Command::BatchInsert(batch, sender) => {
                                needs_flush = self.handle_batch_insert(batch, sender);
                            }
                            proto::Command::Range(r, end_sender, record_sender) => {
                                self.handle_range(r, end_sender, record_sender).await;
                            }
                        }
                    }

                    // Sync WAL once for all commands in this batch
                    if let Err(e) = self.engine.wal.sync() {
                        tracing::error!(error = %e, "backend.engine_loop: failed to sync WAL");
                    }
                }
            };

            if needs_flush {
                let _t0 = Instant::now();
                if let Err(e) = self.engine.flush().await {
                    tracing::error!(error = %e, "backend.engine_loop: failed to flush");
                }
                if let Some(ref m) = self.engine.metrics {
                    m.engine.flush_count.inc(1);
                    m.engine.flush_duration.record_instant(_t0);
                }
            }

            if let Some(ref m) = self.engine.metrics {
                m.engine
                    .memtable_bytes
                    .set(self.engine.memtable.size_hint() as i64);
                m.engine
                    .sstable_count
                    .set(self.engine.sstable_map.len() as i64);
            }
            if let Some(ref tx) = self.engine.stats_tx {
                let _ = tx.send((
                    self.engine.memtable.size_hint(),
                    self.engine.sstable_map.len(),
                ));
            }
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

        match self.engine.insert(i.source_id, i.ts, &i.key, &i.value) {
            Ok(true) => needs_flush = true,
            Ok(false) => {}
            Err(e) => {
                if let Some(ref m) = self.engine.metrics {
                    m.engine.insert_failures.inc(1);
                }
                let _ = sender.send(Err(e));
                return false;
            }
        }

        if let Some(ref m) = self.engine.metrics {
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

        match self.engine.batch_insert(batch.records) {
            Ok(true) => needs_flush = true,
            Ok(false) => {}
            Err(e) => {
                if let Some(ref m) = self.engine.metrics {
                    m.engine.insert_failures.inc(count);
                }
                let _ = sender.send(Err(e));
                return false;
            }
        }

        if let Some(ref m) = self.engine.metrics {
            m.engine.batch_insert_count.inc(1);
            m.engine.records_inserted.inc(count);
            m.engine.batch_insert_latency.record_instant(_t0);
        }
        let _ = sender.send(Ok(()));
        needs_flush
    }

    #[instrument(skip_all)]
    async fn handle_range(
        &mut self,
        r: proto::Range,
        end_sender: oneshot::Sender<anyhow::Result<()>>,
        record_sender: mpsc::Sender<Vec<proto::Record>>,
    ) {
        let _t0 = Instant::now();
        let mut batch = Vec::with_capacity(Self::RANGE_BATCH_SIZE);
        let mut scanned = 0u64;
        let iter = match self
            .engine
            .range(r.source_id, &r.key, r.start_ts, r.end_ts, &r.filter)
        {
            Ok(iter) => iter,
            Err(err) => {
                if let Some(ref m) = self.engine.metrics {
                    m.engine.range_failures.inc(1);
                }
                let _ = end_sender.send(Err(err));
                return;
            }
        };

        let mut loop_err: anyhow::Result<()> = Ok(());
        for raw_rec in iter {
            scanned += 1;
            match proto::Record::try_from(raw_rec) {
                Ok(rec) => {
                    batch.push(rec);
                    if batch.len() >= Self::RANGE_BATCH_SIZE
                        && record_sender
                            .send(std::mem::take(&mut batch))
                            .await
                            .is_err()
                    {
                        break;
                    }
                }
                Err(err) => {
                    if let Some(ref m) = self.engine.metrics {
                        m.engine.range_failures.inc(1);
                    }
                    loop_err = Err(err);
                    break;
                }
            }
        }

        if !batch.is_empty() {
            let _ = record_sender.send(batch).await;
        }
        if let Some(ref m) = self.engine.metrics {
            m.engine.records_scanned.inc(scanned);
            m.engine.range_count.inc(1);
            m.engine.range_latency.record_instant(_t0);
        }
        let _ = end_sender.send(loop_err);
    }
}
