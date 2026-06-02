use std::{path::PathBuf, sync::Arc, time::Duration, time::Instant};

use anyhow::Context;
use tokio::sync::{Notify, mpsc};
use tracing::instrument;

use crate::storage::{
    MemTable, SSTableMeta,
    iter::{MergeIter, RecordIter, SSTableIterUnfiltered},
    wal::WalSyncer,
};

pub enum IoCommand {
    ResyncWal {
        new_syncer_path: PathBuf
    },
    FlushMemtable {
        memtable: Arc<MemTable>,
        base_path: PathBuf,
        sst_id: u64,
    },
    CompactSSTables {
        tables: Vec<SSTableMeta>,
        base_path: PathBuf,
        file_id: u64,
    },
}

pub enum IoResult {
    Flush {
        new_meta: SSTableMeta,
        start_time: Instant,
    },
    Compaction {
        start_time: Instant,
        new_meta: SSTableMeta,
        ids_to_remove: Vec<u64>,
    },
}

// A dedicated thread for handling fsyncs and flushing to reduce latency
// on the main engine loop.
pub(crate) struct IoWorker {
    wal_syncer: WalSyncer,
    io_cmd_recv: mpsc::UnboundedReceiver<IoCommand>,
    io_result_sender: mpsc::Sender<IoResult>,
    wal_sync_notify: Arc<Notify>,
    fsync_interval: Duration,
}

impl IoWorker {
    pub(crate) fn new(
        wal_syncer: WalSyncer,
        io_cmd_recv: mpsc::UnboundedReceiver<IoCommand>,
        wal_sync_notify: Arc<Notify>,
        io_result_sender: mpsc::Sender<IoResult>,
        fsync_interval: Duration,
    ) -> anyhow::Result<Self> {
        Ok(Self {
            wal_syncer,
            io_cmd_recv,
            wal_sync_notify,
            io_result_sender,
            fsync_interval,
        })
    }

    pub(crate) async fn run(self) {
        if self.fsync_interval.is_zero() {
            self.run_legacy().await;
        } else {
            self.run_timer().await;
        }
    }

    async fn run_legacy(mut self) {
        loop {
            tokio::select! {
                _ = self.wal_sync_notify.notified() => {
                    let _ = self.handle_sync();
                },
                Some(cmd) = self.io_cmd_recv.recv() => {
                    self.handle_cmd(cmd).await;
                },
            };
        }
    }

    async fn run_timer(mut self) {
        let mut interval = tokio::time::interval(self.fsync_interval);
        interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);

        loop {
            tokio::select! {
                _ = interval.tick() => {
                    let _ = self.handle_sync();
                },
                Some(cmd) = self.io_cmd_recv.recv() => {
                    self.handle_cmd(cmd).await;
                },
            };
        }
    }

    async fn handle_cmd(&mut self, cmd: IoCommand) {
        let result_tx = self.io_result_sender.clone();
        match cmd {
            IoCommand::ResyncWal { new_syncer_path } => {
                match WalSyncer::open(&new_syncer_path) {
                    Ok(s) => {
                        let _ = self.handle_sync();
                        self.wal_syncer = s;
                    },
                    Err(e) => {
                        tracing::error!(error = %e, "failed to create new WalSyncer");
                    }
                }
            }
            IoCommand::FlushMemtable {
                memtable,
                base_path,
                sst_id,
            } => {
                tokio::task::spawn_blocking(move || {
                    let _ = Self::handle_flush(memtable, base_path, sst_id, &result_tx);
                });
            }
            IoCommand::CompactSSTables {
                tables,
                base_path,
                file_id,
            } => {
                tokio::task::spawn_blocking(move || {
                    let _ = Self::handle_compaction(tables, base_path, file_id, &result_tx);
                });
            }
        }
    }

    #[instrument(skip_all, err)]
    fn handle_sync(&mut self) -> anyhow::Result<()> {
        self.wal_syncer
            .fsync()
            .context("io_worker.handle_sync: fsync() failed")?;
        Ok(())
    }

    #[instrument(skip_all, fields(file_id), err)]
    fn handle_flush(
        memtable: Arc<MemTable>,
        base_path: PathBuf,
        file_id: u64,
        result_tx: &mpsc::Sender<IoResult>,
    ) -> anyhow::Result<()> {
        let start_time = Instant::now();
        let new_meta =
            SSTableMeta::write_to_file(base_path, file_id, memtable.iter(), memtable.len())
                .context("io_worker.handle_flush: failed to write sstable file")?;
        let res = IoResult::Flush {
            new_meta,
            start_time,
        };
        result_tx
            .blocking_send(res)
            .context("io_worker.handle_flush: failed to send flush result")?;
        Ok(())
    }

    #[instrument(skip_all, fields(file_id), err)]
    fn handle_compaction(
        tables: Vec<SSTableMeta>,
        base_path: PathBuf,
        file_id: u64,
        result_tx: &mpsc::Sender<IoResult>,
    ) -> anyhow::Result<()> {
        let start_time = Instant::now();
        let num_records_hint: usize = tables.iter().map(|m| m.num_records).sum();
        let ids_to_remove: Vec<u64> = tables.iter().map(|m| m.id).collect();

        let sstable_scanners: Vec<RecordIter> = tables
            .into_iter()
            .filter_map(|meta| {
                SSTableIterUnfiltered::new(meta)
                .inspect_err(|e| {
                    tracing::error!(error = %e, "io_worker.handle_compaction: failed to turn sstable path to iterator");
                })
                .ok()
                .map(RecordIter::Unfiltered)
            })
            .collect();

        let merge_iter = MergeIter::new(None, sstable_scanners);
        let new_meta = SSTableMeta::write_to_file(base_path, file_id, merge_iter, num_records_hint)
            .context("io_worker.handle_compaction: failed to write sstable file")?;

        let res = IoResult::Compaction {
            start_time,
            new_meta,
            ids_to_remove,
        };
        result_tx
            .blocking_send(res)
            .context("io_worker.handle_compaction: failed to send compaction result")?;
        Ok(())
    }
}
