use std::net::SocketAddr;
use std::time::Duration;
use std::{path::PathBuf, sync::Arc};

use anyhow::Context;
use tokio::sync::{Notify, mpsc};
use tokio::task::JoinHandle;

use crate::{
    proto,
    server::Server,
    storage::{
        Backend, BackendConfig, Engine, Wal, WalSyncer,
        io_worker::{IoCommand, IoResult, IoWorker},
    },
};

use crate::metrics::Metrics;

pub struct Runtime {
    pub metrics: Arc<Metrics>,
    pub client_cmd_sender: mpsc::Sender<proto::Command>,
    pub server_addr: SocketAddr,
    pub stats_rx: Option<mpsc::UnboundedReceiver<(usize, usize)>>,
    io_worker_handle: JoinHandle<()>,
    backend_handle: JoinHandle<()>,
    server_handle: JoinHandle<()>,
}

impl Runtime {
    pub async fn join(self) {
        let _ = futures::future::join3(
            self.io_worker_handle,
            self.backend_handle,
            self.server_handle,
        )
        .await;
    }
}

#[derive(Default)]
pub struct RuntimeBuilder {
    data_dir: Option<PathBuf>,
    server_addr: Option<String>,
    channel_capacity: Option<usize>,
    memtable_limit_bytes: Option<usize>,
    profile_mem: usize,
    fsync_interval_ms: u64,
    backend_config: BackendConfig,
}

impl RuntimeBuilder {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn data_dir(mut self, path: PathBuf) -> Self {
        self.data_dir = Some(path);
        self
    }

    pub fn server_addr(mut self, addr: impl Into<String>) -> Self {
        self.server_addr = Some(addr.into());
        self
    }

    pub fn channel_capacity(mut self, cap: usize) -> Self {
        self.channel_capacity = Some(cap);
        self
    }

    pub fn memtable_limit_bytes(mut self, bytes: usize) -> Self {
        self.memtable_limit_bytes = Some(bytes);
        self
    }

    pub fn profile_mem(mut self, profile: usize) -> Self {
        self.profile_mem = profile;
        self
    }

    pub fn fsync_interval_ms(mut self, ms: u64) -> Self {
        self.fsync_interval_ms = ms;
        self
    }

    pub fn command_batch_size(mut self, sz: usize) -> Self {
        self.backend_config.command_batch_size = sz;
        self
    }

    pub fn range_batch_size(mut self, sz: usize) -> Self {
        self.backend_config.range_batch_size = sz;
        self
    }

    pub fn max_sstable_count(mut self, n: usize) -> Self {
        self.backend_config.max_sstable_count = n;
        self
    }

    pub async fn spawn(self) -> anyhow::Result<Runtime> {
        let data_dir = self
            .data_dir
            .context("RuntimeBuilder: data_dir is required")?;
        let server_addr = self
            .server_addr
            .context("RuntimeBuilder: server_addr is required")?;
        let channel_capacity = self
            .channel_capacity
            .context("RuntimeBuilder: channel_capacity is required")?;

        let memtable_limit = self.memtable_limit_bytes.unwrap_or(0);

        let metrics = Arc::new(Metrics::default());
        let (client_cmd_sender, client_cmd_recv) = mpsc::channel(channel_capacity);

        let (mut engine, _needs_initial_flush) =
            Engine::open(data_dir.clone(), memtable_limit)
                .context("RuntimeBuilder: failed to open Engine")?;
        engine.set_metrics(&metrics);

        let stats_rx = if self.profile_mem > 0 {
            let (stats_tx, stats_rx) = mpsc::unbounded_channel();
            engine.set_stats_tx(stats_tx);
            Some(stats_rx)
        } else {
            None
        };

        let wal_sync_notify = Arc::new(Notify::new());
        let (io_cmd_sender, io_cmd_recv) = mpsc::channel::<IoCommand>(channel_capacity);
        let (io_result_sender, io_result_recv) = mpsc::channel::<IoResult>(channel_capacity);

        let active_wal_path = Wal::format_active_wal_path(&data_dir);
        let wal_syncer = WalSyncer::open(&active_wal_path)
            .context("RuntimeBuilder: failed to open WalSyncer")?;
        let fsync_interval = Duration::from_millis(self.fsync_interval_ms);
        let io_worker =
            IoWorker::new(wal_syncer, io_cmd_recv, wal_sync_notify.clone(), io_result_sender, fsync_interval)
                .context("RuntimeBuilder: failed to create IoWorker")?;
        let mut backend =
            Backend::new(engine, io_cmd_sender, io_result_recv, wal_sync_notify, fsync_interval, self.backend_config);
        backend.set_metrics(metrics.clone());

        let io_worker_handle = tokio::spawn(io_worker.run());
        let backend_handle = tokio::spawn(async move {
            backend.run_engine_loop(client_cmd_recv).await;
        });

        let server = Server::new(Some(server_addr.parse()?))
            .await
            .context("RuntimeBuilder: failed to create Server")?;
        let addr = server.addr();
        let server_metrics = metrics.clone();
        let server_cmd_sender = client_cmd_sender.clone();
        let server_handle = tokio::spawn(async move {
            if let Err(e) = server.run_main_loop(server_cmd_sender, server_metrics).await {
                tracing::error!(error = %e, "server main loop exited with error");
            }
        });

        Ok(Runtime {
            metrics,
            client_cmd_sender,
            server_addr: addr,
            stats_rx,
            io_worker_handle,
            backend_handle,
            server_handle,
        })
    }
}
