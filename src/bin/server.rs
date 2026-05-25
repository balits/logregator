use std::fs;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::SystemTime;
use std::time::UNIX_EPOCH;

use logregator::metrics::Metrics;
use logregator::proto::Command;
use logregator::server::Server;
use logregator::storage;
use logregator::storage::compaction;
use logregator::storage::compaction::{CompactionCommand, CompactionResult};

use anyhow::Context;
use clap::Parser;
use tokio::sync::mpsc;
use tracing::level_filters::LevelFilter;
use tracing_subscriber::fmt::format::FmtSpan;

#[derive(Parser)]
#[command(version, about, long_about = None)]
pub struct Args {
    #[arg(short, long)]
    addr: String,

    #[arg(short, long)]
    batch_size: usize,
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt()
        .with_file(true)
        .with_line_number(true)
        .with_max_level(LevelFilter::TRACE)
        .with_span_events(FmtSpan::NEW | FmtSpan::CLOSE)
        .init();
    let args = Args::parse();

    let now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("Time went backwards")
        .as_millis();
    let path = PathBuf::from(format!("server_data/{now}"));
    fs::create_dir_all(&path).context("failed to create server data directory")?;

    let metrics = Arc::new(Metrics::default());
    let (insert_tx, insert_rx) = mpsc::channel::<Command>(args.batch_size);
    let (range_tx, range_rx) = mpsc::channel::<Command>(args.batch_size);
    let (compaction_tx, compaction_rx) = mpsc::channel::<CompactionCommand>(1);
    let (result_tx, result_rx) = mpsc::channel::<CompactionResult>(1);

    let mut engine = storage::Engine::open(path, 64 * 1024 * 1024, compaction_tx, result_rx)
        .context("failed to open storage engine")?;
    engine.set_metrics(&metrics);

    tokio::spawn(async move { engine.engine_loop(insert_rx, range_rx).await });
    tracing::info!("engine running");
    let comp_metrics = metrics.clone();
    tokio::spawn(async move {
        compaction::compaction_loop(compaction_rx, result_tx, Some(comp_metrics)).await
    });
    tracing::info!("compaction running");

    let server = Server::new(Some(args.addr.parse().unwrap()))
        .await
        .context("failed to start TCP server")?;
    tracing::info!("server running on {}", server.addr());
    let svr_metrics = metrics.clone();
    if let Err(e) = server
        .run_main_loop(insert_tx, range_tx, svr_metrics)
        .await
        .context("server main loop exited with error")
    {
        eprintln!("SERVER LOOP FAILED");
        for (i, e) in e.chain().enumerate() {
            eprintln!("  caused by {:>2}: {:#}", i, e);
        }
    }

    Ok(())
}
