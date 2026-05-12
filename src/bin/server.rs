use std::fs;
use std::path::PathBuf;
use std::time::SystemTime;
use std::time::UNIX_EPOCH;

use logregator::storage;
use logregator::proto::Command;
use logregator::storage::compaction;
use logregator::storage::compaction::{CompactionCommand, CompactionResult};
use logregator::server::Server;

use anyhow::Context;
use tokio::sync::mpsc;
use clap::{Parser};
use tracing::level_filters::LevelFilter;
use tracing_subscriber::fmt::format::FmtSpan;

#[derive(Parser)]
#[command(version, about, long_about = None)]
pub struct Args {
    #[arg(short, long)]
    addr: String,
}

#[tokio::main]
async fn main() -> anyhow::Result<()>{
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
    fs::create_dir_all(&path)
        .context("failed to create server data directory")?;

    let (network_tx, network_rx) = mpsc::channel::<Command>(64);
    let (compaction_tx, compaction_rx) = mpsc::channel::<CompactionCommand>(1);
    let (result_tx, result_rx) = mpsc::channel::<CompactionResult>(1);

    let mut engine = storage::Engine::open(
        path,
        1024,
        compaction_tx,
        result_rx,
    ).context("failed to open storage engine")?;

    tokio::spawn(async move { engine.engine_loop(network_rx).await });
    tracing::info!("engine running");
    tokio::spawn(async move { compaction::compaction_loop(compaction_rx, result_tx).await });
    tracing::info!("compaction running");

    let server = Server::new(Some(args.addr.parse().unwrap())).await
        .context("failed to start TCP server")?;
    tracing::info!("server running on {}", server.addr());
    server.run_main_loop(network_tx).await
        .context("server main loop exited with error")?;

    Ok(())
}