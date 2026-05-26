use std::fs;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::SystemTime;
use std::time::UNIX_EPOCH;

use logregator::metrics::Metrics;
use logregator::proto::Command;
use logregator::server::Server;
use logregator::storage;
use logregator::storage::Backend;
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
    let (cmd_sender, cmd_reciever) = mpsc::channel::<Command>(args.batch_size);
    let (comp_cmd_sender, comp_cmd_reciever) = mpsc::channel::<CompactionCommand>(1);
    let (comp_result_sender, comp_result_reciever) = mpsc::channel::<CompactionResult>(1);

    let mut engine = storage::Engine::open(path, 64 * 1024 * 1024, comp_cmd_sender.clone())
        .context("failed to open storage engine")?;
    engine.set_metrics(&metrics);
    let mut backend = Backend::new(engine, comp_cmd_sender, comp_result_reciever);

    tokio::spawn(async move { backend.engine_loop(cmd_reciever).await });
    tracing::info!("engine running");
    let comp_metrics = metrics.clone();
    tokio::spawn(async move {
        compaction::compaction_loop(comp_cmd_reciever, comp_result_sender, Some(comp_metrics)).await
    });
    tracing::info!("compaction running");

    let server = Server::new(Some(args.addr.parse().unwrap()))
        .await
        .context("failed to start TCP server")?;
    tracing::info!("server running on {}", server.addr());
    let svr_metrics = metrics.clone();
    if let Err(e) = server
        .run_main_loop(cmd_sender, svr_metrics)
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
