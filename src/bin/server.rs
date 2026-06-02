use std::path::PathBuf;
use std::time::{SystemTime, UNIX_EPOCH};
use std::{env, fs};

use anyhow::Context;
use clap::Parser;
use logregator::runtime::RuntimeBuilder;
use logregator::_tracing;

#[derive(Parser)]
#[command(version, about, long_about = None)]
pub struct Args {
    #[arg(short, long)]
    addr: String,

    #[arg(short, long, default_value = "1024")]
    batch_size: usize,

    #[arg(long, default_value = "none")]
    profile: String,

    #[arg(long)]
    flame_output: Option<PathBuf>,

    #[arg(long, default_value = "info")]
    log_level: String,

    #[arg(long, default_value = "32")]
    memtable_mb: usize,

    #[arg(long, default_value = "0")]
    fsync_interval: u64,
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let args = Args::parse();
    _tracing::init(
        _tracing::Config::from_cli(&args.profile, args.flame_output, &args.log_level),
    )?;

    let now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("Time went backwards")
        .as_millis();
    let cwd = env::current_dir().context("failed to get current directory")?;
    let path = cwd.join("server_data").join(now.to_string());
    fs::create_dir_all(&path).context("failed to create server data directory")?;

    let rt = RuntimeBuilder::new()
        .data_dir(path)
        .server_addr(&args.addr)
        .channel_capacity(args.batch_size)
        .memtable_limit_bytes(args.memtable_mb * 1024 * 1024)
        .fsync_interval_ms(args.fsync_interval)
        .spawn()
        .await?;

    tracing::info!("server running on {}", args.addr);
    rt.join().await;
    Ok(())
}
