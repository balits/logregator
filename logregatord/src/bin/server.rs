use std::path::PathBuf;
use std::time::{SystemTime, UNIX_EPOCH};
use std::{env, fs};

use anyhow::Context;
use clap::Parser;
use logregator_core::runtime::RuntimeBuilder;
use logregatord::_tracing;

#[derive(Parser)]
#[command(version, about, long_about = None)]
pub struct Args {
    #[arg(short, long, help = "address of the server")]
    addr: String,

    #[arg(short, long, help = "data directory (defaults to ./server_data/<ts>/)")]
    data_dir: Option<PathBuf>,

    #[arg(short, long, default_value = "1024", help = "size of the client command / io command channels")]
    batch_size: usize,

    #[arg(long, default_value = "none", help = "profiling mode (defaults to none)", value_parser=profile_parser)]
    profile: _tracing::Profile,

    #[arg(long, help = "output of the generated flamegraph (optional)")]
    flame_output: Option<PathBuf>,

    #[arg(long, default_value = "info", help = "level used by the tracing crate (set to \"off\" to disable logging)")]
    log_level: String,

    #[arg(long, default_value = "32", help = "capacity of the memtable in megabytes")]
    memtable_mb: usize,

    #[arg(long, default_value = "0", help = "fsync interval in miliseconds")]
    fsync_interval: u64,

    #[arg(long, default_value = "512", help = "commands drained per recv_many batch")]
    command_batch_size: usize,

    #[arg(long, default_value = "1024", help = "records per range batch sent over the wire")]
    range_batch_size: usize,

    #[arg(long, default_value = "8", help = "max sstable count before compaction triggers")]
    max_sstable_count: usize,
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let args = Args::parse();
    _tracing::init(
        _tracing::Config::from_cli(args.profile, args.flame_output, &args.log_level),
    )?;

    let path = match args.data_dir {
        Some(d) => d,
        None => {
            let now = SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .expect("Time went backwards")
                .as_millis();
            let cwd = env::current_dir().context("failed to get current directory")?;
            cwd.join("server_data").join(now.to_string())
        }
    };
    fs::create_dir_all(&path).context("failed to create server data directory")?;

    let rt = RuntimeBuilder::new()
        .data_dir(path)
        .server_addr(&args.addr)
        .channel_capacity(args.batch_size)
        .memtable_limit_bytes(args.memtable_mb * 1024 * 1024)
        .fsync_interval_ms(args.fsync_interval)
        .command_batch_size(args.command_batch_size)
        .range_batch_size(args.range_batch_size)
        .max_sstable_count(args.max_sstable_count)
        .spawn()
        .await?;

    tracing::info!("server running on {}", args.addr);
    rt.join().await;
    Ok(())
}

fn profile_parser(arg: &str) -> Result<_tracing::Profile, String> {
    match arg {
        "none" => Ok(_tracing::Profile::None),
        "flame" => Ok(_tracing::Profile::Flame),
        "console" => Ok(_tracing::Profile::Console),
        "both" => Ok(_tracing::Profile::Both),
        _ => Err("unrecognised profile".into()),
    }
}