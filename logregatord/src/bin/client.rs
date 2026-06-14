use anyhow::Context;
use chrono::{DateTime, Utc};
use clap::{Parser, Subcommand};
use logregator_core::{client::Client, proto};
use tracing::level_filters::LevelFilter;
use tracing_subscriber::fmt::format::FmtSpan;

#[derive(Parser)]
#[command(version, about, long_about = None)]
pub struct Args {
    #[arg(short, long)]
    addr: String,

    #[command(subcommand)]
    command: Command,
}

#[derive(Subcommand)]
#[command(version, about)]
enum Command {
    Insert {
        #[arg(long, short = 'i', help = "id of the source which produced the log")]
        source_id: i64,
        #[arg(long, short, help = "when this log was produced", value_parser = timestamp_parser)]
        timestamp: i64,
        #[arg(long, short, help = "key of the log")]
        key: String,
        #[arg(long, short, help = "contents of the log")]
        value: String,
    },
    Range {
        #[arg(long, short = 'i', help = "source_id")]
        source_id: i64,
        #[arg(long, short, help = "start of the time range (inclusive)", value_parser = timestamp_parser)]
        start_timestamp: i64,
        #[arg(long, short, help = "end of the time range (exclusive)", value_parser = timestamp_parser)]
        end_timestamp: i64,
        #[arg(long, short, help = "key of the log")]
        key: String,
        #[arg(
            long,
            short,
            help = "optional filter (needle) to search in the value (haystack)"
        )]
        filter: Option<String>,
    },
    BatchInsert {
        #[arg(
            long,
            short,
            help = "path of the JSON input file containing a list of insert request"
        )]
        path: String,
    },
}

#[test]
fn verify_cli() {
    use clap::CommandFactory;
    Args::command().debug_assert();
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt()
        .with_file(true)
        .with_line_number(true)
        .with_max_level(LevelFilter::TRACE)
        .with_span_events(FmtSpan::NEW | FmtSpan::CLOSE)
        .init();

    if let Err(e) = run_main().await {
        tracing::error!("fatal: {e}");
        return Err(e);
    } else {
        Ok(())
    }
}

async fn run_main() -> anyhow::Result<()> {
    let args = Args::parse();
    let mut client = Client::connect(&args.addr)
        .await
        .context("failed to connect to server")?;
    tracing::info!("client connected");

    match args.command {
        Command::Insert {
            source_id,
            timestamp,
            key,
            value,
        } => {
            let res = client
                .insert(proto::Insert {
                    source_id,
                    ts: timestamp,
                    key,
                    value,
                })
                .await;

            match res {
                Err(e) => {
                    tracing::error!("failed to insert record: {e}")
                }
                Ok(_) => {}
            }
        }
        Command::Range {
            source_id,
            start_timestamp,
            end_timestamp,
            key,
            filter,
        } => {
            let res = client
                .range(proto::Range {
                    source_id,
                    start_ts: start_timestamp,
                    end_ts: end_timestamp,
                    key,
                    filter: filter.unwrap_or_default(),
                })
                .await;
            match res {
                Err(e) => {
                    tracing::error!("failed to range over record: {e}")
                }
                Ok(_) => {}
            }
        }
        _ => unimplemented!(),
    }

    Ok(())
}

fn timestamp_parser(arg: &str) -> Result<i64, String> {
    arg.parse::<i64>()
        .or_else(|_| arg.parse::<DateTime<Utc>>().map(|dt| dt.timestamp()))
        .map_err(|_| format!("'{arg}' is neither a Unix timestamp integer nor an RFC3339 datetime"))
}
