use std::path::Path;
use std::sync::Arc;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};
use std::fs;

use anyhow::Context;
use clap::Parser;
use logregator_core::{client::Client, proto, metrics};
use logregator_tools::{bench, mem};
use tracing_subscriber::filter::LevelFilter;
use rand::rngs::StdRng;
use rand::{RngExt, SeedableRng};

#[derive(clap::Parser)]
struct Cli {
    #[command(flatten)]
    inner: bench::BenchmarkArgs,
}

#[tokio::main]
async fn main() {
    let args = Cli::parse();
    let level = if args.inner.verbose {
        LevelFilter::TRACE
    } else {
        LevelFilter::INFO
    };
    tracing_subscriber::fmt()
        .with_writer(std::io::stderr)
        .with_max_level(level)
        .init();
    tracing::info!("> running benchmark cmd...");
    run(args.inner).await.expect("> benchmark cmd failed");
}

pub async fn run(args: bench::BenchmarkArgs) -> anyhow::Result<()> {
    // health check — retry connect + ping until server is ready
    check_server(&args.addr).await?;

    let profiler = if args.profile_mem > 0 {
        Some(mem::MemProfiler::start(Duration::from_millis(
            args.profile_mem,
        )))
    } else {
        None
    };

    let server_start = Instant::now();
    let configs = build_run_configs(&args, server_start);
    let mut results = Vec::with_capacity(configs.len());

    let total = configs.len();
    for (idx, config) in configs.iter().enumerate() {
        tracing::info!(
            "[{}/{}] {} | {}-client ({}s)",
            idx + 1,
            total,
            format!("{:?}", config.workload).to_lowercase(),
            config.concurrency,
            config.duration_secs
        );

        let t0 = Instant::now();
        let mut result = run_single(config).await?;
        let elapsed = t0.elapsed();

        // fetch sstable count from server metrics
        let mut client = Client::connect(&args.addr).await.unwrap();
        if let Ok(json) = client.get_metrics().await {
            if let Ok(snap) = serde_json::from_str::<metrics::MetricsSnapshot>(&json) {
                result.sstable_count = snap.engine.sstable_count as usize;
            }
        }

        let ins = bench::fmt_count(result.inserts_completed);
        let rng = bench::fmt_count(result.range_queries_completed);
        tracing::info!(
            "run {idx}: {} inserts, {} ranges ({:.1}s)",
            ins,
            rng,
            elapsed.as_secs_f64()
        );

        results.push(result);
        tokio::time::sleep(Duration::from_secs(2)).await;
    }

    // fetch final metrics snapshot from the server
    let metrics_snapshot = {
        let mut client = Client::connect(&args.addr).await.unwrap();
        client
            .get_metrics()
            .await
            .ok()
            .and_then(|json| serde_json::from_str::<metrics::MetricsSnapshot>(&json).ok())
    };

    let mem_samples: Vec<mem::MemSample> = if let Some(p) = profiler {
        p.stop().await
    } else {
        Vec::new()
    };
    let mem_report = mem::MemProfiler::new_report(&mem_samples, &[]);

    let saturations = bench::compute_saturations(&results);
    let benchmark = bench::BenchmarkResult {
        args,
        runs: results,
        metrics: metrics_snapshot,
        saturations,
        mem: mem_report,
    };

    let json =
        serde_json::to_string_pretty(&benchmark).context("failed to serialize benchmark result")?;

    // meta.json
    let git_hash = std::process::Command::new("git")
        .args(["rev-parse", "HEAD"])
        .output()
        .ok()
        .and_then(|o| String::from_utf8(o.stdout).ok())
        .map(|s| s.trim().to_string())
        .unwrap_or_else(|| "unknown".into());

    let ts = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_millis();

    let workloads_json: Vec<String> = benchmark
        .args
        .workloads
        .iter()
        .map(|w| format!("{:?}", w).to_lowercase())
        .collect();

    let meta = serde_json::json!({
        "tag": benchmark.args.tag.as_deref().unwrap_or("unknown"),
        "git_hash": git_hash,
        "timestamp_ms": ts,
        "args": {
            "duration": benchmark.args.duration,
            "workloads": workloads_json,
            "addr": benchmark.args.addr,
            "batch_size": benchmark.args.batch_size,
            "concurrency_min": benchmark.args.concurrency_min,
            "concurrency_max": benchmark.args.concurrency_max,
            "concurrency_step": benchmark.args.concurrency_step,
            "sources": benchmark.args.sources,
            "keys": benchmark.args.keys,
            "value_size": benchmark.args.value_size,
            "range_fraction": benchmark.args.range_fraction,
            "range_window": benchmark.args.range_window,
            "profile_mem": benchmark.args.profile_mem,
            "verbose": benchmark.args.verbose,
        },
    });
    let meta_json = serde_json::to_string_pretty(&meta)
        .context("failed to serialize meta")?;

    // resolve output paths
    let result_path = benchmark
        .args
        .out_result
        .clone()
        .or_else(|| benchmark.args.out_dir.as_ref().map(|d| format!("{d}/benchmark_result.json")));
    let meta_path = benchmark
        .args
        .out_meta
        .clone()
        .or_else(|| benchmark.args.out_dir.as_ref().map(|d| format!("{d}/meta.json")));
    let mem_path = result_path
        .as_ref()
        .map(|p| {
            let parent = Path::new(p).parent().unwrap_or(Path::new("."));
            parent.join("mem_report.json").to_string_lossy().to_string()
        })
        .or_else(|| benchmark.args.out_dir.as_ref().map(|d| format!("{d}/mem_report.json")));

    let write_file = |path: &str, content: &str| -> anyhow::Result<()> {
        if let Some(parent) = Path::new(path).parent() {
            fs::create_dir_all(parent)
                .with_context(|| format!("failed to create dir {parent:?}"))?;
        }
        fs::write(path, content)
            .with_context(|| format!("failed to write {path}"))?;
        Ok(())
    };

    match (&result_path, &meta_path) {
        (Some(rp), Some(mp)) => {
            write_file(rp, &json)?;
            write_file(mp, &meta_json)?;
            if let Some(ref mem) = benchmark.mem {
                if let Some(ref mem_p) = mem_path {
                    let mem_json = serde_json::to_string_pretty(mem)
                        .context("failed to serialize mem report")?;
                    write_file(mem_p, &mem_json)?;
                }
            }
        }
        (Some(rp), None) => {
            write_file(rp, &json)?;
            println!("{meta_json}");
            if let Some(ref mem) = benchmark.mem {
                if let Some(ref mem_p) = mem_path {
                    let mem_json = serde_json::to_string_pretty(mem)
                        .context("failed to serialize mem report")?;
                    write_file(mem_p, &mem_json)?;
                }
            }
        }
        (None, Some(mp)) => {
            println!("{json}");
            write_file(mp, &meta_json)?;
        }
        (None, None) => {
            println!("{meta_json}");
            println!("{json}");
        }
    }

    tracing::info!("benchmark complete");
    Ok(())
}

async fn check_server(addr: &str) -> anyhow::Result<()> {
    let deadline = Instant::now() + Duration::from_secs(30);
    let mut last_err = String::new();
    while Instant::now() < deadline {
        match try_ping(addr).await {
            Ok(()) => return Ok(()),
            Err(e) => {
                last_err = format!("{e:#}");
                tokio::time::sleep(Duration::from_millis(500)).await;
            }
        }
    }
    anyhow::bail!(
        "server at {addr} did not become ready within 30s: {last_err}"
    )
}

async fn try_ping(addr: &str) -> anyhow::Result<()> {
    let mut client = Client::connect(addr).await?;
    client.ping().await?;
    Ok(())
}

fn needs_prefill(workload: bench::Workload) -> bool {
    matches!(
        workload,
        bench::Workload::Read
            | bench::Workload::Mixed
            | bench::Workload::Tail
            | bench::Workload::Bulk
    )
}

fn build_run_configs(
    args: &bench::BenchmarkArgs,
    server_start: Instant,
) -> Vec<bench::SingleRunConfig> {
    let mut configs = Vec::new();
    let mut run_id = 0;

    for &workload in &args.workloads {
        let mut concurrency = args.concurrency_min;
        while concurrency <= args.concurrency_max {
            configs.push(bench::SingleRunConfig {
                addr: args.addr.clone(),
                run_id,
                workload,
                concurrency,
                duration_secs: args.duration,
                sources: args.sources,
                keys: args.keys,
                value_size: args.value_size,
                batch_size: args.batch_size,
                range_fraction: args.range_fraction,
                range_window: args.range_window,
                server_start,
            });
            run_id += 1;
            if concurrency == 1 {
                concurrency += 1
            } else {
                concurrency += args.concurrency_step;
            }
        }
    }

    configs
}

async fn run_single(cfg: &bench::SingleRunConfig) -> anyhow::Result<bench::SingleRunResult> {
    let cfg = Arc::new(cfg.clone());
    let mut clients = Vec::with_capacity(cfg.concurrency as usize);
    for i in 0..cfg.concurrency {
        let c = Client::connect(&cfg.addr.to_string())
            .await
            .with_context(|| format!("client {i} failed to connect"))?;
        clients.push(c);
    }
    if needs_prefill(cfg.workload) {
        prefill(cfg.as_ref(), &mut clients).await?;
    }
    let handles: Vec<tokio::task::JoinHandle<bench::ClientResult>> = clients
        .into_iter()
        .enumerate()
        .map(|(id, c)| tokio::spawn(do_client_work(id, c, cfg.clone())))
        .collect();
    let mut total = bench::ClientResult::default();
    for h in handles {
        total.merge(h.await?);
    }

    Ok(total.to_run_result(&cfg))
}

async fn flush_pending(
    client: &mut Client,
    pending: &mut Vec<proto::Insert>,
    result: &mut bench::ClientResult,
) {
    if pending.is_empty() {
        return;
    }
    let batch_size = pending.len();
    let batch = std::mem::take(pending);
    let t0 = Instant::now();
    match tokio::time::timeout(Duration::from_secs(30), client.batch_insert(batch)).await {
        Ok(Ok(_)) => {
            let elapsed = t0.elapsed().as_secs_f64() * 1_000_000.0;
            let _ = result.insert_latencies.record(elapsed as u64);
            result.inserts_completed += batch_size as u64;
        }
        _ => {
            result.inserts_failed += batch_size as u64;
        }
    }
}

async fn do_client_work(
    id: usize,
    mut client: Client,
    cfg: Arc<bench::SingleRunConfig>,
) -> bench::ClientResult {
    let mut result = bench::ClientResult::default();
    let mut rng = StdRng::seed_from_u64(id as u64);

    let sources: Vec<i64> = (0..cfg.sources as i64).collect();
    let keys: Vec<String> = (0..cfg.keys).map(|i| format!("key_{}", i)).collect();
    let value = "a".repeat(cfg.value_size as usize);

    let phase_start = Instant::now();
    let end = phase_start + Duration::from_secs(cfg.duration_secs as u64);
    let mut insert_count: i64 = 0;
    let mut pending: Vec<proto::Insert> = Vec::with_capacity(cfg.batch_size);

    while Instant::now() < end {
        let elapsed_ms = (Instant::now() - cfg.server_start).as_millis() as i64;

        let should_range = match cfg.workload {
            bench::Workload::Write => false,
            bench::Workload::Read => true,
            bench::Workload::Mixed => rng.random_range(0.0..1.0) < cfg.range_fraction,
            bench::Workload::Tail => rng.random_range(0.0..1.0) < 0.1,
            bench::Workload::Bulk => rng.random_range(0.0..1.0) < 0.5,
        };

        if should_range {
            let source_id = sources[rng.random_range(0..sources.len())];
            let key = keys[rng.random_range(0..keys.len())].clone();
            let range_window_ms = (cfg.range_window as i64) * 1000;
            let end_ts = elapsed_ms;
            let start_ts = (elapsed_ms - range_window_ms).max(0);
            let filter = if rng.random_range(0.0..1.0) < 0.1 {
                "a".to_string()
            } else {
                String::new()
            };

            let t0 = Instant::now();
            match client
                .range(proto::Range {
                    source_id,
                    key,
                    start_ts,
                    end_ts,
                    filter,
                })
                .await
            {
                Ok(records) => {
                    let _ = result
                        .range_latencies
                        .record((t0.elapsed().as_secs_f64() * 1_000_000.0) as u64);
                    result.range_queries_completed += 1;
                    result.records_scanned += records.len() as u64;
                }
                Err(_) => {
                    result.ranges_failed += 1;
                }
            }
        } else {
            let source_id = sources[rng.random_range(0..sources.len())];
            let key = keys[rng.random_range(0..keys.len())].clone();
            let ts = elapsed_ms + insert_count;
            insert_count += 1;

            pending.push(proto::Insert {
                source_id,
                ts,
                key,
                value: value.clone(),
            });

            if pending.len() >= cfg.batch_size {
                flush_pending(&mut client, &mut pending, &mut result).await;
            }
        }
    }

    flush_pending(&mut client, &mut pending, &mut result).await;
    result
}

async fn prefill(cfg: &bench::SingleRunConfig, clients: &mut [Client]) -> anyhow::Result<()> {
    let mut rng = StdRng::seed_from_u64(67_u64);
    let val = "s".repeat(cfg.value_size as usize);
    let srcs: Vec<i64> = (0..cfg.sources as i64).collect();
    let keys: Vec<String> = (0..cfg.keys).map(|i| format!("key_{i}")).collect();
    let phase_start = Instant::now();
    let end = phase_start + Duration::from_secs(3);
    while Instant::now() < end {
        for client in clients.iter_mut() {
            let source_id = srcs[rng.random_range(0..srcs.len())];
            let key = keys[rng.random_range(0..keys.len())].clone();
            let ts = (Instant::now() - cfg.server_start).as_millis() as i64;
            client
                .insert(proto::Insert {
                    source_id,
                    ts,
                    key,
                    value: val.clone(),
                })
                .await?;
        }
    }
    Ok(())
}


