use std::{
    fs,
    path::PathBuf,
    sync::Arc,
    time::{Duration, Instant, SystemTime, UNIX_EPOCH},
};

use anyhow::Context;
use clap::Parser;
use logregator::metrics::Metrics;
use logregator::{
    bench,
    client::Client,
    mem_profile::{MemProfiler, MemSample},
    proto,
    server::Server,
    storage::{
        Engine,
        compaction::{self, CompactionCommand, CompactionResult},
    },
};
use rand::rngs::StdRng;
use rand::{RngExt, SeedableRng};
use tokio::{sync::mpsc, task::JoinHandle};

async fn run_server(
    args: &bench::BenchmarkArgs,
) -> anyhow::Result<(
    Arc<Metrics>,
    PathBuf,
    Instant,
    Option<mpsc::UnboundedReceiver<(usize, usize)>>,
)> {
    let server_start = Instant::now();
    let metrics = Arc::new(Metrics::default());
    let ts = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_millis();
    let data_dir = PathBuf::from("load_tests").join(ts.to_string());

    let (insert_tx, insert_rx) = mpsc::channel::<proto::Command>(args.channel_capacity);
    let (range_tx, range_rx) = mpsc::channel::<proto::Command>(args.channel_capacity);
    let (comp_cmd_tx, comp_cmd_rx) = mpsc::channel::<CompactionCommand>(1);
    let (comp_result_tx, comp_result_rx) = mpsc::channel::<CompactionResult>(1);
    let memtable_limit = args.memtable_mb as usize * 1024 * 1024;

    let stats_rx = if args.profile_mem > 0 {
        let (stats_tx, stats_rx) = mpsc::unbounded_channel();
        let mut engine = Engine::open(
            data_dir.join("storage"),
            memtable_limit,
            comp_cmd_tx,
            comp_result_rx,
        )?;
        engine.set_stats_tx(stats_tx);
        engine.set_metrics(&metrics);
        tokio::spawn(async move { engine.engine_loop(insert_rx, range_rx).await });
        Some(stats_rx)
    } else {
        let mut engine = Engine::open(
            data_dir.join("storage"),
            memtable_limit,
            comp_cmd_tx,
            comp_result_rx,
        )?;
        engine.set_metrics(&metrics);
        tokio::spawn(async move { engine.engine_loop(insert_rx, range_rx).await });
        None
    };
    let comp_metrics = metrics.clone();
    tokio::spawn(async move {
        compaction::compaction_loop(comp_cmd_rx, comp_result_tx, Some(comp_metrics)).await
    });
    let server = Server::new(Some(args.addr.parse().unwrap()))
        .await
        .context("failed to start server")?;
    let svr_metrics = metrics.clone();
    tokio::spawn(async move { server.run_main_loop(insert_tx, range_tx, svr_metrics).await });
    Ok((metrics, data_dir, server_start, stats_rx))
}

fn needs_prefill(workload: bench::WorkloadKind) -> bool {
    matches!(
        workload,
        bench::WorkloadKind::Read
            | bench::WorkloadKind::Mixed
            | bench::WorkloadKind::Tail
            | bench::WorkloadKind::Bulk
    )
}

fn build_run_configs(args: &bench::BenchmarkArgs, server_start: Instant) -> Vec<bench::RunConfig> {
    let mut configs = Vec::new();
    let mut run_id = 0;

    for &workload in &args.workloads {
        let mut concurrency = args.concurrency_min;
        while concurrency <= args.concurrency_max {
            configs.push(bench::RunConfig {
                addr: args.addr.clone(),
                run_id,
                workload,
                concurrency,
                duration_secs: args.duration_secs,
                sources: args.sources,
                keys: args.keys,
                value_size: args.value_size,
                batch_size: args.batch_size,
                channel_capacity: args.channel_capacity,
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

async fn run_single(cfg: &bench::RunConfig) -> anyhow::Result<bench::RunResult> {
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
    let handles: Vec<JoinHandle<bench::ClientResult>> = clients
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
    cfg: Arc<bench::RunConfig>,
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
        // Global monotonic timestamp so cross-workload reads see overlapping data.
        let elapsed_ms = (Instant::now() - cfg.server_start).as_millis() as i64;

        let should_range = match cfg.workload {
            bench::WorkloadKind::Write => false,
            bench::WorkloadKind::Read => true,
            bench::WorkloadKind::Mixed => rng.random_range(0.0..1.0) < cfg.range_fraction,
            bench::WorkloadKind::Tail => rng.random_range(0.0..1.0) < 0.1,
            bench::WorkloadKind::Bulk => rng.random_range(0.0..1.0) < 0.5,
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

async fn prefill(cfg: &bench::RunConfig, clients: &mut [Client]) -> anyhow::Result<()> {
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

fn write_summary_to_stderr(b: &bench::BenchmarkResult, verbose: bool) {
    let args = &b.args;
    let runs = &b.runs;

    eprintln!("=> BENCHMARK <=");
    eprintln!(
        "  Workloads: {}  |  Duration: {}s  |  Concurrency: {}..{} (step {})",
        args.workloads
            .iter()
            .map(|w| format!("{:?}", w).to_lowercase())
            .collect::<Vec<_>>()
            .join(", "),
        args.duration_secs,
        args.concurrency_min,
        args.concurrency_max,
        args.concurrency_step
    );
    eprintln!(
        "  Batch: {}  |  Keys: {}  |  Sources: {}  |  Value: {}  |  Channel: {}",
        args.batch_size,
        args.keys,
        args.sources,
        bench::fmt_bytes(args.value_size as u64),
        bench::fmt_count(args.channel_capacity as u64)
    );
    if args.range_fraction != 0.5 || args.range_window != 60 {
        eprintln!(
            "  Range fraction: {}  |  Range window: {}s",
            args.range_fraction, args.range_window
        );
    }
    eprintln!();

    for r in runs {
        let err_total = r.failed_inserts + r.failed_ranges;
        eprintln!(
            "--> Run {} | {} | {} client{} <--",
            r.run_config.run_id,
            format!("{:?}", r.run_config.workload).to_lowercase(),
            r.run_config.concurrency,
            if r.run_config.concurrency != 1 {
                "s"
            } else {
                ""
            }
        );

        eprintln!(
            "  Inserts: {} ({})  |  Ranges: {} ({})  |  Errors: {}  |  SSTs: {}  |  Batch: {}",
            bench::fmt_count(r.inserts_completed),
            bench::fmt_throughput(r.insert_ops_per_sec),
            bench::fmt_count(r.range_queries_completed),
            bench::fmt_throughput(r.range_ops_per_sec),
            err_total,
            r.sstable_count,
            r.run_config.batch_size
        );

        if r.insert_latency.sample_count > 0 {
            if verbose {
                eprintln!(
                    "  Insert lat:  {}",
                    bench::fmt_latency_line(&r.insert_latency)
                );
            } else {
                eprintln!(
                    "  Insert lat:  min={}  p50={}  p90={}  p99={}  max={}",
                    bench::fmt_latency(r.insert_latency.min_us),
                    bench::fmt_latency(r.insert_latency.p50_us),
                    bench::fmt_latency(r.insert_latency.p90_us),
                    bench::fmt_latency(r.insert_latency.p99_us),
                    bench::fmt_latency(r.insert_latency.max_us)
                );
            }
        }

        if r.range_latency.sample_count > 0 {
            if verbose {
                eprintln!(
                    "  Range lat:   {}",
                    bench::fmt_latency_line(&r.range_latency)
                );
            } else {
                eprintln!(
                    "  Range lat:   min={}  p50={}  p90={}  p99={}  max={}",
                    bench::fmt_latency(r.range_latency.min_us),
                    bench::fmt_latency(r.range_latency.p50_us),
                    bench::fmt_latency(r.range_latency.p90_us),
                    bench::fmt_latency(r.range_latency.p99_us),
                    bench::fmt_latency(r.range_latency.max_us)
                );
            }
        }

        if r.avg_records_per_range > 0.0 {
            eprintln!("  Records/range: {:.1}", r.avg_records_per_range);
        }
    }

    // Saturation per workload
    for s in &b.saturations {
        eprintln!();
        let concs: Vec<u32> = s.by_concurrency.iter().map(|&(c, _)| c).collect();
        eprintln!("--> Saturation ({:?}) <--", s.workload);
        eprintln!(
            "  Max throughput: {} at {} clients",
            bench::fmt_throughput(s.max_throughput),
            s.max_concurrency
        );
        if s.saturation_detected {
            for sp in &s.saturation_points {
                eprintln!(
                    "  Drop: {} → {} going from {} → {} clients",
                    bench::fmt_throughput(sp.from_throughput),
                    bench::fmt_throughput(sp.to_throughput),
                    sp.from_concurrency,
                    sp.to_concurrency
                );
            }
        } else {
            eprintln!(
                "  No saturation detected ({}..{} clients)",
                concs.first().copied().unwrap_or(0),
                concs.last().copied().unwrap_or(0)
            );
        }
    }

    // Metrics snapshot
    eprintln!();
    eprintln!("--> Server Metrics (post-run) <--");
    let m = &b.metrics;
    let eng = &m.engine;
    let wal = &m.wal;
    let svr = &m.server;
    eprintln!(
        "  Connections: accepted={}  active={}  frames={}",
        svr.connections_accepted, svr.connections_active, svr.frames_read
    );
    eprintln!(
        "  WAL: {} writes ({}), {} syncs (p50={})",
        bench::fmt_count(wal.write_count),
        bench::fmt_bytes(wal.write_bytes),
        wal.sync_count,
        wal.sync_latency
            .as_ref()
            .map_or("N/A".into(), |s| bench::fmt_latency(s.p50 as f64))
    );
    eprintln!(
        "  Engine: {} records ({} insert cmds), {} flushes, {} compactions, {} SSTs",
        bench::fmt_count(eng.records_inserted),
        eng.batch_insert_count,
        eng.flush_count,
        eng.compaction_count,
        eng.sstable_count
    );
    eprintln!(
        "  Memtable: {} / {}",
        bench::fmt_bytes(eng.memtable_bytes as u64),
        bench::fmt_bytes(eng.memtable_limit as u64)
    );

    if verbose {
        if let Some(ref sl) = wal.sync_latency {
            eprintln!();
            eprintln!(
                "  WAL sync lat:  {}",
                bench::fmt_latency_line(&bench::LatencyStats {
                    min_us: sl.min as f64,
                    p50_us: sl.p50 as f64,
                    p90_us: sl.p90 as f64,
                    p95_us: sl.p95 as f64,
                    p99_us: sl.p99 as f64,
                    p99_9_us: sl.p99_9 as f64,
                    max_us: sl.max as f64,
                    mean_us: sl.mean as f64,
                    stddev_us: 0.0,
                    sample_count: sl.count as usize,
                })
            );
        }
        if let Some(ref bl) = eng.batch_insert_latency {
            eprintln!(
                "  Batch insert lat (server):  {}",
                bench::fmt_latency_line(&bench::LatencyStats {
                    min_us: bl.min as f64,
                    p50_us: bl.p50 as f64,
                    p90_us: bl.p90 as f64,
                    p95_us: bl.p95 as f64,
                    p99_us: bl.p99 as f64,
                    p99_9_us: bl.p99_9 as f64,
                    max_us: bl.max as f64,
                    mean_us: bl.mean as f64,
                    stddev_us: 0.0,
                    sample_count: bl.count as usize,
                })
            );
        }
    }

    // Memory
    if let Some(ref mem) = b.mem {
        eprintln!();
        eprintln!("--> Memory Profile <--");
        let rss_start_kb = mem.rss_values.first().copied().unwrap_or(0);
        let rss_peak_kb = mem.rss_max;
        let rss_end_kb = mem.rss_last;
        eprintln!(
            "  RSS: {} (start) → {} (peak) → {} (end)  |  Grow: {:.1}KB/s  |  Samples: {}",
            bench::fmt_bytes(rss_start_kb * 1024),
            bench::fmt_bytes(rss_peak_kb * 1024),
            bench::fmt_bytes(rss_end_kb * 1024),
            mem.growth_rate_kb,
            mem.sample_size
        );
    }
    eprintln!();
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let args = bench::BenchmarkArgs::parse();
    let verbose = args.verbose;

    let profiler = if args.profile_mem > 0 {
        Some(MemProfiler::start(Duration::from_millis(args.profile_mem)))
    } else {
        None
    };

    let (metrics, data_dir, server_start, mut stats_rx) =
        run_server(&args).await.expect("failed to start server");
    let configs = build_run_configs(&args, server_start);
    let mut results = Vec::with_capacity(configs.len());
    let storage_dir = data_dir.join("storage");

    // background engine stats collector
    let (em_tx, em_rx) = std::sync::mpsc::channel();
    if let Some(mut rx) = stats_rx.take() {
        tokio::spawn(async move {
            let start = Instant::now();
            while let Some((mt, sst)) = rx.recv().await {
                let elapsed = start.elapsed().as_secs_f64();
                if em_tx.send((elapsed, mt, sst)).is_err() {
                    break;
                }
            }
        });
    }

    let total = configs.len();
    for (idx, config) in configs.iter().enumerate() {
        let conc_label = if config.concurrency != 1 { "s" } else { "" };
        eprintln!(
            "[{}/{}] {} | {}-client{} ({}s) ...",
            idx + 1,
            total,
            format!("{:?}", config.workload).to_lowercase(),
            config.concurrency,
            conc_label,
            config.duration_secs
        );

        let t0 = Instant::now();
        let mut result = run_single(config).await?;
        let elapsed = t0.elapsed();

        if storage_dir.exists() {
            result.sstable_count = fs::read_dir(&storage_dir)
                .into_iter()
                .flatten()
                .filter_map(Result::ok)
                .filter(|e| {
                    e.path()
                        .extension()
                        .map(|ext| ext == "sst")
                        .unwrap_or(false)
                })
                .count();
        }

        let ins = bench::fmt_count(result.inserts_completed);
        let rng = bench::fmt_count(result.range_queries_completed);
        eprintln!(
            "  ✓ {} inserts, {} ranges ({:.1}s)",
            ins,
            rng,
            elapsed.as_secs_f64()
        );

        results.push(result);
        tokio::time::sleep(Duration::from_secs(2)).await;
    }

    let metrics_snapshot = metrics.snapshot();

    let mem_samples: Vec<MemSample> = if let Some(p) = profiler {
        p.stop().await
    } else {
        Vec::new()
    };
    let engine_samples: Vec<(f64, usize, usize)> = em_rx.try_iter().collect();
    let mem_report = MemProfiler::new_report(&mem_samples, &engine_samples);

    let saturations = bench::compute_saturations(&results);
    let benchmark = bench::BenchmarkResult {
        args,
        runs: results,
        metrics: metrics_snapshot,
        saturations,
        mem: mem_report,
    };

    // Human-readable summary to stderr
    write_summary_to_stderr(&benchmark, verbose);

    // Always write JSON to prof/json/<ts>_<workload>.json, and optionally to --out
    let json =
        serde_json::to_string_pretty(&benchmark).context("failed to serialize benchmark result")?;

    let workload_suffix = benchmark
        .args
        .workloads
        .iter()
        .map(|w| format!("{:?}", w).to_lowercase())
        .collect::<Vec<_>>()
        .join("_");

    let ts = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_millis();
    let default_path = match benchmark.args.tag {
        Some(tag) => {
            format!("prof/json/{ts}_{workload_suffix}__{tag}.json")
        }
        None => {
            format!("prof/json/{ts}_{workload_suffix}.json")
        }
    };
    fs::write(&default_path, &json)
        .with_context(|| format!("failed to write default output file {}", &default_path))?;

    if let Some(outname) = benchmark.args.out {
        fs::write(&outname, &json)
            .with_context(|| format!("failed to write custom output file {}", &outname))?;
    }

    Ok(())
}
