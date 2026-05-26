use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};
use std::{fs, io};

use anyhow::Context;
use clap::{Args, Parser, Subcommand};
use logregator::bench::{self, BenchmarkResult, LatencyStats, RunResult, WorkloadKind};
use logregator::mem_profile::{self, MemProfiler, MemSample};
use logregator::metrics::{self, Metrics};
use logregator::{
    client::Client,
    proto,
    server::Server,
    storage::{
        Engine,
        compaction::{self, CompactionCommand, CompactionResult},
    },
};
use owo_colors::OwoColorize;
use rand::rngs::StdRng;
use rand::{RngExt, SeedableRng};
use serde::Serialize;
use tokio::{sync::mpsc, task::JoinHandle};

#[derive(Parser)]
#[command(name = "benchmark")]
struct Cli {
    #[command(subcommand)]
    command: Command,
}

#[derive(Subcommand)]
enum Command {
    #[clap(about = "Run benchmark load generator")]
    Loadgen(LoadgenArgs),
    #[clap(about = "Compare two or more benchmark result JSON files")]
    Cmp(CmpArgs),
    #[clap(about = "Generate a human-readable report from a benchmark JSON file")]
    Report(ReportArgs),
}

#[derive(Args)]
struct LoadgenArgs {
    #[command(flatten)]
    inner: bench::BenchmarkArgs,
}

#[derive(Args, Serialize)]
struct CmpArgs {
    #[arg(short, long)]
    targets: Vec<String>,
    #[arg(short, long)]
    baseline: String,
}

#[derive(Args)]
struct ReportArgs {
    /// Path to benchmark JSON file
    json: String,
    #[arg(short, long)]
    verbose: bool,
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let cli = Cli::parse();
    match cli.command {
        Command::Loadgen(args) => loadgen::run(args).await,
        Command::Cmp(args) => cmp::run(args),
        Command::Report(args) => report::run(args),
    }
}

/// human-readable summary
fn write_summary<W: io::Write>(mut w: W, b: &BenchmarkResult, verbose: bool) {
    let args = &b.args;
    let runs = &b.runs;

    writeln!(w, "{}", "--- BENCHMARK ---".cyan());
    writeln!(
        w,
        "  {:<12} {}  |  {:<12} {}s  |  {:<12} {}..{} (step {})",
        "WORKLOADS:",
        args.workloads
            .iter()
            .map(|w| format!("{:?}", w).to_lowercase())
            .collect::<Vec<_>>()
            .join(", "),
        "DURATION:",
        args.duration_secs,
        "CONCURRENCY:",
        args.concurrency_min,
        args.concurrency_max,
        args.concurrency_step
    );
    writeln!(
        w,
        "  {:<12} {}  |  {:<12} {}  |  {:<12} {}  |  {:<12} {}  |  {:<12} {}",
        "BATCH:",
        args.batch_size,
        "KEYS:",
        args.keys,
        "SOURCES:",
        args.sources,
        "VALUE:",
        bench::fmt_bytes(args.value_size as u64),
        "CHANNEL:",
        bench::fmt_count(args.channel_capacity as u64)
    );
    if args.range_fraction != 0.5 || args.range_window != 60 {
        writeln!(
            w,
            "  {:<12} {}  |  {:<12} {}s",
            "RANGE FRAC:", args.range_fraction, "RANGE WINDOW:", args.range_window
        );
    }
    writeln!(w);

    for r in runs {
        let conc_label = if r.run_config.concurrency != 1 {
            "S"
        } else {
            ""
        };
        let title = format!(
            "--- RUN {:02} | {} | {} CLIENT{} ---",
            r.run_config.run_id,
            format!("{:?}", r.run_config.workload).to_uppercase(),
            r.run_config.concurrency,
            conc_label,
        );
        writeln!(w, "{}", title.cyan());

        let err_total = r.failed_inserts + r.failed_ranges;
        let ins = bench::fmt_count(r.inserts_completed);
        let ins_s = bench::fmt_throughput(r.insert_ops_per_sec);
        let rng = bench::fmt_count(r.range_queries_completed);
        let rng_s = bench::fmt_throughput(r.range_ops_per_sec);

        // ops header + data
        writeln!(
            w,
            "  {:>12} {:>12} {:>12} {:>12} {:>8} {:>8} {:>8}",
            "INSERTS", "INSERTS/S", "RANGES", "RANGES/S", "ERRORS", "SSTS", "BATCH"
        );
        writeln!(
            w,
            "  {:>12} {:>12} {:>12} {:>12} {:>8} {:>8} {:>8}",
            ins, ins_s, rng, rng_s, err_total, r.sstable_count, r.run_config.batch_size
        );

        if r.insert_latency.sample_count > 0 {
            if verbose {
                writeln!(
                    w,
                    "  INSERT LAT (MS):  {}",
                    bench::fmt_latency_line(&r.insert_latency)
                );
            } else {
                writeln!(
                    w,
                    "  INSERT LAT (MS):  MIN={}  P50={}  P90={}  P99={}  MAX={}",
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
                writeln!(
                    w,
                    "  RANGE LAT (MS):  {}",
                    bench::fmt_latency_line(&r.range_latency)
                );
            } else {
                writeln!(
                    w,
                    "  RANGE LAT (MS):  MIN={}  P50={}  P90={}  P99={}  MAX={}",
                    bench::fmt_latency(r.range_latency.min_us),
                    bench::fmt_latency(r.range_latency.p50_us),
                    bench::fmt_latency(r.range_latency.p90_us),
                    bench::fmt_latency(r.range_latency.p99_us),
                    bench::fmt_latency(r.range_latency.max_us)
                );
            }
        }

        if r.avg_records_per_range > 0.0 {
            writeln!(w, "  {:>12}  {:.1}", "REC/RANGE:", r.avg_records_per_range);
        }
    }

    for s in &b.saturations {
        writeln!(w);
        let concs: Vec<u32> = s.by_concurrency.iter().map(|&(c, _)| c).collect();
        let title = format!("--- SATURATION ({:?}) ---", s.workload);
        writeln!(w, "{}", title.cyan());
        writeln!(
            w,
            "  {:>12}  {} at {} clients",
            "PEAK THRPUT:",
            bench::fmt_throughput(s.max_throughput),
            s.max_concurrency
        );
        if s.saturation_detected {
            for sp in &s.saturation_points {
                writeln!(
                    w,
                    "  {:>12}  {} -> {} from {} -> {} clients",
                    "DROP:",
                    bench::fmt_throughput(sp.from_throughput),
                    bench::fmt_throughput(sp.to_throughput),
                    sp.from_concurrency,
                    sp.to_concurrency
                );
            }
        } else {
            writeln!(
                w,
                "  {:>12}  {}..{} clients",
                "NONE:",
                concs.first().copied().unwrap_or(0),
                concs.last().copied().unwrap_or(0)
            );
        }
    }

    writeln!(w);
    writeln!(w, "{}", "--- SERVER METRICS ---".cyan());
    let m = &b.metrics;
    let eng = &m.engine;
    let wal = &m.wal;
    let svr = &m.server;
    writeln!(
        w,
        "  {:>12}  accepted={}  active={}  frames={}",
        "CONNECTIONS:", svr.connections_accepted, svr.connections_active, svr.frames_read
    );
    writeln!(
        w,
        "  {:>12}  {} writes ({}), {} syncs (p50={})",
        "WAL:",
        bench::fmt_count(wal.write_count),
        bench::fmt_bytes(wal.write_bytes),
        wal.sync_count,
        wal.sync_latency
            .as_ref()
            .map_or("N/A".into(), |s| bench::fmt_latency(s.p50 as f64))
    );
    writeln!(
        w,
        "  {:>12}  {} records ({} cmds), {} flushes, {} compactions, {} SSTs",
        "ENGINE:",
        bench::fmt_count(eng.records_inserted),
        eng.batch_insert_count,
        eng.flush_count,
        eng.compaction_count,
        eng.sstable_count
    );
    writeln!(
        w,
        "  {:>12}  {} / {}",
        "MEMTABLE:",
        bench::fmt_bytes(eng.memtable_bytes as u64),
        bench::fmt_bytes(eng.memtable_limit as u64)
    );

    if verbose {
        if let Some(ref sl) = wal.sync_latency {
            writeln!(w,);
            writeln!(
                w,
                "  WAL SYNC LAT (MS):  {}",
                bench::fmt_latency_line(&LatencyStats {
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
            writeln!(
                w,
                "  BATCH INSERT LAT (MS):  {}",
                bench::fmt_latency_line(&LatencyStats {
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

    if let Some(ref mem) = b.mem {
        writeln!(w);
        writeln!(w, "{}", "--- MEMORY PROFILE ---".cyan());
        let rss_start_kb = mem.rss_values.first().copied().unwrap_or(0);
        let rss_peak_kb = mem.rss_max;
        let rss_end_kb = mem.rss_last;
        writeln!(
            w,
            "  {:>12}  {} (start) -> {} (peak) -> {} (end)",
            "RSS:",
            bench::fmt_bytes(rss_start_kb * 1024),
            bench::fmt_bytes(rss_peak_kb * 1024),
            bench::fmt_bytes(rss_end_kb * 1024),
        );
        writeln!(w, "  {:>12}  {:.1}KB/s", "GROWTH:", mem.growth_rate_kb,);
        writeln!(w, "  {:>12}  {}", "SAMPLES:", mem.sample_size,);
    }
    writeln!(w);
}

mod loadgen {
    use logregator::storage::Backend;

    use super::*;

    pub async fn run(args: LoadgenArgs) -> anyhow::Result<()> {
        let args = args.inner;
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
        let benchmark = BenchmarkResult {
            args,
            runs: results,
            metrics: metrics_snapshot,
            saturations,
            mem: mem_report,
        };

        let stderr = std::io::stderr().lock();
        write_summary(stderr, &benchmark, verbose);

        let json = serde_json::to_string_pretty(&benchmark)
            .context("failed to serialize benchmark result")?;

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

        let (cmd_sender, cmd_recv) = mpsc::channel::<proto::Command>(args.channel_capacity);
        let (comp_cmd_sender, comp_cmd_recv) = mpsc::channel::<CompactionCommand>(1);
        let (comp_result_sender, comp_result_recv) = mpsc::channel::<CompactionResult>(1);
        let memtable_limit = args.memtable_mb as usize * 1024 * 1024;
        let mut engine = Engine::open(data_dir.join("storage"), memtable_limit, comp_cmd_sender.clone())?;

        let stats_rx = if args.profile_mem > 0 {
            let (stats_tx, stats_rx) = mpsc::unbounded_channel();
            engine.set_stats_tx(stats_tx);
            Some(stats_rx)
        } else {
            None
        };
        let mut backend = Backend::new(engine, comp_cmd_sender, comp_result_recv);
        tokio::spawn(async move {
            backend.engine_loop(cmd_recv).await
        });

        let comp_metrics = metrics.clone();
        tokio::spawn(async move {
            compaction::compaction_loop(comp_cmd_recv, comp_result_sender, Some(comp_metrics)).await
        });
        let server = Server::new(Some(args.addr.parse().unwrap()))
            .await
            .context("failed to start server")?;
        let svr_metrics = metrics.clone();
        tokio::spawn(async move { server.run_main_loop(cmd_sender, svr_metrics).await });
        Ok((metrics, data_dir, server_start, stats_rx))
    }

    fn needs_prefill(workload: WorkloadKind) -> bool {
        matches!(
            workload,
            WorkloadKind::Read | WorkloadKind::Mixed | WorkloadKind::Tail | WorkloadKind::Bulk
        )
    }

    fn build_run_configs(
        args: &bench::BenchmarkArgs,
        server_start: Instant,
    ) -> Vec<bench::RunConfig> {
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

    async fn run_single(cfg: &bench::RunConfig) -> anyhow::Result<RunResult> {
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
            let elapsed_ms = (Instant::now() - cfg.server_start).as_millis() as i64;

            let should_range = match cfg.workload {
                WorkloadKind::Write => false,
                WorkloadKind::Read => true,
                WorkloadKind::Mixed => rng.random_range(0.0..1.0) < cfg.range_fraction,
                WorkloadKind::Tail => rng.random_range(0.0..1.0) < 0.1,
                WorkloadKind::Bulk => rng.random_range(0.0..1.0) < 0.5,
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
}

// ── cmp subcommand ──────────────────────────────────────────────────────

mod cmp {
    use super::*;

    pub fn run(args: super::CmpArgs) -> anyhow::Result<()> {
        let baseline = parse_benchmark(&args.baseline);
        let n_targets = args.targets.len();

        let target_results: Vec<BenchmarkResult> =
            args.targets.iter().map(|t| parse_benchmark(t)).collect();

        let target_indices: Vec<HashMap<ConcWorkload, &RunResult>> = target_results
            .iter()
            .map(|r| build_workload_index(&r.runs))
            .collect();

        let mut cmps: Vec<RunCmp> = vec![];

        for (cw, run_b) in build_workload_index(&baseline.runs) {
            for ti in &target_indices {
                if let Some(run_t) = ti.get(&cw) {
                    cmps.push(cmp_runs(run_b, run_t));
                }
            }
        }

        cmps.sort_by(|a, b| {
            let ord = |(conc, wl): (u32, WorkloadKind)| match wl {
                WorkloadKind::Write => (0, conc),
                WorkloadKind::Read => (1, conc),
                WorkloadKind::Mixed => (2, conc),
                WorkloadKind::Tail => (3, conc),
                WorkloadKind::Bulk => (4, conc),
            };
            ord((a.cw.concurrency, a.cw.workload)).cmp(&ord((b.cw.concurrency, b.cw.workload)))
        });

        let metrics: Vec<FieldCmp> = if n_targets > 0 {
            let last = &target_results[n_targets - 1];
            cmp_metrics(&baseline.metrics, &last.metrics)
        } else {
            vec![]
        };

        let saturations: Vec<FieldCmp> = if n_targets > 0 {
            let last = &target_results[n_targets - 1];
            cmp_saturations(&baseline.saturations, &last.saturations)
        } else {
            vec![]
        };

        #[allow(clippy::unnecessary_unwrap)]
        let mem: Vec<FieldCmp> = if n_targets > 0
            && baseline.mem.is_some()
            && target_results[n_targets - 1].mem.is_some()
        {
            cmp_mem(
                baseline.mem.as_ref().unwrap(),
                target_results[n_targets - 1].mem.as_ref().unwrap(),
            )
        } else {
            vec![]
        };

        let result = CmpResult {
            files: {
                let mut v = vec![args.baseline];
                v.extend(args.targets);
                v
            },
            comparisons: cmps,
            saturations,
            metrics,
            mem,
        };

        let stdout = std::io::stdout().lock();
        result
            .write_to(stdout)
            .expect("failed to write comparison results");

        Ok(())
    }

    #[derive(Debug, Serialize, Hash, PartialEq, Eq)]
    struct ConcWorkload {
        concurrency: u32,
        workload: WorkloadKind,
    }

    #[derive(Debug, Serialize)]
    struct FieldCmp {
        key: String,
        new: f64,
        prev: f64,
        delta: f64,
        delta_pct: f64,
        is_latency: bool,
        direction: ChangeDirection,
    }

    #[derive(Debug, Serialize)]
    enum ChangeDirection {
        HigherBetter,
        LowerBetter,
        Neutral,
    }

    #[derive(Debug, Serialize)]
    struct RunCmp {
        cw: ConcWorkload,
        fields: Vec<FieldCmp>,
    }

    #[derive(Debug, Serialize)]
    struct CmpResult {
        files: Vec<String>,
        comparisons: Vec<RunCmp>,
        saturations: Vec<FieldCmp>,
        metrics: Vec<FieldCmp>,
        mem: Vec<FieldCmp>,
    }

    impl CmpResult {
        fn write_to<W: std::io::Write>(&self, mut w: W) -> std::io::Result<()> {
            let fcount = self.files.len();
            writeln!(
                w,
                "=> cmp: {} vs {} target(s) <=",
                &self.files[0],
                fcount - 1
            )?;

            let hdr = |w: &mut W| -> std::io::Result<()> {
                writeln!(
                    w,
                    "  {:22} {:>12} {:>12} {:>12} {:>10}",
                    "", "BASELINE", "TARGET", "DELTA", "DELTA_PCT"
                )
            };

            for comp in &self.comparisons {
                let conc_label = if comp.cw.concurrency != 1 {
                    " clients"
                } else {
                    " client"
                };
                writeln!(
                    w,
                    "--- {} | {}{} ---",
                    format!("{:?}", comp.cw.workload).to_lowercase(),
                    comp.cw.concurrency,
                    conc_label,
                )?;
                hdr(&mut w)?;

                for f in &comp.fields {
                    if f.prev == 0.0 && f.new == 0.0 {
                        continue;
                    }
                    let (prev_s, new_s) = fmt_val(f);
                    let prev_s = colored_val(&prev_s, 12, true);
                    let new_s = colored_val(&new_s, 12, false);
                    writeln!(
                        w,
                        "  {key:<22} {prev} {new} {d:>12} {p:>10}",
                        key = f.key,
                        prev = prev_s,
                        new = new_s,
                        d = colored_delta(f),
                        p = colored_pct(f),
                    )?;
                }
            }

            if !self.metrics.is_empty() {
                writeln!(w, "\n--- Aggregate Metrics ---")?;
                hdr(&mut w)?;
                for f in &self.metrics {
                    if f.prev == 0.0 && f.new == 0.0 {
                        continue;
                    }
                    let (prev_s, new_s) = fmt_val(f);
                    let prev_s = colored_val(&prev_s, 12, true);
                    let new_s = colored_val(&new_s, 12, false);
                    writeln!(
                        w,
                        "  {key:<22} {prev} {new} {d:>12} {p:>10}",
                        key = f.key,
                        prev = prev_s,
                        new = new_s,
                        d = colored_delta(f),
                        p = colored_pct(f),
                    )?;
                }
            }

            if !self.saturations.is_empty() {
                writeln!(w, "\n--- Saturations ---")?;
                hdr(&mut w)?;
                for f in &self.saturations {
                    if f.prev == 0.0 && f.new == 0.0 {
                        continue;
                    }
                    let (prev_s, new_s) = fmt_val(f);
                    let prev_s = colored_val(&prev_s, 12, true);
                    let new_s = colored_val(&new_s, 12, false);
                    writeln!(
                        w,
                        "  {key:<22} {prev} {new} {d:>12} {p:>10}",
                        key = f.key,
                        prev = prev_s,
                        new = new_s,
                        d = colored_delta(f),
                        p = colored_pct(f),
                    )?;
                }
            }

            if !self.mem.is_empty() {
                writeln!(w, "\n--- Memory Profile ---")?;
                hdr(&mut w)?;
                for f in &self.mem {
                    let (prev_s, new_s) = fmt_val(f);
                    let prev_s = colored_val(&prev_s, 12, true);
                    let new_s = colored_val(&new_s, 12, false);
                    writeln!(
                        w,
                        "  {key:<22} {prev} {new} {d:>12} {p:>10}",
                        key = f.key,
                        prev = prev_s,
                        new = new_s,
                        d = colored_delta(f),
                        p = colored_pct(f),
                    )?;
                }
            }

            Ok(())
        }
    }

    fn parse_benchmark(path: &str) -> BenchmarkResult {
        let f = fs::OpenOptions::new()
            .read(true)
            .open(path)
            .expect("failed to open benchmark file");

        serde_json::from_reader(f).expect("failed to deserialize benchmark result from file")
    }

    fn build_workload_index(results: &[RunResult]) -> HashMap<ConcWorkload, &RunResult> {
        let mut map = HashMap::with_capacity(results.len());
        for res in results.iter() {
            let conc = ConcWorkload {
                workload: res.run_config.workload,
                concurrency: res.run_config.concurrency,
            };
            map.insert(conc, res);
        }
        map
    }

    #[allow(clippy::vec_init_then_push)]
    fn cmp_runs(b: &RunResult, t: &RunResult) -> RunCmp {
        use ChangeDirection::*;
        let mut fields = vec![];

        fields.push(to_field_cmp(
            b.insert_ops_per_sec,
            t.insert_ops_per_sec,
            "inserts/s",
            false,
            HigherBetter,
        ));
        fields.push(to_field_cmp(
            b.range_ops_per_sec,
            t.range_ops_per_sec,
            "ranges/s",
            false,
            HigherBetter,
        ));
        fields.push(to_field_cmp(
            b.avg_records_per_range,
            t.avg_records_per_range,
            "records/range",
            false,
            HigherBetter,
        ));
        fields.push(to_field_cmp(
            b.sstable_count,
            t.sstable_count,
            "SSTs",
            false,
            Neutral,
        ));

        let il = &b.insert_latency;
        let tl = &t.insert_latency;
        fields.push(to_field_cmp(
            il.min_us,
            tl.min_us,
            "insert_latency min",
            true,
            LowerBetter,
        ));
        fields.push(to_field_cmp(
            il.p50_us,
            tl.p50_us,
            "insert_latency p50",
            true,
            LowerBetter,
        ));
        fields.push(to_field_cmp(
            il.p90_us,
            tl.p90_us,
            "insert_latency p90",
            true,
            LowerBetter,
        ));
        fields.push(to_field_cmp(
            il.p99_us,
            tl.p99_us,
            "insert_latency p99",
            true,
            LowerBetter,
        ));
        fields.push(to_field_cmp(
            il.max_us,
            tl.max_us,
            "insert_latency max",
            true,
            LowerBetter,
        ));

        let il = &b.range_latency;
        let tl = &t.range_latency;
        fields.push(to_field_cmp(
            il.min_us,
            tl.min_us,
            "range_latency min",
            true,
            LowerBetter,
        ));
        fields.push(to_field_cmp(
            il.p50_us,
            tl.p50_us,
            "range_latency p50",
            true,
            LowerBetter,
        ));
        fields.push(to_field_cmp(
            il.p90_us,
            tl.p90_us,
            "range_latency p90",
            true,
            LowerBetter,
        ));
        fields.push(to_field_cmp(
            il.p99_us,
            tl.p99_us,
            "range_latency p99",
            true,
            LowerBetter,
        ));
        fields.push(to_field_cmp(
            il.max_us,
            tl.max_us,
            "range_latency max",
            true,
            LowerBetter,
        ));

        RunCmp {
            cw: ConcWorkload {
                concurrency: b.run_config.concurrency,
                workload: b.run_config.workload,
            },
            fields,
        }
    }

    fn cmp_metrics(b: &metrics::MetricsSnapshot, t: &metrics::MetricsSnapshot) -> Vec<FieldCmp> {
        use ChangeDirection::*;
        let mut fields = vec![];

        let b_sync = b.wal.sync_latency.as_ref();
        let t_sync = t.wal.sync_latency.as_ref();
        let b_p50 = b_sync.map(|s| s.p50 as f64).unwrap_or(0.0);
        let t_p50 = t_sync.map(|s| s.p50 as f64).unwrap_or(0.0);
        fields.push(to_field_cmp(
            b_p50,
            t_p50,
            "wal_sync p50 (us)",
            true,
            LowerBetter,
        ));
        fields.push(to_field_cmp(
            b.wal.write_count,
            t.wal.write_count,
            "wal_write_count",
            false,
            Neutral,
        ));
        fields.push(to_field_cmp(
            b.wal.write_bytes,
            t.wal.write_bytes,
            "wal_write_bytes",
            false,
            Neutral,
        ));
        fields.push(to_field_cmp(
            b.wal.sync_count,
            t.wal.sync_count,
            "wal_sync_count",
            false,
            Neutral,
        ));

        fields.push(to_field_cmp(
            b.engine.flush_count,
            t.engine.flush_count,
            "engine_flush_count",
            false,
            LowerBetter,
        ));
        fields.push(to_field_cmp(
            b.engine.compaction_count,
            t.engine.compaction_count,
            "engine_compactions",
            false,
            LowerBetter,
        ));
        fields.push(to_field_cmp(
            b.engine.records_inserted,
            t.engine.records_inserted,
            "engine_records",
            false,
            Neutral,
        ));
        fields.push(to_field_cmp(
            b.engine.memtable_bytes,
            t.engine.memtable_bytes,
            "engine_memtable_bytes",
            false,
            LowerBetter,
        ));
        fields.push(to_field_cmp(
            b.engine.sstable_count,
            t.engine.sstable_count,
            "engine_sst_count",
            false,
            Neutral,
        ));

        fields.push(to_field_cmp(
            b.server.connections_accepted,
            t.server.connections_accepted,
            "server_conns_accepted",
            false,
            Neutral,
        ));
        fields.push(to_field_cmp(
            b.server.frames_read,
            t.server.frames_read,
            "server_frames_read",
            false,
            Neutral,
        ));
        fields.push(to_field_cmp(
            b.server.frames_written,
            t.server.frames_written,
            "server_frames_written",
            false,
            Neutral,
        ));

        fields
    }

    fn cmp_saturations(b: &[bench::Saturation], t: &[bench::Saturation]) -> Vec<FieldCmp> {
        use ChangeDirection::*;
        let mut fields = vec![];

        for b_sat in b {
            let label = format!("{:?}", b_sat.workload).to_lowercase();
            if let Some(t_sat) = t.iter().find(|s| s.workload == b_sat.workload) {
                fields.push(to_field_cmp(
                    b_sat.max_throughput,
                    t_sat.max_throughput,
                    format!("sat_{}_throughput", label),
                    false,
                    HigherBetter,
                ));
                fields.push(to_field_cmp(
                    b_sat.max_concurrency as f64,
                    t_sat.max_concurrency as f64,
                    format!("sat_{}_conc_peak", label),
                    false,
                    HigherBetter,
                ));
            }
        }

        fields
    }

    fn cmp_mem(b: &mem_profile::MemReport, t: &mem_profile::MemReport) -> Vec<FieldCmp> {
        use ChangeDirection::*;
        vec![
            to_field_cmp(b.rss_min, t.rss_min, "mem_rss_min_kb", false, LowerBetter),
            to_field_cmp(b.rss_max, t.rss_max, "mem_rss_max_kb", false, LowerBetter),
            to_field_cmp(b.rss_avg, t.rss_avg, "mem_rss_avg_kb", false, LowerBetter),
            to_field_cmp(b.rss_sum, t.rss_sum, "mem_rss_sum_kb", false, LowerBetter),
            to_field_cmp(b.rss_last, t.rss_last, "mem_rss_end_kb", false, LowerBetter),
            to_field_cmp(
                b.growth_rate_kb,
                t.growth_rate_kb,
                "mem_growth_kbs",
                false,
                LowerBetter,
            ),
            to_field_cmp(
                b.growth_rate_mb,
                t.growth_rate_mb,
                "mem_growth_mbs",
                false,
                LowerBetter,
            ),
            to_field_cmp(
                b.sample_size,
                t.sample_size,
                "mem_sample_size",
                false,
                Neutral,
            ),
        ]
    }

    trait AsF64 {
        fn as_f64(&self) -> f64;
    }

    macro_rules! impl_field_cmp {
        ($($ty:ty),* $(,)?) => {
            $(impl AsF64 for $ty {
                fn as_f64(&self) -> f64 {
                    *self as f64
                }
            })*
        };
    }

    impl_field_cmp!(u64, i64, isize, usize, f64);

    fn to_field_cmp<N, S>(
        prev: N,
        new: N,
        key: S,
        is_latency: bool,
        dir: ChangeDirection,
    ) -> FieldCmp
    where
        N: AsF64,
        S: Into<String>,
    {
        let prev = prev.as_f64();
        let new = new.as_f64();
        let delta = new - prev;
        let delta_pct = if prev != 0.0 {
            (delta / prev.abs()) * 100.0
        } else {
            if delta == 0.0 { 0.0 } else { f64::INFINITY }
        };
        FieldCmp {
            key: key.into(),
            prev,
            new,
            delta,
            delta_pct,
            is_latency,
            direction: dir,
        }
    }

    fn fmt_val(cmp: &FieldCmp) -> (String, String) {
        if cmp.is_latency {
            (fmt_latency(cmp.prev), fmt_latency(cmp.new))
        } else {
            (fmt_count(cmp.prev), fmt_count(cmp.new))
        }
    }

    fn colored_val(s: &str, width: usize, is_baseline: bool) -> String {
        let padded = format!("{s:>width$}");
        if is_baseline {
            padded.cyan().to_string()
        } else {
            padded.yellow().to_string()
        }
    }

    fn fmt_latency(us: f64) -> String {
        if us >= 10_000_000.0 {
            format!("{:.2}s", us / 1_000_000.0)
        } else if us >= 1_000.0 {
            format!("{:.2}ms", us / 1_000.0)
        } else {
            format!("{:.3}ms", us / 1_000.0)
        }
    }

    fn fmt_count(v: f64) -> String {
        if v >= 1_000_000.0 {
            format!("{:.2}M", v / 1_000_000.0)
        } else if v >= 1_000.0 {
            format!("{:.1}K", v / 1_000.0)
        } else if v.fract() == 0.0 && v.abs() < 10_000.0 {
            format!("{:.0}", v)
        } else {
            format!("{:.2}", v)
        }
    }

    fn padded_color(s: &str, width: usize, is_good: bool, has_change: bool) -> String {
        let padded = format!("{s:>width$}");
        if !is_good && has_change {
            padded.red().to_string()
        } else if has_change {
            padded.green().to_string()
        } else {
            padded
        }
    }

    fn colored_delta(cmp: &FieldCmp) -> String {
        let s = if cmp.is_latency {
            fmt_delta_latency(cmp.delta)
        } else {
            fmt_delta_count(cmp.delta)
        };
        padded_color(&s, 12, has_improved_or_neutral(cmp), cmp.delta != 0.0)
    }

    fn colored_pct(cmp: &FieldCmp) -> String {
        let s = fmt_delta_pct(cmp.delta_pct);
        padded_color(&s, 10, has_improved_or_neutral(cmp), cmp.delta != 0.0)
    }

    fn has_improved_or_neutral(cmp: &FieldCmp) -> bool {
        match cmp.direction {
            ChangeDirection::HigherBetter => cmp.delta > 0.0,
            ChangeDirection::LowerBetter => cmp.delta < 0.0,
            ChangeDirection::Neutral => true,
        }
    }

    fn fmt_delta_latency(v: f64) -> String {
        let abs = v.abs();
        let s = if abs >= 10_000_000.0 {
            format!("{:.2}s", abs / 1_000_000.0)
        } else if abs >= 1_000.0 {
            format!("{:.2}ms", abs / 1_000.0)
        } else {
            format!("{:.3}ms", abs / 1_000.0)
        };
        if v < 0.0 {
            format!("-{}", s)
        } else if v > 0.0 {
            format!("+{}", s)
        } else {
            " 0.000ms".into()
        }
    }

    fn fmt_delta_count(v: f64) -> String {
        let abs = v.abs();
        let s = if abs >= 1_000_000.0 {
            format!("{:.2}M", abs / 1_000_000.0)
        } else if abs >= 1_000.0 {
            format!("{:.1}K", abs / 1_000.0)
        } else {
            format!("{:.2}", abs)
        };
        if v < 0.0 {
            format!("-{}", s)
        } else if v > 0.0 {
            format!("+{}", s)
        } else {
            " 0.00".into()
        }
    }

    fn fmt_delta_pct(v: f64) -> String {
        if v.is_infinite() {
            "  N/A".into()
        } else if v > 0.0 {
            format!("+{:.1}%", v)
        } else if v < 0.0 {
            format!("{:.1}%", v)
        } else {
            " 0.0%".into()
        }
    }
}

mod report {
    use super::*;

    pub fn run(args: super::ReportArgs) -> anyhow::Result<()> {
        let content = fs::read_to_string(&args.json)
            .with_context(|| format!("failed to read {}", &args.json))?;
        let benchmark: BenchmarkResult = serde_json::from_str(&content)
            .with_context(|| format!("failed to parse {}", &args.json))?;

        let stdout = std::io::stdout().lock();
        write_summary(stdout, &benchmark, args.verbose);

        Ok(())
    }
}
