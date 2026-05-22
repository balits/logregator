use std::{fmt::Write, fs, path::PathBuf, sync::Arc, time::{Duration, Instant, SystemTime, UNIX_EPOCH}};

use anyhow::Context;
use clap::Parser;
use hdrhistogram::Histogram;
use serde::{Deserialize, Serialize};
use logregator::{client::Client, mem_profile::{MemProfiler, MemReport, MemSample}, metrics::{Metrics, MetricsSnapshot}, proto, server::Server, storage::{Engine, compaction::{self, CompactionCommand, CompactionResult}}};
use rand::{RngExt, SeedableRng};
use rand::rngs::StdRng;
use tokio::{sync::mpsc, task::JoinHandle};

#[derive(Debug, Clone, Copy, PartialEq, Serialize)]
enum WorkloadKind {
    Write,
    Read,
    Mixed,
    Tail,
    Bulk,
}

fn workload_parser(arg: &str) -> Result<WorkloadKind, String> {
    match arg {
        "write" => Ok(WorkloadKind::Write),
        "read" => Ok(WorkloadKind::Read),
        "mixed" => Ok(WorkloadKind::Mixed),
        "tail" => Ok(WorkloadKind::Tail),
        "bulk" => Ok(WorkloadKind::Bulk),
        _ => Err("unrecognised workload, expected: write, read, mixed, tail, or bulk".into()),
    }
}

#[derive(Debug, Parser, Serialize)]
struct Args {
    #[arg(long, default_value = "10")]
    duration_secs: u32,
    #[arg(long, default_value = "bulk", value_parser=workload_parser)]
    workloads: Vec<WorkloadKind>,
    #[arg(long, default_value = "127.0.0.1:4321")] 
    addr: String,
    #[arg(long, default_value = "512")]
    batch_size: usize,
    #[arg(long, default_value = "8192")]
    channel_capacity: usize,

    #[arg(long, default_value = "1")]
    concurrency_min: u32,
    #[arg(long, default_value = "16")]
    concurrency_max: u32,
    #[arg(long, default_value = "2")]
    concurrency_step: u32,

    #[arg(long, default_value = "4")]
    sources: u32,
    #[arg(long, default_value = "4")]
    keys: u32,
    #[arg(long, default_value = "128")]
    value_size: u32,

    #[arg(long, default_value = "0.5")]
    range_fraction: f64,
    #[arg(long, default_value = "60")]
    range_window: u32,

    #[arg(long)]
    out: Option<String>,

    #[arg(long, default_value = "0")]
    profile_mem: u64,

    #[arg(long, default_value = "32")]
    memtable_mb: u64,
}

#[derive(Debug, Clone, Serialize)]
struct RunConfig {
    addr: String,
    run_id: usize,
    workload: WorkloadKind,
    concurrency: u32,
    duration_secs: u32,
    batch_size: usize,
    channel_capacity: usize,

    sources: u32,
    keys: u32,
    value_size: u32,
    range_fraction: f64,
    range_window: u32,
}

#[derive(Debug, Serialize)]
struct RunResult {
    run_config: RunConfig,

    inserts_completed: u64,
    range_queries_completed: u64,
    records_scanned: u64,
    insert_ops_per_sec: f64,
    range_ops_per_sec: f64,

    insert_latency: LatencyStats,
    range_latency: LatencyStats,

    failed_inserts: u64,
    failed_ranges: u64,

    avg_records_per_range: f64,
    sstable_count: usize,
}

#[derive(Debug, Serialize)]
struct LatencyStats {
    min_us: f64,
    p50_us: f64,
    p90_us: f64,
    p95_us: f64,
    p99_us: f64,
    p99_9_us: f64,
    max_us: f64,
    mean_us: f64,
    stddev_us: f64,
    sample_count: usize,
}

const HIST_MAX: u64 = 60_000_000; // 60 seconds in us

impl LatencyStats {
    pub fn from_histogram(h: &Histogram<u64>) -> Self {
        if h.len() == 0 {
            return Self {
                min_us: 0.0,
                p50_us: 0.0,
                p90_us: 0.0,
                p95_us: 0.0,
                p99_us: 0.0,
                p99_9_us: 0.0,
                max_us: 0.0,
                mean_us: 0.0,
                stddev_us: 0.0,
                sample_count: 0,
            };
        }
        Self {
            min_us: h.min() as f64,
            p50_us: h.value_at_percentile(50.0) as f64,
            p90_us: h.value_at_percentile(90.0) as f64,
            p95_us: h.value_at_percentile(95.0) as f64,
            p99_us: h.value_at_percentile(99.0) as f64,
            p99_9_us: h.value_at_percentile(99.9) as f64,
            max_us: h.max() as f64,
            mean_us: h.mean(),
            stddev_us: h.stdev(),
            sample_count: h.len() as usize,
        }
    }
}

struct ClientResult {
    inserts_completed: u64,
    inserts_failed: u64,
    insert_latencies: Histogram<u64>,

    range_queries_completed: u64,
    ranges_failed: u64,
    range_latencies: Histogram<u64>,
    records_scanned: u64,
}

impl Default for ClientResult {
    fn default() -> Self {
        Self {
            inserts_completed: 0,
            inserts_failed: 0,
            insert_latencies: Histogram::new_with_bounds(1, HIST_MAX, 3).unwrap(),
            range_queries_completed: 0,
            ranges_failed: 0,
            range_latencies: Histogram::new_with_bounds(1, HIST_MAX, 3).unwrap(),
            records_scanned: 0,
        }
    }
}

impl ClientResult {
    pub fn merge(&mut self, other: ClientResult) {
        self.inserts_completed += other.inserts_completed;
        self.range_queries_completed += other.range_queries_completed;
        self.records_scanned += other.records_scanned;
        self.inserts_failed += other.inserts_failed;
        self.ranges_failed += other.ranges_failed;
        let _ = self.insert_latencies.add(&other.insert_latencies);
        let _ = self.range_latencies.add(&other.range_latencies);
    }

    pub fn to_run_result(&self, cfg: &RunConfig) -> RunResult {
        let duration_secs = cfg.duration_secs as f64;
        let insert_ops = self.inserts_completed as f64 / duration_secs;
        let range_ops = self.range_queries_completed as f64 / duration_secs;
        let avg_rec = if self.range_queries_completed > 0 {
            self.records_scanned as f64 / self.range_queries_completed as f64
        } else {
            0.0
        };

        RunResult {
            run_config: cfg.clone(),
            inserts_completed: self.inserts_completed,
            range_queries_completed: self.range_queries_completed,
            records_scanned: self.records_scanned,
            insert_ops_per_sec: insert_ops,
            range_ops_per_sec: range_ops,
            insert_latency: LatencyStats::from_histogram(&self.insert_latencies),
            range_latency: LatencyStats::from_histogram(&self.range_latencies),
            failed_inserts: self.inserts_failed,
            failed_ranges: self.ranges_failed,
            avg_records_per_range: avg_rec,
            sstable_count: 0,
        }
    }
}

/// top level result for the whole loadgen benchmark session
#[derive(Debug, Serialize)]
struct BenchmarkResult {
    args: Args,
    runs: Vec<RunResult>,
    metrics: MetricsSnapshot,
    saturation: Option<Saturation>,
    mem: Option<MemReport>,
}

async fn run_server(args: &Args) -> anyhow::Result<(Arc<Metrics>, PathBuf, Option<mpsc::UnboundedReceiver<(usize, usize)>>)> {
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
        let mut engine = Engine::open(data_dir.join("storage"), memtable_limit, comp_cmd_tx, comp_result_rx)?;
        engine.set_stats_tx(stats_tx);
        engine.set_metrics(&metrics);
        tokio::spawn(async move { engine.engine_loop(insert_rx, range_rx).await });
        Some(stats_rx)
    } else {
        let mut engine = Engine::open(data_dir.join("storage"), memtable_limit, comp_cmd_tx, comp_result_rx)?;
        engine.set_metrics(&metrics);
        tokio::spawn(async move { engine.engine_loop(insert_rx, range_rx).await });
        None
    };
    let comp_metrics = metrics.clone();
    tokio::spawn(async move { compaction::compaction_loop(comp_cmd_rx, comp_result_tx, Some(comp_metrics)).await });
    println!(">> engine & compactor running");

    let server = Server::new(Some(args.addr.parse().unwrap()))
        .await
        .context("failed to start server")?;
    let svr_metrics = metrics.clone();
    tokio::spawn(async move { server.run_main_loop(insert_tx, range_tx, svr_metrics).await });
    println!(">> server listening on {}", args.addr);
    Ok((metrics, data_dir, stats_rx))
}

fn needs_prefill(workload: WorkloadKind) -> bool {
    matches!(workload, WorkloadKind::Read | WorkloadKind::Mixed | WorkloadKind::Tail | WorkloadKind::Bulk)
}

fn build_run_configs(args: &Args) -> Vec<RunConfig> {
    let mut configs = Vec::new();
    let mut run_id = 0;

    for &workload in &args.workloads {
        let mut concurrency = args.concurrency_min;
        while concurrency <= args.concurrency_max {
            configs.push(RunConfig {
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

async fn run_single(cfg: &RunConfig) -> anyhow::Result<RunResult> {
    let cfg = Arc::new(cfg.clone());
    let mut clients = Vec::with_capacity(cfg.concurrency as usize);
    for i in 0..cfg.concurrency {
        let c = Client::connect(&cfg.addr.to_string())
            .await
            .with_context(|| format!("client {i} failed to connect"))?;
        clients.push(c);
    }
    if needs_prefill(cfg.workload) {
        println!(">> prefilling...");
        prefill(cfg.as_ref(), &mut clients).await?;
    }
    println!(">> clients doing work...");
    let handles: Vec<JoinHandle<ClientResult>> = clients
        .into_iter()
        .enumerate()
        .map(|(id, c)| tokio::spawn(do_client_work(id, c, cfg.clone())))
        .collect();
    let mut total = ClientResult::default();
    for h in handles {
        total.merge(h.await?);
    }
    println!(">> merging client results...");

    Ok(total.to_run_result(&cfg))
}

async fn flush_pending(
    client: &mut Client,
    pending: &mut Vec<proto::Insert>,
    result: &mut ClientResult,
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

async fn do_client_work(id: usize, mut client: Client, cfg: Arc<RunConfig>) -> ClientResult {
    let mut result = ClientResult::default();
    let mut rng = StdRng::seed_from_u64(id as u64);

    let sources: Vec<i64> = (0..cfg.sources as i64).collect();
    let keys: Vec<String> = (0..cfg.keys).map(|i| format!("key_{}", i)).collect();
    let value = "a".repeat(cfg.value_size as usize);

    let phase_start = Instant::now();
    let end = phase_start + Duration::from_secs(cfg.duration_secs as u64);
    let mut insert_count: i64 = 0;
    let mut pending: Vec<proto::Insert> = Vec::with_capacity(cfg.batch_size);

    while Instant::now() < end {
        let elapsed_ms = (Instant::now() - phase_start).as_millis() as i64;

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
            match client.range(proto::Range { source_id, key, start_ts, end_ts, filter }).await {
                Ok(records) => {
                    let _ = result.range_latencies.record((t0.elapsed().as_secs_f64() * 1_000_000.0) as u64);
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

            pending.push(proto::Insert { source_id, ts, key, value: value.clone() });

            if pending.len() >= cfg.batch_size {
                flush_pending(&mut client, &mut pending, &mut result).await;
            }
        }
    }

    flush_pending(&mut client, &mut pending, &mut result).await;
    result
}

async fn prefill(cfg: &RunConfig, clients: &mut [Client]) -> anyhow::Result<()> {
    let mut rng = StdRng::seed_from_u64(67 as u64);
    let val = "s".repeat(cfg.value_size as usize);
    let srcs: Vec<i64> = (0..cfg.sources as i64).collect();
    let keys: Vec<String> = (0..cfg.keys).map(|i| format!("key_{i}")).collect();
    let start = Instant::now();
    let end = start + Duration::from_secs(3);
    while Instant::now() < end {
        for client in clients.iter_mut() {
            let source_id = srcs[rng.random_range(0..srcs.len())];
            let key = keys[rng.random_range(0..keys.len())].clone(); 
            let ts = (Instant::now() - start).as_millis() as i64;
            client
                .insert(proto::Insert { source_id, ts, key, value: val.clone() })
                .await?;
        }
    }
    Ok(())
}

fn fmt_latency(us: f64) -> String {
    if us >= 10_000_000.0 {
        format!("{:.1}s", us / 1_000_000.0)
    } else if us >= 10_000.0 {
        format!("{:.1}ms", us / 1_000.0)
    } else if us >= 1_000.0 {
        format!("{:.2}ms", us / 1_000.0)
    } else {
        format!("{:.0}µs", us)
    }
}

fn write_results(results: &[RunResult], w: &mut impl Write) -> std::fmt::Result {
    writeln!(w, "\n==> RESULTS <==\n")?;

    writeln!(w, "{:>5} {:>8} {:>8} {:>8} {:>8} {:>8} {:>8} {:>8} {:>9} {:>9} {:>8} {:>8}",
        "RUN", "CLIENTS", "INSERTS", "INS/S", "RNG/S",
        "REC/QRY", "ERR", "SST", "INS_P99", "RNG_P99", "BATCH", "WORKLOAD")?;

    for r in results {
        let err_total = r.failed_inserts + r.failed_ranges;
        writeln!(w, "{:>5} {:>8} {:>8} {:>8.0} {:>8.0} {:>8.1} {:>8} {:>8} {:>9} {:>9} {:>8} {:>8}",
            r.run_config.run_id,
            r.run_config.concurrency,
            r.inserts_completed,
            r.insert_ops_per_sec,
            r.range_ops_per_sec,
            r.avg_records_per_range,
            err_total,
            r.sstable_count,
            fmt_latency(r.insert_latency.p99_us),
            fmt_latency(r.range_latency.p99_us),
            r.run_config.batch_size,
            format!("{:?}", r.run_config.workload).to_lowercase(),
        )?;
    }

    writeln!(w)?;
    writeln!(w, "==> INSERT LATENCY <==\n")?;
    writeln!(w, "{:>5} {:>9} {:>9} {:>9} {:>9} {:>9} {:>9} {:>9} {:>9} {:>9}",
        "RUN", "MIN", "P50", "P90", "P95", "P99", "P99.9",
        "MAX", "MEAN", "STDDEV")?;
    writeln!(w, "{}", "-".repeat(95))?;
    for r in results {
        if r.insert_latency.sample_count == 0 { continue; }
        writeln!(w, "{:>5} {:>9} {:>9} {:>9} {:>9} {:>9} {:>9} {:>9} {:>9} {:>9}",
            r.run_config.run_id,
            fmt_latency(r.insert_latency.min_us),
            fmt_latency(r.insert_latency.p50_us),
            fmt_latency(r.insert_latency.p90_us),
            fmt_latency(r.insert_latency.p95_us),
            fmt_latency(r.insert_latency.p99_us),
            fmt_latency(r.insert_latency.p99_9_us),
            fmt_latency(r.insert_latency.max_us),
            fmt_latency(r.insert_latency.mean_us),
            fmt_latency(r.insert_latency.stddev_us),
        )?;
    }

    writeln!(w)?;
    writeln!(w, "==> RANGE LATENCY <==\n")?;
    writeln!(w, "{:>5} {:>9} {:>9} {:>9} {:>9} {:>9} {:>9} {:>9} {:>9} {:>9}",
        "RUN", "MIN", "P50", "P90", "P95", "P99", "P99.9",
        "MAX", "MEAN", "STDDEV")?;
    writeln!(w, "{}", "-".repeat(95))?;
    for r in results {
        if r.range_latency.sample_count == 0 { continue; }
        writeln!(w, "{:>5} {:>9} {:>9} {:>9} {:>9} {:>9} {:>9} {:>9} {:>9} {:>9}",
            r.run_config.run_id,
            fmt_latency(r.range_latency.min_us),
            fmt_latency(r.range_latency.p50_us),
            fmt_latency(r.range_latency.p90_us),
            fmt_latency(r.range_latency.p95_us),
            fmt_latency(r.range_latency.p99_us),
            fmt_latency(r.range_latency.p99_9_us),
            fmt_latency(r.range_latency.max_us),
            fmt_latency(r.range_latency.mean_us),
            fmt_latency(r.range_latency.stddev_us),
        )?;
    }

    writeln!(w)?;
    for r in results {
        if r.failed_inserts > 0 || r.failed_ranges > 0 {
            writeln!(w, ">> Errors [run {}]: {} failed inserts, {} failed ranges",
                r.run_config.run_id, r.failed_inserts, r.failed_ranges)?;
        }
    }
    Ok(())
}

#[derive(Debug, Serialize)]
struct SaturationPoint {
    from_concurrency: u32,
    to_concurrency: u32,
    from_throughput: f64,
    to_throughput: f64,
}

#[derive(Debug, Serialize)]
struct Saturation {
    workload: WorkloadKind,
    by_concurrency: Vec<(u32, f64)>,
    max_throughput: f64,
    max_concurrency: u32,
    saturation_detected: bool,
    saturation_points: Vec<SaturationPoint>,
}

fn compute_saturation(results: &[RunResult]) -> Option<Saturation> {
    if results.is_empty() {
        return None;
    }

    let workload = results[0].run_config.workload;
    let by_concurrency: Vec<(u32, f64)> = results
        .iter()
        .filter(|r| r.run_config.workload == workload)
        .map(|r| (r.run_config.concurrency, r.insert_ops_per_sec))
        .collect();

    if by_concurrency.len() < 2 {
        return None;
    }

    let mut max_throughput = 0.0;
    let mut max_concurrency = 0u32;

    for &(conc, throughput) in &by_concurrency {
        if throughput > max_throughput {
            max_throughput = throughput;
            max_concurrency = conc;
        }
    }

    let mut saturation_points = Vec::new();
    for window in by_concurrency.windows(2) {
        let (prev_conc, prev) = window[0];
        let (conc, curr) = window[1];
        if curr < prev * 0.95 {
            saturation_points.push(SaturationPoint {
                from_concurrency: prev_conc,
                to_concurrency: conc,
                from_throughput: prev,
                to_throughput: curr,
            });
        }
    }

    let saturation_detected = !saturation_points.is_empty();

    Some(Saturation {
        workload,
        by_concurrency,
        max_throughput,
        max_concurrency,
        saturation_detected,
        saturation_points,
    })
}

fn write_saturation(results: &[RunResult], w: &mut impl Write) -> std::fmt::Result {
    let Some(s) = compute_saturation(results) else {
        writeln!(w, "Not enough data points to detect saturation.")?;
        return Ok(());
    };

    writeln!(w, "==> SATURATION ANALYSIS <==")?;
    writeln!(w, "Workload: {:?}", s.workload)?;
    writeln!(w, "Concurrency vs Insert Throughput:")?;

    for &(conc, throughput) in &s.by_concurrency {
        writeln!(w, "  {:>2} clients: {:.0} ops/s", conc, throughput)?;
    }

    writeln!(w, "\nMax throughput: {:.0} ops/s at {:>} clients", s.max_throughput, s.max_concurrency)?;

    for sp in &s.saturation_points {
        writeln!(w, "SATURATION DETECTED: Throughput dropped from {:.0} to {:.0} ops/s from concurrency {} to {}",
            sp.from_throughput, sp.to_throughput, sp.from_concurrency, sp.to_concurrency)?;
    }

    if !s.saturation_detected {
        writeln!(w, "No clear saturation point detected within tested concurrency range.")?;
    }
    Ok(())
}


#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let args = Args::parse();
    println!("==> LOAD GEN ARGS <==\n{:#?}", &args);
    let configs = build_run_configs(&args);  // Vec<RunConfig>
    let mut results = Vec::with_capacity(configs.len());

    // -- optionally start memory profiler
    let profiler = if args.profile_mem > 0 {
        Some(MemProfiler::start(Duration::from_millis(args.profile_mem)))
    } else {
        None
    };

    let (metrics, data_dir, mut stats_rx) = run_server(&args).await.expect("failed to start server");
    let storage_dir = data_dir.join("storage");

    // background metrics snapshot (every 10 seconds)
    let dump_metrics = metrics.clone();
    tokio::spawn(async move {
        let mut interval = tokio::time::interval(Duration::from_secs(10));
        loop {
            interval.tick().await;
            println!("{}", dump_metrics.dump());
        }
    });

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

    for config in configs {
        println!(">> running: {:?} with {} clients for {}s",
            config.workload, config.concurrency, config.duration_secs);
        
        let mut result = run_single(&config).await?;

        // count SSTable files after this run
        if storage_dir.exists() {
            result.sstable_count = fs::read_dir(&storage_dir)
                .into_iter()
                .flatten()
                .filter_map(Result::ok)
                .filter(|e| e.path().extension().map(|ext| ext == "sst").unwrap_or(false))
                .count();
        }

        results.push(result);
        
        println!(">> 2s cooldown between runs...");
        tokio::time::sleep(Duration::from_secs(2)).await;
        println!(">>");
    }
    // -- final metrics snapshot
    let metrics_snapshot = metrics.snapshot();

    // -- stop profiler and collect memory data
    let mem_samples: Vec<MemSample> = if let Some(p) = profiler {
        p.stop().await
    } else {
        Vec::new()
    };
    let engine_samples: Vec<(f64, usize, usize)> = em_rx.try_iter().collect();
    let mem_report = MemProfiler::new_report(&mem_samples, &engine_samples);

    // -- pretty-print to stdout
    let mut output = String::new();
    write_results(&results, &mut output).expect("format results");
    write_saturation(&results, &mut output).expect("format saturation");
    output.push_str(&metrics.dump());
    if let Some(ref _r) = mem_report {
        output.push_str(&MemProfiler::new_report_str(&mem_samples, &engine_samples));
    }
    print!("{output}");

    // -- serialized JSON to file
    let saturation = compute_saturation(&results);
    let benchmark = BenchmarkResult {
        args: args,
        runs: results,
        metrics: metrics_snapshot,
        saturation,
        mem: mem_report,
    };
    let json = serde_json::to_string_pretty(&benchmark).context("failed to serialize benchmark result")?;

    let workload_suffix = benchmark.args.workloads.iter()
        .map(|w| format!("{:?}", w).to_lowercase())
        .collect::<Vec<_>>()
        .join("_");

    if let Some(outname) = benchmark.args.out {
        fs::write(&outname, &json).context("failed to write output file")?;
        println!(">> output saved to {outname}");
    } else {
        let ts = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_millis();
        let path = format!("prof/json/{ts}_{workload_suffix}.json");
        fs::write(&path, &json).context("failed to write output file")?;
        println!(">> output saved to {path}");
    }
    println!("{}", json);

    Ok(())
}
