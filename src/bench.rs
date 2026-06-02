use std::time::Instant;

use clap::{Args, ValueEnum};
use hdrhistogram::Histogram;
use serde::{Deserialize, Serialize};

use crate::{mem_profile, metrics};

#[derive(
    Debug, Clone, Copy, Hash, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize, ValueEnum,
)]

pub enum WorkloadKind {
    /// 100% inserts. Stresses insert throughput, WAL append + sync, memtable fill,
    /// and flush when the memtable limit is exceeded.
    Write,
    /// 100% range queries. Stresses SSTable reads, bloom filter lookups,
    /// index block scans, and MergeIter across memtable + multiple SSTables.
    Read,
    /// Mix of inserts and range queries (controlled by `range_fraction`).
    /// Stresses read/write contention, the tokio::select! starvation path
    /// where range queries may be starved by a flood of inserts.
    Mixed,
    /// 90% inserts, 10% range queries on a recent time window (`range_window`).
    /// Stresses the cache-hot path and the engine's ability to serve
    /// latest-record lookups (recent data may still be in the memtable).
    Tail,
    /// 50% inserts, 50% range queries. Intended for bulk-scan throughput.
    /// Stresses large sequential range scans across the entire dataset.
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

#[derive(Debug, Clone, Args, Serialize, Deserialize)]
pub struct BenchmarkArgs {
    #[arg(long, default_value = "10")]
    pub duration_secs: u32,
    #[arg(long, default_value = "bulk", value_delimiter = ',', value_parser=workload_parser)]
    pub workloads: Vec<WorkloadKind>,
    #[arg(long, default_value = "127.0.0.1:4321")]
    pub addr: String,
    #[arg(long, default_value = "512")]
    pub batch_size: usize,
    #[arg(long, default_value = "8192")]
    pub channel_capacity: usize,

    #[arg(long, default_value = "1")]
    pub concurrency_min: u32,
    #[arg(long, default_value = "16")]
    pub concurrency_max: u32,
    #[arg(long, default_value = "2")]
    pub concurrency_step: u32,

    #[arg(long, default_value = "4")]
    pub sources: u32,
    #[arg(long, default_value = "4")]
    pub keys: u32,
    #[arg(long, default_value = "128")]
    pub value_size: u32,

    #[arg(long, default_value = "0.5")]
    pub range_fraction: f64,
    #[arg(long, default_value = "60")]
    pub range_window: u32,

    #[arg(long)]
    pub out: Option<String>,

    #[arg(long, default_value = "0")]
    pub profile_mem: u64,

    #[arg(long, default_value = "32")]
    pub memtable_mb: u64,

    #[arg(long)]
    #[serde(default)]
    pub verbose: bool,

    #[arg(long)]
    pub tag: Option<String>,

    #[arg(long)]
    pub external_server: Option<bool>,
}

// for `Instant` fields: uses the current time.
pub fn instant_now() -> Instant {
    Instant::now()
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RunConfig {
    pub addr: String,
    pub run_id: usize,
    pub workload: WorkloadKind,
    pub concurrency: u32,
    pub duration_secs: u32,
    pub batch_size: usize,
    pub channel_capacity: usize,

    pub sources: u32,
    pub keys: u32,
    pub value_size: u32,
    pub range_fraction: f64,
    pub range_window: u32,

    /// Global monotonic timestamp baseline. All runs in a benchmark session
    /// use the same server_start so timestamps are globally comparable.
    #[serde(skip, default = "instant_now")]
    pub server_start: Instant,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct RunResult {
    pub run_config: RunConfig,

    pub inserts_completed: u64,
    pub range_queries_completed: u64,
    pub records_scanned: u64,
    pub insert_ops_per_sec: f64,
    pub range_ops_per_sec: f64,

    pub insert_latency: LatencyStats,
    pub range_latency: LatencyStats,

    pub failed_inserts: u64,
    pub failed_ranges: u64,

    pub avg_records_per_range: f64,
    pub sstable_count: usize,
}

#[derive(Debug, Default, Serialize, Deserialize)]
pub struct LatencyStats {
    pub min_us: f64,
    pub p50_us: f64,
    pub p90_us: f64,
    pub p95_us: f64,
    pub p99_us: f64,
    pub p99_9_us: f64,
    pub max_us: f64,
    pub mean_us: f64,
    pub stddev_us: f64,
    pub sample_count: usize,
}

pub const HIST_MAX: u64 = 60_000_000; // 60 seconds in us

impl LatencyStats {
    pub fn from_histogram(h: &Histogram<u64>) -> Self {
        if h.is_empty() {
            return Self::default();
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

pub struct ClientResult {
    pub inserts_completed: u64,
    pub inserts_failed: u64,
    pub insert_latencies: Histogram<u64>,

    pub range_queries_completed: u64,
    pub ranges_failed: u64,
    pub range_latencies: Histogram<u64>,
    pub records_scanned: u64,
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
            // populated post-construction by the benchmark runner from engine metrics or disk count
            sstable_count: 0,
        }
    }
}

/// top level result for the whole loadgen benchmark session
#[derive(Debug, Serialize, Deserialize)]
pub struct BenchmarkResult {
    pub args: BenchmarkArgs,
    pub runs: Vec<RunResult>,
    #[serde(default)]
    pub metrics: Option<metrics::MetricsSnapshot>,
    #[serde(default)]
    pub saturations: Vec<Saturation>,
    pub mem: Option<mem_profile::MemReport>,
}

pub fn fmt_latency(us: f64) -> String {
    if us >= 10_000_000.0 {
        format!("{:.2}s", us / 1_000_000.0)
    } else if us >= 1_000.0 {
        format!("{:.2}ms", us / 1_000.0)
    } else {
        format!("{:.3}ms", us / 1_000.0)
    }
}

pub fn fmt_throughput(v: f64) -> String {
    if v >= 1_000_000.0 {
        format!("{:.2}M/s", v / 1_000_000.0)
    } else if v >= 1_000.0 {
        format!("{:.1}K/s", v / 1_000.0)
    } else {
        format!("{:.0}/s", v)
    }
}

pub fn fmt_bytes(b: u64) -> String {
    if b >= 1_000_000_000 {
        format!("{:.2}GB", b as f64 / 1_000_000_000.0)
    } else if b >= 1_000_000 {
        format!("{:.2}MB", b as f64 / 1_000_000.0)
    } else if b >= 1_000 {
        format!("{:.1}KB", b as f64 / 1_000.0)
    } else {
        format!("{}B", b)
    }
}

pub fn fmt_count(n: u64) -> String {
    if n >= 1_000_000_000 {
        format!("{:.2}B", n as f64 / 1_000_000_000.0)
    } else if n >= 1_000_000 {
        format!("{:.2}M", n as f64 / 1_000_000.0)
    } else if n >= 10_000 {
        format!("{:.0}K", n as f64 / 1_000.0)
    } else if n >= 1_000 {
        format!("{:.1}K", n as f64 / 1_000.0)
    } else {
        format!("{}", n)
    }
}

pub fn fmt_latency_line(l: &LatencyStats) -> String {
    format!(
        "min={}  p50={}  p90={}  p95={}  p99={}  max={}  mean={}  count={}",
        fmt_latency(l.min_us),
        fmt_latency(l.p50_us),
        fmt_latency(l.p90_us),
        fmt_latency(l.p95_us),
        fmt_latency(l.p99_us),
        fmt_latency(l.max_us),
        fmt_latency(l.mean_us),
        l.sample_count
    )
}

#[derive(Debug, Serialize, Deserialize)]
pub struct SaturationPoint {
    pub from_concurrency: u32,
    pub to_concurrency: u32,
    pub from_throughput: f64,
    pub to_throughput: f64,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct Saturation {
    pub workload: WorkloadKind,
    pub by_concurrency: Vec<(u32, f64)>,
    pub max_throughput: f64,
    pub max_concurrency: u32,
    pub saturation_detected: bool,
    pub saturation_points: Vec<SaturationPoint>,
}

pub fn compute_saturations(results: &[RunResult]) -> Vec<Saturation> {
    let mut workloads: Vec<WorkloadKind> = results.iter().map(|r| r.run_config.workload).collect();
    workloads.sort();
    workloads.dedup();

    workloads
        .into_iter()
        .filter_map(|workload| {
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
        })
        .collect()
}
