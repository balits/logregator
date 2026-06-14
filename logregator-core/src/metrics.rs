use std::sync::atomic::{AtomicI64, AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Instant;

use hdrhistogram::Histogram as HdrHist;
use serde::{Deserialize, Serialize};

#[derive(Debug, Default)]
pub struct Counter(AtomicU64);

impl Counter {
    pub fn inc(&self, n: u64) {
        self.0.fetch_add(n, Ordering::Relaxed);
    }
    pub fn value(&self) -> u64 {
        self.0.load(Ordering::Relaxed)
    }
}

#[derive(Debug, Default)]
pub struct Gauge(AtomicI64);

impl Gauge {
    pub fn add(&self, delta: i64) {
        self.0.fetch_add(delta, Ordering::Relaxed);
    }
    pub fn dec(&self, delta: i64) {
        self.0.fetch_sub(delta, Ordering::Relaxed);
    }
    pub fn set(&self, v: i64) {
        self.0.store(v, Ordering::Relaxed);
    }
    pub fn value(&self) -> i64 {
        self.0.load(Ordering::Relaxed)
    }
}

#[derive(Debug)]
pub struct Latency(Mutex<HdrHist<u64>>);

impl Default for Latency {
    fn default() -> Self {
        Self::new()
    }
}

impl Latency {
    pub fn new() -> Self {
        Latency(Mutex::new(
            HdrHist::new_with_bounds(1, 3_600_000_000, 3).unwrap(),
        ))
    }

    pub fn record_us(&self, us: f64) {
        let v = us.clamp(0.0, 3_600_000_000.0) as u64;
        if let Ok(mut h) = self.0.lock() {
            let _ = h.record(v);
        }
    }

    pub fn record_instant(&self, start: Instant) {
        self.record_us(start.elapsed().as_secs_f64() * 1_000_000.0);
    }

    pub fn snapshot(&self) -> Option<LatencySnapshot> {
        let h = self.0.lock().ok()?;
        if h.is_empty() {
            return None;
        }
        Some(LatencySnapshot {
            min: h.min(),
            p50: h.value_at_percentile(50.0),
            p90: h.value_at_percentile(90.0),
            p95: h.value_at_percentile(95.0),
            p99: h.value_at_percentile(99.0),
            p99_9: h.value_at_percentile(99.9),
            max: h.max(),
            mean: h.mean(),
            stddev: h.stdev(),
            sample_count: h.len(),
        })
    }
}

fn de_u64_from_f64_or_u64<'de, D>(d: D) -> Result<u64, D::Error>
where
    D: serde::Deserializer<'de>,
{
    struct U64OrF64;
    impl<'de> serde::de::Visitor<'de> for U64OrF64 {
        type Value = u64;
        fn expecting(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
            f.write_str("an integer or floating point number")
        }
        fn visit_u64<E: serde::de::Error>(self, v: u64) -> Result<u64, E> {
            Ok(v)
        }
        fn visit_f64<E: serde::de::Error>(self, v: f64) -> Result<u64, E> {
            Ok(v as u64)
        }
    }
    d.deserialize_any(U64OrF64)
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LatencySnapshot {
    #[serde(alias = "min_us", deserialize_with = "de_u64_from_f64_or_u64")]
    pub min: u64,
    #[serde(alias = "p50_us", deserialize_with = "de_u64_from_f64_or_u64")]
    pub p50: u64,
    #[serde(alias = "p90_us", deserialize_with = "de_u64_from_f64_or_u64")]
    pub p90: u64,
    #[serde(alias = "p95_us", deserialize_with = "de_u64_from_f64_or_u64")]
    pub p95: u64,
    #[serde(alias = "p99_us", deserialize_with = "de_u64_from_f64_or_u64")]
    pub p99: u64,
    #[serde(alias = "p99_9_us", deserialize_with = "de_u64_from_f64_or_u64")]
    pub p99_9: u64,
    #[serde(alias = "max_us", deserialize_with = "de_u64_from_f64_or_u64")]
    pub max: u64,
    #[serde(alias = "mean_us")]
    pub mean: f64,
    #[serde(alias = "stddev_us", default)]
    pub stddev: f64,
    #[serde(alias = "count")]
    pub sample_count: u64,
}

impl LatencySnapshot {
    pub fn from_histogram(h: &HdrHist<u64>) -> Self {
        LatencySnapshot {
            min: h.min(),
            p50: h.value_at_percentile(50.0),
            p90: h.value_at_percentile(90.0),
            p95: h.value_at_percentile(95.0),
            p99: h.value_at_percentile(99.0),
            p99_9: h.value_at_percentile(99.9),
            max: h.max(),
            mean: h.mean(),
            stddev: h.stdev(),
            sample_count: h.len(),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WalSnapshot {
    pub write_count: u64,
    pub write_bytes: u64,
    pub sync_count: u64,
    pub sync_latency: Option<LatencySnapshot>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct EngineSnapshot {
    pub insert_count: u64,
    pub batch_insert_count: u64,
    pub records_inserted: u64,
    pub insert_failures: u64,
    pub range_count: u64,
    pub range_failures: u64,
    pub records_scanned: u64,
    pub memtable_bytes: i64,
    pub memtable_limit: i64,
    pub sstable_count: i64,
    pub flush_count: u64,
    pub compaction_count: u64,
    pub insert_latency: Option<LatencySnapshot>,
    pub batch_insert_latency: Option<LatencySnapshot>,
    pub range_latency: Option<LatencySnapshot>,
    pub flush_duration: Option<LatencySnapshot>,
    pub compaction_duration: Option<LatencySnapshot>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ServerSnapshot {
    pub connections_accepted: u64,
    pub connections_active: i64,
    pub frames_read: u64,
    pub frames_written: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MetricsSnapshot {
    pub wal: WalSnapshot,
    pub engine: EngineSnapshot,
    pub server: ServerSnapshot,
}

#[derive(Debug, Default)]
pub struct WalMetrics {
    pub write_count: Counter,
    pub write_bytes: Counter,
    pub sync_count: Counter,
    pub sync_latency: Latency,
}

#[derive(Debug, Default)]
pub struct EngineMetrics {
    pub insert_count: Counter,
    pub batch_insert_count: Counter,
    pub records_inserted: Counter,
    pub insert_failures: Counter,
    pub range_count: Counter,
    pub range_failures: Counter,
    pub records_scanned: Counter,
    pub memtable_bytes: Gauge,
    pub memtable_limit: Gauge,
    pub sstable_count: Gauge,
    pub flush_count: Counter,
    pub compaction_count: Counter,

    pub insert_latency: Latency,
    pub batch_insert_latency: Latency,
    pub range_latency: Latency,
    pub flush_duration: Latency,
    pub compaction_duration: Latency,
}

#[derive(Debug, Default)]
pub struct ServerMetrics {
    pub connections_accepted: Counter,
    pub connections_active: Gauge,
    pub frames_read: Counter,
    pub frames_written: Counter,
}

#[derive(Debug, Default)]
pub struct Metrics {
    pub wal: WalMetrics,
    pub engine: EngineMetrics,
    pub server: ServerMetrics,
}

impl Metrics {
    pub fn snapshot(&self) -> MetricsSnapshot {
        MetricsSnapshot {
            wal: WalSnapshot {
                write_count: self.wal.write_count.value(),
                write_bytes: self.wal.write_bytes.value(),
                sync_count: self.wal.sync_count.value(),
                sync_latency: self.wal.sync_latency.snapshot(),
            },
            engine: EngineSnapshot {
                insert_count: self.engine.insert_count.value(),
                batch_insert_count: self.engine.batch_insert_count.value(),
                records_inserted: self.engine.records_inserted.value(),
                insert_failures: self.engine.insert_failures.value(),
                range_count: self.engine.range_count.value(),
                range_failures: self.engine.range_failures.value(),
                records_scanned: self.engine.records_scanned.value(),
                memtable_bytes: self.engine.memtable_bytes.value(),
                memtable_limit: self.engine.memtable_limit.value(),
                sstable_count: self.engine.sstable_count.value(),
                flush_count: self.engine.flush_count.value(),
                compaction_count: self.engine.compaction_count.value(),
                insert_latency: self.engine.insert_latency.snapshot(),
                batch_insert_latency: self.engine.batch_insert_latency.snapshot(),
                range_latency: self.engine.range_latency.snapshot(),
                flush_duration: self.engine.flush_duration.snapshot(),
                compaction_duration: self.engine.compaction_duration.snapshot(),
            },
            server: ServerSnapshot {
                connections_accepted: self.server.connections_accepted.value(),
                connections_active: self.server.connections_active.value(),
                frames_read: self.server.frames_read.value(),
                frames_written: self.server.frames_written.value(),
            },
        }
    }
}

pub type SharedMetrics = Arc<Metrics>;

pub const HIST_MAX: u64 = 60_000_000;
