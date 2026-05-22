use std::fmt::Write;
use std::sync::atomic::{AtomicI64, AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Instant;

use hdrhistogram::Histogram as HdrHist;
use serde::Serialize;

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
        let v = us.max(0.0).min(3_600_000_000.0) as u64;
        if let Ok(mut h) = self.0.lock() {
            let _ = h.record(v);
        }
    }
    pub fn record_instant(&self, start: Instant) {
        self.record_us(start.elapsed().as_secs_f64() * 1_000_000.0);
    }
    pub fn snapshot(&self) -> Option<Snapshot> {
        let h = self.0.lock().ok()?;
        if h.len() == 0 {
            return None;
        }
        Some(Snapshot {
            min: h.min(),
            p50: h.value_at_percentile(50.0),
            p90: h.value_at_percentile(90.0),
            p95: h.value_at_percentile(95.0),
            p99: h.value_at_percentile(99.0),
            p99_9: h.value_at_percentile(99.9),
            max: h.max(),
            mean: h.mean() as u64,
            count: h.len(),
        })
    }
}

#[derive(Debug, Clone, Serialize)]
pub struct Snapshot {
    pub min: u64,
    pub p50: u64,
    pub p90: u64,
    pub p95: u64,
    pub p99: u64,
    pub p99_9: u64,
    pub max: u64,
    pub mean: u64,
    pub count: u64,
}

#[derive(Debug, Clone, Serialize)]
pub struct WalSnapshot {
    pub write_count: u64,
    pub write_bytes: u64,
    pub sync_count: u64,
    pub sync_latency: Option<Snapshot>,
}

#[derive(Debug, Clone, Serialize)]
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
    pub insert_latency: Option<Snapshot>,
    pub batch_insert_latency: Option<Snapshot>,
    pub range_latency: Option<Snapshot>,
    pub flush_duration: Option<Snapshot>,
    pub compaction_duration: Option<Snapshot>,
}

#[derive(Debug, Clone, Serialize)]
pub struct ServerSnapshot {
    pub connections_accepted: u64,
    pub connections_active: i64,
    pub frames_read: u64,
    pub frames_written: u64,
}

#[derive(Debug, Clone, Serialize)]
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
    pub fn dump(&self) -> String {
        let mut out = String::new();

        writeln!(out, "\n==> SERVER METRICS <==").unwrap();
        writeln!(
            out,
            "{:>8} {:>8} {:>8} {:>8}",
            "ACCEPTED", "ACTIVE", "FRM_RD", "FRM_WR"
        )
        .unwrap();
        writeln!(
            out,
            "{:>8} {:>8} {:>8} {:>8}",
            self.server.connections_accepted.value(),
            self.server.connections_active.value(),
            self.server.frames_read.value(),
            self.server.frames_written.value(),
        )
        .unwrap();

        writeln!(out, "\n==> ENGINE METRICS <==").unwrap();
        writeln!(
            out,
            "{:>8} {:>8} {:>10} {:>8} {:>8} {:>8} {:>10} {:>10} {:>10} {:>5} {:>8} {:>8}",
            "INSERTS", "BATCH", "RECORDS", "FAIL", "RANGES", "R_FAIL", "SCANNED",
            "MT_BYTES", "MT_LIM", "SST", "FLUSHES", "COMPACT"
        )
        .unwrap();
        writeln!(
            out,
            "{:>8} {:>8} {:>10} {:>8} {:>8} {:>8} {:>10} {:>10} {:>10} {:>5} {:>8} {:>8}",
            self.engine.insert_count.value(),
            self.engine.batch_insert_count.value(),
            self.engine.records_inserted.value(),
            self.engine.insert_failures.value(),
            self.engine.range_count.value(),
            self.engine.range_failures.value(),
            self.engine.records_scanned.value(),
            self.engine.memtable_bytes.value(),
            self.engine.memtable_limit.value(),
            self.engine.sstable_count.value(),
            self.engine.flush_count.value(),
            self.engine.compaction_count.value(),
        )
        .unwrap();

        writeln!(out, "\n==> WAL METRICS <==").unwrap();
        writeln!(
            out,
            "{:>10} {:>12} {:>8}",
            "WRITES", "BYTES", "SYNCS"
        )
        .unwrap();
        writeln!(
            out,
            "{:>10} {:>12} {:>8}",
            self.wal.write_count.value(),
            self.wal.write_bytes.value(),
            self.wal.sync_count.value(),
        )
        .unwrap();

        writeln!(out, "\n==> LATENCIES (µs) <==").unwrap();
        writeln!(
            out,
            "{:>16} {:>10} {:>10} {:>10} {:>10} {:>10} {:>10} {:>10} {:>10}",
            "", "MIN", "P50", "P90", "P95", "P99", "P99.9", "MAX", "COUNT"
        )
        .unwrap();
        let _ = writeln!(out, "{}", "-".repeat(106));
        for (name, snap) in [
            ("insert", self.engine.insert_latency.snapshot()),
            ("batch_insert", self.engine.batch_insert_latency.snapshot()),
            ("range", self.engine.range_latency.snapshot()),
            ("flush", self.engine.flush_duration.snapshot()),
            ("compaction", self.engine.compaction_duration.snapshot()),
            ("wal_sync", self.wal.sync_latency.snapshot()),
        ] {
            if let Some(s) = snap {
                let _ = writeln!(
                    out,
                    "{:>16} {:>10} {:>10} {:>10} {:>10} {:>10} {:>10} {:>10} {:>10}",
                    name, s.min, s.p50, s.p90, s.p95, s.p99, s.p99_9, s.max, s.count
                );
            }
        }

        out
    }

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
