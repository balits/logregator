use std::fmt::Write;

use logregator_core::metrics::Metrics;

pub fn dump_metrics(metrics: &Metrics) -> String {
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
        metrics.server.connections_accepted.value(),
        metrics.server.connections_active.value(),
        metrics.server.frames_read.value(),
        metrics.server.frames_written.value(),
    )
    .unwrap();

    writeln!(out, "\n==> ENGINE METRICS <==").unwrap();
    writeln!(
        out,
        "{:>8} {:>8} {:>10} {:>8} {:>8} {:>8} {:>10} {:>10} {:>10} {:>5} {:>8} {:>8}",
        "INSERTS",
        "BATCH",
        "RECORDS",
        "FAIL",
        "RANGES",
        "R_FAIL",
        "SCANNED",
        "MT_BYTES",
        "MT_LIM",
        "SST",
        "FLUSHES",
        "COMPACT"
    )
    .unwrap();
    writeln!(
        out,
        "{:>8} {:>8} {:>10} {:>8} {:>8} {:>8} {:>10} {:>10} {:>10} {:>5} {:>8} {:>8}",
        metrics.engine.insert_count.value(),
        metrics.engine.batch_insert_count.value(),
        metrics.engine.records_inserted.value(),
        metrics.engine.insert_failures.value(),
        metrics.engine.range_count.value(),
        metrics.engine.range_failures.value(),
        metrics.engine.records_scanned.value(),
        metrics.engine.memtable_bytes.value(),
        metrics.engine.memtable_limit.value(),
        metrics.engine.sstable_count.value(),
        metrics.engine.flush_count.value(),
        metrics.engine.compaction_count.value(),
    )
    .unwrap();

    writeln!(out, "\n==> WAL METRICS <==").unwrap();
    writeln!(out, "{:>10} {:>12} {:>8}", "WRITES", "BYTES", "SYNCS").unwrap();
    writeln!(
        out,
        "{:>10} {:>12} {:>8}",
        metrics.wal.write_count.value(),
        metrics.wal.write_bytes.value(),
        metrics.wal.sync_count.value(),
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
        ("insert", metrics.engine.insert_latency.snapshot()),
        ("batch_insert", metrics.engine.batch_insert_latency.snapshot()),
        ("range", metrics.engine.range_latency.snapshot()),
        ("flush", metrics.engine.flush_duration.snapshot()),
        ("compaction", metrics.engine.compaction_duration.snapshot()),
        ("wal_sync", metrics.wal.sync_latency.snapshot()),
    ] {
        if let Some(s) = snap {
            let _ = writeln!(
                out,
                "{:>16} {:>10} {:>10} {:>10} {:>10} {:>10} {:>10} {:>10} {:>10}",
                name, s.min, s.p50, s.p90, s.p95, s.p99, s.p99_9, s.max, s.sample_count
            );
        }
    }

    out
}
