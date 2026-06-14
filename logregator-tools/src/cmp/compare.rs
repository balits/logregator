use std::collections::HashMap;

use logregator_core::metrics;
use crate::{bench, mem};

use super::{FieldCmp, RunComparisons, CmpResult, Context, ChangeDirection, ChangeDirection::*};

pub fn compare_benchmarks(
    baseline: String,
    targets: Vec<String>,
) -> anyhow::Result<CmpResult> {
    let baseline_result = parse_benchmark(&baseline);
    let n_targets = targets.len();

    let target_results: Vec<bench::BenchmarkResult> =
        targets.iter().map(|t| parse_benchmark(t)).collect();

    let target_indices: Vec<HashMap<Context, &bench::SingleRunResult>> = target_results
        .iter()
        .map(|r| build_workload_index(&r.runs))
        .collect();

    let mut cmps: Vec<RunComparisons> = vec![];

    for (cw, run_b) in build_workload_index(&baseline_result.runs) {
        for ti in &target_indices {
            if let Some(run_t) = ti.get(&cw) {
                cmps.push(cmp_runs(run_b, run_t));
            }
        }
    }

    cmps.sort_by(|a, b| {
        let ord = |(conc, wl): (u32, bench::Workload)| match wl {
            bench::Workload::Write => (0, conc),
            bench::Workload::Read => (1, conc),
            bench::Workload::Mixed => (2, conc),
            bench::Workload::Tail => (3, conc),
            bench::Workload::Bulk => (4, conc),
        };
        ord((a.cw.concurrency, a.cw.workload))
            .cmp(&ord((b.cw.concurrency, b.cw.workload)))
    });

    let metrics: Vec<FieldCmp> = if n_targets > 0 {
        let last = &target_results[n_targets - 1];
        cmp_metrics(&baseline_result.metrics, &last.metrics)
    } else {
        vec![]
    };

    let saturations: Vec<FieldCmp> = if n_targets > 0 {
        let last = &target_results[n_targets - 1];
        cmp_saturations(&baseline_result.saturations, &last.saturations)
    } else {
        vec![]
    };

    #[allow(clippy::unnecessary_unwrap)]
    let mem: Vec<FieldCmp> = if n_targets > 0
        && baseline_result.mem.is_some()
        && target_results[n_targets - 1].mem.is_some()
    {
        cmp_mem(
            baseline_result.mem.as_ref().unwrap(),
            target_results[n_targets - 1].mem.as_ref().unwrap(),
        )
    } else {
        vec![]
    };

    let mut result = CmpResult {
        files: {
            let mut v = vec![baseline];
            v.extend(targets);
            v
        },
        comparisons: cmps,
        saturations,
        metrics,
        mem,
        regressed_fields: vec![],
    };

    result.regressed_fields = find_regressions(&result);
    Ok(result)
}

fn find_regressions(cmp: &CmpResult) -> Vec<FieldCmp> {
    let mut regressed = Vec::new();
    for run in &cmp.comparisons {
        for f in &run.fields {
            if is_regressed(f) {
                regressed.push(f.clone());
            }
        }
    }
    for f in &cmp.saturations {
        if is_regressed(f) {
            regressed.push(f.clone());
        }
    }
    for f in &cmp.metrics {
        if is_regressed(f) {
            regressed.push(f.clone());
        }
    }
    for f in &cmp.mem {
        if is_regressed(f) {
            regressed.push(f.clone());
        }
    }
    regressed
}

fn is_regressed(f: &FieldCmp) -> bool {
    match f.direction {
        ChangeDirection::HigherBetter => f.delta_pct < -5.0,
        ChangeDirection::LowerBetter => f.delta_pct > 20.0,
        ChangeDirection::Neutral => false,
    }
}

fn cmp_runs(
    b: &bench::SingleRunResult,
    t: &bench::SingleRunResult,
) -> RunComparisons {
    let ctx = Context {
        concurrency: b.run_config.concurrency,
        workload: b.run_config.workload,
    };
    let mut fields = Vec::new();

    let c = Some(ctx.clone());
    fields.extend(throughput_fields(c.clone(), b, t));
    fields.extend(latency_fields("insert", c.clone(), &b.insert_latency, &t.insert_latency));
    fields.extend(latency_fields("range", c, &b.range_latency, &t.range_latency));

    RunComparisons { cw: ctx, fields }
}

fn throughput_fields(
    ctx: Option<Context>,
    b: &bench::SingleRunResult,
    t: &bench::SingleRunResult,
) -> Vec<FieldCmp> {
    vec![
        field("inserts/s", b.insert_ops_per_sec, t.insert_ops_per_sec, false, HigherBetter, ctx.clone()),
        field("ranges/s", b.range_ops_per_sec, t.range_ops_per_sec, false, HigherBetter, ctx.clone()),
        field("records/range", b.avg_records_per_range, t.avg_records_per_range, false, HigherBetter, ctx.clone()),
        field("SSTs", b.sstable_count as f64, t.sstable_count as f64, false, Neutral, ctx),
    ]
}

fn latency_fields(
    prefix: &str,
    ctx: Option<Context>,
    b: &metrics::LatencySnapshot,
    t: &metrics::LatencySnapshot,
) -> Vec<FieldCmp> {
    vec![
        field_us(&format!("{prefix}_latency min"), b.min, t.min, LowerBetter, ctx.clone()),
        field_us(&format!("{prefix}_latency p50"), b.p50, t.p50, LowerBetter, ctx.clone()),
        field_us(&format!("{prefix}_latency p90"), b.p90, t.p90, LowerBetter, ctx.clone()),
        field_us(&format!("{prefix}_latency p99"), b.p99, t.p99, LowerBetter, ctx.clone()),
        field_us(&format!("{prefix}_latency max"), b.max, t.max, LowerBetter, ctx),
    ]
}

fn field_us(
    key: &str,
    prev: u64,
    new: u64,
    dir: ChangeDirection,
    ctx: Option<Context>,
) -> FieldCmp {
    field(key, prev as f64, new as f64, true, dir, ctx)
}

fn field(
    key: &str,
    prev: f64,
    new: f64,
    is_latency: bool,
    dir: ChangeDirection,
    ctx: Option<Context>,
) -> FieldCmp {
    let delta = new - prev;
    let delta_pct = if prev != 0.0 {
        (delta / prev.abs()) * 100.0
    } else if delta == 0.0 {
        0.0
    } else {
        f64::INFINITY
    };
    FieldCmp {
        key: key.into(),
        prev,
        new,
        delta,
        delta_pct,
        is_latency,
        direction: dir,
        context: ctx,
    }
}

fn cmp_metrics(
    b: &Option<metrics::MetricsSnapshot>,
    t: &Option<metrics::MetricsSnapshot>,
) -> Vec<FieldCmp> {
    let (Some(ref b), Some(ref t)) = (b.as_ref(), t.as_ref()) else {
        return vec![];
    };

    let b_sync = b.wal.sync_latency.as_ref();
    let t_sync = t.wal.sync_latency.as_ref();
    let b_p50 = b_sync.map(|s| s.p50 as f64).unwrap_or(0.0);
    let t_p50 = t_sync.map(|s| s.p50 as f64).unwrap_or(0.0);

    vec![
        field("wal_sync p50 (us)", b_p50, t_p50, true, LowerBetter, None),
        field("wal_write_count", b.wal.write_count as f64, t.wal.write_count as f64, false, Neutral, None),
        field("wal_write_bytes", b.wal.write_bytes as f64, t.wal.write_bytes as f64, false, Neutral, None),
        field("wal_sync_count", b.wal.sync_count as f64, t.wal.sync_count as f64, false, Neutral, None),
        field("engine_flush_count", b.engine.flush_count as f64, t.engine.flush_count as f64, false, LowerBetter, None),
        field("engine_compactions", b.engine.compaction_count as f64, t.engine.compaction_count as f64, false, LowerBetter, None),
        field("engine_records", b.engine.records_inserted as f64, t.engine.records_inserted as f64, false, Neutral, None),
        field("engine_memtable_bytes", b.engine.memtable_bytes as f64, t.engine.memtable_bytes as f64, false, LowerBetter, None),
        field("engine_sst_count", b.engine.sstable_count as f64, t.engine.sstable_count as f64, false, Neutral, None),
        field("server_conns_accepted", b.server.connections_accepted as f64, t.server.connections_accepted as f64, false, Neutral, None),
        field("server_frames_read", b.server.frames_read as f64, t.server.frames_read as f64, false, Neutral, None),
        field("server_frames_written", b.server.frames_written as f64, t.server.frames_written as f64, false, Neutral, None),
    ]
}

fn cmp_saturations(
    b: &[bench::Saturation],
    t: &[bench::Saturation],
) -> Vec<FieldCmp> {
    let mut fields = vec![];

    for b_sat in b {
        let label = format!("{:?}", b_sat.workload).to_lowercase();
        if let Some(t_sat) = t.iter().find(|s| s.workload == b_sat.workload) {
            fields.push(field(
                &format!("sat_{label}_throughput"),
                b_sat.max_throughput,
                t_sat.max_throughput,
                false,
                HigherBetter,
                None,
            ));
            fields.push(field(
                &format!("sat_{label}_conc_peak"),
                b_sat.max_concurrency as f64,
                t_sat.max_concurrency as f64,
                false,
                HigherBetter,
                None,
            ));
        }
    }

    fields
}

fn cmp_mem(b: &mem::MemReport, t: &mem::MemReport) -> Vec<FieldCmp> {
    vec![
        field("mem_rss_min_kb", b.rss_min as f64, t.rss_min as f64, false, LowerBetter, None),
        field("mem_rss_max_kb", b.rss_max as f64, t.rss_max as f64, false, LowerBetter, None),
        field("mem_rss_avg_kb", b.rss_avg as f64, t.rss_avg as f64, false, LowerBetter, None),
        field("mem_rss_sum_kb", b.rss_sum as f64, t.rss_sum as f64, false, LowerBetter, None),
        field("mem_rss_end_kb", b.rss_last as f64, t.rss_last as f64, false, LowerBetter, None),
        field("mem_growth_kbs", b.growth_rate_kb as f64, t.growth_rate_kb as f64, false, LowerBetter, None),
        field("mem_growth_mbs", b.growth_rate_mb as f64, t.growth_rate_mb as f64, false, LowerBetter, None),
        field("mem_sample_size", b.sample_size as f64, t.sample_size as f64, false, Neutral, None),
    ]
}

fn parse_benchmark(path: &str) -> bench::BenchmarkResult {
    let f = std::fs::OpenOptions::new()
        .read(true)
        .open(path)
        .expect("failed to open benchmark file");
    let reader = std::io::BufReader::new(f);
    serde_json::from_reader(reader).expect("failed to deserialize benchmark result from file")
}

fn build_workload_index(
    results: &[bench::SingleRunResult],
) -> HashMap<Context, &bench::SingleRunResult> {
    let mut map = HashMap::with_capacity(results.len());
    for res in results.iter() {
        let conc = Context {
            workload: res.run_config.workload,
            concurrency: res.run_config.concurrency,
        };
        map.insert(conc, res);
    }
    map
}

pub fn extract_tag(result: &bench::BenchmarkResult, path: &str) -> String {
    if let Some(ref tag) = result.args.tag {
        if !tag.is_empty() {
            return tag.clone();
        }
    }
    let p = std::path::Path::new(path);
    if let Some(parent) =
        p.parent().and_then(|d| d.file_name()).and_then(|s| s.to_str())
    {
        if let Some(tag_part) = parent.split("__").nth(1) {
            return tag_part.to_string();
        }
        return parent.to_string();
    }
    p.file_stem()
        .and_then(|s| s.to_str())
        .unwrap_or("unknown")
        .to_string()
}
