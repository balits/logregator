use std::collections::HashMap;
use std::fs;

use clap::Parser;
use logregator::bench::{self, BenchmarkResult, RunResult, WorkloadKind};
use logregator::mem_profile;
use logregator::metrics;
use owo_colors::OwoColorize;
use serde::Serialize;

#[derive(Parser, Serialize)]
struct CmpArgs {
    #[arg(short, long)]
    targets: Vec<String>,
    #[arg(short, long)]
    baseline: String,
}

#[derive(Debug, Serialize, Hash, PartialEq, Eq)]
struct ConcWorkload {
    concurrency: u32,
    workload: bench::WorkloadKind,
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

fn main() {
    let args = CmpArgs::parse();
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

    let mem: Vec<FieldCmp> =
        if n_targets > 0 && baseline.mem.is_some() && target_results[n_targets - 1].mem.is_some() {
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
}

fn parse_benchmark(path: &str) -> bench::BenchmarkResult {
    let f = fs::OpenOptions::new()
        .read(true)
        .open(path)
        .expect("failed to open benchmark file");

    serde_json::from_reader(f).expect("failed to deserialize benchmark result from file")
}

fn build_workload_index(results: &[bench::RunResult]) -> HashMap<ConcWorkload, &bench::RunResult> {
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

fn cmp_runs(b: &bench::RunResult, t: &bench::RunResult) -> RunCmp {
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

fn to_field_cmp<N, S>(prev: N, new: N, key: S, is_latency: bool, dir: ChangeDirection) -> FieldCmp
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
