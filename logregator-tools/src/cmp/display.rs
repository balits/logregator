use std::collections::HashMap;
use std::io;

use owo_colors::OwoColorize;

use super::{CmpResult, FieldCmp, ChangeDirection, goals::Goals};
use crate::bench::{fmt_latency_f64, Workload};

impl CmpResult {
    pub fn write_to<W: io::Write>(&self, mut w: W, goals: Option<&Goals>, verbose: bool) -> io::Result<()> {
        writeln!(w, "=> cmp: {} vs {} target(s) <=", &self.files[0], self.files.len() - 1)?;

        if verbose {
            for comp in &self.comparisons {
                let conc_label = if comp.cw.concurrency != 1 { " clients" } else { " client" };
                writeln!(w, "--- {} | {}{} ---",
                    format!("{:?}", comp.cw.workload).to_lowercase(),
                    comp.cw.concurrency,
                    conc_label,
                )?;
                write_header(&mut w, goals.is_some())?;
                write_fields(&mut w, &comp.fields, goals)?;
            }
        } else {
            write_condensed(&mut w, &self.comparisons, goals)?;
        }

        write_section(&mut w, "\n--- Aggregate Metrics ---", &self.metrics, goals, verbose)?;
        write_section(&mut w, "\n--- Saturations ---", &self.saturations, goals, verbose)?;
        write_section(&mut w, "\n--- Memory Profile ---", &self.mem, goals, verbose)?;

        if !self.regressed_fields.is_empty() {
            let regressed: Vec<&FieldCmp> = self.regressed_fields.iter()
                .filter(|f| verbose || f.context.is_none() || is_core_metric(&f.key))
                .collect();
            if !regressed.is_empty() {
                writeln!(w, "\n--- REGRESSIONS ({}) ---\n  Fields that crossed the regression threshold (throughput -5%, latency +20%).", regressed.len())?;
                write_header(&mut w, false)?;
                for f in regressed {
                    write_field_line(&mut w, f, None)?;
                }
            }
        }

        Ok(())
    }
}

fn write_condensed<W: io::Write>(w: &mut W, comparisons: &[super::RunComparisons], goals: Option<&Goals>) -> io::Result<()> {
    let mut by_workload: HashMap<Workload, Vec<&super::RunComparisons>> = HashMap::new();
    for comp in comparisons {
        by_workload.entry(comp.cw.workload).or_default().push(comp);
    }

    let mut workloads: Vec<Workload> = by_workload.keys().copied().collect();
    workloads.sort_by_key(|w| match w {
        Workload::Write => 0,
        Workload::Read => 1,
        Workload::Mixed => 2,
        Workload::Tail => 3,
        Workload::Bulk => 4,
    });

    for (idx, workload) in workloads.iter().enumerate() {
        if idx > 0 {
            writeln!(w)?;
        }
        writeln!(w, "--- {:?} ---", format!("{:?}", workload).to_lowercase())?;
        write_header(w, goals.is_some())?;

        let comps = &by_workload[workload];
        for comp in comps {
            for f in &comp.fields {
                if f.prev == 0.0 && f.new == 0.0 {
                    continue;
                }
                if is_core_metric(&f.key) {
                    write_field_line(w, f, goals)?;
                }
            }
        }
    }

    Ok(())
}

fn write_section<W: io::Write>(w: &mut W, title: &str, fields: &[FieldCmp], goals: Option<&Goals>, _verbose: bool) -> io::Result<()> {
    if fields.is_empty() {
        return Ok(());
    }
    writeln!(w, "{title}")?;
    write_header(w, goals.is_some())?;
    write_fields(w, fields, goals)
}

fn write_header<W: io::Write>(w: &mut W, show_goals: bool) -> io::Result<()> {
    if show_goals {
        writeln!(w, "  {:22} {:>12} {:>12} {:>12} {:>12} {:>10}", "", "BASELINE", "TARGET", "GOALS", "DELTA", "DELTA_PCT")
    } else {
        writeln!(w, "  {:22} {:>12} {:>12} {:>12} {:>10}", "", "BASELINE", "TARGET", "DELTA", "DELTA_PCT")
    }
}

fn write_fields<W: io::Write>(w: &mut W, fields: &[FieldCmp], goals: Option<&Goals>) -> io::Result<()> {
    for f in fields {
        if f.prev == 0.0 && f.new == 0.0 {
            continue;
        }
        write_field_line(w, f, goals)?;
    }
    Ok(())
}

fn is_core_metric(key: &str) -> bool {
    matches!(
        key,
        "inserts/s" | "ranges/s" | "insert_latency p50" | "insert_latency p99" | "range_latency p50" | "range_latency p99"
    )
}

fn write_field_line<W: io::Write>(w: &mut W, f: &FieldCmp, goals: Option<&Goals>) -> io::Result<()> {
    let key = match &f.context {
        Some(ctx) => format!("{} ({})", f.key, ctx.label()),
        None => f.key.clone(),
    };
    let (prev_s, new_s) = fmt_val(f);
    let prev_s = colored_val(&prev_s, 12, true);
    let new_s = colored_val(&new_s, 12, false);

    let goal_s = match goals.and_then(|g| g.goal_for(f.context.as_ref().map(|c| c.workload), &f.key)) {
        Some(goal) => {
            let s = if f.is_latency {
                fmt_latency_f64(goal)
            } else if goal >= 1_000_000.0 {
                format!("{:.2}M", goal / 1_000_000.0)
            } else if goal >= 1_000.0 {
                format!("{:.1}K", goal / 1_000.0)
            } else {
                format!("{:.0}", goal)
            };
            let is_met = match f.direction {
                ChangeDirection::HigherBetter => f.new >= goal,
                ChangeDirection::LowerBetter => f.new <= goal,
                ChangeDirection::Neutral => true,
            };
            let padded = format!("{s:>12}");
            if is_met {
                padded.green().to_string()
            } else {
                padded.red().to_string()
            }
        }
        None => "          —".to_string(),
    };

    if goals.is_some() {
        writeln!(
            w,
            "  {key:<30} {prev} {new} {goal} {d:>12} {p:>10}",
            key = key,
            prev = prev_s,
            new = new_s,
            goal = goal_s,
            d = colored_delta(f),
            p = colored_pct(f),
        )
    } else {
        writeln!(
            w,
            "  {key:<30} {prev} {new} {d:>12} {p:>10}",
            key = key,
            prev = prev_s,
            new = new_s,
            d = colored_delta(f),
            p = colored_pct(f),
        )
    }
}

fn fmt_val(cmp: &FieldCmp) -> (String, String) {
    if cmp.is_latency {
        (fmt_latency_f64(cmp.prev), fmt_latency_f64(cmp.new))
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
    if has_change && !is_good {
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
        format!("-{s}")
    } else if v > 0.0 {
        format!("+{s}")
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
        format!("-{s}")
    } else if v > 0.0 {
        format!("+{s}")
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
