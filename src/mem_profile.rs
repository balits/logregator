use std::{fmt::Write, fs, sync::Arc, time::{Duration, Instant}};

use serde::{Serialize, Deserialize};

/// A single memory sample point.
#[derive(Debug, Clone)]
pub struct MemSample {
    pub elapsed_secs: f64,
    pub rss_kb: u64,
    pub vms_kb: u64,
    pub data_kb: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct EngineSample {
    pub elapsed_secs: f64,
    pub memtable_bytes: u64, 
    pub sst_count: u64,
    pub rss_bytes: u64,
    pub delta: i64,
}

/// A compilation of mutliple sample points.
#[derive(Debug, Default, Serialize, Deserialize)]
pub struct MemReport {
    pub rss_values: Vec<u64>,
    pub rss_min: u64,
    pub rss_max: u64,
    pub rss_avg: u64,
    pub rss_sum: u64,
    pub rss_last: u64,
    pub duration: f64,
    pub growth_rate_kb: f64,
    pub growth_rate_mb: f64,
    pub sample_size: u64,
    pub engine_samples: Vec<EngineSample>,
}

/// Collects /proc/self/status samples at a fixed interval.
pub struct MemProfiler {
    rx: tokio::sync::mpsc::Receiver<MemSample>,
    handle: tokio::task::JoinHandle<()>,
    stop: Arc<tokio::sync::Notify>,
}

fn read_vm_rss() -> (u64, u64, u64) {
    let status = match fs::read_to_string("/proc/self/status") {
        Ok(s) => s,
        Err(_) => return (0, 0, 0),
    };
    let mut rss = 0u64;
    let mut vms = 0u64;
    let mut data = 0u64;
    for line in status.lines() {
        if let Some(val) = line.strip_prefix("VmRSS:") {
            rss = val.trim().split_whitespace().next().and_then(|s| s.parse().ok()).unwrap_or(0);
        } else if let Some(val) = line.strip_prefix("VmSize:") {
            vms = val.trim().split_whitespace().next().and_then(|s| s.parse().ok()).unwrap_or(0);
        } else if let Some(val) = line.strip_prefix("VmData:") {
            data = val.trim().split_whitespace().next().and_then(|s| s.parse().ok()).unwrap_or(0);
        }
    }
    (rss, vms, data)
}

impl MemProfiler {
    pub fn start(interval: Duration) -> Self {
        let (tx, rx) = tokio::sync::mpsc::channel(10_000);
        let stop = Arc::new(tokio::sync::Notify::new());
        let stop_clone = stop.clone();
        let start = Instant::now();

        let handle = tokio::spawn(async move {
            loop {
                tokio::select! {
                    _ = tokio::time::sleep(interval) => {}
                    _ = stop_clone.notified() => {
                        // Send one final sample then exit
                        let (rss, vms, data) = read_vm_rss();
                        let _ = tx.send(MemSample {
                            elapsed_secs: start.elapsed().as_secs_f64(),
                            rss_kb: rss, vms_kb: vms, data_kb: data,
                        }).await;
                        break;
                    }
                }
                let (rss, vms, data) = read_vm_rss();
                if tx.try_send(MemSample {
                    elapsed_secs: start.elapsed().as_secs_f64(),
                    rss_kb: rss,
                    vms_kb: vms,
                    data_kb: data,
                }).is_err() {
                    // channel full skip this sample
                }
            }
        });

        Self { rx, handle, stop }
    }

    pub async fn stop(mut self) -> Vec<MemSample> {
        self.stop.notify_one();
        let _ = self.handle.await;
        let mut samples = Vec::new();
        while let Ok(s) = self.rx.try_recv() {
            samples.push(s);
        }
        samples
    }

    pub fn new_report_str(samples: &[MemSample], engine_samples: &[(f64, usize, usize)]) -> String {
        let mut out = String::new();

        if samples.is_empty() {
            writeln!(out, "No memory samples collected.").unwrap();
            return out;
        }

        let rss_values: Vec<u64> = samples.iter().map(|s| s.rss_kb).collect();
        let rss_min = *rss_values.iter().min().unwrap_or(&0);
        let rss_max = *rss_values.iter().max().unwrap_or(&0);
        let rss_sum: u64 = rss_values.iter().sum();
        let rss_avg = rss_sum / rss_values.len() as u64;
        let rss_last = rss_values[rss_values.len() - 1];

        let dur = samples.last().unwrap().elapsed_secs - samples.first().unwrap().elapsed_secs;
        let growth_rate = if dur > 0.0 {
            (rss_last as f64 - rss_values[0] as f64) / dur
        } else {
            0.0
        };

        let em_samples = engine_samples.len();

        writeln!(out, "==> MEMORY PROFILE <==\n").unwrap();
        writeln!(out, "{:>10} {:>10} {:>10} {:>10} {:>8} {:>8}",
            "RSS_MIN", "RSS_AVG", "RSS_MAX", "RSS_END", "SAMPLES", "DUR_S").unwrap();
        writeln!(out, "{:>10} {:>10} {:>10} {:>10} {:>8} {:>8.1}",
            format_size(rss_min),
            format_size(rss_avg),
            format_size(rss_max),
            format_size(rss_last),
            samples.len(),
            dur,
        ).unwrap();
        writeln!(out, "Growth rate: {:.1} KB/s ({} MB over {:.0}s)",
            growth_rate,
            (growth_rate * dur / 1024.0) as u64,
            dur,
        ).unwrap();

        if em_samples > 0 {
            writeln!(out, "\n==> ENGINE STATE SAMPLES ({} total) <==", em_samples).unwrap();
            writeln!(out, "{:>10} {:>12} {:>12} {:>12} {:>12}",
                "ELAPSED", "MEMTBL_KB", "SST_CNT", "RSS_KB", "DELTA_KB").unwrap();
            for (i, (elapsed, mt_bytes, sst_cnt)) in engine_samples.iter().enumerate() {
                if i % std::cmp::max(1, em_samples / 20) == 0 || i == em_samples - 1 {
                    let rss = samples.iter()
                        .find(|s| (s.elapsed_secs - elapsed).abs() < 0.5)
                        .map(|s| s.rss_kb)
                        .unwrap_or(0);
                    let delta = if i > 0 {
                        rss as i64 - engine_samples[0].1 as i64 / 1024
                    } else {
                        0
                    };
                    writeln!(out, "{:>10.2} {:>12} {:>12} {:>12} {:>12}",
                        elapsed, mt_bytes / 1024, sst_cnt, rss, delta
                    ).unwrap();
                }
            }
        }

        writeln!(out, "\n==> RSS OVER TIME <==").unwrap();
        let width = 60usize;
        let min = rss_values.iter().min().copied().unwrap_or(0);
        let max = rss_values.iter().max().copied().unwrap_or(1);
        let range = (max - min).max(1);
        let mut prev_bar: Option<f64> = None;

        for (i, s) in samples.iter().enumerate() {
            if i % std::cmp::max(1, samples.len() / 20) != 0 && i != samples.len() - 1 {
                continue;
            }
            let bar_len = ((s.rss_kb - min) as f64 / range as f64 * width as f64) as usize;
            let label = format!("{:.0}s", s.elapsed_secs);
            let delta = if let Some(p) = prev_bar {
                let d = s.rss_kb as f64 - p;
                format!("{:+.0}K", d)
            } else {
                String::new()
            };
            writeln!(out, "{:>6} |{:-<width$}| {} {:>8} KB",
                label,
                "█".repeat(bar_len),
                delta,
                s.rss_kb,
                width = width,
            ).unwrap();
            prev_bar = Some(s.rss_kb as f64);
        }

        out
    }

    pub fn new_report(samples: &[MemSample], engine_samples: &[(f64, usize, usize)]) -> Option<MemReport> {
        if samples.is_empty() {
            tracing::info!("No memory samples collected.");
            return None;
        }

        let mut report = MemReport::default();

        report.rss_values = samples.iter().map(|s| s.rss_kb).collect();
        report.rss_min = *report.rss_values.iter().min().unwrap_or(&0);
        report.rss_max = *report.rss_values.iter().max().unwrap_or(&0);
        report.rss_sum = report.rss_values.iter().sum();
        report.rss_avg = report.rss_sum / report.rss_values.len() as u64;
        report.rss_last = report.rss_values[report.rss_values.len() - 1];

        report.duration = samples.last().unwrap().elapsed_secs - samples.first().unwrap().elapsed_secs;
        if report.duration > 0.0 {
            report.growth_rate_kb = (report.rss_last as f64 - report.rss_values[0] as f64) / report.duration;
            report.growth_rate_mb = report.growth_rate_kb * report.duration / 1024.0;
        }

        let em_samples = engine_samples.len();

        report.engine_samples.extend(
            engine_samples
                .iter()
                .enumerate()
                .filter_map(|(i, (elapsed, mt_bytes, sst_cnt))| {
                    if i % std::cmp::max(1, em_samples / 20) != 0 && i != em_samples - 1 {
                        return None
                    }
                    let rss = samples.iter()
                        .find(|s| (s.elapsed_secs - elapsed).abs() < 0.5)
                        .map(|s| s.rss_kb)
                        .unwrap_or(0);
                    let delta = if i > 0 {
                        rss as i64 - engine_samples[0].1 as i64 / 1024
                    } else {
                        0
                    };

                    Some(EngineSample {
                        elapsed_secs: *elapsed,
                        memtable_bytes: *mt_bytes as u64 / 1024,
                        sst_count: *sst_cnt as u64,
                        rss_bytes: rss,
                        delta: delta as i64,
                    })
            })
        );

        Some(report)
    }
}

fn format_size(kb: u64) -> String {
    if kb < 1024 {
        format!("{}KB", kb)
    } else if kb < 1024 * 1024 {
        format!("{:.1}MB", kb as f64 / 1024.0)
    } else {
        format!("{:.1}GB", kb as f64 / (1024.0 * 1024.0))
    }
}
