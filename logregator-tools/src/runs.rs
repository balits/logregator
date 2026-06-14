use std::{
    fs, io, path::{Component, Path, PathBuf}, time::{SystemTime, UNIX_EPOCH}
};

use anyhow::Context;
use crate::{
    bench::{BenchmarkResult, v2::{RunBuilder, RunMeta}},
    mem::MemReport,
};

#[allow(dead_code)]
mod consts {
    pub const BENCH_RUNS_DIR: &str = "bench/runs";
    pub const BENCH_BASELINE_DIR: &str = "bench/runs/baseline";

    pub const BENCHMARK_META: &str = "meta.json";
    pub const BENCHMARK_RESULT: &str = "benchmark_result.json";
    pub const BENCHMARK_SERVER_LOG: &str = "server.log";
    pub const BENCHMARK_MEM_REPORT: &str = "mem_report.json";
    pub const PROFILING_DIR: &str = "prof";

    pub const PROF_FLAMEGRAPH_FILE: &str = "flamegraph.svg";
    pub const PROF_PERF_FILE: &str = "perf.txt";
    pub const PROF_STRACE_FILE: &str = "strace.txt";
    pub const PROF_SYSCALL_FILE: &str = "syscall.txt";
}

fn read_json<T: serde::de::DeserializeOwned>(path: &Path) -> anyhow::Result<Option<T>> {
    let file =
        fs::File::open(path).with_context(|| format!("failed to open {}", path.display()))?;

    if file.metadata()?.len() == 0 {
        eprintln!("warning: {} is empty, skipping", path.display());
        return Ok(None);
    }

    serde_json::from_reader(io::BufReader::new(file))
        .with_context(|| format!("failed to deserialize {}", path.display()))
        .map(Some)
}

pub fn unpack_rundir(run_path: &Path) -> anyhow::Result<RunBuilder> {
    let mut builder = RunBuilder::default();
    builder.base_path = Some(run_path.to_path_buf());

    for entry in fs::read_dir(run_path)
        .with_context(|| format!("failed to read run directory {}", run_path.display()))?
    {
        let entry =
            entry.with_context(|| format!("failed to read entry in {}", run_path.display()))?;
        let entry_path = entry.path();
        let last_comp = entry_path
            .components()
            .last()
            .expect("failed to get entry's last component");

        if entry_path.is_dir() {
            if let Component::Normal(inner) = last_comp {
                if inner == consts::PROFILING_DIR {
                    parse_prof_dir(&mut builder, &entry_path)
                        .with_context(|| format!("failed to process {}", entry_path.display()))?;
                }
            } else {
                eprintln!(
                    "warning: skipping entry with non-normal path: {}",
                    entry_path.display()
                );
            }
            continue;
        }

        let Some(file_name) = last_comp.as_os_str().to_str() else {
            eprintln!(
                "warning: skipping entry with non-UTF-8 name: {}",
                entry_path.display()
            );
            continue;
        };

        eprintln!("parsing entry {}", entry_path.display());

        match file_name {
            consts::BENCHMARK_META => {
                if let Some(m) = read_json::<RunMeta>(&entry_path)? {
                    builder.meta = Some(m);
                }
            }
            consts::BENCHMARK_RESULT => {
                if let Some(r) = read_json::<BenchmarkResult>(&entry_path)? {
                    builder.benchmark_result = Some(r);
                }
            }
            consts::BENCHMARK_MEM_REPORT => {
                if let Some(m) = read_json::<MemReport>(&entry_path)? {
                    builder.mem_report = Some(m);
                }
            }
            consts::BENCHMARK_SERVER_LOG => {
                let file = match fs::File::open(&entry_path) {
                    Ok(f) if f.metadata().map(|m| m.len() > 0).unwrap_or(false) => f,
                    Ok(_) => {
                        eprintln!(
                            "warning: server log is empty ({}), skipping",
                            entry_path.display()
                        );
                        continue;
                    }
                    Err(e) => {
                        eprintln!(
                            "warning: failed to open server log ({}): {}",
                            entry_path.display(),
                            e
                        );
                        continue;
                    }
                };
                builder.server_log = Some(file);
            }
            _ => {}
        }
    }

    Ok(builder)
}

fn parse_prof_dir(builder: &mut RunBuilder, dir: &Path) -> anyhow::Result<()> {
    for entry in fs::read_dir(dir)
        .with_context(|| format!("failed to read prof directory {}", dir.display()))?
    {
        let entry = entry.with_context(|| format!("failed to read entry in {}", dir.display()))?;
        let entry_path = entry.path();
        let file_name = entry_path.components().last().and_then(|c| {
            if let Component::Normal(s) = c {
                s.to_str()
            } else {
                None
            }
        });

        let Some(file_name) = file_name else {
            continue;
        };

        match file_name {
            consts::PROF_FLAMEGRAPH_FILE => {
                let file = match fs::File::open(&entry_path) {
                    Ok(f) if f.metadata().map(|m| m.len() > 0).unwrap_or(false) => f,
                    Ok(_) => {
                        eprintln!("warning: {} is empty, skipping", entry_path.display());
                        continue;
                    }
                    Err(e) => {
                        eprintln!(
                            "warning: failed to open flamegraph file ({}): {}",
                            entry_path.display(),
                            e
                        );
                        continue;
                    }
                };
                builder.prof_flamegraph = Some(file);
            }
            consts::PROF_PERF_FILE => {
                let s = fs::read_to_string(&entry_path).with_context(|| {
                    format!("failed to read perf file ({})", entry_path.display())
                })?;
                if !s.is_empty() {
                    builder.prof_perf_output = Some(s);
                }
            }
            consts::PROF_STRACE_FILE => {
                let s = fs::read_to_string(&entry_path).with_context(|| {
                    format!("failed to read strace file ({})", entry_path.display())
                })?;
                if !s.is_empty() {
                    builder.prof_strace_output = Some(s);
                }
            }
            consts::PROF_SYSCALL_FILE => {
                let s = fs::read_to_string(&entry_path).with_context(|| {
                    format!("failed to read syscall file ({})", entry_path.display())
                })?;
                if !s.is_empty() {
                    builder.prof_syscall_output = Some(s);
                }
            }
            _ => {}
        }
    }

    Ok(())
}

#[allow(dead_code)]
fn read_baseline() -> anyhow::Result<BenchmarkResult> {
    let baseline_result_path = PathBuf::from(format!(
        "{}/{}",
        consts::BENCH_BASELINE_DIR,
        consts::BENCHMARK_RESULT
    ));
    read_json(&baseline_result_path)?
        .ok_or(anyhow::format_err!("failed to load baseline benchmark_result.json"))
}

#[allow(dead_code)]
fn create_rundir(tag: String) -> anyhow::Result<PathBuf> {
    let ts = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_millis();
    let rundir_path = PathBuf::from(format!("{}/{}__{}", consts::BENCH_RUNS_DIR, ts, tag));
    fs::create_dir_all(&rundir_path)?;

    Ok(rundir_path)
}