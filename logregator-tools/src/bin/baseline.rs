use std::path::{Path, PathBuf};
use std::fs;

use anyhow::Context;
use clap::Parser;
use serde::{Deserialize, Serialize};
use tracing_subscriber::filter::LevelFilter;

const BASELINE_DIR: &str = "bench/runs/baseline";

#[derive(Debug, Parser)]
struct Cli {
    #[command(subcommand)]
    cmd: Command,
}

#[derive(Debug, Parser)]
enum Command {
    /// Capture a run as a new baseline (copies into baseline collection)
    Current {
        /// Path to benchmark_result.json (or a directory containing it)
        path: String,
        #[arg(long)]
        tag: Option<String>,
    },
    /// List all saved baselines
    List,
    /// Switch the active baseline pointer to a given id
    Set {
        /// Baseline id (e.g. "01") — or "latest"
        id: String,
    },
    /// Alias for `set`
    Switch {
        id: String,
    },
}

#[derive(Debug, Serialize, Deserialize)]
struct BaselinePointer {
    baseline: String,
}

fn main() {
    tracing_subscriber::fmt()
        .with_writer(std::io::stderr)
        .with_max_level(LevelFilter::INFO)
        .init();

    let cli = Cli::parse();
    let res = match cli.cmd {
        Command::Current { path, tag } => cmd_current(&path, tag.as_deref()),
        Command::List => cmd_list(),
        Command::Set { id } => cmd_set(&id),
        Command::Switch { id } => cmd_set(&id),
    };
    if let Err(e) = res {
        eprintln!("FATAL: {e:#}\nTRACE:\n");
        for (i, e) in e.chain().enumerate() {
            eprintln!("  {}: {:#}", i, e);
        }
        std::process::exit(1);
    }
}

fn baseline_dir() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR", "no cargo manifest env found"))
        .parent()
        .unwrap()
        .join(BASELINE_DIR)
}

fn cmd_current(src: &str, tag: Option<&str>) -> anyhow::Result<()> {
    let bdir = baseline_dir();
    fs::create_dir_all(&bdir).context("failed to create baseline dir")?;

    // resolve source path
    let src_path = Path::new(src);
    let result_file = if src_path.is_dir() {
        src_path.join("benchmark_result.json")
    } else {
        src_path.to_path_buf()
    };

    if !result_file.exists() {
        anyhow::bail!("source file not found: {}", result_file.display());
    }

    // determine new id
    let existing = existing_baselines(&bdir)?;
    let next_id = existing
        .iter()
        .filter_map(|(id, _)| id.parse::<u32>().ok())
        .max()
        .map(|n| n + 1)
        .unwrap_or(1);

    let tag = tag.unwrap_or("baseline");
    let dir_name = format!("{:02}__{tag}", next_id);
    let dest_dir = bdir.join(&dir_name);
    fs::create_dir_all(&dest_dir).context("failed to create baseline dest dir")?;

    // copy files
    let src_dir = result_file.parent().unwrap();
    for fname in &[
        "benchmark_result.json",
        "meta.json",
        "mem_report.json",
        "reg.json",
    ] {
        let src_f = src_dir.join(fname);
        if src_f.exists() {
            fs::copy(&src_f, dest_dir.join(fname))
                .with_context(|| format!("failed to copy {fname}"))?;
        }
    }

    // update pointer
    let pointer = BaselinePointer {
        baseline: format!("{BASELINE_DIR}/{dir_name}"),
    };
    let ptr_json = serde_json::to_string(&pointer)?;
    fs::write(bdir.join("__baseline.json"), &ptr_json)?;

    tracing::info!("baseline saved: {dir_name}");
    Ok(())
}

fn cmd_list() -> anyhow::Result<()> {
    let bdir = baseline_dir();
    if !bdir.exists() {
        tracing::info!("no baselines yet ({} does not exist)", bdir.display());
        return Ok(());
    }

    let current_ptr = bdir.join("__baseline.json");
    let active = if current_ptr.exists() {
        let ptr: BaselinePointer = serde_json::from_str(
            &fs::read_to_string(&current_ptr)?,
        )?;
        Some(ptr.baseline)
    } else {
        None
    };

    let entries = existing_baselines(&bdir)?;
    if entries.is_empty() {
        tracing::info!("no baselines found in {}", bdir.display());
        return Ok(());
    }

    println!("baselines:");
    for (id, dir) in &entries {
        let marker = match &active {
            Some(a) if a == dir => " <-- active",
            _ => "",
        };
        println!("  {id}: {dir}{marker}");
    }
    Ok(())
}

fn cmd_set(id: &str) -> anyhow::Result<()> {
    let bdir = baseline_dir();
    if !bdir.exists() {
        anyhow::bail!("baseline dir {} does not exist", bdir.display());
    }

    let entries = existing_baselines(&bdir)?;

    let target = if id == "latest" {
        entries.last().map(|(_, dir)| dir.clone())
    } else {
        entries
            .iter()
            .find(|(eid, _)| eid == id)
            .map(|(_, dir)| dir.clone())
    };

    match target {
        Some(dir) => {
            let pointer = BaselinePointer { baseline: dir.clone() };
            let ptr_json = serde_json::to_string(&pointer)?;
            fs::write(bdir.join("__baseline.json"), &ptr_json)?;
            tracing::info!("active baseline switched to: {dir}");
            Ok(())
        }
        None => {
            let available: Vec<_> = entries.iter().map(|(id, _)| id.as_str()).collect();
            anyhow::bail!(
                "baseline '{id}' not found. Available: {}",
                available.join(", ")
            );
        }
    }
}

fn existing_baselines(bdir: &Path) -> anyhow::Result<Vec<(String, String)>> {
    let mut entries: Vec<(String, String)> = Vec::new();
    for entry in fs::read_dir(bdir)? {
        let entry = entry?;
        let path = entry.path();
        if path.is_dir() {
            if let Some(dirname) = path.file_name().and_then(|n| n.to_str()) {
                if let Some(underscore) = dirname.find("__") {
                    let id = &dirname[..underscore];
                    let full = format!("{BASELINE_DIR}/{dirname}");
                    entries.push((id.to_string(), full));
                }
            }
        }
    }
    entries.sort_by(|a, b| a.0.parse::<u32>().unwrap_or(0).cmp(&b.0.parse::<u32>().unwrap_or(0)));
    Ok(entries)
}
