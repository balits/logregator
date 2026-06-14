use std::path::{Path, PathBuf};

use clap::Parser;
use logregator_tools::cmp;
use logregator_tools::cmp::goals::Goals;
use serde::Serialize;

#[derive(Debug, Serialize, Parser)]
pub struct CmpArgs {
    #[arg(short, long)]
    pub targets: Vec<String>,

    #[arg(short, long)]
    pub baseline: String,

    /// Direct output path for comparison JSON (takes precedence over --out-dir)
    #[arg(long)]
    pub out_file: Option<PathBuf>,

    /// Output directory (used when --out-file is not given)
    #[arg(long, default_value = "bench/cmps")]
    pub out_dir: PathBuf,

    #[arg(short, long)]
    pub reg: bool,

    /// Path to goals file (default: bench/goals_shortterm.toml)
    #[arg(long, default_value = "bench/goals_shortterm.toml")]
    pub goals: PathBuf,

    /// Show full output with all metrics (min, max, p90, SSTs, records/range)
    #[arg(long)]
    pub verbose: bool,
}

fn main() -> anyhow::Result<()> {
    let args = CmpArgs::parse();
    let cmp = cmp::compare_benchmarks(args.baseline.clone(), args.targets.clone())?;

    // save JSON comparison
    {
        let json = serde_json::to_string_pretty(&cmp)
            .map_err(|e| anyhow::anyhow!("failed to serialize cmp result: {e}"))?;

        let out_path = if let Some(ref file) = args.out_file {
            if let Some(parent) = Path::new(file).parent() {
                std::fs::create_dir_all(parent)?;
            }
            file.clone()
        } else {
            let baseline_result: logregator_tools::bench::BenchmarkResult =
                serde_json::from_reader(std::io::BufReader::new(std::fs::File::open(&args.baseline)?))?;
            let target_result: logregator_tools::bench::BenchmarkResult =
                serde_json::from_reader(std::io::BufReader::new(std::fs::File::open(&args.targets[0])?))?;

            let baseline_tag = cmp::extract_tag(&baseline_result, &args.baseline);
            let target_tag = cmp::extract_tag(&target_result, &args.targets[0]);

            std::fs::create_dir_all(&args.out_dir)?;
            args.out_dir.join(format!("{target_tag}__vs__{baseline_tag}.json"))
        };

        std::fs::write(&out_path, &json)
            .map_err(|e| anyhow::anyhow!("failed to write {out_path:?}: {e}"))?;
        eprintln!("Cmp:     {}", out_path.display());
    }

    // load goals
    let goals = Goals::from_file(&args.goals)?;

    // human-readable output
    let stdout = std::io::stdout().lock();
    cmp.write_to(stdout, goals.as_ref(), args.verbose)
        .expect("failed to write comparison results");

    if args.reg && cmp.has_regression() {
        eprintln!("REGRESSION(S) DETECTED");
        std::process::exit(1);
    }

    Ok(())
}
