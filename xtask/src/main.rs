mod bench;
mod prep;
mod print;

use std::process::Command;

use clap::{Parser, Subcommand, ValueEnum};

#[derive(Debug, Clone, ValueEnum)]
#[clap(rename_all = "snake_case")]
pub enum ProfileTool {
    Perf,
    Flamegraph,
    StraceSyscall,
    StraceIo,
}

#[derive(Debug, Clone, ValueEnum)]
#[clap(rename_all = "snake_case")]
pub enum RegressionMetric {
    InsertPerSecond,
    RangePerSecond,
    InsertLatencyP50,
    RangeLatencyP50,
    InsertLatencyP99,
    RangeLatencyP99,
    RssMaxKb,
}

#[derive(Debug, Parser)]
#[command(name = "cargo xtask")]
struct Cli {
    #[command(subcommand)]
    subcommand: XtaskCmd,
}

#[derive(Debug, Subcommand)]
enum XtaskCmd {
    #[command(about = "run the benchmark load generator")]
    Bench(bench::BenchArgs),

    #[command(about = "run build & test smoke check")]
    Smoke,

    #[command(about = "build, lint, and format the codebase")]
    Prep,

    Profile {
        #[arg(short, long)]
        tool: ProfileTool,
    },
    History {
        #[arg(short, long)]
        workload: Option<String>,
        #[arg(long)]
        tag_contains: Option<String>,
        #[arg(long, default_value = "chronological")]
        sort_by: String,
        #[arg(short, long, default_value_t = 10)]
        limit: usize,
    },
    #[command(about = "compare benchmark results against a baseline")]
    Cmp {
        #[arg(short, long, required = true)]
        target: String,

        #[arg(short, long, default_value = "bench/runs/latest/benchmark_result.json")]
        baseline: String,

        #[arg(short, long)]
        goal: Option<String>,

        #[arg(short, long)]
        reg: bool,

        #[arg(long)]
        out_file: Option<String>,

        #[arg(long)]
        verbose: bool,
    },
    Trend {
        #[arg(short, long, required = true)]
        metric: RegressionMetric,
        #[arg(short, long, default_value_t = 5.0)]
        tolerance: f32,
    },
}

impl std::fmt::Display for XtaskCmd {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let s = match self {
            Self::Bench(_) => "xtask bench",
            Self::Smoke => "xtask smoke",
            Self::Prep => "xtask prep",
            Self::Profile{..} => "xtask profile",
            Self::Cmp{..} => "xtask cmp",
            Self::History{..} => "xtask history",
            Self::Trend{..} => "xtask trend",
        };
        std::fmt::write(f, format_args!("{}", s))
    }
}

fn main() -> color_eyre::Result<()> {
    print::info("xtask", "Starting xtask..");
    color_eyre::install()?;
    let args = Cli::parse();

    match args.subcommand {
        XtaskCmd::Prep => {
            prep::run()?;
        }
        XtaskCmd::Bench(bench_args) => {
            bench::run_bench(bench_args)?;
        }
        XtaskCmd::Smoke => {
            let status = Command::new("cargo")
                .args(["build", "--color", "always"])
                .status()?;
            if !status.success() {
                color_eyre::eyre::bail!("cargo build failed");
            }
            let status = Command::new("cargo")
                .args(["test", "--color", "always"])
                .status()?;
            if !status.success() {
                color_eyre::eyre::bail!("cargo test failed");
            }
        }
        XtaskCmd::Cmp { target, baseline, goal, reg, out_file, verbose } => {
            let mut cmd = Command::new("cargo");
            cmd.args([
                "run", "-p", "logregator-tools", "--bin", "cmp", "--",
                "-b", &baseline,
                "-t", &target,
            ]);
            if let Some(g) = &goal {
                cmd.args(["--goals", g]);
            }
            if reg {
                cmd.arg("--reg");
            }
            if verbose {
                cmd.arg("--verbose");
            }
            if let Some(out) = &out_file {
                cmd.args(["--out-file", out]);
            }
            let status = cmd.status()?;
            if !status.success() {
                std::process::exit(status.code().unwrap_or(1));
            }
        }
        c => {
            print::error(&format!("{c} subcommand not implemented yet"));
        }
    }

    print::info("xtask", "Finished xtask..");
    Ok(())
}

