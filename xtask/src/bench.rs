use std::net::TcpStream;
use std::os::unix::fs as unixfs;
use std::path::PathBuf;
use std::process::Command;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use clap::Args;
use color_eyre::eyre::{Context, Result, bail};
use crate::print::{info, warn};

const BENCH_RUN_DIR: &str = "bench/runs";
const BASELINE_POINTER: &str = "bench/runs/baseline/__baseline.json";

#[derive(Debug, Args)]
pub struct BenchArgs {
    #[arg(short, long, required = true)]
    pub tag: String,

    #[arg(short, long, default_value_t = 30)]
    pub duration: u32,

    #[arg(long, default_value = "write,read,mixed,tail", value_delimiter = ',')]
    pub workloads: Vec<String>,

    #[arg(long, default_value_t = 1)]
    pub concurrency_min: u32,

    #[arg(long, default_value_t = 16)]
    pub concurrency_max: u32,

    #[arg(long, default_value_t = 2)]
    pub concurrency_step: u32,

    #[arg(long, default_value_t = 32)]
    pub memtable_mb: u64,

    #[arg(long, default_value = "127.0.0.1:4321")]
    pub addr: String,

    #[arg(long)]
    pub noreg: bool,

    #[arg(long)]
    pub set_baseline: bool,

    #[arg(long)]
    pub baseline: Option<PathBuf>,

    #[arg(long, num_args = 0.., allow_hyphen_values = true)]
    pub server_args: Vec<String>,

    #[arg(trailing_var_arg = true)]
    pub bench_args: Vec<String>,

    #[arg(long, default_value = "false")]
    pub prep: bool,
}

pub fn run_bench(args: BenchArgs) -> Result<()> {
    if args.prep {
        info("bench", "0) prepping project");
        crate::prep::run()?
    }

    info("bench", "1) creating rundir...");
    let ts = SystemTime::now()
        .duration_since(UNIX_EPOCH)?
        .as_millis()
        .to_string();
    let rundir = PathBuf::from(format!("{}/{}__{}", BENCH_RUN_DIR, ts, args.tag));
    std::fs::create_dir_all(&rundir)
        .with_context(|| format!("failed to create rundir {rundir:?}"))?;

    info("bench", "2) creating latest symlink");
    let latest = PathBuf::from(format!("{BENCH_RUN_DIR}/latest"));
    let _ = std::fs::remove_file(&latest);
    unixfs::symlink(rundir.file_name().unwrap(), &latest)
        .with_context(|| "failed to create latest symlink")?;

    info("bench", "3) freeing port if occupied...");
    // ensure port is free before spawning
    let sock_addr: std::net::SocketAddr = args.addr.parse()
        .with_context(|| format!("invalid addr '{}'", args.addr))?;
    let deadline = SystemTime::now() + Duration::from_secs(10);
    while SystemTime::now() < deadline {
        if std::net::TcpListener::bind(sock_addr).is_ok() {
            break;
        }
        warn(&format!("port {} in use, trying to free it...", sock_addr.port()));
        let _ = Command::new("fuser")
            .args(["-k", &format!("{}/tcp", sock_addr.port())])
            .output();
        std::thread::sleep(Duration::from_millis(1000));
    }

    info("bench", "4) spawning server...");
    let server_data_dir = rundir.join("server_data");
    let mut server_cmd = Command::new("cargo");
    server_cmd.args([
        "run",
        "-p",
        "logregatord",
        "--bin",
        "server",
        "--",
        "--addr",
        &args.addr,
        "--data-dir",
        &server_data_dir.to_string_lossy(),
        "--memtable-mb",
        &args.memtable_mb.to_string(),
        "--batch-size",
        "8192",
    ]);
    for extra in &args.server_args {
        server_cmd.arg(extra);
    }
    let server = server_cmd
        .stdout(std::process::Stdio::null())
        .stderr(std::process::Stdio::inherit())
        .spawn()
        .with_context(|| "failed to spawn logregatord")?;

    info("bench", "5) waiting for server readiness...");
    let deadline = SystemTime::now() + Duration::from_secs(30);
    let mut connected = false;
    while SystemTime::now() < deadline {
        if TcpStream::connect_timeout(
            &args.addr.parse().unwrap(),
            Duration::from_millis(500),
        )
        .is_ok()
        {
            connected = true;
            break;
        }
        std::thread::sleep(Duration::from_millis(500));
    }
    if !connected {
        bail!("server at {} did not become ready within 30s", args.addr);
    }

    info("bench", "6) spawning benchmark...");
    let mut bench_cmd = Command::new("cargo");
    bench_cmd.args([
        "run",
        "-p",
        "logregator-tools",
        "--bin",
        "bench",
        "--",
        "--tag",
        &args.tag,
        "--duration",
        &args.duration.to_string(),
        "--workloads",
        &args.workloads.join(","),
        "--concurrency-min",
        &args.concurrency_min.to_string(),
        "--concurrency-max",
        &args.concurrency_max.to_string(),
        "--concurrency-step",
        &args.concurrency_step.to_string(),
        "--addr",
        &args.addr,
        "--out-dir",
        &rundir.to_string_lossy(),
    ]);
    for extra in &args.bench_args {
        bench_cmd.arg(extra);
    }
    let bench_status = bench_cmd
        .stdout(std::process::Stdio::inherit())
        .stderr(std::process::Stdio::inherit())
        .status()
        .with_context(|| "failed to run bench binary")?;
    if !bench_status.success() {
        bail!("bench binary exited with failure");
    }

    info("bench", "7) droping server...");
    drop(server);

    let mut _regression = false;
    if !args.noreg {
        let baseline_dir = match &args.baseline {
            Some(p) => p.clone(),
            None => {
                let contents = std::fs::read_to_string(BASELINE_POINTER).unwrap_or_default();
                let v: serde_json::Value =
                    serde_json::from_str(&contents).unwrap_or(serde_json::json!({}));
                PathBuf::from(v["baseline"].as_str().unwrap_or(""))
            }
        };
        let baseline_result = baseline_dir.join("benchmark_result.json");
        if baseline_result.exists() {
            info("bench", "8) Regression check...");
            let cmp_status = Command::new("cargo")
                .args([
                    "run",
                    "-p",
                    "logregator-tools",
                    "--bin",
                    "cmp",
                    "--",
                    "-b",
                    &baseline_result.to_string_lossy(),
                    "-t",
                    &rundir.join("benchmark_result.json").to_string_lossy(),
                    "--out-file",
                    &rundir.join("reg.json").to_string_lossy(),
                    "--reg",
                ])
                .stdout(std::process::Stdio::inherit())
                .stderr(std::process::Stdio::inherit())
                .status()
                .with_context(|| "failed to run cmp binary")?;
            _regression = !cmp_status.success();
        } else {
            warn(&format!(
                "no baseline result file at {baseline_result:?}, skipping regression check"
            ));
        }
    }

    if args.set_baseline {
        info("bench", "9) capturing baseline...");
        let bas_status = Command::new("cargo")
            .args([
                "run",
                "-p",
                "logregator-tools",
                "--bin",
                "baseline",
                "--",
                "current",
                &rundir.to_string_lossy(),
                "--tag",
                &args.tag,
            ])
            .stdout(std::process::Stdio::null())
            .stderr(std::process::Stdio::inherit())
            .status()
            .with_context(|| "failed to run baseline binary")?;
        if !bas_status.success() {
            warn("baseline capture failed");
        }
    }

    info("bench", &format!("10) results: {}", rundir.display()));
    Ok(())
}
