use std::path::PathBuf;

use anyhow::Context;
use clap::Parser;
use logregator_tools::runs;

#[derive(Parser)]
struct Cli {
    target: PathBuf,
}

fn main() -> anyhow::Result<()> {
    let args = Cli::parse();
    let builder = runs::unpack_rundir(&args.target)
        .context("failed to read run directory")?;
    let _run = builder.build()
        .context("failed to build Run struct")?;
    Ok(())
}
