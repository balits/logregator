use std::process::Command;

use color_eyre::eyre::{bail, Result};

use crate::print;

pub fn run() -> Result<()> {
    print::info("prep", "building...");
    let status = Command::new("cargo")
        .args(["build", "--color", "always"])
        .status()?;
    if !status.success() {
        bail!("cargo build failed");
    }

    print::info("prep", "linting...");
    let status = Command::new("cargo")
        .args(["clippy", "--color", "always", "--", "-D", "warnings"])
        .status()?;
    if !status.success() {
        bail!("cargo clippy failed");
    }

    print::info("prep", "formatting...");
    let status = Command::new("cargo")
        .args(["fmt", "--color", "always"])
        .status()?;
    if !status.success() {
        bail!("cargo fmt failed");
    }

    print::info("prep", "all checks passed");
    Ok(())
}
