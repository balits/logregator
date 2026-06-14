pub mod compare;
pub mod display;
pub mod goals;

use serde::Serialize;

use crate::bench;

#[derive(Debug, Clone, Serialize, Hash, PartialEq, Eq)]
pub struct Context {
    pub concurrency: u32,
    pub workload: bench::Workload,
}

impl Context {
    pub fn label(&self) -> String {
        let conc = if self.concurrency == 1 { "client" } else { "clients" };
        format!(
            "{}, {} {}",
            format!("{:?}", self.workload).to_lowercase(),
            self.concurrency,
            conc
        )
    }
}

#[derive(Debug, Clone, Serialize)]
pub struct FieldCmp {
    pub key: String,
    pub new: f64,
    pub prev: f64,
    pub delta: f64,
    pub delta_pct: f64,
    pub is_latency: bool,
    pub direction: ChangeDirection,
    pub context: Option<Context>,
}

#[derive(Debug, Clone, Serialize)]
pub enum ChangeDirection {
    HigherBetter,
    LowerBetter,
    Neutral,
}

#[derive(Debug, Clone, Serialize)]
pub struct RunComparisons {
    pub cw: Context,
    pub fields: Vec<FieldCmp>,
}

#[derive(Debug, Serialize)]
pub struct CmpResult {
    pub files: Vec<String>,
    pub comparisons: Vec<RunComparisons>,
    pub saturations: Vec<FieldCmp>,
    pub metrics: Vec<FieldCmp>,
    pub mem: Vec<FieldCmp>,
    pub regressed_fields: Vec<FieldCmp>,
}

impl CmpResult {
    pub fn has_regression(&self) -> bool {
        !self.regressed_fields.is_empty()
    }
}

pub use compare::compare_benchmarks;
pub use compare::extract_tag;
