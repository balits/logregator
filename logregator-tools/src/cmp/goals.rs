use std::collections::HashMap;
use std::path::Path;

use crate::bench::Workload;

#[derive(Debug, Clone)]
pub struct Goals {
    by_workload: HashMap<Workload, HashMap<String, f64>>,
    aggregate: HashMap<String, f64>,
}

impl Goals {
    pub fn from_file(path: impl AsRef<Path>) -> anyhow::Result<Option<Self>> {
        let path = path.as_ref();
        if !path.exists() {
            return Ok(None);
        }
        let content = std::fs::read_to_string(path)?;
        let parsed: GoalsToml = toml::from_str(&content)?;
        Ok(Some(parsed.into()))
    }

    pub fn goal_for(&self, workload: Option<Workload>, key: &str) -> Option<f64> {
        match workload {
            Some(wl) => self.by_workload.get(&wl)?.get(key).copied(),
            None => self.aggregate.get(key).copied(),
        }
    }
}

#[derive(serde::Deserialize)]
struct GoalsToml {
    #[serde(default)]
    write: Option<HashMap<String, f64>>,
    #[serde(default)]
    read: Option<HashMap<String, f64>>,
    #[serde(default)]
    mixed: Option<HashMap<String, f64>>,
    #[serde(default)]
    tail: Option<HashMap<String, f64>>,
    #[serde(default)]
    bulk: Option<HashMap<String, f64>>,
    #[serde(default)]
    aggregate: Option<HashMap<String, f64>>,
}

impl From<GoalsToml> for Goals {
    fn from(g: GoalsToml) -> Self {
        let mut by_workload = HashMap::new();
        if let Some(m) = g.write { by_workload.insert(Workload::Write, m); }
        if let Some(m) = g.read { by_workload.insert(Workload::Read, m); }
        if let Some(m) = g.mixed { by_workload.insert(Workload::Mixed, m); }
        if let Some(m) = g.tail { by_workload.insert(Workload::Tail, m); }
        if let Some(m) = g.bulk { by_workload.insert(Workload::Bulk, m); }
        Goals {
            by_workload,
            aggregate: g.aggregate.unwrap_or_default(),
        }
    }
}
