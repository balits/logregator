pub enum RegressionStatus {
    Passed,
    Regressed {
        metric: RegressionMetric,
        baseline: f64,
        target: f64,
        delta_pct: f64,
    },
}

#[derive(Debug, Clone, clap::ValueEnum)]
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

pub fn check_for_regression(
    baseline_tolerance_pct: f64,
    metric: RegressionMetric,
    target: f64,
    baseline: f64,
) -> RegressionStatus {
    let delta = target - baseline;
    if delta == 0.0 {
        return RegressionStatus::Passed;
    }
    let delta_pct = (delta / baseline.abs()) * 100.0;

    let (multiplier, lower_is_better) = match metric {
        RegressionMetric::InsertPerSecond => (1.0, false),
        RegressionMetric::RangePerSecond => (1.5, true),
        RegressionMetric::InsertLatencyP50 => (4.0, true),
        RegressionMetric::RangeLatencyP50 => (4.0, true),
        RegressionMetric::InsertLatencyP99 => (1.5, true),
        RegressionMetric::RangeLatencyP99 => (4.0, true),
        RegressionMetric::RssMaxKb => (2.0, true),
    };

    let allowed_pct = baseline_tolerance_pct * multiplier;

    if lower_is_better {
        if delta_pct > allowed_pct {
            return RegressionStatus::Regressed {
                metric,
                baseline,
                target,
                delta_pct,
            };
        }
    } else {
        if delta_pct < -allowed_pct {
            return RegressionStatus::Regressed {
                metric,
                baseline,
                target,
                delta_pct,
            };
        }
    }

    RegressionStatus::Passed
}
