use std::path::PathBuf;

use anyhow::Context;
use tracing_appender::non_blocking::WorkerGuard;
use tracing_subscriber::filter::LevelFilter;
use tracing_subscriber::layer::SubscriberExt;
use tracing_subscriber::util::SubscriberInitExt;
use tracing_subscriber::Layer;

fn open_flame_writer(path: PathBuf) -> anyhow::Result<(impl std::io::Write, WorkerGuard)> {
    if let Some(parent) = path.parent() {
        std::fs::create_dir_all(parent)
            .with_context(|| format!("failed to create flame output directory {}", parent.display()))?;
    }
    let file = std::fs::File::create(&path)
        .with_context(|| format!("failed to create flame output file at {}", path.display()))?;
    let writer = std::io::BufWriter::new(file);
    Ok(tracing_appender::non_blocking(writer))
}

pub fn init(config: Config) -> anyhow::Result<Handle> {
    match config.profile {
        Profile::None => {
            tracing_subscriber::fmt()
                .with_file(true)
                .with_line_number(true)
                .with_max_level(config.log_level)
                .init();
            Ok(Handle { _guard: None })
        }
        Profile::Flame => {
            let path = config.flame_output.unwrap_or_else(default_flame_path);
            let (writer, guard) = open_flame_writer(path)?;
            let flame_layer = tracing_flame::FlameLayer::new(writer);

            let fmt_layer = tracing_subscriber::fmt::Layer::default()
                .with_file(true)
                .with_line_number(true)
                .with_target(false);

            tracing_subscriber::registry()
                .with(flame_layer)
                .with(fmt_layer.with_filter(config.log_level))
                .init();

            Ok(Handle { _guard: Some(guard) })
        }
        Profile::Console => {
            let console_layer = console_subscriber::spawn();
            tracing_subscriber::registry()
                .with(tracing_subscriber::fmt::layer().with_filter(config.log_level))
                .with(console_layer)
                .init();
            Ok(Handle { _guard: None })
        }
        Profile::Both => {
            let path = config.flame_output.unwrap_or_else(default_flame_path);
            let (writer, guard) = open_flame_writer(path)?;
            let flame_layer = tracing_flame::FlameLayer::new(writer);

            let console_layer = console_subscriber::spawn();
            let fmt_layer = tracing_subscriber::fmt::Layer::default()
                .with_file(true)
                .with_line_number(true)
                .with_target(false);

            tracing_subscriber::registry()
                .with(console_layer)
                .with(flame_layer)
                .with(fmt_layer.with_filter(config.log_level))
                .init();

            Ok(Handle { _guard: Some(guard) })
        }
    }
}

fn default_flame_path() -> PathBuf {
    let ts = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_millis();
    PathBuf::from("prof").join("tracing").join(format!("{ts}.folded"))
}

#[derive(Debug, Clone)]
pub struct Config {
    pub profile: Profile,
    pub flame_output: Option<PathBuf>,
    pub log_level: LevelFilter,
}

impl Config {
    pub fn from_cli(profile: &str, flame_output: Option<PathBuf>, log_level: &str) -> Self {
        Self {
            profile: Profile::from_str(profile),
            flame_output,
            log_level: parse_level(log_level),
        }
    }
}

fn parse_level(s: &str) -> LevelFilter {
    match s.to_lowercase().as_str() {
        "off" => LevelFilter::OFF,
        "error" => LevelFilter::ERROR,
        "warn" => LevelFilter::WARN,
        "info" => LevelFilter::INFO,
        "debug" => LevelFilter::DEBUG,
        "trace" => LevelFilter::TRACE,
        _ => LevelFilter::INFO,
    }
}

impl Default for Config {
    fn default() -> Self {
        Self {
            profile: Profile::default(),
            flame_output: None,
            log_level: LevelFilter::INFO,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum Profile {
    #[default]
    None,
    Flame,
    Console,
    Both,
}

impl Profile {
    pub fn as_str(&self) -> &'static str {
        match self {
            Profile::None => "none",
            Profile::Flame => "flame",
            Profile::Console => "console",
            Profile::Both => "both",
        }
    }

    pub fn from_str(s: &str) -> Self {
        match s {
            "flame" => Profile::Flame,
            "console" => Profile::Console,
            "both" => Profile::Both,
            _ => Profile::None,
        }
    }
}

impl std::fmt::Display for Profile {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.as_str())
    }
}

pub struct Handle {
    _guard: Option<WorkerGuard>,
}
