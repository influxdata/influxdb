mod bucket;
mod metrics;
mod sampler;
mod sender;
mod stats;
pub mod store;

use std::fmt::{Display, Formatter};
use thiserror::Error;

#[derive(Debug, Error)]
pub enum TelemetryError {
    #[error("cannot serialize to JSON: {0}")]
    CannotSerializeJson(#[from] serde_json::Error),

    #[error("failed to get pid: {0}")]
    CannotGetPid(&'static str),

    #[error("cannot send telemetry: {0}")]
    CannotSendToTelemetryServer(#[from] reqwest::Error),
}

pub type Result<T, E = TelemetryError> = std::result::Result<T, E>;

pub trait ParquetMetrics: Send + Sync + std::fmt::Debug + 'static {
    fn get_metrics(&self) -> (u64, f64, u64);
}

pub trait ProcessingEngineMetrics: Send + Sync + std::fmt::Debug + 'static {
    fn num_triggers(&self) -> (u64, u64, u64, u64);
}

#[derive(Debug, Copy, Clone)]
pub enum ServeInvocationMethod {
    Explicit,
    QuickStart,
    InstallScript,
    DockerHub,
    DockerOther,
    Tests,
    // numeric values not mapped to named variants are Custom; this is for extensible
    Custom(u64),
    Unknown, // for unparsable values
}

impl ServeInvocationMethod {
    // for serialization as telemetry
    pub fn as_u64(&self) -> u64 {
        match self {
            ServeInvocationMethod::Explicit => 0,
            ServeInvocationMethod::QuickStart => 1,
            ServeInvocationMethod::InstallScript => 2,
            ServeInvocationMethod::DockerHub => 3,
            ServeInvocationMethod::DockerOther => 4,
            ServeInvocationMethod::Tests => 0x1F643,
            ServeInvocationMethod::Custom(i) => *i,
            ServeInvocationMethod::Unknown => 0xFFFFFFFF,
        }
    }

    pub fn parse(s: &str) -> std::result::Result<Self, std::io::Error> {
        Ok(match s {
            "explicit" => ServeInvocationMethod::Explicit,
            "quick-start" => ServeInvocationMethod::QuickStart,
            "install-script" => ServeInvocationMethod::InstallScript,
            "docker-hub" => ServeInvocationMethod::DockerHub,
            "docker-other" => ServeInvocationMethod::DockerOther,
            "tests" => ServeInvocationMethod::Tests,
            other => {
                if let Ok(value) = other.parse::<u64>() {
                    match value {
                        0 => ServeInvocationMethod::Explicit,
                        1 => ServeInvocationMethod::QuickStart,
                        2 => ServeInvocationMethod::InstallScript,
                        3 => ServeInvocationMethod::DockerHub,
                        4 => ServeInvocationMethod::DockerOther,
                        0x1F643 => ServeInvocationMethod::Tests,
                        0xFFFFFFFF => ServeInvocationMethod::Unknown,
                        _ => ServeInvocationMethod::Custom(value),
                    }
                } else {
                    ServeInvocationMethod::Unknown
                }
            }
        })
    }
}

impl Display for ServeInvocationMethod {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            ServeInvocationMethod::Explicit => f.write_str("explicit"),
            ServeInvocationMethod::QuickStart => f.write_str("quick-start"),
            ServeInvocationMethod::InstallScript => f.write_str("install-script"),
            ServeInvocationMethod::DockerHub => f.write_str("docker-hub"),
            ServeInvocationMethod::DockerOther => f.write_str("docker-other"),
            ServeInvocationMethod::Custom(i) => write!(f, "{i}"),
            ServeInvocationMethod::Tests => f.write_str("tests"),
            ServeInvocationMethod::Unknown => f.write_str("unknown"),
        }
    }
}

#[cfg(test)]
mod tests;
