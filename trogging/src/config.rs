use std::num::NonZeroU16;

#[derive(Debug, Clone, Copy, PartialEq)]
pub enum LogFormat {
    Full,
    Pretty,
    Json,
    Logfmt,
}

impl std::str::FromStr for LogFormat {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s.to_ascii_lowercase().as_str() {
            "full" => Ok(Self::Full),
            "pretty" => Ok(Self::Pretty),
            "json" => Ok(Self::Json),
            "logfmt" => Ok(Self::Logfmt),
            _ => Err(format!(
                "Invalid log format '{}'. Valid options: full, pretty, json, logfmt",
                s
            )),
        }
    }
}

impl std::fmt::Display for LogFormat {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Full => write!(f, "full"),
            Self::Pretty => write!(f, "pretty"),
            Self::Json => write!(f, "json"),
            Self::Logfmt => write!(f, "logfmt"),
        }
    }
}

#[derive(Debug, Clone, Copy)]
pub enum LogDestination {
    Stdout,
    Stderr,
}

impl std::str::FromStr for LogDestination {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s.to_ascii_lowercase().as_str() {
            "stdout" => Ok(Self::Stdout),
            "stderr" => Ok(Self::Stderr),
            _ => Err(format!(
                "Invalid log destination '{}'. Valid options: stdout, stderr",
                s
            )),
        }
    }
}

impl std::fmt::Display for LogDestination {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Stdout => write!(f, "stdout"),
            Self::Stderr => write!(f, "stderr"),
        }
    }
}

#[derive(Debug, Clone, Copy)]
pub enum TracesExporter {
    None,
    Jaeger,
    Otlp,
}

impl std::str::FromStr for TracesExporter {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s.to_ascii_lowercase().as_str() {
            "none" => Ok(Self::None),
            "jaeger" => Ok(Self::Jaeger),
            "otlp" => Ok(Self::Otlp),
            _ => Err(format!(
                "Invalid traces exporter '{}'. Valid options: none, jaeger, otlp",
                s
            )),
        }
    }
}

impl std::fmt::Display for TracesExporter {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::None => write!(f, "none"),
            Self::Jaeger => write!(f, "jaeger"),
            Self::Otlp => write!(f, "otlp"),
        }
    }
}

#[derive(Debug, Clone, Copy)]
pub enum TracesSampler {
    AlwaysOn,
    AlwaysOff,
    TraceIdRatio,
    ParentBasedAlwaysOn,
    ParentBasedAlwaysOff,
    ParentBasedTraceIdRatio,
}

impl std::str::FromStr for TracesSampler {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s.to_ascii_lowercase().as_str() {
            "always_on" => Ok(Self::AlwaysOn),
            "always_off" => Ok(Self::AlwaysOff),
            "traceidratio" => Ok(Self::TraceIdRatio),
            "parentbased_always_on" => Ok(Self::ParentBasedAlwaysOn),
            "parentbased_always_off" => Ok(Self::ParentBasedAlwaysOff),
            "parentbased_traceidratio" => Ok(Self::ParentBasedTraceIdRatio),
            _ => Err(format!("Invalid traces sampler '{}'. Valid options: always_on, always_off, traceidratio, parentbased_always_on, parentbased_always_off, parentbased_traceidratio", s)),
        }
    }
}

impl std::fmt::Display for TracesSampler {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::AlwaysOn => write!(f, "always_on"),
            Self::AlwaysOff => write!(f, "always_off"),
            Self::TraceIdRatio => write!(f, "traceidratio"),
            Self::ParentBasedAlwaysOn => write!(f, "parentbased_always_on"),
            Self::ParentBasedAlwaysOff => write!(f, "parentbased_always_off"),
            Self::ParentBasedTraceIdRatio => write!(f, "parentbased_traceidratio"),
        }
    }
}

#[derive(Debug)]
pub struct JaegerConfig {
    pub agent_host: String,
    pub agent_port: NonZeroU16,
    pub service_name: String,
    pub max_packet_size: usize,
}

#[derive(Debug)]
pub struct OtlpConfig {
    pub host: String,
    pub port: NonZeroU16,
}
