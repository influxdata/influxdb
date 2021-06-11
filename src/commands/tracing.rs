//! Log and trace initialization and setup

use observability_deps::tracing::dispatcher::SetGlobalDefaultError;
use observability_deps::{
    opentelemetry,
    opentelemetry::sdk::trace,
    opentelemetry::sdk::Resource,
    opentelemetry::KeyValue,
    tracing,
    tracing_subscriber::{self, fmt, layer::SubscriberExt, EnvFilter},
};

/// Start simple logger. Panics on error.
pub fn init_simple_logs(log_verbose_count: u8) -> Result<TracingGuard, SetGlobalDefaultError> {
    let log_layer_filter = match log_verbose_count {
        0 => EnvFilter::try_new("warn").unwrap(),
        1 => EnvFilter::try_new("info").unwrap(),
        2 => EnvFilter::try_new("debug,hyper::proto::h1=info,h2=info").unwrap(),
        _ => EnvFilter::try_new("trace,hyper::proto::h1=info,h2=info").unwrap(),
    };
    let subscriber = tracing_subscriber::Registry::default()
        .with(log_layer_filter)
        .with(fmt::layer());

    tracing::subscriber::set_global_default(subscriber)?;
    Ok(TracingGuard)
}

/// Start log or trace emitter. Panics on error.
pub fn init_logs_and_tracing(
    log_verbose_count: u8,
    config: &crate::commands::run::Config,
) -> Result<TracingGuard, SetGlobalDefaultError> {
    // Handle the case if -v/-vv is specified both before and after the server
    // command
    let log_verbose_count = if log_verbose_count > config.log_verbose_count {
        log_verbose_count
    } else {
        config.log_verbose_count
    };

    let (traces_layer_filter, traces_layer_otel) = match construct_opentelemetry_tracer(config) {
        None => (None, None),
        Some(tracer) => {
            let traces_layer_otel = Some(tracing_opentelemetry::OpenTelemetryLayer::new(tracer));
            match &config.traces_filter {
                None => (None, traces_layer_otel),
                Some(traces_filter) => (
                    Some(EnvFilter::try_new(traces_filter).unwrap()),
                    traces_layer_otel,
                ),
            }
        }
    };

    let (
        log_layer_filter,
        log_layer_format_full,
        log_layer_format_pretty,
        log_layer_format_json,
        log_layer_format_logfmt,
    ) = {
        match traces_layer_otel {
            Some(_) => (None, None, None, None, None),
            None => {
                // Note: Use LineWriter to ensure each log line is
                // written as a unit and avoid interleaving
                // see https://github.com/influxdata/influxdb_iox/issues/1615
                let log_writer = match config.log_destination {
                    LogDestination::Stdout => fmt::writer::BoxMakeWriter::new(|| {
                        std::io::LineWriter::new(std::io::stdout())
                    }),
                    LogDestination::Stderr => fmt::writer::BoxMakeWriter::new(|| {
                        std::io::LineWriter::new(std::io::stderr())
                    }),
                };
                let (log_format_full, log_format_pretty, log_format_json, log_format_logfmt) =
                    match config.log_format {
                        LogFormat::Full => {
                            (Some(fmt::layer().with_writer(log_writer)), None, None, None)
                        }
                        LogFormat::Pretty => (
                            None,
                            Some(fmt::layer().pretty().with_writer(log_writer)),
                            None,
                            None,
                        ),
                        LogFormat::Json => (
                            None,
                            None,
                            Some(fmt::layer().json().with_writer(log_writer)),
                            None,
                        ),
                        LogFormat::Logfmt => {
                            (None, None, None, Some(logfmt::LogFmtLayer::new(log_writer)))
                        }
                    };

                let log_layer_filter = match log_verbose_count {
                    0 => EnvFilter::try_new(&config.log_filter).unwrap(),
                    1 => EnvFilter::try_new("info").unwrap(),
                    2 => EnvFilter::try_new("debug,hyper::proto::h1=info,h2=info").unwrap(),
                    _ => EnvFilter::try_new("trace,hyper::proto::h1=info,h2=info").unwrap(),
                };
                (
                    Some(log_layer_filter),
                    log_format_full,
                    log_format_pretty,
                    log_format_json,
                    log_format_logfmt,
                )
            }
        }
    };

    let subscriber = tracing_subscriber::Registry::default()
        .with(log_layer_format_logfmt)
        .with(log_layer_format_json)
        .with(log_layer_format_pretty)
        .with(log_layer_format_full)
        .with(log_layer_filter)
        .with(traces_layer_otel)
        .with(traces_layer_filter);

    tracing::subscriber::set_global_default(subscriber)?;
    Ok(TracingGuard)
}

fn construct_opentelemetry_tracer(config: &crate::commands::run::Config) -> Option<trace::Tracer> {
    let trace_config = {
        let sampler = match config.traces_sampler {
            TracesSampler::AlwaysOn => trace::Sampler::AlwaysOn,
            TracesSampler::AlwaysOff => {
                return None;
            }
            TracesSampler::TraceIdRatio => {
                trace::Sampler::TraceIdRatioBased(config.traces_sampler_arg)
            }
            TracesSampler::ParentBasedAlwaysOn => {
                trace::Sampler::ParentBased(Box::new(trace::Sampler::AlwaysOn))
            }
            TracesSampler::ParentBasedAlwaysOff => {
                trace::Sampler::ParentBased(Box::new(trace::Sampler::AlwaysOff))
            }
            TracesSampler::ParentBasedTraceIdRatio => trace::Sampler::ParentBased(Box::new(
                trace::Sampler::TraceIdRatioBased(config.traces_sampler_arg),
            )),
        };
        let resource = Resource::new(vec![KeyValue::new("service.name", "influxdb-iox")]);
        trace::Config::default()
            .with_sampler(sampler)
            .with_resource(resource)
    };

    match config.traces_exporter {
        TracesExporter::Jaeger => {
            let agent_endpoint = format!(
                "{}:{}",
                config.traces_exporter_jaeger_agent_host.trim(),
                config.traces_exporter_jaeger_agent_port
            );
            opentelemetry::global::set_text_map_propagator(opentelemetry_jaeger::Propagator::new());
            Some({
                let builder = opentelemetry_jaeger::new_pipeline()
                    .with_trace_config(trace_config)
                    .with_agent_endpoint(agent_endpoint)
                    .with_service_name(&config.traces_exporter_jaeger_service_name)
                    .with_max_packet_size(config.traces_exporter_jaeger_max_packet_size);

                // Batching is hard to tune because the max batch size
                // is not currently exposed as a tunable from the trace config, and even then
                // it's defined in terms of max number of spans, and not their size in bytes.
                // Thus we enable batching only when the MTU size is 65000 which is the value suggested
                // by jaeger when exporting to localhost.
                if config.traces_exporter_jaeger_max_packet_size >= 65_000 {
                    builder.install_batch(opentelemetry::runtime::Tokio)
                } else {
                    builder.install_simple()
                }
                .unwrap()
            })
        }

        TracesExporter::Otlp => {
            let jaeger_endpoint = format!(
                "{}:{}",
                config.traces_exporter_otlp_host.trim(),
                config.traces_exporter_otlp_port
            );
            Some(
                opentelemetry_otlp::new_pipeline()
                    .with_trace_config(trace_config)
                    .with_endpoint(jaeger_endpoint)
                    .with_protocol(opentelemetry_otlp::Protocol::Grpc)
                    .with_tonic()
                    .install_batch(opentelemetry::runtime::Tokio)
                    .unwrap(),
            )
        }

        TracesExporter::None => None,
    }
}

/// A RAII guard. On Drop, tracing and OpenTelemetry are flushed and shut down.
pub struct TracingGuard;

impl Drop for TracingGuard {
    fn drop(&mut self) {
        opentelemetry::global::shutdown_tracer_provider();
    }
}

#[derive(Debug, Clone, Copy)]
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
