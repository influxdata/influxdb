#![deny(rustdoc::broken_intra_doc_links, rustdoc::bare_urls, rust_2018_idioms)]
#![warn(
    missing_debug_implementations,
    clippy::explicit_iter_loop,
    clippy::use_self,
    clippy::clone_on_ref_ptr,
    clippy::future_not_send
)]

use snafu::Snafu;
use std::num::NonZeroU16;
use std::sync::Arc;
use structopt::StructOpt;
use trace::TraceCollector;

pub mod otel;

/// CLI config for distributed tracing options
#[derive(Debug, StructOpt, Clone)]
pub struct TracingConfig {
    /// Tracing: exporter type
    ///
    /// Can be one of: none, jaeger
    #[structopt(
        long = "--traces-exporter",
        env = "TRACES_EXPORTER",
        default_value = "none"
    )]
    pub traces_exporter: TracesExporter,

    /// Tracing: Jaeger agent network hostname
    ///
    /// Protocol is Thrift/Compact over UDP.
    ///
    /// Only used if `--traces-exporter` is "jaeger".
    #[structopt(
        long = "--traces-exporter-jaeger-agent-host",
        env = "TRACES_EXPORTER_JAEGER_AGENT_HOST",
        default_value = "0.0.0.0"
    )]
    pub traces_exporter_jaeger_agent_host: String,

    /// Tracing: Jaeger agent network port
    ///
    /// Protocol is Thrift/Compact over UDP.
    ///
    /// Only used if `--traces-exporter` is "jaeger".
    #[structopt(
        long = "--traces-exporter-jaeger-agent-port",
        env = "TRACES_EXPORTER_JAEGER_AGENT_PORT",
        default_value = "6831"
    )]
    pub traces_exporter_jaeger_agent_port: NonZeroU16,

    /// Tracing: Jaeger service name.
    ///
    /// Only used if `--traces-exporter` is "jaeger".
    #[structopt(
        long = "--traces-exporter-jaeger-service-name",
        env = "TRACES_EXPORTER_JAEGER_SERVICE_NAME",
        default_value = "iox-conductor"
    )]
    pub traces_exporter_jaeger_service_name: String,

    /// Tracing: specifies the header name used for passing trace context
    ///
    /// Only used if `--traces-exporter` is "jaeger".
    #[structopt(
        long = "--traces-exporter-jaeger-trace-context-header-name",
        env = "JAEGER_TRACE_CONTEXT_HEADER_NAME",
        default_value = "uber-trace-id"
    )]
    pub traces_jaeger_trace_context_header_name: String,

    /// Tracing: specifies the header name used for force sampling
    ///
    /// Only used if `--traces-exporter` is "jaeger".
    #[structopt(
        long = "--traces-jaeger-debug-name",
        env = "JAEGER_DEBUG_NAME",
        default_value = "jaeger-debug-id"
    )]
    pub traces_jaeger_debug_name: String,
}

impl TracingConfig {
    pub fn build(&self) -> Result<Option<Arc<dyn TraceCollector>>> {
        match self.traces_exporter {
            TracesExporter::None => Ok(None),
            TracesExporter::Jaeger => Ok(Some(jaeger_exporter(self)?)),
        }
    }
}

#[derive(Debug, Clone, Copy)]
pub enum TracesExporter {
    None,
    Jaeger,
}

impl std::str::FromStr for TracesExporter {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s.to_ascii_lowercase().as_str() {
            "none" => Ok(Self::None),
            "jaeger" => Ok(Self::Jaeger),
            _ => Err(format!(
                "Invalid traces exporter '{}'. Valid options: none, jaeger",
                s
            )),
        }
    }
}

#[derive(Debug, Snafu)]
pub enum Error {
    #[snafu(display("failed to construct trace exporter: {}", source))]
    TraceExporter {
        source: opentelemetry::trace::TraceError,
    },

    #[snafu(display(
        "'jaeger' not supported with this build. Hint: recompile with appropriate features"
    ))]
    JaegerNotBuilt {},
}

pub type Result<T, E = Error> = std::result::Result<T, E>;

#[cfg(feature = "jaeger")]
fn jaeger_exporter(config: &TracingConfig) -> Result<Arc<dyn TraceCollector>> {
    use observability_deps::tracing::info;

    let agent_endpoint = format!(
        "{}:{}",
        config.traces_exporter_jaeger_agent_host.trim(),
        config.traces_exporter_jaeger_agent_port
    );

    let service_name = &config.traces_exporter_jaeger_service_name;
    info!(%agent_endpoint, %service_name, "Creating jaeger tracing exporter");

    let exporter = opentelemetry_jaeger::new_pipeline()
        .with_agent_endpoint(agent_endpoint)
        .with_service_name(&config.traces_exporter_jaeger_service_name)
        .init_sync_exporter()
        .map_err(|source| Error::TraceExporter { source })?;

    Ok(Arc::new(otel::OtelExporter::new(exporter)))
}

#[cfg(not(feature = "jaeger"))]
fn jaeger_exporter(_config: &TracingConfig) -> Result<Arc<dyn TraceCollector>> {
    Err(Error::JaegerNotBuilt {})
}
