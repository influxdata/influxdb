//! Log and trace initialization and setup

#![deny(broken_intra_doc_links, rust_2018_idioms)]
#![warn(
    missing_copy_implementations,
    missing_debug_implementations,
    clippy::explicit_iter_loop,
    clippy::future_not_send,
    clippy::use_self,
    clippy::clone_on_ref_ptr
)]

#[cfg(feature = "structopt")]
pub mod cli;
pub mod config;

pub use config::*;

use observability_deps::{
    opentelemetry,
    opentelemetry::sdk::trace,
    opentelemetry::sdk::Resource,
    opentelemetry::KeyValue,
    tracing::{self, Subscriber},
    tracing_subscriber::{
        self,
        fmt::{self, writer::BoxMakeWriter, MakeWriter},
        layer::SubscriberExt,
        EnvFilter,
    },
};
use std::cmp::min;
use std::io;
use std::io::Write;
use thiserror::Error;

/// Maximum length of a log line.
const MAX_LINE_LENGTH: usize = 16 * 1024;

#[derive(Debug, Error)]
pub enum Error {
    #[error("Jaeger exporter selected but jaeger config passed to builder")]
    JaegerConfigMissing,

    #[error("OTLP exporter selected but OTLP config passed to builder")]
    OtlpConfigMissing,

    #[error("Cannot set global tracing subscriber")]
    SetGlobalDefaultError(#[from] tracing::dispatcher::SetGlobalDefaultError),
}

pub type Result<T, E = Error> = std::result::Result<T, E>;

/// Builder for tracing and logging.
#[derive(Debug)]
pub struct Builder<W = fn() -> io::Stdout> {
    log_format: LogFormat,
    log_filter: Option<EnvFilter>,
    // used when log_filter is none.
    default_log_filter: EnvFilter,
    traces_filter: Option<EnvFilter>,
    traces_exporter: TracesExporter,
    traces_sampler: TracesSampler,
    traces_sampler_arg: f64,
    make_writer: W,
    with_target: bool,
    with_ansi: bool,
    jaeger_config: Option<JaegerConfig>,
    otlp_config: Option<OtlpConfig>,
}

impl Default for Builder {
    fn default() -> Self {
        Self {
            log_format: LogFormat::Full,
            log_filter: None,
            default_log_filter: EnvFilter::try_new(Self::DEFAULT_LOG_FILTER).unwrap(),
            traces_filter: None,
            traces_exporter: TracesExporter::None,
            traces_sampler: TracesSampler::ParentBasedTraceIdRatio,
            traces_sampler_arg: 1.0,
            make_writer: io::stdout,
            with_target: true,
            with_ansi: true,
            jaeger_config: None,
            otlp_config: None,
        }
    }
}

impl Builder {
    pub fn new() -> Self {
        Self::default()
    }
}

// This needs to be a separate impl block because they place different bounds on the type parameters.
impl<W> Builder<W> {
    pub fn with_writer<W2>(self, make_writer: W2) -> Builder<W2>
    where
        W2: MakeWriter + Send + Sync + 'static,
    {
        Builder::<W2> {
            make_writer,
            // cannot use `..self` because W type parameter changes
            log_format: self.log_format,
            log_filter: self.log_filter,
            default_log_filter: self.default_log_filter,
            traces_filter: self.traces_filter,
            traces_exporter: self.traces_exporter,
            traces_sampler: self.traces_sampler,
            traces_sampler_arg: self.traces_sampler_arg,
            with_target: self.with_target,
            with_ansi: self.with_ansi,
            jaeger_config: self.jaeger_config,
            otlp_config: self.otlp_config,
        }
    }
}

// This needs to be a separate impl block because they place different bounds on the type parameters.
impl<W> Builder<W>
where
    W: MakeWriter + Send + Sync + 'static,
{
    pub const DEFAULT_LOG_FILTER: &'static str = "warn";

    /// Set log_filter using a simple numeric "verbosity level".
    ///
    /// 0 means, keep existing `log_filter` value.
    pub fn with_log_verbose_count(self, log_verbose_count: u8) -> Self {
        let log_filter = match log_verbose_count {
            0 => self.log_filter,
            1 => Some(EnvFilter::try_new("info").unwrap()),
            2 => Some(EnvFilter::try_new("debug,hyper::proto::h1=info,h2=info").unwrap()),
            _ => Some(EnvFilter::try_new("trace,hyper::proto::h1=info,h2=info").unwrap()),
        };
        Self { log_filter, ..self }
    }

    pub fn with_log_filter(self, log_filter: &Option<String>) -> Self {
        let log_filter = log_filter
            .as_ref()
            .map(|log_filter| EnvFilter::try_new(log_filter).unwrap());
        Self { log_filter, ..self }
    }

    pub fn with_default_log_filter(self, default_log_filter: impl AsRef<str>) -> Self {
        let default_log_filter = EnvFilter::try_new(default_log_filter).unwrap();
        Self {
            default_log_filter,
            ..self
        }
    }

    pub fn with_log_format(self, log_format: LogFormat) -> Self {
        Self { log_format, ..self }
    }

    pub fn with_log_destination(self, log_destination: LogDestination) -> Builder<BoxMakeWriter> {
        // Ideally we need a synchronized writer so that threads don't stomp on each others.
        // Unfortunately with the current release of `tracing-subscriber`, the trait doesn't expose
        // a lifetime parameter. The HEAD version fixes that and even implements MakeWriter for Mutex<Writer>
        // and shows some examples of how to use StdoutLock
        //
        // https://docs.rs/tracing-subscriber/0.2.18/tracing_subscriber/fmt/trait.MakeWriter.html)
        // vs
        // https://github.com/tokio-rs/tracing/blob/master/tracing-subscriber/src/fmt/writer.rs#L161
        //
        // The current hack is to ensure the LineWriter has enough buffer space to perform one single
        // large call to the underlying writer. This is not a guarantee that the write won't be interrupted
        // but hopefully it will happen much less often and not cause a pain until tracing_subscriber
        // gets upgraded.
        //
        // For that to work we must cap the line length, which is also a good idea because docker
        // caps log lines at 16K (and fluentd caps them at 32K).

        let make_writer = match log_destination {
            LogDestination::Stdout => make_writer(std::io::stdout),
            LogDestination::Stderr => make_writer(std::io::stderr),
        };
        Builder {
            make_writer,
            // cannot use `..self` because W type parameter changes
            log_format: self.log_format,
            log_filter: self.log_filter,
            default_log_filter: self.default_log_filter,
            traces_filter: self.traces_filter,
            traces_exporter: self.traces_exporter,
            traces_sampler: self.traces_sampler,
            traces_sampler_arg: self.traces_sampler_arg,
            with_target: self.with_target,
            with_ansi: self.with_ansi,
            jaeger_config: self.jaeger_config,
            otlp_config: self.otlp_config,
        }
    }

    /// Sets whether or not an event’s target and location are displayed.
    ///
    /// Defaults to true. See [observability_deps::tracing_subscriber::fmt::Layer::with_target]
    pub fn with_target(self, with_target: bool) -> Self {
        Self {
            with_target,
            ..self
        }
    }

    /// Enable/disable ANSI encoding for formatted events (i.e. colors).
    ///
    /// Defaults to true. See [observability_deps::tracing_subscriber::fmt::Layer::with_ansi]
    pub fn with_ansi(self, with_ansi: bool) -> Self {
        Self { with_ansi, ..self }
    }

    /// Sets an optional event filter for the tracing pipeline.
    ///
    /// The filter will be parsed with [observability_deps::tracing_subscriber::EnvFilter]
    /// and applied to all events before they reach the tracing exporter.
    pub fn with_traces_filter(self, traces_filter: &Option<String>) -> Self {
        if let Some(traces_filter) = traces_filter {
            let traces_filter = EnvFilter::try_new(traces_filter).unwrap();
            Self {
                traces_filter: Some(traces_filter),
                ..self
            }
        } else {
            self
        }
    }

    pub fn with_traces_exporter(self, traces_exporter: TracesExporter) -> Self {
        Self {
            traces_exporter,
            ..self
        }
    }

    pub fn with_traces_sampler(
        self,
        traces_sampler: TracesSampler,
        traces_sampler_arg: f64,
    ) -> Self {
        Self {
            traces_sampler,
            traces_sampler_arg,
            ..self
        }
    }

    pub fn with_jaeger_config(self, config: JaegerConfig) -> Self {
        Self {
            jaeger_config: Some(config),
            ..self
        }
    }

    pub fn with_oltp_config(self, config: OtlpConfig) -> Self {
        Self {
            otlp_config: Some(config),
            ..self
        }
    }

    fn construct_opentelemetry_tracer(&self) -> Result<Option<trace::Tracer>> {
        let trace_config = {
            let sampler = match self.traces_sampler {
                TracesSampler::AlwaysOn => trace::Sampler::AlwaysOn,
                TracesSampler::AlwaysOff => {
                    return Ok(None);
                }
                TracesSampler::TraceIdRatio => {
                    trace::Sampler::TraceIdRatioBased(self.traces_sampler_arg)
                }
                TracesSampler::ParentBasedAlwaysOn => {
                    trace::Sampler::ParentBased(Box::new(trace::Sampler::AlwaysOn))
                }
                TracesSampler::ParentBasedAlwaysOff => {
                    trace::Sampler::ParentBased(Box::new(trace::Sampler::AlwaysOff))
                }
                TracesSampler::ParentBasedTraceIdRatio => trace::Sampler::ParentBased(Box::new(
                    trace::Sampler::TraceIdRatioBased(self.traces_sampler_arg),
                )),
            };
            let resource = Resource::new(vec![KeyValue::new("service.name", "influxdb-iox")]);
            trace::Config::default()
                .with_sampler(sampler)
                .with_resource(resource)
        };

        Ok(match self.traces_exporter {
            TracesExporter::Jaeger => {
                let config = self
                    .jaeger_config
                    .as_ref()
                    .ok_or(Error::JaegerConfigMissing)?;
                let agent_endpoint = format!("{}:{}", config.agent_host.trim(), config.agent_port);
                opentelemetry::global::set_text_map_propagator(
                    opentelemetry_jaeger::Propagator::new(),
                );
                Some({
                    let builder = opentelemetry_jaeger::new_pipeline()
                        .with_trace_config(trace_config)
                        .with_agent_endpoint(agent_endpoint)
                        .with_service_name(&config.service_name)
                        .with_max_packet_size(config.max_packet_size);

                    // Batching is hard to tune because the max batch size
                    // is not currently exposed as a tunable from the trace config, and even then
                    // it's defined in terms of max number of spans, and not their size in bytes.
                    // Thus we enable batching only when the MTU size is 65000 which is the value suggested
                    // by jaeger when exporting to localhost.
                    if config.max_packet_size >= 65_000 {
                        builder.install_batch(opentelemetry::runtime::Tokio)
                    } else {
                        builder.install_simple()
                    }
                    .unwrap()
                })
            }

            TracesExporter::Otlp => {
                let config = self.otlp_config.as_ref().ok_or(Error::OtlpConfigMissing)?;
                let jaeger_endpoint = format!("{}:{}", config.host.trim(), config.port);
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
        })
    }

    pub fn build(self) -> Result<impl Subscriber> {
        let (traces_layer_filter, traces_layer_otel) =
            match self.construct_opentelemetry_tracer()? {
                None => (None, None),
                Some(tracer) => (
                    self.traces_filter,
                    Some(tracing_opentelemetry::OpenTelemetryLayer::new(tracer)),
                ),
            };

        let log_writer = self.make_writer;
        let log_format = self.log_format;
        let with_target = self.with_target;
        let with_ansi = self.with_ansi;

        let (log_format_full, log_format_pretty, log_format_json, log_format_logfmt) =
            match log_format {
                LogFormat::Full => (
                    Some(
                        fmt::layer()
                            .with_writer(log_writer)
                            .with_target(with_target)
                            .with_ansi(with_ansi),
                    ),
                    None,
                    None,
                    None,
                ),
                LogFormat::Pretty => (
                    None,
                    Some(
                        fmt::layer()
                            .pretty()
                            .with_writer(log_writer)
                            .with_target(with_target)
                            .with_ansi(with_ansi),
                    ),
                    None,
                    None,
                ),
                LogFormat::Json => (
                    None,
                    None,
                    Some(
                        fmt::layer()
                            .json()
                            .with_writer(log_writer)
                            .with_target(with_target)
                            .with_ansi(with_ansi),
                    ),
                    None,
                ),
                LogFormat::Logfmt => (
                    None,
                    None,
                    None,
                    Some(logfmt::LogFmtLayer::new(log_writer).with_target(with_target)),
                ),
            };

        let subscriber = tracing_subscriber::Registry::default()
            .with(self.log_filter.unwrap_or(self.default_log_filter))
            .with(log_format_full)
            .with(log_format_pretty)
            .with(log_format_json)
            .with(log_format_logfmt)
            .with(traces_layer_otel)
            .with(traces_layer_filter);

        Ok(subscriber)
    }

    /// Build a tracing subscriber and install it as a global default subscriber for all threads.
    ///
    /// It returns a RAII guard that will ensure all events are flushed to the tracing exporter.
    pub fn install_global(self) -> Result<TracingGuard> {
        let subscriber = self.build()?;
        tracing::subscriber::set_global_default(subscriber)?;
        Ok(TracingGuard)
    }
}

/// A RAII guard. On Drop, tracing and OpenTelemetry are flushed and shut down.
#[derive(Debug)]
pub struct TracingGuard;

impl Drop for TracingGuard {
    fn drop(&mut self) {
        opentelemetry::global::shutdown_tracer_provider();
    }
}

fn make_writer<M>(m: M) -> BoxMakeWriter
where
    M: MakeWriter + Send + Sync + 'static,
{
    fmt::writer::BoxMakeWriter::new(move || {
        LimitedWriter(
            MAX_LINE_LENGTH,
            std::io::LineWriter::with_capacity(MAX_LINE_LENGTH, m.make_writer()),
        )
    })
}

struct LimitedWriter<W: Write>(usize, W);
impl<W: Write> Write for LimitedWriter<W> {
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        // in case of interrupted syscalls we prefer to write a garbled log line.
        // than to just truncate the logs.
        self.1
            .write_all(&buf[..min(self.0, buf.len())])
            .map(|_| buf.len())
    }

    fn flush(&mut self) -> std::io::Result<()> {
        self.1.flush()
    }
}

#[cfg(test)]
pub mod test_util {
    ///! Utilities for testing logging and tracing.
    use super::*;

    use observability_deps::tracing::{self, debug, error, info, trace, warn};
    use observability_deps::tracing_subscriber::fmt::MakeWriter;
    use std::sync::{Arc, Mutex};
    use synchronized_writer::SynchronizedWriter;

    /// Log writer suitable for using in tests.
    /// It captures log output in a buffer and provides ways to filter out
    /// non-deterministic parts such as timestamps.
    #[derive(Default, Debug, Clone)]
    pub struct TestWriter {
        buffer: Arc<Mutex<Vec<u8>>>,
    }

    impl TestWriter {
        /// Return a writer and reference to the to-be captured output.
        pub fn new() -> (Self, Captured) {
            let writer = Self::default();
            let captured = Captured(Arc::clone(&writer.buffer));
            (writer, captured)
        }
    }

    impl MakeWriter for TestWriter {
        type Writer = SynchronizedWriter<Vec<u8>>;

        fn make_writer(&self) -> Self::Writer {
            SynchronizedWriter::new(Arc::clone(&self.buffer))
        }
    }

    #[derive(Debug)]
    pub struct Captured(Arc<Mutex<Vec<u8>>>);

    impl Captured {
        /// Removes non-determinism by removing timestamps from the log lines.
        /// It supports the built-in tracing timestamp format and the logfmt timestamps.
        pub fn without_timestamps(&self) -> String {
            // logfmt or fmt::layer() time format
            let timestamp = regex::Regex::new(
                r"(?m)( ?time=[0-9]+|^([A-Z][a-z]{2}) \d{1,2} \d{2}:\d{2}:\d{2}.\d{3} *)",
            )
            .unwrap();
            timestamp.replace_all(&self.to_string(), "").to_string()
        }
    }

    impl std::fmt::Display for Captured {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            let bytes = self.0.lock().unwrap();
            write!(f, "{}", std::str::from_utf8(&bytes).unwrap())
        }
    }

    /// This is a test helper that sets a few test-friendly parameters
    /// such as disabled ANSI escape sequences on the provided builder.
    /// This helper then calls the provided function within the context
    /// of the test subscriber, and returns the captured output of all
    /// the logging macros invoked by the function.
    pub fn log_test<W, F>(builder: Builder<W>, f: F) -> Captured
    where
        W: MakeWriter + Send + Sync + 'static,
        F: Fn(),
    {
        let (writer, output) = TestWriter::new();
        let subscriber = builder
            .with_writer(make_writer(writer))
            .with_target(false)
            .with_ansi(false)
            .build()
            .expect("subscriber");

        tracing::subscriber::with_default(subscriber, f);

        output
    }

    /// This is a test helper that sets a few test-friendly parameters
    /// such as disabled ANSI escape sequences on the provided builder.
    /// This helper then emits a few logs of different verbosity levels
    /// and returns the captured output.
    pub fn simple_test<W>(builder: Builder<W>) -> Captured
    where
        W: MakeWriter + Send + Sync + 'static,
    {
        log_test(builder, || {
            error!("foo");
            warn!("woo");
            info!("bar");
            debug!("baz");
            trace!("trax");
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use crate::test_util::*;
    use observability_deps::tracing::{debug, error};
    use std::sync::atomic::{AtomicBool, Ordering};
    use std::sync::Arc;

    #[test]
    fn simple_logging() {
        assert_eq!(
            simple_test(Builder::new()).without_timestamps(),
            r#"
ERROR foo
WARN woo
"#
            .trim_start(),
        );
    }

    #[test]
    fn simple_logging_logfmt() {
        assert_eq!(
            simple_test(Builder::new().with_log_format(LogFormat::Logfmt)).without_timestamps(),
            r#"
level=error msg=foo
level=warn msg=woo
"#
            .trim_start(),
        );
    }

    #[test]
    fn verbose_count() {
        assert_eq!(
            simple_test(Builder::new().with_log_verbose_count(0)).without_timestamps(),
            r#"
ERROR foo
WARN woo
"#
            .trim_start(),
        );

        assert_eq!(
            simple_test(Builder::new().with_log_verbose_count(1)).without_timestamps(),
            r#"
ERROR foo
WARN woo
INFO bar
"#
            .trim_start(),
        );

        assert_eq!(
            simple_test(Builder::new().with_log_verbose_count(2)).without_timestamps(),
            r#"
ERROR foo
WARN woo
INFO bar
DEBUG baz
"#
            .trim_start(),
        );

        assert_eq!(
            simple_test(Builder::new().with_log_verbose_count(3)).without_timestamps(),
            r#"
ERROR foo
WARN woo
INFO bar
DEBUG baz
TRACE trax
"#
            .trim_start(),
        );
    }

    #[test]
    fn test_override_default_log_filter() {
        const DEFAULT_LOG_FILTER: &str = "error";

        assert_eq!(
            simple_test(
                Builder::new()
                    .with_default_log_filter(DEFAULT_LOG_FILTER)
                    .with_log_verbose_count(0)
            )
            .without_timestamps(),
            r#"
ERROR foo
"#
            .trim_start(),
        );

        assert_eq!(
            simple_test(
                Builder::new()
                    .with_default_log_filter(DEFAULT_LOG_FILTER)
                    .with_log_verbose_count(1)
            )
            .without_timestamps(),
            r#"
ERROR foo
WARN woo
INFO bar
"#
            .trim_start(),
        );
    }

    #[test]
    fn test_side_effects() {
        let called = Arc::new(AtomicBool::new(false));
        let called_captured = Arc::clone(&called);

        fn call(called: &AtomicBool) -> bool {
            called.store(true, Ordering::SeqCst);
            true
        }

        assert_eq!(
            log_test(
                Builder::new().with_log_filter(&Some("error".to_string())),
                move || {
                    error!("foo");
                    debug!(called=?call(&called_captured), "bar");
                }
            )
            .without_timestamps(),
            r#"
ERROR foo
"#
            .trim_start(),
        );

        assert_eq!(called.load(Ordering::SeqCst), false);
    }

    #[test]
    fn test_long_lines() {
        let long = std::iter::repeat("X").take(20 * 1024).collect::<String>();

        let captured = log_test(
            Builder::new().with_log_filter(&Some("error".to_string())),
            move || {
                error!(%long);
            },
        )
        .without_timestamps();

        assert!(
            captured.len() <= MAX_LINE_LENGTH,
            "{} <= {}",
            captured.len(),
            MAX_LINE_LENGTH
        );
    }
}
