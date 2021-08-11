//! Log and trace initialization and setup

#![deny(rustdoc::broken_intra_doc_links, rustdoc::bare_urls, rust_2018_idioms)]
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
pub mod layered_tracing;

use crate::layered_tracing::{CloneableEnvFilter, FilteredLayer, UnionFilter};
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
        EnvFilter, Layer,
    },
};
use std::cmp::min;
use std::io;
use std::io::Write;
use thiserror::Error;

/// Maximum length of a log line.
/// Space for a final trailing newline if truncated.
///
/// Docker "chunks" log message in 16KB chunks. The log driver receives log lines in such chunks
/// but not all of the log drivers properly recombine the lines, and even those who do have their
/// own max buffer sizes (e.g. for our fluentd config it's 32KB).
/// To avoid surprises, for now, let's just truncate log lines right below 16K and make sure
/// they are properly terminated with a newline if they were so before the truncation.
const MAX_LINE_LENGTH: usize = 16 * 1024 - 1;

#[derive(Debug, Error)]
pub enum Error {
    #[error("Jaeger exporter selected but jaeger config passed to builder")]
    JaegerConfigMissing,

    #[error("'jaeger' not supported with this build. Hint: recompile with appropriate features")]
    JaegerNotBuilt {},

    #[error("OTLP exporter selected but OTLP config passed to builder")]
    OtlpConfigMissing,

    #[error("'otlp' not supported with this build. Hint: recompile with appropriate features")]
    OtlpNotBuilt {},

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
            TracesExporter::Jaeger => Some(self.construct_jaeger_tracer(trace_config)?),
            TracesExporter::Otlp => Some(self.construct_otlp_tracer(trace_config)?),
            TracesExporter::None => None,
        })
    }

    #[cfg(feature = "jaeger")]
    fn construct_jaeger_tracer(&self, trace_config: trace::Config) -> Result<trace::Tracer> {
        let config = self
            .jaeger_config
            .as_ref()
            .ok_or(Error::JaegerConfigMissing)?;
        let agent_endpoint = format!("{}:{}", config.agent_host.trim(), config.agent_port);
        opentelemetry::global::set_text_map_propagator(opentelemetry_jaeger::Propagator::new());
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
        let tracer = if config.max_packet_size >= 65_000 {
            builder.install_batch(opentelemetry::runtime::Tokio)
        } else {
            builder.install_simple()
        }
        .unwrap();
        Ok(tracer)
    }

    #[cfg(not(feature = "jaeger"))]
    fn construct_jaeger_tracer(&self, _trace_config: trace::Config) -> Result<trace::Tracer> {
        Err(Error::JaegerNotBuilt {})
    }

    #[cfg(feature = "otlp")]
    fn construct_otlp_tracer(&self, trace_config: trace::Config) -> Result<trace::Tracer> {
        let config = self.otlp_config.as_ref().ok_or(Error::OtlpConfigMissing)?;
        let jaeger_endpoint = format!("{}:{}", config.host.trim(), config.port);
        Ok(opentelemetry_otlp::new_pipeline()
            .with_trace_config(trace_config)
            .with_endpoint(jaeger_endpoint)
            .with_protocol(opentelemetry_otlp::Protocol::Grpc)
            .with_tonic()
            .install_batch(opentelemetry::runtime::Tokio)
            .unwrap())
    }

    #[cfg(not(feature = "otlp"))]
    fn construct_otlp_tracer(&self, _trace_config: trace::Config) -> Result<trace::Tracer> {
        Err(Error::OtlpNotBuilt {})
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

        let log_filter = self.log_filter.unwrap_or(self.default_log_filter);

        // construct the union filter which allows us to skip evaluating the expensive field values unless
        // at least one of the filters is interested in the events.
        // e.g. consider: `debug!(foo=bar(), "baz");`
        // `bar()` will only be called if either the log_filter or the traces_layer_filter is at debug level for that module.
        let log_filter = CloneableEnvFilter::new(log_filter);
        let traces_layer_filter = traces_layer_filter.map(CloneableEnvFilter::new);
        let union_filter = UnionFilter::new(vec![
            Some(Box::new(log_filter.clone())),
            traces_layer_filter.clone().map(|l| Box::new(l) as _),
        ]);

        let subscriber = tracing_subscriber::Registry::default()
            .with(union_filter)
            .with(FilteredLayer::new(
                log_filter
                    .and_then(log_format_full)
                    .and_then(log_format_pretty)
                    .and_then(log_format_json)
                    .and_then(log_format_logfmt),
            ))
            .with(
                traces_layer_filter
                    .map(|filter| FilteredLayer::new(filter.and_then(traces_layer_otel))),
            );

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
        std::io::LineWriter::with_capacity(
            MAX_LINE_LENGTH,
            LimitedWriter(MAX_LINE_LENGTH, m.make_writer()),
        )
    })
}

struct LimitedWriter<W: Write>(usize, W);
impl<W: Write> Write for LimitedWriter<W> {
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        if buf.is_empty() {
            return Ok(0);
        }
        let truncated = &buf[..min(self.0, buf.len())];
        let had_trailing_newline = buf[buf.len() - 1] == b'\n';
        if had_trailing_newline && (truncated[truncated.len() - 1] != b'\n') {
            // slow path; copy buffer and append a newline at the end
            // we still want to perform a single write syscall (if possible).
            let mut tmp = truncated.to_vec();
            tmp.push(b'\n');
            self.1.write_all(&tmp).map(|_| buf.len())
        } else {
            self.1.write_all(truncated).map(|_| buf.len())
        }
        // ^^^ `write_all`:
        // in case of interrupted syscalls we prefer to write a garbled log line.
        // than to just truncate the logs.
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

        assert!(!called.load(Ordering::SeqCst));
    }

    #[test]
    fn test_long_lines() {
        let test_cases = vec![
            0..1,
            10..11,
            // test all the values around the line length limit; the logger adds some
            // prefix text such as level and field name, so the field value length that
            // actually trigger a line overflow is a bit smaller than MAX_LINE_LENGTH.
            MAX_LINE_LENGTH - 40..MAX_LINE_LENGTH + 20,
            20 * 1024..20 * 1024,
        ];

        for range in test_cases {
            for len in range {
                let long = "X".repeat(len);

                let captured = log_test(
                    Builder::new().with_log_filter(&Some("error".to_string())),
                    move || {
                        error!(%long);
                    },
                )
                .without_timestamps();

                assert_eq!(captured.chars().last().unwrap(), '\n');

                assert!(
                    captured.len() <= MAX_LINE_LENGTH,
                    "{} <= {}",
                    captured.len(),
                    MAX_LINE_LENGTH
                );
            }
        }
    }

    // This test checks that [`make_writer`] returns a writer that implement line buffering, which means
    // that written data is not flushed to the underlying IO writer until a a whole line is been written
    // to the line buffered writer, possibly by multiple calls to the `write` method.
    // (In fact, the `logfmt` layer does call `write` multiple times for each log line).
    //
    // What happens to the truncated strings w.r.t to newlines is out of scope for this test,
    // see the [`limited_writer`] test instead.
    #[test]
    fn line_buffering() {
        let (test_writer, captured) = TestWriter::new();
        let mut writer = make_writer(test_writer).make_writer();
        writer.write_all("foo".as_bytes()).unwrap();
        // wasn't flushed yet because there was no newline yet
        assert_eq!(captured.to_string(), "");
        writer.write_all("\nbar".as_bytes()).unwrap();
        // a newline caused the first line to be flushed but the trailing string is still buffered
        assert_eq!(captured.to_string(), "foo\n");
        writer.flush().unwrap();
        // an explicit call to flush flushes even if there is no trailing newline
        assert_eq!(captured.to_string(), "foo\nbar");

        // another case when the line buffer flushes even before a newline is when the internal buffer limit
        let (test_writer, captured) = TestWriter::new();
        let mut writer = make_writer(test_writer).make_writer();
        let long = std::iter::repeat(b'X')
            .take(MAX_LINE_LENGTH)
            .collect::<Vec<u8>>();
        writer.write_all(&long).unwrap();
        assert_eq!(captured.to_string().len(), MAX_LINE_LENGTH);
    }

    #[test]
    fn limited_writer() {
        const TEST_MAX_LINE_LENGTH: usize = 3;
        let test_cases = vec![
            ("", ""),
            ("a", "a"),
            ("ab", "ab"),
            ("abc", "abc"),
            ("abc", "abc"),
            ("abcd", "abc"),
            ("abcd\n", "abc\n"),
            ("abcd\n\n", "abc\n"),
            ("abcd\nx", "abc"),
            ("\n", "\n"),
            ("\nabc", "\nab"),
        ];
        for (input, want) in test_cases {
            let mut buf = Vec::new();
            {
                let mut lw = LimitedWriter(TEST_MAX_LINE_LENGTH, &mut buf);
                write!(&mut lw, "{}", input).unwrap();
            }
            assert_eq!(std::str::from_utf8(&buf).unwrap(), want);
        }
    }
}
