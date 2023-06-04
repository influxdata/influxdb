//! Log and trace initialization and setup

#![deny(rustdoc::broken_intra_doc_links, rustdoc::bare_urls, rust_2018_idioms)]
#![warn(
    missing_copy_implementations,
    missing_debug_implementations,
    clippy::explicit_iter_loop,
    // See https://github.com/influxdata/influxdb_iox/pull/1671
    clippy::future_not_send,
    clippy::use_self,
    clippy::clone_on_ref_ptr,
    clippy::todo,
    clippy::dbg_macro,
    unused_crate_dependencies
)]

#[cfg(feature = "clap")]
pub mod cli;
pub mod config;

pub use config::*;

use is_terminal::IsTerminal;
// Re-export tracing_subscriber
pub use tracing_subscriber;

use observability_deps::tracing::{self, Subscriber};
use std::cmp::min;
use std::io;
use std::io::Write;
use thiserror::Error;
use tracing_subscriber::{
    fmt::{self, writer::BoxMakeWriter, MakeWriter},
    layer::SubscriberExt,
    registry::LookupSpan,
    EnvFilter, Layer,
};

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
    #[error("Cannot set global tracing subscriber")]
    SetGlobalDefaultError(#[from] tracing::dispatcher::SetGlobalDefaultError),

    #[error("Cannot set global log subscriber")]
    SetLoggerError(#[from] tracing_log::log_tracer::SetLoggerError),
}

pub type Result<T, E = Error> = std::result::Result<T, E>;

/// Builder to configure tracing and logging.
#[derive(Debug)]
pub struct Builder<W = fn() -> io::Stdout> {
    log_format: LogFormat,
    log_filter: Option<EnvFilter>,
    // used when log_filter is none.
    default_log_filter: EnvFilter,
    make_writer: W,
    with_target: bool,
    with_ansi: bool,
}

impl Default for Builder {
    fn default() -> Self {
        Self {
            log_format: LogFormat::Full,
            log_filter: None,
            default_log_filter: EnvFilter::try_new(Self::DEFAULT_LOG_FILTER).unwrap(),
            make_writer: io::stdout,
            with_target: true,
            // use ansi control codes for color if connected to a TTY
            with_ansi: std::io::stdout().is_terminal(),
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
        W2: for<'writer> MakeWriter<'writer> + Send + Sync + 'static,
    {
        Builder::<W2> {
            make_writer,
            // cannot use `..self` because W type parameter changes
            log_format: self.log_format,
            log_filter: self.log_filter,
            default_log_filter: self.default_log_filter,
            with_target: self.with_target,
            with_ansi: self.with_ansi,
        }
    }
}

// This needs to be a separate impl block because they place different bounds on the type parameters.
impl<W> Builder<W>
where
    W: for<'writer> MakeWriter<'writer> + Send + Sync + 'static,
{
    pub const DEFAULT_LOG_FILTER: &'static str = "warn";

    /// Set log_filter using a simple numeric "verbosity level".
    ///
    /// 0 means, keep existing `log_filter` value.
    pub fn with_log_verbose_count(self, log_verbose_count: u8) -> Self {
        let log_filter = match log_verbose_count {
            0 => self.log_filter,
            1 => Some(EnvFilter::try_new("info,sqlx=warn").unwrap()),
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
            with_target: self.with_target,
            with_ansi: self.with_ansi,
        }
    }

    /// Sets whether or not an event’s target and location are displayed.
    ///
    /// Defaults to true. See [tracing_subscriber::fmt::Layer::with_target]
    pub fn with_target(self, with_target: bool) -> Self {
        Self {
            with_target,
            ..self
        }
    }

    /// Enable/disable ANSI encoding for formatted events (i.e. colors).
    ///
    /// Defaults to true if connected to a TTY, false otherwise. See
    /// [tracing_subscriber::fmt::Layer::with_ansi]
    pub fn with_ansi(self, with_ansi: bool) -> Self {
        Self { with_ansi, ..self }
    }

    /// Returns a [`Layer`] that emits logs as specified by the configuration of
    /// `self`.
    pub fn build<S>(self) -> Result<impl Layer<S> + 'static>
    where
        S: Subscriber,
        for<'a> S: LookupSpan<'a>,
    {
        let log_writer = self.make_writer;
        let log_format = self.log_format;
        let with_target = self.with_target;
        let with_ansi = self.with_ansi;

        let log_filter = self.log_filter.unwrap_or(self.default_log_filter);

        let res: Box<dyn Layer<S> + Send + Sync> = match log_format {
            LogFormat::Full => Box::new(
                log_filter.and_then(
                    fmt::layer()
                        .with_writer(log_writer)
                        .with_target(with_target)
                        .with_ansi(with_ansi),
                ),
            ),
            LogFormat::Pretty => Box::new(
                log_filter.and_then(
                    fmt::layer()
                        .pretty()
                        .with_writer(log_writer)
                        .with_target(with_target)
                        .with_ansi(with_ansi),
                ),
            ),
            LogFormat::Json => Box::new(
                log_filter.and_then(
                    fmt::layer()
                        .json()
                        .with_writer(log_writer)
                        .with_target(with_target)
                        .with_ansi(with_ansi),
                ),
            ),
            LogFormat::Logfmt => Box::new(
                log_filter.and_then(logfmt::LogFmtLayer::new(log_writer).with_target(with_target)),
            ),
        };

        Ok(res)
    }

    /// Build a tracing subscriber and install it as a global default subscriber
    /// for all threads.
    ///
    /// It returns a RAII guard that will ensure all events are flushed on drop
    pub fn install_global(self) -> Result<TroggingGuard> {
        let layer = self.build()?;
        let subscriber = tracing_subscriber::Registry::default().with(layer);
        install_global(subscriber)
    }
}

/// Install a global tracing/logging subscriber.
///
/// Call this function when installing a subscriber instead of calling
/// `tracing::subscriber::set_global_default` directly.
///
/// This function also sets up the `log::Log` -> `tracing` bridge.
pub fn install_global<S>(subscriber: S) -> Result<TroggingGuard>
where
    S: Subscriber + Send + Sync + 'static,
{
    tracing::subscriber::set_global_default(subscriber)?;
    tracing_log::LogTracer::init()?;
    Ok(TroggingGuard)
}

/// A RAII guard. On Drop, ensures all events are flushed
///
/// Note: This is currently unnecessary but has been kept in case we choose to
/// switch to using tracing-appender which writes logs in a background worker
#[derive(Debug)]
pub struct TroggingGuard;

impl Drop for TroggingGuard {
    fn drop(&mut self) {}
}

fn make_writer<M>(m: M) -> BoxMakeWriter
where
    M: for<'writer> MakeWriter<'writer> + Send + Sync + 'static,
{
    BoxMakeWriter::new(MakeWriterHelper {
        inner: BoxMakeWriter::new(m),
    })
}

struct MakeWriterHelper {
    inner: BoxMakeWriter,
}

impl<'a> MakeWriter<'a> for MakeWriterHelper {
    type Writer = Box<dyn Write + 'a>;

    fn make_writer(&'a self) -> Self::Writer {
        Box::new(std::io::LineWriter::with_capacity(
            MAX_LINE_LENGTH,
            LimitedWriter(MAX_LINE_LENGTH, self.inner.make_writer()),
        ))
    }
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
    /// Utilities for testing logging and tracing.
    use super::*;

    use observability_deps::tracing::{self, debug, error, info, trace, warn};
    use std::sync::{Arc, Mutex};
    use synchronized_writer::SynchronizedWriter;
    use tracing_subscriber::fmt::MakeWriter;

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

    impl MakeWriter<'_> for TestWriter {
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
            // logfmt (e.g. `time=12345`) or fmt::layer() (e.g. `2021-10-25T13:48:50.555258`) time format
            let timestamp = regex::Regex::new(
                r"(?m)( ?time=[0-9]+|^(\d{4})-\d{1,2}-\d{1,2}T\d{2}:\d{2}:\d{2}.\d+Z *)",
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
        W: for<'writer> MakeWriter<'writer> + Send + Sync + 'static,
        F: Fn(),
    {
        let (writer, output) = TestWriter::new();
        let layer = builder
            .with_writer(make_writer(writer))
            .with_target(false)
            .with_ansi(false)
            .build()
            .expect("subscriber");

        let subscriber = tracing_subscriber::Registry::default().with(layer);
        tracing::subscriber::with_default(subscriber, f);

        output
    }

    /// This is a test helper that sets a few test-friendly parameters
    /// such as disabled ANSI escape sequences on the provided builder.
    /// This helper then emits a few logs of different verbosity levels
    /// and returns the captured output.
    pub fn simple_test<W>(builder: Builder<W>) -> Captured
    where
        W: for<'writer> MakeWriter<'writer> + Send + Sync + 'static,
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
        let mw = make_writer(test_writer);
        let mut writer = mw.make_writer();
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
        let mw = make_writer(test_writer);
        let mut writer = mw.make_writer();
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
                write!(&mut lw, "{input}").unwrap();
            }
            assert_eq!(std::str::from_utf8(&buf).unwrap(), want);
        }
    }
}
