//! Logging initization and setup

use tracing_subscriber::{prelude::*, EnvFilter};

use super::server::{LogFormat, RunConfig};

/// Handles setting up logging levels
#[derive(Debug)]
pub enum LoggingLevel {
    // Default log level is warn level for all components
    Default,

    // Verbose log level is info level for all components
    Verbose,

    // Debug log level is debug for everything except
    // some especially noisy low level libraries
    Debug,
}

impl LoggingLevel {
    /// Creates a logging level usig the following rules.
    ///
    /// 1. if `-vv` (multiple instances of verbose), use Debug
    /// 2. if `-v` (single instances of verbose), use Verbose
    /// 3. Otherwise use Default
    pub fn new(num_verbose: u64) -> Self {
        match num_verbose {
            0 => Self::Default,
            1 => Self::Verbose,
            _ => Self::Debug,
        }
    }

    /// Return a LoggingLevel that represents the most verbose logging
    /// of `self` and `other`
    pub fn combine(self, other: Self) -> Self {
        Self::new(std::cmp::max(self as u64, other as u64))
    }

    /// set RUST_LOG to the level represented by self, unless RUST_LOG
    /// is already set
    fn set_rust_log_if_needed(&self, level: Option<String>) {
        /// Default debug level is debug for everything except
        /// some especially noisy low level libraries
        const DEFAULT_DEBUG_LOG_LEVEL: &str = "debug,hyper::proto::h1=info,h2=info";

        // Default verbose log level is info level for all components
        const DEFAULT_VERBOSE_LOG_LEVEL: &str = "info";

        // Default log level is warn level for all components
        const DEFAULT_LOG_LEVEL: &str = "info";

        match level {
            Some(lvl) => {
                if !matches!(self, Self::Default) {
                    eprintln!(
                        "WARNING: Using RUST_LOG='{}' environment, ignoring -v command line",
                        lvl
                    );
                } else {
                    std::env::set_var("RUST_LOG", lvl);
                }
            }
            None => {
                match self {
                    Self::Default => std::env::set_var("RUST_LOG", DEFAULT_LOG_LEVEL),
                    Self::Verbose => std::env::set_var("RUST_LOG", DEFAULT_VERBOSE_LOG_LEVEL),
                    Self::Debug => std::env::set_var("RUST_LOG", DEFAULT_DEBUG_LOG_LEVEL),
                };
            }
        }
    }

    /// Configures basic logging for 'simple' command line tools. Note
    /// this does not setup tracing or open telemetry
    pub fn setup_basic_logging(&self) {
        self.set_rust_log_if_needed(std::env::var("RUST_LOG").ok());
        env_logger::init();
    }

    /// Configures logging and tracing, based on the configuration
    /// values, for the IOx server (the whole enchalada)
    pub fn setup_logging(&self, config: &RunConfig) -> Option<opentelemetry_jaeger::Uninstall> {
        // Copy anything from the config to the rust log environment
        self.set_rust_log_if_needed(config.rust_log.clone());

        // Configure the OpenTelemetry tracer, if requested.
        let (opentelemetry, drop_handle) =
            if std::env::var("OTEL_EXPORTER_JAEGER_AGENT_HOST").is_ok() {
                // For now, configure open telemetry directly from the
                // environment. Eventually it would be cool to document
                // all of the open telemetry options in IOx and pass them
                // explicitly to opentelemetry for additional visibility
                let (tracer, drop_handle) = opentelemetry_jaeger::new_pipeline()
                    .with_service_name("iox")
                    .from_env()
                    .install()
                    .expect("failed to initialise the Jaeger tracing sink");

                // Initialise the opentelemetry tracing layer, giving it the jaeger emitter
                let opentelemetry = tracing_opentelemetry::layer().with_tracer(tracer);

                (Some(opentelemetry), Some(drop_handle))
            } else {
                (None, None)
            };

        // Register the chain of event subscribers:
        //
        //      - Jaeger tracing emitter
        //      - Env filter (using RUST_LOG as the filter env)
        //      - A stderr logger
        //
        let subscriber = tracing_subscriber::registry()
            .with(opentelemetry)
            // filter messages to only those specified by RUST_LOG environment
            .with(EnvFilter::from_default_env());

        // Configure the logger to write to stderr and install it
        let output_stream = std::io::stderr;

        let log_format = config.log_format.as_ref().cloned().unwrap_or_default();

        match log_format {
            LogFormat::Rust => {
                let logger = tracing_subscriber::fmt::layer().with_writer(output_stream);
                subscriber.with(logger).init();
            }
            LogFormat::LogFmt => {
                let logger = logfmt::LogFmtLayer::new(output_stream);
                subscriber.with(logger).init();
            }
        };

        drop_handle
    }
}
