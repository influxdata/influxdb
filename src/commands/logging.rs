//! Logging initization and setup

use config::Config;
use tracing_subscriber::{prelude::*, EnvFilter};

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

    /// set RUST_LOG to the level represented by self, unless RUST_LOG
    /// is already set
    fn set_rust_log_if_needed(&self) {
        let rust_log_env = std::env::var("RUST_LOG");

        /// Default debug level is debug for everything except
        /// some especially noisy low level libraries
        const DEFAULT_DEBUG_LOG_LEVEL: &str = "debug,hyper::proto::h1=info,h2=info";

        // Default verbose log level is info level for all components
        const DEFAULT_VERBOSE_LOG_LEVEL: &str = "info";

        // Default log level is warn level for all components
        const DEFAULT_LOG_LEVEL: &str = "warn";

        match rust_log_env {
            Ok(lvl) => {
                if !matches!(self, Self::Default) {
                    eprintln!(
                        "WARNING: Using RUST_LOG='{}' environment, ignoring -v command line",
                        lvl
                    );
                }
            }
            Err(_) => {
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
        self.set_rust_log_if_needed();
        env_logger::init();
    }

    /// Configures logging and tracing, based on the configuration
    /// values, for the IOx server (the whole enchalada)
    pub fn setup_logging(&self, config: &Config) -> Option<opentelemetry_jaeger::Uninstall> {
        // Copy anything from the config to the rust log environment
        if let Some(rust_log) = &config.rust_log {
            println!("Setting RUST_LOG: {}", rust_log);
            std::env::set_var("RUST_LOG", rust_log);
        }
        self.set_rust_log_if_needed();

        // Configure the OpenTelemetry tracer, if requested.
        let (opentelemetry, drop_handle) = if config.otel_jaeger_host.is_some() {
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

        // Configure the logger to write to stderr
        let logger = tracing_subscriber::fmt::layer().with_writer(std::io::stderr);

        // Register the chain of event subscribers:
        //
        //      - Jaeger tracing emitter
        //      - Env filter (using RUST_LOG as the filter env)
        //      - A stdout logger
        //
        tracing_subscriber::registry()
            .with(opentelemetry)
            .with(EnvFilter::from_default_env())
            .with(logger)
            .init();

        drop_handle
    }
}
