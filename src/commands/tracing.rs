//! Log and trace initialization and setup

use std::cmp::max;
use trogging::cli::LoggingConfigBuilderExt;
pub use trogging::config::*;
pub use trogging::TracingGuard;

/// Start simple logger. Panics on error.
pub fn init_simple_logs(log_verbose_count: u8) -> Result<TracingGuard, trogging::Error> {
    trogging::Builder::new()
        .with_log_verbose_count(log_verbose_count)
        .install_global()
}

/// Start log or trace emitter. Panics on error.
pub fn init_logs_and_tracing(
    log_verbose_count: u8,
    config: &crate::commands::run::Config,
) -> Result<TracingGuard, trogging::Error> {
    let mut logging_config = config.logging_config.clone();

    // Handle the case if -v/-vv is specified both before and after the server
    // command
    logging_config.log_verbose_count = max(logging_config.log_verbose_count, log_verbose_count);

    trogging::Builder::new()
        .with_logging_config(&logging_config)
        .install_global()
}
