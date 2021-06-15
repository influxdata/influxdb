//! Log and trace initialization and setup

use std::cmp::max;
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
    let mut config = config.tracing_config.clone();

    // Handle the case if -v/-vv is specified both before and after the server
    // command
    config.log_verbose_count = max(config.log_verbose_count, log_verbose_count);

    config.to_builder().install_global()
}
