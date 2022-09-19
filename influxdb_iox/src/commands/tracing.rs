//! Log and trace initialization and setup

use std::cmp::max;
pub use trogging::config::*;
pub use trogging::{self, TroggingGuard};
use trogging::{
    cli::LoggingConfigBuilderExt,
    tracing_subscriber::{prelude::*, Registry},
};

/// Start simple logger. Panics on error.
pub fn init_simple_logs(log_verbose_count: u8) -> Result<TroggingGuard, trogging::Error> {
    trogging::Builder::new()
        .with_log_verbose_count(log_verbose_count)
        .install_global()
}

/// Start log or trace emitter. Panics on error.
pub fn init_logs_and_tracing(
    log_verbose_count: u8,
    config: &crate::commands::run::Config,
) -> Result<TroggingGuard, trogging::Error> {
    let mut logging_config = config.logging_config().clone();

    // Handle the case if -v/-vv is specified both before and after the server
    // command
    logging_config.log_verbose_count = max(logging_config.log_verbose_count, log_verbose_count);

    let log_layer = trogging::Builder::new()
        .with_logging_config(&logging_config)
        .build()?;

    let layers = log_layer;

    // Optionally enable the tokio console exporter layer, if enabled.
    //
    // This spawns a background tokio task to serve the instrumentation data,
    // and hooks the instrumentation into the tracing pipeline.
    #[cfg(feature = "tokio_console")]
    let layers = {
        use console_subscriber::ConsoleLayer;
        let console_layer = ConsoleLayer::builder().with_default_env().spawn();
        layers.and_then(console_layer)
    };

    let subscriber = Registry::default().with(layers);
    trogging::install_global(subscriber)
}
