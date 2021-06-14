//! Log and trace initialization and setup

pub use trogging::config::*;
pub use trogging::TracingGuard;
use trogging::{JaegerConfig, OtlpConfig};

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
    // Handle the case if -v/-vv is specified both before and after the server
    // command
    let log_verbose_count = if log_verbose_count > config.log_verbose_count {
        log_verbose_count
    } else {
        config.log_verbose_count
    };

    trogging::Builder::new()
        .with_log_filter(&config.log_filter)
        // with_verbose_count goes after with_log_filter because our CLI flag state
        // that --v overrides --log-filter.
        .with_log_verbose_count(log_verbose_count)
        .with_log_destination(config.log_destination)
        .with_log_format(config.log_format)
        .with_traces_filter(&config.traces_filter)
        .with_traces_exporter(config.traces_exporter)
        .with_traces_sampler(config.traces_sampler, config.traces_sampler_arg)
        .with_jaeger_config(JaegerConfig {
            agent_host: config.traces_exporter_jaeger_agent_host.clone(),
            agent_port: config.traces_exporter_jaeger_agent_port,
            service_name: config.traces_exporter_jaeger_service_name.clone(),
            max_packet_size: config.traces_exporter_jaeger_max_packet_size,
        })
        .with_oltp_config(OtlpConfig {
            host: config.traces_exporter_otlp_host.clone(),
            port: config.traces_exporter_otlp_port,
        })
        .install_global()
}
