///! Common CLI flags for logging and tracing
use crate::{config::*, Builder, Result, TracingGuard};
use std::num::NonZeroU16;
use structopt::StructOpt;
use tracing_subscriber::fmt::{writer::BoxMakeWriter, MakeWriter};

/// CLI config for the logging related subset of options.
#[derive(Debug, StructOpt, Clone)]
pub struct LoggingConfig {
    /// Logs: filter directive
    ///
    /// Configures log severity level filter, by target.
    ///
    /// Simplest options: error, warn, info, debug, trace
    ///
    /// Levels for different modules can be specified. For example
    /// `debug,hyper::proto::h1=info` specifies debug logging for all modules
    /// except for the `hyper::proto::h1' module which will only display info
    /// level logging.
    ///
    /// Extended syntax provided by `tracing-subscriber` includes span/field
    /// filters. See <https://docs.rs/tracing-subscriber/0.2.17/tracing_subscriber/filter/struct.EnvFilter.html> for more details.
    ///
    /// Overridden by `-v`.
    ///
    /// If None, [`crate::Builder`] sets a default, by default [`crate::Builder::DEFAULT_LOG_FILTER`],
    /// but overrideable with [`crate::Builder::with_default_log_filter`].
    #[structopt(long = "--log-filter", env = "LOG_FILTER")]
    pub log_filter: Option<String>,

    /// Logs: filter short-hand
    ///
    /// Convenient way to set log severity level filter.
    /// Overrides `--log-filter`.
    ///
    /// -v   'info'
    ///
    /// -vv  'debug,hyper::proto::h1=info,h2=info'
    ///
    /// -vvv 'trace,hyper::proto::h1=info,h2=info'
    #[structopt(
        short = "-v",
        long = "--verbose",
        multiple = true,
        takes_value = false,
        parse(from_occurrences)
    )]
    pub log_verbose_count: u8,

    /// Logs: destination
    ///
    /// Can be one of: stdout, stderr
    //
    // TODO(jacobmarble): consider adding file path, file rotation, syslog, ?
    #[structopt(
        long = "--log-destination",
        env = "LOG_DESTINATION",
        default_value = "stdout",
        verbatim_doc_comment
    )]
    pub log_destination: LogDestination,

    #[rustfmt::skip]
    /// Logs: message format
    ///
    /// Can be one of:
    ///
    /// full: human-readable, single line
    ///
    ///   Oct 24 12:55:47.815 ERROR shaving_yaks{yaks=3}: fmt::yak_shave: failed to shave yak yak=3 error=missing yak
    ///   Oct 24 12:55:47.815 TRACE shaving_yaks{yaks=3}: fmt::yak_shave: yaks_shaved=2
    ///   Oct 24 12:55:47.815  INFO fmt: yak shaving completed all_yaks_shaved=false
    ///
    /// pretty: human-readable, multi line
    ///
    ///   Oct 24 12:57:29.387 fmt_pretty::yak_shave: failed to shave yak, yak: 3, error: missing yak
    ///     at examples/examples/fmt/yak_shave.rs:48 on main
    ///     in fmt_pretty::yak_shave::shaving_yaks with yaks: 3
    ///
    ///   Oct 24 12:57:29.387 fmt_pretty::yak_shave: yaks_shaved: 2
    ///     at examples/examples/fmt/yak_shave.rs:52 on main
    ///     in fmt_pretty::yak_shave::shaving_yaks with yaks: 3
    ///
    ///   Oct 24 12:57:29.387 fmt_pretty: yak shaving completed, all_yaks_shaved: false
    ///     at examples/examples/fmt-pretty.rs:19 on main
    ///
    /// json: machine-parseable
    ///
    ///   {"timestamp":"Oct 24 13:00:00.875","level":"ERROR","fields":{"message":"failed to shave yak","yak":3,"error":"missing yak"},"target":"fmt_json::yak_shave","spans":[{"yaks":3,"name":"shaving_yaks"}]}
    ///   {"timestamp":"Oct 24 13:00:00.875","level":"TRACE","fields":{"yaks_shaved":2},"target":"fmt_json::yak_shave","spans":[{"yaks":3,"name":"shaving_yaks"}]}
    ///   {"timestamp":"Oct 24 13:00:00.875","level":"INFO","fields":{"message":"yak shaving completed","all_yaks_shaved":false},"target":"fmt_json"}
    ///
    /// logfmt: human-readable and machine-parseable
    ///
    ///   level=info msg="This is an info message" target="logging" location="logfmt/tests/logging.rs:36" time=1612181556329599000
    ///   level=debug msg="This is a debug message" target="logging" location="logfmt/tests/logging.rs:37" time=1612181556329618000
    ///   level=trace msg="This is a trace message" target="logging" location="logfmt/tests/logging.rs:38" time=1612181556329634000
    #[structopt(long = "--log-format", env = "LOG_FORMAT", default_value = "full", verbatim_doc_comment)]
    pub log_format: LogFormat,
}

impl LoggingConfig {
    pub fn to_builder(&self) -> Builder<BoxMakeWriter> {
        self.with_builder(Builder::new())
    }

    pub fn with_builder<W>(&self, builder: Builder<W>) -> Builder<BoxMakeWriter>
    where
        W: MakeWriter + Send + Sync + Clone + 'static,
    {
        builder
            .with_log_filter(&self.log_filter)
            // with_verbose_count goes after with_log_filter because our CLI flag state
            // that --v overrides --log-filter.
            .with_log_verbose_count(self.log_verbose_count)
            .with_log_destination(self.log_destination)
            .with_log_format(self.log_format)
    }

    pub fn install_global_subscriber(&self) -> Result<TracingGuard> {
        self.to_builder().install_global()
    }
}

/// Extends the trogging [`crate::Builder`] API.
pub trait LoggingConfigBuilderExt {
    /// Applies all config entries from a [`LoggingConfig`] to a [`crate::Builder`].
    fn with_logging_config(self, config: &LoggingConfig) -> Builder<BoxMakeWriter>;
}

impl<W> LoggingConfigBuilderExt for Builder<W>
where
    W: MakeWriter + Send + Sync + Clone + 'static,
{
    fn with_logging_config(self, config: &LoggingConfig) -> Builder<BoxMakeWriter> {
        config.with_builder(self)
    }
}

/// CLI config for the logging+tracing related subset of options.
#[derive(Debug, StructOpt, Clone)]
pub struct TracingConfig {
    /// Tracing: exporter type
    ///
    /// Can be one of: none, jaeger, otlp
    ///
    /// When enabled, additional flags are considered (see flags related to OTLP
    /// and Jaeger).
    #[structopt(
        long = "--traces-exporter",
        env = "TRACES_EXPORTER",
        default_value = "none"
    )]
    pub traces_exporter: TracesExporter,

    /// Tracing: filter directive
    ///
    /// Configures traces severity level filter, by target.
    ///
    /// Simplest options: error, warn, info, debug, trace
    ///
    /// Levels for different modules can be specified. For example
    /// `debug,hyper::proto::h1=info` specifies debug tracing for all modules
    /// except for the `hyper::proto::h1` module which will only display info
    /// level tracing.
    ///
    /// Extended syntax provided by `tracing-subscriber` includes span/field
    /// filters. See <https://docs.rs/tracing-subscriber/0.2.17/tracing_subscriber/filter/struct.EnvFilter.html> for more details.
    ///
    /// No filter by default.
    #[structopt(long = "--traces-filter", env = "TRACES_FILTER")]
    pub traces_filter: Option<String>,

    /// Tracing: sampler type
    ///
    /// Can be one of:
    /// always_on, always_off, traceidratio,
    /// parentbased_always_on, parentbased_always_off, parentbased_traceidratio
    ///
    /// These alternatives are described in detail at <https://github.com/open-telemetry/opentelemetry-specification/blob/v1.1.0/specification/sdk-environment-variables.md#general-sdk-configuration>.
    #[structopt(
        long = "--traces-sampler",
        env = "TRACES_SAMPLER",
        default_value = "parentbased_traceidratio"
    )]
    pub traces_sampler: TracesSampler,

    #[rustfmt::skip]
    /// Tracing: sampler argument
    ///
    /// Valid range: [0.0, 1.0].
    ///
    /// Only used if `--traces-sampler` is set to
    /// parentbased_traceidratio (default) or traceidratio.
    ///
    /// With sample parentbased_traceidratio, the following rules apply:
    /// - if parent is sampled, then all of its children are sampled
    /// - else sample this portion of traces (0.5 = 50%)
    ///
    /// More details about this sampling argument at <https://github.com/open-telemetry/opentelemetry-specification/blob/v1.1.0/specification/sdk-environment-variables.md#general-sdk-configuration>.
    #[structopt(
    long = "--traces-sampler-arg",
    env = "TRACES_SAMPLER_ARG",
    default_value = "1.0",
    verbatim_doc_comment
    )]
    pub traces_sampler_arg: f64,

    /// Tracing: OTLP (eg OpenTelemetry collector) network hostname
    ///
    /// Only used if `--traces-exporter` is "otlp".
    ///
    /// Protocol is gRPC. HTTP is not supported.
    #[structopt(
        long = "--traces-exporter-otlp-host",
        env = "TRACES_EXPORTER_OTLP_HOST",
        default_value = "localhost"
    )]
    pub traces_exporter_otlp_host: String,

    /// Tracing: OTLP (eg OpenTelemetry collector) network port
    ///
    /// Only used if `--traces-exporter` is "otlp".
    ///
    /// Protocol is gRPC. HTTP is not supported.
    #[structopt(
        long = "--traces-exporter-otlp-port",
        env = "TRACES_EXPORTER_OTLP_PORT",
        default_value = "4317"
    )]
    pub traces_exporter_otlp_port: NonZeroU16,

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

    /// Tracing: Jaeger max UDP packet size
    ///
    /// Default to 1300, which is a safe MTU.
    ///
    /// You can increase it to 65000 if the target is a jaeger collector
    /// on localhost. If so, the batching exporter will be enabled for
    /// extra efficiency. Otherwise an UDP packet will be sent for each exported span.
    ///
    /// Only used if `--traces-exporter` is "jaeger".
    #[structopt(
        long = "--traces-exporter-jaeger-max-packet-size",
        env = "TRACES_EXPORTER_JAEGER_MAX_PACKET_SIZE",
        default_value = "1300"
    )]
    pub traces_exporter_jaeger_max_packet_size: usize,
}

impl TracingConfig {
    pub fn to_builder(&self) -> Builder {
        self.with_builder(Builder::new())
    }

    pub fn with_builder<W>(&self, builder: Builder<W>) -> Builder<W>
    where
        W: MakeWriter + Send + Sync + Clone + 'static,
    {
        builder
            .with_traces_filter(&self.traces_filter)
            .with_traces_exporter(self.traces_exporter)
            .with_traces_sampler(self.traces_sampler, self.traces_sampler_arg)
            .with_jaeger_config(JaegerConfig {
                agent_host: self.traces_exporter_jaeger_agent_host.clone(),
                agent_port: self.traces_exporter_jaeger_agent_port,
                service_name: self.traces_exporter_jaeger_service_name.clone(),
                max_packet_size: self.traces_exporter_jaeger_max_packet_size,
            })
            .with_oltp_config(OtlpConfig {
                host: self.traces_exporter_otlp_host.clone(),
                port: self.traces_exporter_otlp_port,
            })
    }

    pub fn install_global_subscriber(&self) -> Result<TracingGuard> {
        self.to_builder().install_global()
    }
}

/// Extends the trogging [`crate::Builder`] API.
pub trait TracingConfigBuilderExt<W> {
    /// Applies all config entries from a [`TracingConfig`] to a [`crate::Builder`].
    fn with_tracing_config(self, config: &TracingConfig) -> Builder<W>;
}

impl<W> TracingConfigBuilderExt<W> for Builder<W>
where
    W: MakeWriter + Send + Sync + Clone + 'static,
{
    fn with_tracing_config(self, config: &TracingConfig) -> Self {
        config.with_builder(self)
    }
}

impl From<LoggingConfig> for Builder<BoxMakeWriter> {
    fn from(config: LoggingConfig) -> Self {
        config.to_builder()
    }
}

impl From<TracingConfig> for Builder {
    fn from(config: TracingConfig) -> Self {
        config.to_builder()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::test_util::simple_test;

    fn to_vec(v: &[&str]) -> Vec<String> {
        v.iter().map(|s| s.to_string()).collect()
    }

    #[test]
    fn test_log_verbose_count() {
        let cfg = LoggingConfig::from_iter_safe(to_vec(&["cli"])).unwrap();
        assert_eq!(cfg.log_verbose_count, 0);

        assert_eq!(
            simple_test(cfg.into()).without_timestamps(),
            r#"
ERROR foo
WARN woo
"#
            .trim_start(),
        );

        let cfg = LoggingConfig::from_iter_safe(to_vec(&["cli", "-v"])).unwrap();
        assert_eq!(cfg.log_verbose_count, 1);

        assert_eq!(
            simple_test(cfg.into()).without_timestamps(),
            r#"
ERROR foo
WARN woo
INFO bar
"#
            .trim_start(),
        );

        let cfg = LoggingConfig::from_iter_safe(to_vec(&["cli", "-vv"])).unwrap();
        assert_eq!(cfg.log_verbose_count, 2);

        assert_eq!(
            simple_test(cfg.into()).without_timestamps(),
            r#"
ERROR foo
WARN woo
INFO bar
DEBUG baz
"#
            .trim_start(),
        );

        let cfg = LoggingConfig::from_iter_safe(to_vec(&["cli", "-vvv"])).unwrap();
        assert_eq!(cfg.log_verbose_count, 3);

        assert_eq!(
            simple_test(cfg.into()).without_timestamps(),
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
    fn test_custom_default_log_level() {
        let cfg = LoggingConfig::from_iter_safe(to_vec(&["cli"])).unwrap();

        assert_eq!(
            simple_test(
                Builder::new()
                    .with_default_log_filter("debug")
                    .with_logging_config(&cfg)
            )
            .without_timestamps(),
            r#"
ERROR foo
WARN woo
INFO bar
DEBUG baz
"#
            .trim_start(),
        );

        let cfg = LoggingConfig::from_iter_safe(to_vec(&["cli", "--log-filter=info"])).unwrap();

        assert_eq!(
            simple_test(
                Builder::new()
                    .with_default_log_filter("debug")
                    .with_logging_config(&cfg)
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
}
