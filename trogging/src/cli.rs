//! Common CLI flags for logging and tracing
use crate::{config::*, Builder};
use tracing_subscriber::fmt::{writer::BoxMakeWriter, MakeWriter};

/// CLI config for the logging related subset of options.
#[derive(Debug, Clone, clap::Parser)]
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
    #[clap(long = "log-filter", env = "LOG_FILTER", action)]
    pub log_filter: Option<String>,

    /// Logs: filter short-hand
    ///
    /// Convenient way to set log severity level filter.
    /// Overrides `--log-filter`.
    ///
    /// -v   'info,sqlx=warn'
    ///
    /// -vv  'debug,hyper::proto::h1=info,h2=info'
    ///
    /// -vvv 'trace,hyper::proto::h1=info,h2=info'
    #[clap(
        short = 'v',
        long = "verbose",
        action = clap::ArgAction::Count,
    )]
    pub log_verbose_count: u8,

    /// Logs: destination
    ///
    /// Can be one of: stdout, stderr
    //
    // TODO(jacobmarble): consider adding file path, file rotation, syslog, ?
    #[clap(
        long = "log-destination",
        env = "LOG_DESTINATION",
        default_value = "stdout",
        verbatim_doc_comment,
        action
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
    #[clap(
        long = "log-format",
        env = "LOG_FORMAT",
        default_value = "full",
        verbatim_doc_comment,
        action,
    )]
    pub log_format: LogFormat,
}

impl LoggingConfig {
    pub fn to_builder(&self) -> Builder<BoxMakeWriter> {
        self.with_builder(Builder::new())
    }

    pub fn with_builder<W>(&self, builder: Builder<W>) -> Builder<BoxMakeWriter>
    where
        W: for<'writer> MakeWriter<'writer> + Send + Sync + Clone + 'static,
    {
        builder
            .with_log_filter(&self.log_filter)
            // with_verbose_count goes after with_log_filter because our CLI flag state
            // that --v overrides --log-filter.
            .with_log_verbose_count(self.log_verbose_count)
            .with_log_destination(self.log_destination)
            .with_log_format(self.log_format)
    }
}

/// Extends the trogging [`crate::Builder`] API.
pub trait LoggingConfigBuilderExt {
    /// Applies all config entries from a [`LoggingConfig`] to a [`crate::Builder`].
    fn with_logging_config(self, config: &LoggingConfig) -> Builder<BoxMakeWriter>;
}

impl<W> LoggingConfigBuilderExt for Builder<W>
where
    W: for<'writer> MakeWriter<'writer> + Send + Sync + Clone + 'static,
{
    fn with_logging_config(self, config: &LoggingConfig) -> Builder<BoxMakeWriter> {
        config.with_builder(self)
    }
}

impl From<LoggingConfig> for Builder<BoxMakeWriter> {
    fn from(config: LoggingConfig) -> Self {
        config.to_builder()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::test_util::simple_test;
    use clap::Parser;

    #[ignore] // REVERT THIS WHEN REVERTING CIRCLECI LOGGING CONFIG
    #[test]
    fn test_log_verbose_count() {
        let cfg = LoggingConfig::try_parse_from(["cli"]).unwrap();
        assert_eq!(cfg.log_verbose_count, 0);

        assert_eq!(
            simple_test(cfg.into()).without_timestamps(),
            r#"
ERROR foo
WARN woo
"#
            .trim_start(),
        );

        let cfg = LoggingConfig::try_parse_from(["cli", "-v"]).unwrap();
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

        let cfg = LoggingConfig::try_parse_from(["cli", "-vv"]).unwrap();
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

        let cfg = LoggingConfig::try_parse_from(["cli", "-vvv"]).unwrap();
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
        let cfg = LoggingConfig::try_parse_from(["cli"]).unwrap();

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

        let cfg = LoggingConfig::try_parse_from(["cli", "--log-filter=info"]).unwrap();

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
