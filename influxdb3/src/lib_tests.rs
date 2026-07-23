use super::{apply_noise_reduction, expand_log_filter};
use clap::{Arg, Parser as _, parser::ValueSource};

#[test]
fn user_provided_value_source_only_matches_cli_and_env() {
    assert!(!super::user_provided_value_source(None));
    assert!(!super::user_provided_value_source(Some(
        ValueSource::DefaultValue
    )));
    assert!(super::user_provided_value_source(Some(
        ValueSource::CommandLine
    )));
    assert!(super::user_provided_value_source(Some(
        ValueSource::EnvVariable
    )));
}

#[test]
fn deprecated_serve_option_warning_requires_user_provided_value() {
    let matches = clap::Command::new("serve")
        .arg(
            Arg::new("package_manager")
                .long("package-manager")
                .default_value("discover"),
        )
        .try_get_matches_from(["serve"])
        .unwrap();

    let warnings: Vec<_> =
        super::deprecated_serve_option_warnings(&matches, super::DEPRECATED_SERVE_OPTIONS)
            .collect();

    assert!(warnings.is_empty());
}

#[test]
fn deprecated_serve_option_warning_detects_cli_value() {
    let matches = clap::Command::new("serve")
        .arg(Arg::new("package_manager").long("package-manager"))
        .try_get_matches_from(["serve", "--package-manager", "pip"])
        .unwrap();

    let warnings: Vec<_> =
        super::deprecated_serve_option_warnings(&matches, super::DEPRECATED_SERVE_OPTIONS)
            .collect();

    assert_eq!(
        warnings,
        vec![
            super::DEPRECATED_SERVE_OPTIONS
                .iter()
                .find(|option| option.arg_id == "package_manager")
                .unwrap()
                .message
        ]
    );
}

#[test]
fn disabled_package_manager_warning_requires_user_provided_disabled_value() {
    use influxdb3_clap_blocks::plugins::PackageManager;

    assert_eq!(
        super::disabled_package_manager_deprecation_warning(
            PackageManager::Disabled,
            Some(ValueSource::CommandLine),
        ),
        Some(super::PACKAGE_MANAGER_DISABLED_DEPRECATED_MESSAGE)
    );
    assert_eq!(
        super::disabled_package_manager_deprecation_warning(
            PackageManager::Disabled,
            Some(ValueSource::EnvVariable),
        ),
        Some(super::PACKAGE_MANAGER_DISABLED_DEPRECATED_MESSAGE)
    );
    assert_eq!(
        super::disabled_package_manager_deprecation_warning(
            PackageManager::Disabled,
            Some(ValueSource::DefaultValue),
        ),
        None
    );
    assert_eq!(
        super::disabled_package_manager_deprecation_warning(
            PackageManager::Pip,
            Some(ValueSource::CommandLine)
        ),
        None
    );
}

#[test]
fn bare_debug_expands() {
    let result = expand_log_filter("debug", false);
    assert!(result.starts_with("debug,"), "got: {result}");
    assert!(result.contains("reqwest=info"), "got: {result}");
}

#[test]
fn bare_trace_expands() {
    let result = expand_log_filter("trace", false);
    assert!(result.starts_with("trace,"), "got: {result}");
    assert!(result.contains("reqwest=info"), "got: {result}");
}

#[test]
fn composite_debug_expands() {
    let result = expand_log_filter("debug,influxdb3_write_buffer=debug", false);
    assert!(
        result.starts_with("debug,influxdb3_write_buffer=debug,"),
        "got: {result}"
    );
    assert!(result.contains("reqwest=info"), "got: {result}");
}

#[test]
fn info_not_expanded() {
    assert_eq!(expand_log_filter("info", false), "info");
}

#[test]
fn warn_not_expanded() {
    assert_eq!(expand_log_filter("warn", false), "warn");
}

#[test]
fn arbitrary_not_expanded() {
    let input = "info,influxdb3_wal=debug";
    assert_eq!(expand_log_filter(input, false), input);
}

#[test]
fn expansion_disabled() {
    assert_eq!(expand_log_filter("debug", true), "debug");
}

#[test]
fn verbose_vv_expands() {
    let mut config = trogging::cli::LoggingConfig::try_parse_from(["cmd", "-vv"]).unwrap();
    apply_noise_reduction(&mut config, false);
    assert_eq!(config.log_verbose_count, 0);
    let filter = config.log_filter.unwrap();
    assert!(filter.starts_with("debug,"), "got: {filter}");
    assert!(filter.contains("reqwest=info"), "got: {filter}");
}

#[test]
fn verbose_vvv_expands() {
    let mut config = trogging::cli::LoggingConfig::try_parse_from(["cmd", "-vvv"]).unwrap();
    apply_noise_reduction(&mut config, false);
    let filter = config.log_filter.unwrap();
    assert!(filter.starts_with("trace,"), "got: {filter}");
    assert!(filter.contains("reqwest=info"), "got: {filter}");
}

#[test]
fn verbose_vvvv_expands() {
    // trogging maps count >= 3 to "trace"; 4+ behaves identically to 3
    let mut config = trogging::cli::LoggingConfig::try_parse_from(["cmd", "-vvvv"]).unwrap();
    apply_noise_reduction(&mut config, false);
    assert_eq!(config.log_verbose_count, 0);
    let filter = config.log_filter.unwrap();
    assert!(filter.starts_with("trace,"), "got: {filter}");
    assert!(filter.contains("reqwest=info"), "got: {filter}");
}

#[test]
fn verbose_v_not_expanded() {
    let mut config = trogging::cli::LoggingConfig::try_parse_from(["cmd", "-v"]).unwrap();
    apply_noise_reduction(&mut config, false);
    assert_eq!(config.log_verbose_count, 1);
    assert!(config.log_filter.is_none());
}

#[test]
fn verbose_vv_expansion_disabled() {
    let mut config = trogging::cli::LoggingConfig::try_parse_from(["cmd", "-vv"]).unwrap();
    apply_noise_reduction(&mut config, true);
    assert_eq!(config.log_verbose_count, 0);
    assert_eq!(config.log_filter.as_deref(), Some("debug"));
}

#[test]
fn warning_messages_stay_in_sync_with_consts() {
    use influxdb3_processing_engine::TRIGGER_QUEUE_SIZE;

    // The warning texts hardcode numbers as prose; keep them tied to the
    // constants the checks actually use.
    assert!(
        super::ASYNC_TRIGGER_CONCURRENCY_SATURATES_QUEUE_MESSAGE
            .contains(&format!("({TRIGGER_QUEUE_SIZE})"))
    );
    assert!(
        super::ASYNC_TRIGGER_CONCURRENCY_DEFAULT_CHANGE_MESSAGE.contains(&format!(
            "change to {}",
            super::FUTURE_DEFAULT_ASYNC_TRIGGER_CONCURRENCY_LIMIT
        ))
    );
    assert!(
        super::ASYNC_TRIGGER_CONCURRENCY_ABOVE_FUTURE_DEFAULT_MESSAGE.contains(&format!(
            "above {}",
            super::FUTURE_DEFAULT_ASYNC_TRIGGER_CONCURRENCY_LIMIT
        ))
    );
}

#[test]
fn warning_arg_ids_exist_on_serve_command() {
    use clap::CommandFactory;

    // value_source lookups silently return None (misclassifying explicit
    // values as defaults) if these ids drift from the clap field names.
    let cmd = super::Config::command();
    let serve = cmd
        .find_subcommand("serve")
        .expect("serve subcommand exists");
    for id in [
        super::PACKAGE_MANAGER_ARG_ID,
        super::ASYNC_TRIGGER_CONCURRENCY_LIMIT_ARG_ID,
    ] {
        assert!(
            serve.get_arguments().any(|arg| arg.get_id().as_str() == id),
            "arg id `{id}` not found on the serve command"
        );
    }
}

#[test]
fn async_trigger_concurrency_warning_fires_for_unlimited_default() {
    use std::num::NonZeroUsize;

    // Omitted option: clap reports the default value source and the limit is
    // NonZeroUsize::MAX — the behavior that changes in a future release.
    assert_eq!(
        super::async_trigger_concurrency_default_change_warning(
            NonZeroUsize::MAX,
            Some(ValueSource::DefaultValue),
        ),
        Some(super::ASYNC_TRIGGER_CONCURRENCY_DEFAULT_CHANGE_MESSAGE)
    );
    assert_eq!(
        super::async_trigger_concurrency_default_change_warning(NonZeroUsize::MAX, None),
        Some(super::ASYNC_TRIGGER_CONCURRENCY_DEFAULT_CHANGE_MESSAGE)
    );
}

#[test]
fn async_trigger_concurrency_warning_fires_for_explicit_value_above_future_default() {
    use std::num::NonZeroUsize;

    assert_eq!(
        super::async_trigger_concurrency_default_change_warning(
            NonZeroUsize::new(9).unwrap(),
            Some(ValueSource::CommandLine),
        ),
        Some(super::ASYNC_TRIGGER_CONCURRENCY_ABOVE_FUTURE_DEFAULT_MESSAGE)
    );
    assert_eq!(
        super::async_trigger_concurrency_default_change_warning(
            NonZeroUsize::new(100).unwrap(),
            Some(ValueSource::EnvVariable),
        ),
        Some(super::ASYNC_TRIGGER_CONCURRENCY_ABOVE_FUTURE_DEFAULT_MESSAGE)
    );
}

#[test]
fn async_trigger_concurrency_saturation_warning_fires_at_or_above_queue_capacity() {
    use influxdb3_processing_engine::TRIGGER_QUEUE_SIZE;
    use std::num::NonZeroUsize;

    assert_eq!(
        super::async_trigger_concurrency_saturates_queue_warning(
            NonZeroUsize::new(TRIGGER_QUEUE_SIZE).unwrap()
        ),
        Some(super::ASYNC_TRIGGER_CONCURRENCY_SATURATES_QUEUE_MESSAGE)
    );
    assert_eq!(
        super::async_trigger_concurrency_saturates_queue_warning(
            NonZeroUsize::new(TRIGGER_QUEUE_SIZE + 1).unwrap()
        ),
        Some(super::ASYNC_TRIGGER_CONCURRENCY_SATURATES_QUEUE_MESSAGE)
    );
    assert_eq!(
        super::async_trigger_concurrency_saturates_queue_warning(
            NonZeroUsize::new(TRIGGER_QUEUE_SIZE - 1).unwrap()
        ),
        None
    );
    // The unlimited default is excluded: its capacity is unreachable, and the
    // default-change warning already covers it.
    assert_eq!(
        super::async_trigger_concurrency_saturates_queue_warning(NonZeroUsize::MAX),
        None
    );
}

#[test]
fn async_trigger_concurrency_warning_silent_at_or_below_future_default() {
    use std::num::NonZeroUsize;

    assert_eq!(
        super::async_trigger_concurrency_default_change_warning(
            NonZeroUsize::new(super::FUTURE_DEFAULT_ASYNC_TRIGGER_CONCURRENCY_LIMIT).unwrap(),
            Some(ValueSource::CommandLine),
        ),
        None
    );
    assert_eq!(
        super::async_trigger_concurrency_default_change_warning(
            NonZeroUsize::new(1).unwrap(),
            Some(ValueSource::EnvVariable),
        ),
        None
    );
}
