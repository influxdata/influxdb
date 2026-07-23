use super::*;
use clap::Parser;

#[test]
fn disable_package_management_defaults_to_false() {
    let config = ProcessingEngineConfig::try_parse_from(["test"]).unwrap();
    assert!(!config.disable_package_management);
}

#[test]
fn disable_package_management_flag_sets_true() {
    let config =
        ProcessingEngineConfig::try_parse_from(["test", "--disable-package-management"]).unwrap();
    assert!(config.disable_package_management);
}

#[test]
fn async_trigger_concurrency_limit_flag_sets_value() {
    let config =
        ProcessingEngineConfig::try_parse_from(["test", "--async-trigger-concurrency-limit", "4"])
            .unwrap();
    assert_eq!(
        config.async_trigger_concurrency_limit,
        NonZeroUsize::new(4).unwrap()
    );
}

#[test]
fn async_trigger_concurrency_limit_default_and_env() {
    // Default and env behavior are combined into one test because environment
    // variables are process-global and tests run in parallel.
    unsafe {
        let config = ProcessingEngineConfig::try_parse_from(["test"]).unwrap();
        assert_eq!(config.async_trigger_concurrency_limit, NonZeroUsize::MAX);

        std::env::set_var("INFLUXDB3_ASYNC_TRIGGER_CONCURRENCY_LIMIT", "7");
        let config = ProcessingEngineConfig::try_parse_from(["test"]).unwrap();
        assert_eq!(
            config.async_trigger_concurrency_limit,
            NonZeroUsize::new(7).unwrap()
        );
        std::env::remove_var("INFLUXDB3_ASYNC_TRIGGER_CONCURRENCY_LIMIT");

        let config = ProcessingEngineConfig::try_parse_from(["test"]).unwrap();
        assert_eq!(config.async_trigger_concurrency_limit, NonZeroUsize::MAX);
    }
}

#[test]
fn async_trigger_concurrency_limit_rejects_zero() {
    ProcessingEngineConfig::try_parse_from(["test", "--async-trigger-concurrency-limit", "0"])
        .unwrap_err();
}
