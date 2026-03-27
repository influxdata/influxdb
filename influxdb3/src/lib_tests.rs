use super::{apply_noise_reduction, expand_log_filter};
use clap::Parser as _;

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
