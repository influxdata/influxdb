use clap::Parser;
use iox_query::{config::IoxConfigExt, exec::Executor};

use super::IoxQueryDatafusionConfig;

#[test_log::test]
fn max_parquet_fanout() {
    let datafusion_config =
        IoxQueryDatafusionConfig::parse_from(["", "--datafusion-max-parquet-fanout", "5"]).build();
    let exec = Executor::new_testing();
    let mut session_config = exec.new_session_config();
    for (k, v) in &datafusion_config {
        session_config = session_config.with_config_option(k, v);
    }
    let ctx = session_config.build();
    let inner_ctx = ctx.inner().state();
    let config = inner_ctx.config();
    let iox_config_ext = config.options().extensions.get::<IoxConfigExt>().unwrap();
    assert_eq!(5, iox_config_ext.max_parquet_fanout);
}

#[test_log::test]
fn share_cached_parquet_loader_fetches_enabled_by_default() {
    // The influxdb3 server opts in to per-scan fetch sharing in the cached parquet loader
    // (influxdb_pro#6009); the iox_query crate default stays off for other consumers (IOx).
    let datafusion_config = IoxQueryDatafusionConfig::parse_from([""]).build();
    let exec = Executor::new_testing();
    let mut session_config = exec.new_session_config();
    for (k, v) in &datafusion_config {
        session_config = session_config.with_config_option(k, v);
    }
    let ctx = session_config.build();
    let inner_ctx = ctx.inner().state();
    let config = inner_ctx.config();
    let iox_config_ext = config.options().extensions.get::<IoxConfigExt>().unwrap();
    assert!(iox_config_ext.share_cached_parquet_loader_fetches);
}

#[test_log::test]
fn share_cached_parquet_loader_fetches_uppercase_override_respected() {
    // DataFusion lowercases bool config values before parsing, so `FALSE` is a valid
    // override — the build-time validity check must not misread it and re-enable sharing.
    let datafusion_config = IoxQueryDatafusionConfig::parse_from([
        "",
        "--datafusion-config",
        "iox.share_cached_parquet_loader_fetches:FALSE",
    ])
    .build();
    let exec = Executor::new_testing();
    let mut session_config = exec.new_session_config();
    for (k, v) in &datafusion_config {
        session_config = session_config.with_config_option(k, v);
    }
    let ctx = session_config.build();
    let inner_ctx = ctx.inner().state();
    let config = inner_ctx.config();
    let iox_config_ext = config.options().extensions.get::<IoxConfigExt>().unwrap();
    assert!(!iox_config_ext.share_cached_parquet_loader_fetches);
}

#[test_log::test]
fn share_cached_parquet_loader_fetches_malformed_override_keeps_default() {
    // A value that does not parse as a bool would otherwise occupy the map entry (bypassing
    // the opt-in default) and then be silently ignored by the session config layer — a typo
    // must not silently disable fetch sharing.
    let datafusion_config = IoxQueryDatafusionConfig::parse_from([
        "",
        "--datafusion-config",
        "iox.share_cached_parquet_loader_fetches:fasle",
    ])
    .build();
    let exec = Executor::new_testing();
    let mut session_config = exec.new_session_config();
    for (k, v) in &datafusion_config {
        session_config = session_config.with_config_option(k, v);
    }
    let ctx = session_config.build();
    let inner_ctx = ctx.inner().state();
    let config = inner_ctx.config();
    let iox_config_ext = config.options().extensions.get::<IoxConfigExt>().unwrap();
    assert!(iox_config_ext.share_cached_parquet_loader_fetches);
}

#[test_log::test]
fn share_cached_parquet_loader_fetches_datafusion_config_override() {
    // The generic `--datafusion-config` path stays an operational escape hatch: an explicit
    // value is not clobbered by the opt-in default.
    let datafusion_config = IoxQueryDatafusionConfig::parse_from([
        "",
        "--datafusion-config",
        "iox.share_cached_parquet_loader_fetches:false",
    ])
    .build();
    let exec = Executor::new_testing();
    let mut session_config = exec.new_session_config();
    for (k, v) in &datafusion_config {
        session_config = session_config.with_config_option(k, v);
    }
    let ctx = session_config.build();
    let inner_ctx = ctx.inner().state();
    let config = inner_ctx.config();
    let iox_config_ext = config.options().extensions.get::<IoxConfigExt>().unwrap();
    assert!(!iox_config_ext.share_cached_parquet_loader_fetches);
}
