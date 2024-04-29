use std::sync::Arc;

use iox_time::{SystemProvider, Time, TimeProvider};
use metric::U64Gauge;
use once_cell::sync::Lazy;

/// The process name on the local OS running `influxdb3`
pub const INFLUXDB3_PROCESS_NAME: &str = "influxdb3";

#[cfg(all(not(feature = "heappy"), feature = "jemalloc_replacing_malloc"))]
pub mod jemalloc;

#[cfg(tokio_unstable)]
use tokio_metrics_bridge::setup_tokio_metrics;

#[cfg(all(not(feature = "heappy"), not(feature = "jemalloc_replacing_malloc")))]
pub fn build_malloc_conf() -> String {
    "system".to_string()
}

#[cfg(all(feature = "heappy", not(feature = "jemalloc_replacing_malloc")))]
pub fn build_malloc_conf() -> String {
    "heappy".to_string()
}

#[cfg(all(not(feature = "heappy"), feature = "jemalloc_replacing_malloc"))]
pub fn build_malloc_conf() -> String {
    tikv_jemalloc_ctl::config::malloc_conf::mib()
        .unwrap()
        .read()
        .unwrap()
        .to_string()
}

#[cfg(all(
    feature = "heappy",
    feature = "jemalloc_replacing_malloc",
    not(feature = "clippy")
))]
pub fn build_malloc_conf() -> String {
    compile_error!("must use exactly one memory allocator")
}

#[cfg(feature = "clippy")]
pub fn build_malloc_conf() -> String {
    "clippy".to_string()
}

/// Package version.
pub static INFLUXDB3_VERSION: Lazy<&'static str> =
    Lazy::new(|| option_env!("CARGO_PKG_VERSION").unwrap_or("UNKNOWN"));

/// Build-time GIT revision hash.
pub static INFLUXDB3_GIT_HASH: &str = env!(
    "GIT_HASH",
    "Can not find find GIT HASH in build environment"
);

/// Build-time GIT revision hash.
pub static INFLUXDB3_GIT_HASH_SHORT: &str = env!(
    "GIT_HASH_SHORT",
    "Can not find find GIT HASH in build environment"
);

/// Version string that is combined from [`INFLUXDB3_VERSION`] and [`INFLUXDB3_GIT_HASH`].
pub static VERSION_STRING: Lazy<&'static str> = Lazy::new(|| {
    let s = format!(
        "{}, revision {}",
        &INFLUXDB3_VERSION[..],
        INFLUXDB3_GIT_HASH
    );
    let s: Box<str> = Box::from(s);
    Box::leak(s)
});

/// A UUID that is unique for the process lifetime.
pub static PROCESS_UUID: Lazy<&'static str> = Lazy::new(|| {
    let s = uuid::Uuid::new_v4().to_string();
    let s: Box<str> = Box::from(s);
    Box::leak(s)
});

/// Process start time.
pub static PROCESS_START_TIME: Lazy<Time> = Lazy::new(|| SystemProvider::new().now());

pub fn setup_metric_registry() -> Arc<metric::Registry> {
    let registry = Arc::new(metric::Registry::default());

    // See https://prometheus.io/docs/instrumenting/writing_clientlibs/#process-metrics
    registry
        .register_metric::<U64Gauge>(
            "process_start_time_seconds",
            "Start time of the process since unix epoch in seconds.",
        )
        .recorder(&[
            ("version", INFLUXDB3_VERSION.as_ref()),
            ("git_hash", INFLUXDB3_GIT_HASH),
            ("uuid", PROCESS_UUID.as_ref()),
        ])
        .set(PROCESS_START_TIME.timestamp() as u64);

    // Register jemalloc metrics
    #[cfg(all(not(feature = "heappy"), feature = "jemalloc_replacing_malloc"))]
    registry.register_instrument("jemalloc_metrics", crate::jemalloc::JemallocMetrics::new);

    // Register tokio metric for main runtime
    #[cfg(tokio_unstable)]
    setup_tokio_metrics(
        tokio::runtime::Handle::current().metrics(),
        "main",
        Arc::clone(&registry),
    );

    registry
}

/// String version of [`usize::MAX`].
#[allow(dead_code)]
pub static USIZE_MAX: Lazy<&'static str> = Lazy::new(|| {
    let s = usize::MAX.to_string();
    let s: Box<str> = Box::from(s);
    Box::leak(s)
});
