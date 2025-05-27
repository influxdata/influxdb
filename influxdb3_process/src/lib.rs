use std::sync::LazyLock;

use iox_time::{SystemProvider, Time, TimeProvider};

/// The process name on the local OS running `influxdb3`
pub const INFLUXDB3_PROCESS_NAME: &str = "influxdb3";

/// Package version.
pub static INFLUXDB3_VERSION: LazyLock<&'static str> =
    LazyLock::new(|| option_env!("CARGO_PKG_VERSION").unwrap_or("UNKNOWN"));

/// Build information.
pub static INFLUXDB3_BUILD: LazyLock<&'static str> =
    LazyLock::new(|| env!("INFLUXDB3_BUILD_VERSION"));

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
pub static VERSION_STRING: LazyLock<&'static str> = LazyLock::new(|| {
    let s = format!(
        "{}, revision {}",
        &INFLUXDB3_VERSION[..],
        INFLUXDB3_GIT_HASH
    );
    let s: Box<str> = Box::from(s);
    Box::leak(s)
});

/// A UUID that is unique for the process lifetime.
pub static PROCESS_UUID_STR: LazyLock<&'static str> = LazyLock::new(|| {
    let s = PROCESS_UUID.to_string();
    let s: Box<str> = Box::from(s);
    Box::leak(s)
});

/// A UUID that is unique for the process lifetime.
pub static PROCESS_UUID: LazyLock<uuid::Uuid> = LazyLock::new(uuid::Uuid::new_v4);

/// Process start time.
pub static PROCESS_START_TIME: LazyLock<Time> = LazyLock::new(|| SystemProvider::new().now());

/// String version of [`usize::MAX`].
#[allow(dead_code)]
pub static USIZE_MAX: LazyLock<&'static str> = LazyLock::new(|| {
    let s = usize::MAX.to_string();
    let s: Box<str> = Box::from(s);
    Box::leak(s)
});
