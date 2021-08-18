//! Memory profiling support using heappy
//!
//! Compiled only when the "heappy" feature is enabled

use heappy::{self, HeapReport};
use observability_deps::tracing::info;
use tokio::time::Duration;

pub(crate) async fn dump_heappy_rsprof(seconds: u64, interval: i32) -> HeapReport {
    let guard = heappy::HeapProfilerGuard::new(interval as usize);
    info!(
        "start allocs profiling {} seconds with interval {} bytes",
        seconds, interval
    );

    tokio::time::sleep(Duration::from_secs(seconds)).await;

    info!(
        "done allocs profiling {} seconds with interval {} bytes",
        seconds, interval
    );

    guard.report()
}
