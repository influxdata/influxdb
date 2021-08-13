//! Memory profiling support using heappy
//!
//! Compiled only when the "heappy" feature is enabled

use heappy::{self, HeapReport};
use observability_deps::tracing::info;
use tokio::time::Duration;

pub(crate) async fn dump_heappy_rsprof(seconds: u64, frequency: i32) -> HeapReport {
    let guard = heappy::HeapProfilerGuard::new(frequency as usize);
    info!(
        "start heappy profiling {} seconds with frequency {} /s",
        seconds, frequency
    );

    tokio::time::sleep(Duration::from_secs(seconds)).await;

    info!(
        "done heappy  profiling {} seconds with frequency {} /s",
        seconds, frequency
    );

    guard.report()
}
