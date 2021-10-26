//! Memory profiling support using heappy
//!
//! Compiled only when the "heappy" feature is enabled

use heappy::{self, HeapReport};
use observability_deps::tracing::info;
use snafu::{ResultExt, Snafu};
use std::{thread, time};

#[derive(Debug, Snafu)]
pub enum Error {
    #[snafu(display("{}", source))]
    HeappyError { source: heappy::Error },

    #[snafu(display("{}", source))]
    JoinError { source: tokio::task::JoinError },
}

pub(crate) async fn dump_heappy_rsprof(seconds: u64, interval: i32) -> Result<HeapReport, Error> {
    // heap profiler guard is not Send so it can't be used in async code.
    let report = tokio::task::spawn_blocking(move || {
        let guard = heappy::HeapProfilerGuard::new(interval as usize)?;
        info!(
            "start allocs profiling {} seconds with interval {} bytes",
            seconds, interval
        );

        thread::sleep(time::Duration::from_secs(seconds));

        info!(
            "done allocs profiling {} seconds with interval {} bytes, computing report",
            seconds, interval
        );

        let report = guard.report();
        info!("heap allocation report computed",);
        Ok(report)
    })
    .await
    .context(JoinError)?
    .context(HeappyError)?;

    Ok(report)
}
