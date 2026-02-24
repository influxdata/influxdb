#![expect(missing_copy_implementations)]

use std::{sync::OnceLock, time::Duration};

use tikv_jemalloc_ctl::{epoch as epoch_ctl, stats};
use tokio::{sync::watch, task::JoinHandle};

use workspace_hack as _;

mod monitor;

pub use monitor::{AllocationMonitor, AllocationMonitorError};

/// A singleton handle to the jemalloc [`Refresher`] task.
///
/// Callers interested in obtaining jemalloc statistics should call
/// [`Refresher::handle()`] to obtain periodic updates.
///
/// The first reference to [`STATS`] MUST be made from within an async tokio
/// runtime because a background tokio task is spawned by the initialised
/// [`Refresher`].
pub static STATS: OnceLock<Refresher> = OnceLock::new();

/// Defines the frequency at which updated [`Stats`] are obtained and published.
///
/// Obtaining refreshed statistics from jemalloc is not free, so tuning this to
/// a short duration is discouraged due to additional overhead.
///
/// This value is intentionally not a round multiple of some scrape interval to
/// avoid sample aliasing.
pub const REFRESH_INTERVAL: Duration = Duration::from_millis(9142);

/// A snapshot of Jemalloc's internal allocation statistics.
#[derive(Debug, Default, Clone)]
pub struct Stats {
    pub active: usize,
    pub allocated: usize,
    pub metadata: usize,
    pub mapped: usize,
    pub resident: usize,
    pub retained: usize,
}

/// A handle to the singleton statistic refresh task.
///
/// Callers can obtain the latest [`Stats`] and change notifications from the
/// [`Refresher::handle()`].
///
/// Note that the inital [`Stats`] values before the first refresh is always 0.
///
/// Dropping this type stops updating the published [`Stats`].
#[derive(Debug)]
pub struct Refresher {
    rx: watch::Receiver<Stats>,
    refresh_task: JoinHandle<()>,
}

impl Refresher {
    /// Construct a new [`Stats`].
    ///
    /// Intentionally non-pub to enforce a singleton exposed via [`STATS`].
    pub fn new(tick_duration: Duration) -> Self {
        let (tx, rx) = watch::channel(Stats::default());

        Self {
            rx,

            // Spawn a background task to ask jemalloc to refresh the statistics
            // periodically, and publish the result.
            refresh_task: tokio::task::spawn(refresh(tx, tick_duration)),
        }
    }

    /// Obtain a [`watch::Receiver`] that receives the latest [`Stats`] as they
    /// are published.
    ///
    /// Statistics are refreshed every [`REFRESH_INTERVAL`].
    ///
    /// Callers SHOULD clone the [`Stats`] to reference them, rather than
    /// holding a long-lived reference to prevent blocking the publisher. See
    /// the [`watch::Receiver`] docs.
    ///
    /// ```rust
    /// # fn do_slow_thing() {}
    /// # let _guard = tokio::runtime::Runtime::new().unwrap().enter();
    /// # let REFRESH_INTERVAL = std::time::Duration::from_millis(9100);
    /// let handle = jemalloc_stats::STATS.get_or_init(|| jemalloc_stats::Refresher::new(REFRESH_INTERVAL)).handle();
    ///
    /// // Good:
    /// let stats = handle.borrow().clone();
    ///
    /// // Bad:
    /// let stats = handle.borrow();
    /// do_slow_thing();
    /// drop(stats);
    /// ```
    pub fn handle(&self) -> watch::Receiver<Stats> {
        self.rx.clone()
    }
}

impl Drop for Refresher {
    fn drop(&mut self) {
        self.refresh_task.abort()
    }
}

async fn refresh(tx: watch::Sender<Stats>, tick_duration: Duration) {
    let epoch = epoch_ctl::mib().unwrap();
    let active = stats::active::mib().unwrap();
    let allocated = stats::allocated::mib().unwrap();
    let metadata = stats::metadata::mib().unwrap();
    let mapped = stats::mapped::mib().unwrap();
    let resident = stats::resident::mib().unwrap();
    let retained = stats::retained::mib().unwrap();

    loop {
        // Have jemalloc refresh the internal statistics.
        epoch.advance().unwrap();

        // Read those statistics.
        let s = Stats {
            active: active.read().unwrap(),
            allocated: allocated.read().unwrap(),
            metadata: metadata.read().unwrap(),
            mapped: mapped.read().unwrap(),
            resident: resident.read().unwrap(),
            retained: retained.read().unwrap(),
        };

        // Make them available to all consumers.
        if tx.send(s).is_err() {
            // The Refresher type always retains a receiver handle, so if there
            // are no receivers, the Refresher has dropped and this task should
            // stop.
            return;
        }

        tokio::time::sleep(tick_duration).await;
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Assert that a non-zero amount of data is allocated and eventually
    /// reported.
    #[tokio::test]
    async fn test_stats() {
        let stats = STATS.get_or_init(|| Refresher::new(REFRESH_INTERVAL));
        let handle = stats.handle();

        tokio::time::timeout(Duration::from_secs(10), async move {
            loop {
                if handle.borrow().active > 0 {
                    // Success!
                    return;
                }

                tokio::time::sleep(Duration::from_millis(50)).await;
            }
        })
        .await
        .expect("timeout waiting to see non-zero allocator stats");
    }
}
