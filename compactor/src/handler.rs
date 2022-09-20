//! Compactor handler

use crate::{compact::Compactor, hot};
use async_trait::async_trait;
use futures::{
    future::{BoxFuture, Shared},
    FutureExt, TryFutureExt,
};
use iox_query::exec::Executor;
use observability_deps::tracing::*;
use std::sync::Arc;
use thiserror::Error;
use tokio::{
    task::{JoinError, JoinHandle},
    time::Duration,
};
use tokio_util::sync::CancellationToken;

#[derive(Debug, Error)]
#[allow(missing_copy_implementations, missing_docs)]
pub enum Error {}

/// The [`CompactorHandler`] does nothing at this point
#[async_trait]
pub trait CompactorHandler: Send + Sync {
    /// Wait until the handler finished  to shutdown.
    ///
    /// Use [`shutdown`](Self::shutdown) to trigger a shutdown.
    async fn join(&self);

    /// Shut down background workers.
    fn shutdown(&self);
}

/// A [`JoinHandle`] that can be cloned
type SharedJoinHandle = Shared<BoxFuture<'static, Result<(), Arc<JoinError>>>>;

/// Convert a [`JoinHandle`] into a [`SharedJoinHandle`].
fn shared_handle(handle: JoinHandle<()>) -> SharedJoinHandle {
    handle.map_err(Arc::new).boxed().shared()
}

/// Implementation of the `CompactorHandler` trait (that currently does nothing)
#[derive(Debug)]
pub struct CompactorHandlerImpl {
    /// Data to compact
    #[allow(dead_code)]
    compactor_data: Arc<Compactor>,

    /// A token that is used to trigger shutdown of the background worker
    shutdown: CancellationToken,

    /// Runner to check for compaction work and kick it off
    runner_handle: SharedJoinHandle,

    /// Executor, required for clean shutdown.
    exec: Arc<Executor>,
}

impl CompactorHandlerImpl {
    /// Initialize the Compactor
    pub fn new(compactor: Compactor) -> Self {
        let compactor_data = Arc::new(compactor);

        let shutdown = CancellationToken::new();
        let runner_handle = tokio::task::spawn(run_compactor(
            Arc::clone(&compactor_data),
            shutdown.child_token(),
        ));
        let runner_handle = shared_handle(runner_handle);
        info!("compactor started with config {:?}", compactor_data.config);

        let exec = Arc::clone(&compactor_data.exec);

        Self {
            compactor_data,
            shutdown,
            runner_handle,
            exec,
        }
    }
}

/// The configuration options for the compactor.
#[derive(Debug, Clone, Copy)]
pub struct CompactorConfig {
    /// Desired max size of compacted parquet files
    /// It is a target desired value than a guarantee
    pub max_desired_file_size_bytes: u64,

    /// Percentage of desired max file size.
    /// If the estimated compacted result is too small, no need to split it.
    /// This percentage is to determine how small it is:
    ///    < percentage_max_file_size * max_desired_file_size_bytes:
    /// This value must be between (0, 100)
    pub percentage_max_file_size: u16,

    /// Split file percentage
    /// If the estimated compacted result is neither too small nor too large, it will be split
    /// into 2 files determined by this percentage.
    ///    . Too small means: < percentage_max_file_size * max_desired_file_size_bytes
    ///    . Too large means: > max_desired_file_size_bytes
    ///    . Any size in the middle will be considered neither too small nor too large
    /// This value must be between (0, 100)
    pub split_percentage: u16,

    /// Max number of partitions per shard we want to compact per cycle
    pub max_number_partitions_per_shard: usize,

    /// Min number of recent ingested files a partition needs to be considered for compacting
    pub min_number_recent_ingested_files_per_partition: usize,

    /// The multiple of times that compacting hot partitions should run for every one time that
    /// compacting cold partitions runs. Set to 1 to compact hot partitions and cold partitions
    /// equally.
    pub hot_multiple: usize,

    /// The memory budget assigned to this compactor.
    ///
    /// For each partition candidate, we will estimate the memory needed to compact each file
    /// and only add more files if their needed estimated memory is below this memory budget.
    /// Since we must compact L1 files that overlapped with L0 files, if their total estimated
    /// memory does not allow us to compact a part of a partition at all, we will not compact
    /// it and will log the partition and its related information in a table in our catalog for
    /// further diagnosis of the issue.
    ///
    /// The number of candidates compacted concurrently is also decided using this estimation and
    /// budget.
    pub memory_budget_bytes: u64,
}

/// How long to pause before checking for more work again if there was
/// no work to do
const PAUSE_BETWEEN_NO_WORK: Duration = Duration::from_secs(1);

/// Checks for candidate partitions to compact and spawns tokio tasks to compact as many
/// as the configuration will allow. Once those are done it rechecks the catalog for the
/// next top partitions to compact.
async fn run_compactor(compactor: Arc<Compactor>, shutdown: CancellationToken) {
    while !shutdown.is_cancelled() {
        debug!("compactor main loop tick.");

        run_compactor_once(Arc::clone(&compactor)).await;
    }
}

/// Checks for candidate partitions to compact and spawns tokio tasks to compact as many
/// as the configuration will allow.
pub async fn run_compactor_once(compactor: Arc<Compactor>) {
    let num_hot_cycles = compactor.config.hot_multiple;
    debug!(
        ?num_hot_cycles,
        num_cold_cycles = 1,
        "start running compactor once that includes"
    );
    let mut compacted_partitions = 0;
    for i in 0..num_hot_cycles {
        debug!(?i, "start hot cycle");
        compacted_partitions += hot::compact(Arc::clone(&compactor)).await;
        if compacted_partitions == 0 {
            // No hot candidates, should move to compact cold partitions
            break;
        }
    }

    // Temorarily disable cold compaction again. This time we know the first step like hot comapction hits OOM
    // See https://github.com/influxdata/conductor/issues/1167#issuecomment-1252832794
    // debug!("start cold cycle");
    // compacted_partitions +=
    //     cold::compact(Arc::clone(&compactor), false /* not do full compact */).await;

    if compacted_partitions == 0 {
        // sleep for a second to avoid a busy loop when the catalog is polled
        tokio::time::sleep(PAUSE_BETWEEN_NO_WORK).await;
    }
    debug!(
        ?num_hot_cycles,
        num_cold_cycles = 1,
        "complete running compactor once that includes"
    );
}

#[async_trait]
impl CompactorHandler for CompactorHandlerImpl {
    async fn join(&self) {
        self.runner_handle
            .clone()
            .await
            .expect("compactor task failed");
        self.exec.join().await;
    }

    fn shutdown(&self) {
        self.shutdown.cancel();
        self.exec.shutdown();
    }
}

impl Drop for CompactorHandlerImpl {
    fn drop(&mut self) {
        if !self.shutdown.is_cancelled() {
            warn!("CompactorHandlerImpl dropped without calling shutdown()");
            self.shutdown.cancel();
        }
    }
}
