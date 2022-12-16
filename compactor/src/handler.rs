//! Compactor handler

use crate::{cold, compact::Compactor, hot, warm};
use async_trait::async_trait;
use data_types::{PartitionId, SkippedCompaction};
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

/// The [`CompactorHandler`] runs the compactor as a service and handles skipped compactions.
#[async_trait]
pub trait CompactorHandler: Send + Sync {
    /// Return skipped compactions from the catalog
    async fn skipped_compactions(
        &self,
    ) -> Result<Vec<SkippedCompaction>, ListSkippedCompactionsError>;

    /// Delete skipped compactions from the catalog
    async fn delete_skipped_compactions(
        &self,
        partition_id: PartitionId,
    ) -> Result<Option<SkippedCompaction>, DeleteSkippedCompactionsError>;

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

/// Implementation of the `CompactorHandler` trait
#[derive(Debug)]
pub struct CompactorHandlerImpl {
    /// Management of all data relevant to compaction
    compactor: Arc<Compactor>,

    /// A token that is used to trigger shutdown of the background worker
    shutdown: CancellationToken,

    /// Runner to check for compaction work and kick it off
    runner_handle: SharedJoinHandle,

    /// Executor, required for clean shutdown.
    exec: Arc<Executor>,
}

impl CompactorHandlerImpl {
    /// Initialize the Compactor
    pub fn new(compactor: Arc<Compactor>) -> Self {
        let shutdown = CancellationToken::new();
        let runner_handle = tokio::task::spawn(run_compactor(
            Arc::clone(&compactor),
            shutdown.child_token(),
        ));
        let runner_handle = shared_handle(runner_handle);
        info!("compactor started with config {:?}", compactor.config);

        let exec = Arc::clone(&compactor.exec);

        Self {
            compactor,
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

    /// The multiple of times that compacting warm partitions should run for every one time that
    /// compacting cold partitions runs. Set to 1 to compact warm partitions and cold partitions
    /// equally.
    pub warm_multiple: usize,

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

    /// Minimum number of rows allocated for each record batch fed into DataFusion plan
    ///
    /// We will use:
    ///
    /// ```text
    /// max(
    ///     parquet_file's row_count,
    ///     min_num_rows_allocated_per_record_batch_to_datafusion_plan
    /// )
    /// ```
    ///
    /// to estimate number of rows allocated for each record batch fed into DataFusion plan.
    pub min_num_rows_allocated_per_record_batch_to_datafusion_plan: u64,

    /// Max number of files to compact per partition
    ///
    /// Due to limit in fan-in of datafusion plan, we need to limit the number of files to compact
    /// per partition.
    pub max_num_compacting_files: usize,

    /// Max number of files to compact for a partition in which the first file and its
    /// overlaps push the file count limit over `max_num_compacting_files`.
    /// It's a special case of `max_num_compacting_files` that's higher just for the first
    /// file in a partition
    pub max_num_compacting_files_first_in_partition: usize,

    /// Minutes without any new data before a partition is considered cold
    pub minutes_without_new_writes_to_be_cold: u64,

    /// When querying for partitions with data for hot compaction, how many hours to look
    /// back for a first pass.
    pub hot_compaction_hours_threshold_1: u64,

    /// When querying for partitions with data for hot compaction, how many hours to look
    /// back for a second pass if we found nothing in the first pass.
    pub hot_compaction_hours_threshold_2: u64,

    /// Max number of partitions that can be compacted in parallel at once
    /// We use memory budget to estimate how many partitions can be compacted in parallel at once.
    /// However, we do not want to have that number too large which will cause the high usage of CPU cores
    /// and may also lead to inaccuracy of memory estimation. This number is to cap that.
    pub max_parallel_partitions: u64,

    /// Upper bound on file size to be counted as "small" for warm compaction.
    pub warm_compaction_small_size_threshold_bytes: i64,

    /// Minimum number of small files a partition must have in order for it to be selected
    /// as a candidate for warm compaction.
    pub warm_compaction_min_small_file_count: usize,
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
    let num_warm_cycles = compactor.config.warm_multiple;
    debug!(
        ?num_hot_cycles,
        ?num_warm_cycles,
        num_cold_cycles = 1,
        "start running compactor once that includes"
    );
    let mut compacted_partitions = 0;
    for i in 0..num_hot_cycles {
        debug!(?i, "start hot cycle");
        compacted_partitions += hot::compact(Arc::clone(&compactor)).await;
        if compacted_partitions == 0 {
            // No hot candidates, should move on to warm compaction
            break;
        }
    }
    for i in 0..num_warm_cycles {
        debug!(?i, "start warm cycle");
        compacted_partitions += warm::compact(Arc::clone(&compactor)).await;
        if compacted_partitions == 0 {
            // No warm candidates, should move to compact cold partitions
            break;
        }
    }

    debug!("start cold cycle");
    compacted_partitions += cold::compact(Arc::clone(&compactor), true).await;

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

#[derive(Debug, Error)]
#[allow(missing_copy_implementations, missing_docs)]
pub enum ListSkippedCompactionsError {
    #[error(transparent)]
    SkippedCompactionLookup(iox_catalog::interface::Error),
}

#[derive(Debug, Error)]
#[allow(missing_copy_implementations, missing_docs)]
pub enum DeleteSkippedCompactionsError {
    #[error(transparent)]
    SkippedCompactionDelete(iox_catalog::interface::Error),
}

#[async_trait]
impl CompactorHandler for CompactorHandlerImpl {
    async fn skipped_compactions(
        &self,
    ) -> Result<Vec<SkippedCompaction>, ListSkippedCompactionsError> {
        self.compactor
            .catalog
            .repositories()
            .await
            .partitions()
            .list_skipped_compactions()
            .await
            .map_err(ListSkippedCompactionsError::SkippedCompactionLookup)
    }

    async fn delete_skipped_compactions(
        &self,
        partition_id: PartitionId,
    ) -> Result<Option<SkippedCompaction>, DeleteSkippedCompactionsError> {
        self.compactor
            .catalog
            .repositories()
            .await
            .partitions()
            .delete_skipped_compactions(partition_id)
            .await
            .map_err(DeleteSkippedCompactionsError::SkippedCompactionDelete)
    }

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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::tests::{test_setup_with_default_budget, TestSetup};

    #[tokio::test]
    async fn list_skipped_compactions() {
        let TestSetup {
            compactor,
            table,
            shard,
            ..
        } = test_setup_with_default_budget().await;

        let compactor_handler = CompactorHandlerImpl::new(Arc::clone(&compactor));

        // no skipped compactions
        let skipped_compactions = compactor_handler.skipped_compactions().await.unwrap();
        assert!(
            skipped_compactions.is_empty(),
            "Expected no compactions, got {skipped_compactions:?}"
        );

        // insert a partition and a skipped compaction
        let partition = table.with_shard(&shard).create_partition("one").await;
        {
            let mut repos = compactor.catalog.repositories().await;
            repos
                .partitions()
                .record_skipped_compaction(
                    partition.partition.id,
                    "Not today",
                    3,
                    2,
                    4,
                    100_000,
                    100,
                )
                .await
                .unwrap()
        }

        let skipped_compactions = compactor_handler.skipped_compactions().await.unwrap();
        assert_eq!(skipped_compactions.len(), 1);
        assert_eq!(skipped_compactions[0].partition_id, partition.partition.id);
    }

    #[tokio::test]
    async fn delete_skipped_compactions() {
        let TestSetup {
            compactor,
            table,
            shard,
            ..
        } = test_setup_with_default_budget().await;

        let compactor_handler = CompactorHandlerImpl::new(Arc::clone(&compactor));

        // no skipped compactions to delete
        let partition_id_that_does_not_exist = PartitionId::new(0);
        let deleted_skipped_compaction = compactor_handler
            .delete_skipped_compactions(partition_id_that_does_not_exist)
            .await
            .unwrap();

        assert!(deleted_skipped_compaction.is_none());

        // insert a partition and a skipped compaction
        let partition = table.with_shard(&shard).create_partition("one").await;
        {
            let mut repos = compactor.catalog.repositories().await;
            repos
                .partitions()
                .record_skipped_compaction(
                    partition.partition.id,
                    "Not today",
                    3,
                    2,
                    4,
                    100_000,
                    100,
                )
                .await
                .unwrap();
        }

        let deleted_skipped_compaction = compactor_handler
            .delete_skipped_compactions(partition.partition.id)
            .await
            .unwrap()
            .expect("Should have deleted one skipped compaction");

        assert_eq!(
            deleted_skipped_compaction.partition_id,
            partition.partition.id,
        );
    }
}
