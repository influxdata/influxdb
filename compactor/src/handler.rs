//! Compactor handler

use async_trait::async_trait;
use backoff::{Backoff, BackoffConfig};
use data_types::SequencerId;
use futures::{
    future::{BoxFuture, Shared},
    FutureExt, TryFutureExt,
};
use iox_catalog::interface::Catalog;
use iox_query::exec::Executor;
use iox_time::TimeProvider;
use observability_deps::tracing::*;
use parquet_file::storage::ParquetStorage;
use std::sync::Arc;
use thiserror::Error;
use tokio::task::{JoinError, JoinHandle};
use tokio_util::sync::CancellationToken;

use crate::compact::Compactor;

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
}

impl CompactorHandlerImpl {
    /// Initialize the Compactor
    pub fn new(
        sequencers: Vec<SequencerId>,
        catalog: Arc<dyn Catalog>,
        store: ParquetStorage,
        exec: Arc<Executor>,
        time_provider: Arc<dyn TimeProvider>,
        registry: Arc<metric::Registry>,
        config: CompactorConfig,
    ) -> Self {
        let compactor_data = Arc::new(Compactor::new(
            sequencers,
            catalog,
            store,
            exec,
            time_provider,
            BackoffConfig::default(),
            config,
            registry,
        ));

        let shutdown = CancellationToken::new();
        let runner_handle = tokio::task::spawn(run_compactor(
            Arc::clone(&compactor_data),
            shutdown.child_token(),
        ));
        let runner_handle = shared_handle(runner_handle);
        info!("compactor started with config {:?}", config);

        Self {
            compactor_data,
            shutdown,
            runner_handle,
        }
    }
}

/// The configuration options for the compactor.
#[derive(Debug, Clone, Copy)]
pub struct CompactorConfig {
    /// Desired max size of compacted parquet files
    /// It is a target desired value than a guarantee
    max_desired_file_size_bytes: u64,

    /// Percentage of desired max file size.
    /// If the estimated compacted result is too small, no need to split it.
    /// This percentage is to determine how small it is:
    ///    < percentage_max_file_size * max_desired_file_size_bytes:
    /// This value must be between (0, 100)
    percentage_max_file_size: u16,

    /// Split file percentage
    /// If the estimated compacted result is neither too small nor too large, it will be split
    /// into 2 files determined by this percentage.
    ///    . Too small means: < percentage_max_file_size * max_desired_file_size_bytes
    ///    . Too large means: > max_desired_file_size_bytes
    ///    . Any size in the middle will be considered neither too small nor too large
    /// This value must be between (0, 100)
    split_percentage: u16,

    /// The compactor will limit the number of simultaneous compaction jobs based on the
    /// size of the input files to be compacted.  This number should be less than 1/10th
    /// of the available memory to ensure compactions have
    /// enough space to run.
    max_concurrent_compaction_size_bytes: u64,

    /// Max number of partitions per sequencer we want to compact per cycle
    max_number_partitions_per_sequencer: usize,

    /// Min number of recent ingested files a partition needs to be considered for compacting
    min_number_recent_ingested_files_per_partition: usize,

    /// A compaction operation will gather as many L0 files with their overlapping L1 files to
    /// compact together until the total size of input files crosses this threshold. Later
    /// compactions will pick up the remaining L0 files.
    ///
    /// A compaction operation will be limited by this or by the file count threshold, whichever is
    /// hit first.
    input_size_threshold_bytes: u64,

    /// A compaction operation will gather as many L0 files with their overlapping L1 files to
    /// compact together until the total number of L0 + L1 files crosses this threshold. Later
    /// compactions will pick up the remaining L0 files.
    ///
    /// A compaction operation will be limited by this or by the input size threshold, whichever is
    /// hit first.
    input_file_count_threshold: usize,
}

impl CompactorConfig {
    /// Initialize a valid config
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        max_desired_file_size_bytes: u64,
        percentage_max_file_size: u16,
        split_percentage: u16,
        max_concurrent_compaction_size_bytes: u64,
        max_number_partitions_per_sequencer: usize,
        min_number_recent_ingested_files_per_partition: usize,
        input_size_threshold_bytes: u64,
        input_file_count_threshold: usize,
    ) -> Self {
        assert!(split_percentage > 0 && split_percentage <= 100);

        Self {
            max_desired_file_size_bytes,
            percentage_max_file_size,
            split_percentage,
            max_concurrent_compaction_size_bytes,
            max_number_partitions_per_sequencer,
            min_number_recent_ingested_files_per_partition,
            input_size_threshold_bytes,
            input_file_count_threshold,
        }
    }

    /// Desired max file of a compacted file
    pub fn max_desired_file_size_bytes(&self) -> u64 {
        self.max_desired_file_size_bytes
    }

    /// Percentage of desired max file size to determine a size is too small
    pub fn percentage_max_file_size(&self) -> u16 {
        self.percentage_max_file_size
    }

    /// Percentage of least recent data we want to split to reduce compacting non-overlapped data
    pub fn split_percentage(&self) -> u16 {
        self.split_percentage
    }

    /// The compactor will limit the number of simultaneous compaction jobs based on the
    /// size of the input files to be compacted. Currently this only takes into account the
    /// level 0 files, but should later also consider the level 1 files to be compacted. This
    /// number should be less than 1/10th of the available memory to ensure compactions have
    /// enough space to run.
    pub fn max_concurrent_compaction_size_bytes(&self) -> u64 {
        self.max_concurrent_compaction_size_bytes
    }

    /// Max number of partitions per sequencer we want to compact per cycle
    pub fn max_number_partitions_per_sequencer(&self) -> usize {
        self.max_number_partitions_per_sequencer
    }

    /// Min number of recent ingested files a partition needs to be considered for compacting
    pub fn min_number_recent_ingested_files_per_partition(&self) -> usize {
        self.min_number_recent_ingested_files_per_partition
    }

    /// A compaction operation will gather as many L0 files with their overlapping L1 files to
    /// compact together until the total size of input files crosses this threshold. Later
    /// compactions will pick up the remaining L0 files.
    ///
    /// A compaction operation will be limited by this or by the file count threshold, whichever is
    /// hit first.
    pub fn input_size_threshold_bytes(&self) -> u64 {
        self.input_size_threshold_bytes
    }

    /// A compaction operation will gather as many L0 files with their overlapping L1 files to
    /// compact together until the total number of L0 + L1 files crosses this threshold. Later
    /// compactions will pick up the remaining L0 files.
    ///
    /// A compaction operation will be limited by this or by the input size threshold, whichever is
    /// hit first.
    pub fn input_file_count_threshold(&self) -> usize {
        self.input_file_count_threshold
    }
}

/// Checks for candidate partitions to compact and spawns tokio tasks to compact as many
/// as the configuration will allow. Once those are done it rechecks the catalog for the
/// next top partitions to compact.
async fn run_compactor(compactor: Arc<Compactor>, shutdown: CancellationToken) {
    while !shutdown.is_cancelled() {
        let candidates = Backoff::new(&compactor.backoff_config)
            .retry_all_errors("partitions_to_compact", || async {
                compactor
                    .partitions_to_compact(
                        compactor.config.max_number_partitions_per_sequencer(),
                        compactor
                            .config
                            .min_number_recent_ingested_files_per_partition(),
                    )
                    .await
            })
            .await
            .expect("retry forever");
        let candidates = Backoff::new(&compactor.backoff_config)
            .retry_all_errors("partitions_to_compact", || async {
                compactor.add_info_to_partitions(&candidates).await
            })
            .await
            .expect("retry forever");

        let n_candidates = candidates.len();
        debug!(n_candidates, "found compaction candidates");

        // Serially compact all candidates
        // TODO: we will parallelize this when everything runs smoothly in serial
        for c in candidates {
            let compactor = Arc::clone(&compactor);
            let compact_and_upgrade = compactor
                .groups_to_compact_and_files_to_upgrade(
                    c.candidate.partition_id,
                    &c.namespace.name,
                    &c.table.name,
                )
                .await;

            match compact_and_upgrade {
                Err(e) => {
                    warn!(
                        "groups file to compact and upgrade on partition {} failed with: {:?}",
                        c.candidate.partition_id, e
                    );
                }
                Ok(compact_and_upgrade) => {
                    if compact_and_upgrade.compactable() {
                        let res = compactor
                            .compact_partition(
                                &c.namespace,
                                &c.table,
                                &c.table_schema,
                                c.candidate.partition_id,
                                compact_and_upgrade,
                            )
                            .await;
                        if let Err(e) = res {
                            warn!(
                                "compaction on partition {} failed with: {:?}",
                                c.candidate.partition_id, e
                            );
                        }
                        debug!(candidate=?c, "compaction complete");
                    } else {
                        // All candidates should be compactable (have files to compact and/or
                        // upgrade).
                        // Reaching here means we do not choose the right candidates and
                        // it would be a waste of time to repeat this cycle
                        warn!(
                            "The candidate partition {} has no files to be either compacted or \
                                upgraded",
                            c.candidate.partition_id
                        );
                    }
                }
            }
        }
    }
}

#[async_trait]
impl CompactorHandler for CompactorHandlerImpl {
    async fn join(&self) {
        self.runner_handle
            .clone()
            .await
            .expect("compactor task failed");
    }

    fn shutdown(&self) {
        self.shutdown.cancel();
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
