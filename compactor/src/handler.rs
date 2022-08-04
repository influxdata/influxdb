//! Compactor handler

use async_trait::async_trait;
use backoff::{Backoff, BackoffConfig};
use data_types::SequencerId;
use futures::{
    future::{BoxFuture, Shared},
    FutureExt, StreamExt, TryFutureExt,
};
use iox_catalog::interface::Catalog;
use iox_query::exec::Executor;
use iox_time::TimeProvider;
use metric::Attributes;
use observability_deps::tracing::*;
use parquet_file::storage::ParquetStorage;
use std::sync::Arc;
use thiserror::Error;
use tokio::{
    task::{JoinError, JoinHandle},
    time::Duration,
};
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

    /// Executor, required for clean shutdown.
    exec: Arc<Executor>,
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
            Arc::clone(&exec),
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
            exec,
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

    /// The compactor will limit the number of simultaneous hot partition compaction jobs based on
    /// the size of the input files to be compacted.  This number should be less than 1/10th of the
    /// available memory to ensure compactions have enough space to run.
    max_concurrent_size_bytes: u64,

    /// The compactor will limit the number of simultaneous cold partition compaction jobs based on
    /// the size of the input files to be compacted. This number should be less than 1/10th of the
    /// available memory to ensure compactions have enough space to run.
    max_cold_concurrent_size_bytes: u64,

    /// Max number of partitions per sequencer we want to compact per cycle
    max_number_partitions_per_sequencer: usize,

    /// Min number of recent ingested files a partition needs to be considered for compacting
    min_number_recent_ingested_files_per_partition: usize,

    /// A compaction operation for hot partitions will gather as many L0 files with their
    /// overlapping L1 files to compact together until the total size of input files crosses this
    /// threshold. Later compactions will pick up the remaining L0 files.
    ///
    /// A compaction operation will be limited by this or by the file count threshold, whichever is
    /// hit first.
    input_size_threshold_bytes: u64,

    /// A compaction operation for cold partitions will gather as many L0 files with their
    /// overlapping L1 files to compact together until the total size of input files crosses this
    /// threshold. Later compactions will pick up the remaining L0 files.
    cold_input_size_threshold_bytes: u64,

    /// A compaction operation will gather as many L0 files with their overlapping L1 files to
    /// compact together until the total number of L0 + L1 files crosses this threshold. Later
    /// compactions will pick up the remaining L0 files.
    ///
    /// A compaction operation will be limited by this or by the input size threshold, whichever is
    /// hit first.
    input_file_count_threshold: usize,

    /// The multiple of times that compacting hot partitions should run for every one time that
    /// compacting cold partitions runs. Set to 1 to compact hot partitions and cold partitions
    /// equally.
    hot_multiple: usize,
}

impl CompactorConfig {
    /// Initialize a valid config
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        max_desired_file_size_bytes: u64,
        percentage_max_file_size: u16,
        split_percentage: u16,
        max_concurrent_size_bytes: u64,
        max_cold_concurrent_size_bytes: u64,
        max_number_partitions_per_sequencer: usize,
        min_number_recent_ingested_files_per_partition: usize,
        input_size_threshold_bytes: u64,
        cold_input_size_threshold_bytes: u64,
        input_file_count_threshold: usize,
        hot_multiple: usize,
    ) -> Self {
        assert!(split_percentage > 0 && split_percentage <= 100);

        Self {
            max_desired_file_size_bytes,
            percentage_max_file_size,
            split_percentage,
            max_concurrent_size_bytes,
            max_cold_concurrent_size_bytes,
            max_number_partitions_per_sequencer,
            min_number_recent_ingested_files_per_partition,
            input_size_threshold_bytes,
            cold_input_size_threshold_bytes,
            input_file_count_threshold,
            hot_multiple,
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
    pub fn max_concurrent_size_bytes(&self) -> u64 {
        self.max_concurrent_size_bytes
    }

    /// Max number of partitions per sequencer we want to compact per cycle
    pub fn max_number_partitions_per_sequencer(&self) -> usize {
        self.max_number_partitions_per_sequencer
    }

    /// Min number of recent ingested files a partition needs to be considered for compacting
    pub fn min_number_recent_ingested_files_per_partition(&self) -> usize {
        self.min_number_recent_ingested_files_per_partition
    }

    /// A compaction operation for hot partitions will gather as many L0 files with their
    /// overlapping L1 files to compact together until the total size of input files crosses this
    /// threshold. Later compactions will pick up the remaining L0 files.
    ///
    /// A compaction operation will be limited by this or by the file count threshold, whichever is
    /// hit first.
    pub fn input_size_threshold_bytes(&self) -> u64 {
        self.input_size_threshold_bytes
    }

    /// A compaction operation for cold partitions will gather as many L0 files with their
    /// overlapping L1 files to compact together until the total size of input files crosses this
    /// threshold. Later compactions will pick up the remaining L0 files.
    pub fn cold_input_size_threshold_bytes(&self) -> u64 {
        self.cold_input_size_threshold_bytes
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

/// How long to pause before checking for more work again if there was
/// no work to do
const PAUSE_BETWEEN_NO_WORK: Duration = Duration::from_secs(1);

/// Checks for candidate partitions to compact and spawns tokio tasks to compact as many
/// as the configuration will allow. Once those are done it rechecks the catalog for the
/// next top partitions to compact.
async fn run_compactor(compactor: Arc<Compactor>, shutdown: CancellationToken) {
    while !shutdown.is_cancelled() {
        debug!("compactor main loop tick.");

        for _ in 0..compactor.config.hot_multiple {
            compact_hot_partitions(Arc::clone(&compactor)).await;
        }
        compact_cold_partitions(Arc::clone(&compactor)).await;
    }
}

async fn compact_hot_partitions(compactor: Arc<Compactor>) {
    // Select hot partition candidates
    let hot_attributes = Attributes::from(&[("partition_type", "hot")]);
    let start_time = compactor.time_provider.now();
    let candidates = Backoff::new(&compactor.backoff_config)
        .retry_all_errors("hot_partitions_to_compact", || async {
            compactor
                .hot_partitions_to_compact(
                    compactor.config.max_number_partitions_per_sequencer(),
                    compactor
                        .config
                        .min_number_recent_ingested_files_per_partition(),
                )
                .await
        })
        .await
        .expect("retry forever");
    if let Some(delta) = compactor
        .time_provider
        .now()
        .checked_duration_since(start_time)
    {
        let duration = compactor
            .candidate_selection_duration
            .recorder(hot_attributes.clone());
        duration.record(delta);
    }

    // Add other compaction-needed info into selected partitions
    let start_time = compactor.time_provider.now();
    let candidates = Backoff::new(&compactor.backoff_config)
        .retry_all_errors("add_info_to_partitions", || async {
            compactor.add_info_to_partitions(&candidates).await
        })
        .await
        .expect("retry forever");
    if let Some(delta) = compactor
        .time_provider
        .now()
        .checked_duration_since(start_time)
    {
        let duration = compactor
            .partitions_extra_info_reading_duration
            .recorder(hot_attributes.clone());
        duration.record(delta);
    }

    let n_candidates = candidates.len();
    if n_candidates == 0 {
        debug!("no hot compaction candidates found");
        // sleep for a second to avoid a hot busy loop when the catalog is polled
        tokio::time::sleep(PAUSE_BETWEEN_NO_WORK).await;
        return;
    } else {
        debug!(n_candidates, "found hot compaction candidates");
    }

    let start_time = compactor.time_provider.now();

    // Repeat compacting n partitions in parallel until all candidates are compacted.
    // Concurrency level calculation (this is estimated from previous experiments. The actual
    // resource management will be more complicated and a future feature):
    //   . Each `compact partititon` takes max of this much memory input_size_threshold_bytes
    //   . We have this memory budget: max_concurrent_size_bytes
    // --> num_parallel_partitions = max_concurrent_size_bytes/
    //     input_size_threshold_bytes
    let num_parallel_partitions = (compactor.config.max_concurrent_size_bytes
        / compactor.config.input_size_threshold_bytes) as usize;

    futures::stream::iter(candidates)
        .map(|p| {
            // run compaction in its own task
            let comp = Arc::clone(&compactor);
            tokio::task::spawn(async move {
                let partition_id = p.candidate.partition_id;
                let compaction_result = crate::compact_hot_partition(&comp, p).await;

                match compaction_result {
                    Err(e) => {
                        warn!(?e, ?partition_id, "hot compaction failed");
                    }
                    Ok(_) => {
                        debug!(?partition_id, "hot compaction complete");
                    }
                };
            })
        })
        // Assume we have enough resources to run
        // num_parallel_partitions compactions in parallel
        .buffer_unordered(num_parallel_partitions)
        // report any JoinErrors (aka task panics)
        .map(|join_result| {
            if let Err(e) = join_result {
                warn!(?e, "hot compaction task failed");
            }
            Ok(())
        })
        // Errors are reported during execution, so ignore results here
        // https://stackoverflow.com/questions/64443085/how-to-run-stream-to-completion-in-rust-using-combinators-other-than-for-each
        .forward(futures::sink::drain())
        .await
        .ok();

    // Done compacting all candidates in the cycle, record its time
    if let Some(delta) = compactor
        .time_provider
        .now()
        .checked_duration_since(start_time)
    {
        let duration = compactor.compaction_cycle_duration.recorder(hot_attributes);
        duration.record(delta);
    }
}

async fn compact_cold_partitions(compactor: Arc<Compactor>) {
    let cold_attributes = Attributes::from(&[("partition_type", "cold")]);
    // Select cold partition candidates
    let start_time = compactor.time_provider.now();
    let candidates = Backoff::new(&compactor.backoff_config)
        .retry_all_errors("cold_partitions_to_compact", || async {
            compactor
                .cold_partitions_to_compact(compactor.config.max_number_partitions_per_sequencer())
                .await
        })
        .await
        .expect("retry forever");
    if let Some(delta) = compactor
        .time_provider
        .now()
        .checked_duration_since(start_time)
    {
        let duration = compactor
            .candidate_selection_duration
            .recorder(cold_attributes.clone());
        duration.record(delta);
    }

    // Add other compaction-needed info into selected partitions
    let start_time = compactor.time_provider.now();
    let candidates = Backoff::new(&compactor.backoff_config)
        .retry_all_errors("add_info_to_partitions", || async {
            compactor.add_info_to_partitions(&candidates).await
        })
        .await
        .expect("retry forever");
    if let Some(delta) = compactor
        .time_provider
        .now()
        .checked_duration_since(start_time)
    {
        let duration = compactor
            .partitions_extra_info_reading_duration
            .recorder(cold_attributes.clone());
        duration.record(delta);
    }

    let n_candidates = candidates.len();
    if n_candidates == 0 {
        debug!("no cold compaction candidates found");
        return;
    } else {
        debug!(n_candidates, "found cold compaction candidates");
    }

    let start_time = compactor.time_provider.now();

    // Repeat compacting n cold partitions in parallel until all candidates are compacted.
    // Concurrency level calculation (this is estimated from previous experiments. The actual
    // resource management will be more complicated and a future feature):
    //
    //   . Each `compact partititon` takes max of this much memory cold_input_size_threshold_bytes
    //   . We have this memory budget: max_cold_concurrent_size_bytes
    // --> num_parallel_partitions = max_cold_concurrent_size_bytes/
    //     cold_input_size_threshold_bytes
    let num_parallel_partitions = (compactor.config.max_cold_concurrent_size_bytes
        / compactor.config.cold_input_size_threshold_bytes)
        as usize;

    futures::stream::iter(candidates)
        .map(|p| {
            // run compaction in its own task
            let comp = Arc::clone(&compactor);
            tokio::task::spawn(async move {
                let partition_id = p.candidate.partition_id;
                let compaction_result = crate::compact_cold_partition(&comp, p).await;

                match compaction_result {
                    Err(e) => {
                        warn!(?e, ?partition_id, "cold compaction failed");
                    }
                    Ok(_) => {
                        debug!(?partition_id, "cold compaction complete");
                    }
                };
            })
        })
        // Assume we have enough resources to run
        // num_parallel_partitions compactions in parallel
        .buffer_unordered(num_parallel_partitions)
        // report any JoinErrors (aka task panics)
        .map(|join_result| {
            if let Err(e) = join_result {
                warn!(?e, "cold compaction task failed");
            }
            Ok(())
        })
        // Errors are reported during execution, so ignore results here
        // https://stackoverflow.com/questions/64443085/how-to-run-stream-to-completion-in-rust-using-combinators-other-than-for-each
        .forward(futures::sink::drain())
        .await
        .ok();

    // Done compacting all candidates in the cycle, record its time
    if let Some(delta) = compactor
        .time_provider
        .now()
        .checked_duration_since(start_time)
    {
        let duration = compactor
            .compaction_cycle_duration
            .recorder(cold_attributes);
        duration.record(delta);
    }
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
