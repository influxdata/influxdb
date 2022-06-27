//! Compactor handler

use async_trait::async_trait;
use backoff::{Backoff, BackoffConfig};
use data_types::SequencerId;
use futures::{
    future::{BoxFuture, Shared},
    select, FutureExt, TryFutureExt,
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
use std::time::Duration;

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
    /// Percentage of least recent data we want to split to reduce compacting non-overlapped data
    /// Must be between 0 and 100.
    split_percentage: i64,
    /// The compactor will limit the number of simultaneous compaction jobs based on the
    /// size of the input files to be compacted. Currently this only takes into account the
    /// level 0 files, but should later also consider the level 1 files to be compacted. This
    /// number should be less than 1/10th of the available memory to ensure compactions have
    /// enough space to run.
    max_concurrent_compaction_size_bytes: i64,
    /// The compactor will compact overlapped files no matter how much large they are.
    /// For non-overlapped and contiguous files, compactor will also compact them into
    /// a larger file of max size defined by this config value.
    compaction_max_size_bytes: i64,
    /// Limit the number of files to compact into one file
    compaction_max_file_count: i64,
}

impl CompactorConfig {
    /// Initialize a valid config
    pub fn new(
        split_percentage: i64,
        max_concurrent_compaction_size_bytes: i64,
        compaction_max_size_bytes: i64,
        compaction_max_file_count: i64,
    ) -> Self {
        assert!(split_percentage > 0 && split_percentage <= 100);

        Self {
            split_percentage,
            max_concurrent_compaction_size_bytes,
            compaction_max_size_bytes,
            compaction_max_file_count,
        }
    }

    /// Percentage of least recent data we want to split to reduce compacting non-overlapped data
    pub fn split_percentage(&self) -> i64 {
        self.split_percentage
    }

    /// The compactor will limit the number of simultaneous compaction jobs based on the
    /// size of the input files to be compacted. Currently this only takes into account the
    /// level 0 files, but should later also consider the level 1 files to be compacted. This
    /// number should be less than 1/10th of the available memory to ensure compactions have
    /// enough space to run.
    pub fn max_concurrent_compaction_size_bytes(&self) -> i64 {
        self.max_concurrent_compaction_size_bytes
    }

    /// The compactor will compact overlapped files no matter how much large they are.
    /// For non-overlapped and contiguous files, compactor will also compact them into
    /// a larger file of max size defined by this config value.
    pub fn compaction_max_size_bytes(&self) -> i64 {
        self.compaction_max_size_bytes
    }

    /// Max number of files to compact at a time
    pub fn compaction_max_file_count(&self) -> i64 {
        self.compaction_max_file_count
    }
}

/// Checks for candidate partitions to compact and spawns tokio tasks to compact as many
/// as the configuration will allow. Once those are done it rechecks the catalog for the
/// next top partitions to compact.
async fn run_compactor(compactor: Arc<Compactor>, shutdown: CancellationToken) {
    while !shutdown.is_cancelled() {
        let candidates = Backoff::new(&compactor.backoff_config)
            .retry_all_errors("partitions_to_compact", || async {
                compactor.partitions_to_compact().await
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

        let mut used_size = 0;
        let max_size = compactor.config.max_concurrent_compaction_size_bytes();
        let max_desired_file_size = compactor.config.compaction_max_size_bytes();
        let max_file_count = compactor.config.compaction_max_file_count();
        let mut handles = vec![];

        for c in candidates {
            let compactor = Arc::clone(&compactor);
            let compact_and_upgrade = compactor
                .groups_to_compact_and_files_to_upgrade(
                    c.candidate.partition_id,
                    max_desired_file_size,
                    max_file_count,
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
                        used_size += c.candidate.file_size_bytes;
                        let handle = tokio::task::spawn(async move {
                            debug!(candidate=?c, "compacting candidate");
                            let res = compactor
                                .compact_partition(
                                    &c.namespace,
                                    &c.table,
                                    &c.table_schema,
                                    c.candidate.partition_id,
                                    compact_and_upgrade,
                                    max_desired_file_size,
                                )
                                .await;
                            if let Err(e) = res {
                                warn!(
                                    "compaction on partition {} failed with: {:?}",
                                    c.candidate.partition_id, e
                                );
                            }
                            debug!(candidate=?c, "compaction complete");
                        });
                        handles.push(handle);
                        if used_size > max_size {
                            debug!(%max_size, %used_size, n_compactions=%handles.len(), "reached maximum concurrent compaction size limit");
                            break;
                        }
                    }
                }
            }
        }

        let compactions_run = handles.len();
        debug!(
            ?compactions_run,
            "Number of concurrent partitions are being compacted in this cycle"
        );

        let _ = futures::future::join_all(handles).await;

        // if all candidate partitions have been compacted, wait a bit
        // before checking again, waking early if cancel arrives
        if compactions_run == n_candidates {
            select! {
                () = tokio::time::sleep(Duration::from_secs(5)).fuse() => {},
                () = shutdown.cancelled().fuse() => {}
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
