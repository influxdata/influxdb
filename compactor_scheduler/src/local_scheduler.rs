//! Internals used by [`LocalScheduler`].
pub(crate) mod catalog_commit;
pub(crate) mod combos;
pub(crate) mod id_only_partition_filter;
pub(crate) mod partition_done_sink;
pub(crate) mod partitions_source;
pub(crate) mod partitions_source_config;
pub(crate) mod shard_config;

use std::{sync::Arc, time::Duration};

use async_trait::async_trait;
use backoff::BackoffConfig;
use iox_catalog::interface::Catalog;
use iox_time::TimeProvider;
use observability_deps::tracing::{info, warn};

use crate::{
    commit::{logging::LoggingCommitWrapper, metrics::MetricsCommitWrapper},
    Commit, CommitUpdate, CommitWrapper, CompactionJob, CompactionJobStatus,
    CompactionJobStatusResult, CompactionJobStatusVariant, MockCommit, MockPartitionsSource,
    PartitionsSource, PartitionsSourceConfig, Scheduler, ShardConfig, SkipReason,
};

use self::{
    catalog_commit::CatalogCommit,
    combos::{throttle_partition::throttle_partition, unique_partitions::unique_partitions},
    id_only_partition_filter::{
        and::AndIdOnlyPartitionFilter, shard::ShardPartitionFilter, IdOnlyPartitionFilter,
    },
    partition_done_sink::{
        catalog::CatalogPartitionDoneSink, mock::MockPartitionDoneSink, PartitionDoneSink,
    },
    partitions_source::{
        catalog_all::CatalogAllPartitionsSource,
        catalog_to_compact::CatalogToCompactPartitionsSource,
        filter::FilterPartitionsSourceWrapper,
    },
};

/// Configuration specific to the local scheduler.
#[derive(Debug, Default, Clone)]
pub struct LocalSchedulerConfig {
    /// Optionally wrap the `Commit` instance
    ///
    /// This is mostly used for testing
    pub commit_wrapper: Option<Arc<dyn CommitWrapper>>,
    /// The partitions source config used by the local sceduler.
    pub partitions_source_config: PartitionsSourceConfig,
    /// The shard config used by the local sceduler.
    pub shard_config: Option<ShardConfig>,
}

/// Implementation of the scheduler for local (per compactor) scheduling.
#[derive(Debug)]
pub(crate) struct LocalScheduler {
    /// Commits changes (i.e. deletion and creation) to the catalog
    pub(crate) commit: Arc<dyn Commit>,
    /// The partitions source to use for scheduling.
    partitions_source: Arc<dyn PartitionsSource>,
    /// The actions to take when a partition is done.
    ///
    /// Includes partition (PartitionId) tracking of uniqueness and throttling.
    partition_done_sink: Arc<dyn PartitionDoneSink>,
    /// The shard config used for generating the PartitionsSource.
    shard_config: Option<ShardConfig>,
}

impl LocalScheduler {
    /// Create a new [`LocalScheduler`].
    pub(crate) fn new(
        config: LocalSchedulerConfig,
        backoff_config: BackoffConfig,
        catalog: Arc<dyn Catalog>,
        time_provider: Arc<dyn TimeProvider>,
        metrics: Arc<metric::Registry>,
        shadow_mode: bool,
    ) -> Self {
        let commit = Self::build_commit(
            config.clone(),
            backoff_config.clone(),
            Arc::clone(&catalog),
            metrics,
            shadow_mode,
        );

        let partitions_source = Self::build_partitions_source(
            config.clone(),
            backoff_config.clone(),
            Arc::clone(&catalog),
            Arc::clone(&time_provider),
        );

        let (partitions_source, commit, partition_done_sink) = Self::build_partition_done_sink(
            partitions_source,
            commit,
            backoff_config,
            catalog,
            time_provider,
            shadow_mode,
        );

        Self {
            commit,
            partitions_source,
            partition_done_sink,
            shard_config: config.shard_config,
        }
    }

    fn build_partitions_source(
        config: LocalSchedulerConfig,
        backoff_config: BackoffConfig,
        catalog: Arc<dyn Catalog>,
        time_provider: Arc<dyn TimeProvider>,
    ) -> Arc<dyn PartitionsSource> {
        let shard_config = config.shard_config;
        let partitions_source: Arc<dyn PartitionsSource> = match &config.partitions_source_config {
            PartitionsSourceConfig::CatalogRecentWrites { threshold } => {
                Arc::new(CatalogToCompactPartitionsSource::new(
                    backoff_config,
                    Arc::clone(&catalog),
                    *threshold,
                    None, // Recent writes is `threshold` ago to now
                    time_provider,
                ))
            }
            PartitionsSourceConfig::CatalogAll => Arc::new(CatalogAllPartitionsSource::new(
                backoff_config,
                Arc::clone(&catalog),
            )),
            PartitionsSourceConfig::Fixed(ids) => {
                Arc::new(MockPartitionsSource::new(ids.iter().cloned().collect()))
            }
        };

        let mut id_only_partition_filters: Vec<Arc<dyn IdOnlyPartitionFilter>> = vec![];
        if let Some(shard_config) = &shard_config {
            // add shard filter before performing any catalog IO
            info!(
                "starting compactor {} of {}",
                shard_config.shard_id, shard_config.n_shards
            );
            id_only_partition_filters.push(Arc::new(ShardPartitionFilter::new(
                shard_config.n_shards,
                shard_config.shard_id,
            )));
        }
        Arc::new(FilterPartitionsSourceWrapper::new(
            AndIdOnlyPartitionFilter::new(id_only_partition_filters),
            partitions_source,
        ))
    }

    fn build_partition_done_sink(
        partitions_source: Arc<dyn PartitionsSource>,
        commit: Arc<dyn Commit>,
        backoff_config: BackoffConfig,
        catalog: Arc<dyn Catalog>,
        time_provider: Arc<dyn TimeProvider>,
        shadow_mode: bool,
    ) -> (
        Arc<dyn PartitionsSource>,
        Arc<dyn Commit>,
        Arc<dyn PartitionDoneSink>,
    ) {
        let partition_done_sink: Arc<dyn PartitionDoneSink> = if shadow_mode {
            Arc::new(MockPartitionDoneSink::new())
        } else {
            Arc::new(CatalogPartitionDoneSink::new(
                backoff_config,
                Arc::clone(&catalog),
            ))
        };

        let (partitions_source, partition_done_sink) =
            unique_partitions(partitions_source, partition_done_sink, 1);

        let (partitions_source, commit, partition_done_sink) = throttle_partition(
            partitions_source,
            commit,
            partition_done_sink,
            Arc::clone(&time_provider),
            Duration::from_secs(60),
            1,
        );

        (
            Arc::new(partitions_source),
            Arc::new(commit),
            Arc::new(partition_done_sink),
        )
    }

    fn build_commit(
        config: LocalSchedulerConfig,
        backoff_config: BackoffConfig,
        catalog: Arc<dyn Catalog>,
        metrics_registry: Arc<metric::Registry>,
        shadow_mode: bool,
    ) -> Arc<dyn Commit> {
        let commit: Arc<dyn Commit> = if shadow_mode {
            Arc::new(MockCommit::new())
        } else {
            Arc::new(CatalogCommit::new(backoff_config, Arc::clone(&catalog)))
        };

        let commit = if let Some(commit_wrapper) = &config.commit_wrapper {
            commit_wrapper.wrap(commit)
        } else {
            commit
        };

        Arc::new(LoggingCommitWrapper::new(MetricsCommitWrapper::new(
            commit,
            &metrics_registry,
        )))
    }
}

#[async_trait]
impl Scheduler for LocalScheduler {
    async fn get_jobs(&self) -> Vec<CompactionJob> {
        self.partitions_source
            .fetch()
            .await
            .into_iter()
            .map(CompactionJob::new)
            .collect()
    }

    async fn job_status(
        &self,
        job_status: CompactionJobStatus,
    ) -> Result<CompactionJobStatusResult, Box<dyn std::error::Error>> {
        match job_status.status {
            CompactionJobStatusVariant::Update(commit_update) => {
                let CommitUpdate {
                    partition_id,
                    delete,
                    upgrade,
                    target_level,
                    create,
                } = commit_update;

                let result = self
                    .commit
                    .commit(partition_id, &delete, &upgrade, &create, target_level)
                    .await;

                // verify create commit counts
                assert_eq!(result.len(), create.len());

                Ok(CompactionJobStatusResult::UpdatedParquetFiles(result))
            }
            CompactionJobStatusVariant::RequestToSkip(SkipReason::CompactionError(msg)) => {
                self.partition_done_sink
                    .record(job_status.job.partition_id, Err(msg.into()))
                    .await;

                Ok(CompactionJobStatusResult::Ack)
            }
            CompactionJobStatusVariant::Error(error_kind) => {
                warn!("Error processing job: {:?}: {}", job_status.job, error_kind);
                Ok(CompactionJobStatusResult::Ack)
            }
            CompactionJobStatusVariant::Complete => {
                // TODO: once uuid is handled properly, we can track the job completion
                Ok(CompactionJobStatusResult::Ack)
            }
        }
    }
}

impl std::fmt::Display for LocalScheduler {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match &self.shard_config {
            None => write!(f, "local_compaction_scheduler"),
            Some(shard_config) => write!(f, "local_compaction_scheduler({shard_config})",),
        }
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashSet;

    use data_types::{ColumnType, PartitionId};
    use iox_tests::{ParquetFileBuilder, TestCatalog, TestParquetFile, TestParquetFileBuilder};
    use iox_time::{MockProvider, Time};

    use super::*;

    #[test]
    fn test_display() {
        let scheduler = LocalScheduler::new(
            LocalSchedulerConfig::default(),
            BackoffConfig::default(),
            TestCatalog::new().catalog(),
            Arc::new(MockProvider::new(Time::MIN)),
            Arc::new(metric::Registry::default()),
            false,
        );

        assert_eq!(scheduler.to_string(), "local_compaction_scheduler",);
    }

    #[test]
    fn test_display_with_sharding() {
        let shard_config = Some(ShardConfig {
            n_shards: 2,
            shard_id: 1,
        });

        let config = LocalSchedulerConfig {
            commit_wrapper: None,
            partitions_source_config: PartitionsSourceConfig::default(),
            shard_config,
        };

        let scheduler = LocalScheduler::new(
            config,
            BackoffConfig::default(),
            TestCatalog::new().catalog(),
            Arc::new(MockProvider::new(Time::MIN)),
            Arc::new(metric::Registry::default()),
            false,
        );

        assert_eq!(
            scheduler.to_string(),
            "local_compaction_scheduler(shard_cfg(n_shards=2,shard_id=1))",
        );
    }

    async fn create_scheduler_with_partitions() -> (LocalScheduler, TestParquetFile, TestParquetFile)
    {
        let catalog = TestCatalog::new();
        let ns = catalog.create_namespace_with_retention("ns", None).await;
        let table = ns.create_table("table1").await;
        table.create_column("time", ColumnType::Time).await;
        table.create_column("load", ColumnType::F64).await;

        let partition1 = table.create_partition("k").await;
        let partition2 = table.create_partition("k").await;
        let partition_ids = vec![partition1.partition.id, partition2.partition.id];

        // two files on partition1, to be replaced by one compacted file
        let file_builder = TestParquetFileBuilder::default().with_line_protocol("table1 load=1 11");
        let file1_1 = partition1.create_parquet_file(file_builder.clone()).await;
        let file1_2 = partition1.create_parquet_file(file_builder).await;

        let config = LocalSchedulerConfig {
            commit_wrapper: None,
            partitions_source_config: PartitionsSourceConfig::Fixed(
                partition_ids.into_iter().collect::<HashSet<PartitionId>>(),
            ),
            shard_config: None,
        };

        let scheduler = LocalScheduler::new(
            config,
            BackoffConfig::default(),
            catalog.catalog(),
            Arc::new(MockProvider::new(Time::MIN)),
            Arc::new(metric::Registry::default()),
            false,
        );

        (scheduler, file1_1, file1_2)
    }

    #[tokio::test]
    #[should_panic]
    async fn test_status_update_none_should_panic() {
        test_helpers::maybe_start_logging();

        let (scheduler, _, _) = create_scheduler_with_partitions().await;
        let jobs = scheduler.get_jobs().await;

        for job in jobs {
            let commit_update = CommitUpdate {
                partition_id: job.partition_id,
                delete: vec![],
                upgrade: vec![],
                target_level: data_types::CompactionLevel::Final,
                create: vec![],
            };

            let _ = scheduler
                .job_status(CompactionJobStatus {
                    job,
                    status: CompactionJobStatusVariant::Update(commit_update),
                })
                .await;
        }
    }

    #[tokio::test]
    async fn test_status_update_replacement() {
        test_helpers::maybe_start_logging();

        let (scheduler, existing_1, existing_2) = create_scheduler_with_partitions().await;
        let jobs = scheduler.get_jobs().await;
        let job = jobs
            .into_iter()
            .find(|job| job.partition_id == existing_1.partition.partition.id)
            .unwrap();

        let created = ParquetFileBuilder::new(1002)
            .with_partition(job.partition_id.get())
            .build();

        let commit_update = CommitUpdate {
            partition_id: job.partition_id,
            delete: vec![existing_1.into(), existing_2.into()],
            upgrade: vec![],
            target_level: data_types::CompactionLevel::Final,
            create: vec![created.into()],
        };

        assert!(scheduler
            .job_status(CompactionJobStatus {
                job,
                status: CompactionJobStatusVariant::Update(commit_update),
            })
            .await
            .is_ok());
    }

    #[tokio::test]
    #[should_panic]
    async fn test_status_update_replacement_args_incomplete() {
        test_helpers::maybe_start_logging();

        let (scheduler, _, _) = create_scheduler_with_partitions().await;
        let jobs = scheduler.get_jobs().await;

        for job in jobs {
            let created_1 = ParquetFileBuilder::new(1002)
                .with_partition(job.partition_id.get())
                .build();

            let commit_update = CommitUpdate {
                partition_id: job.partition_id,
                delete: vec![],
                upgrade: vec![],
                target_level: data_types::CompactionLevel::Final,
                create: vec![created_1.into()],
            };

            let _ = scheduler
                .job_status(CompactionJobStatus {
                    job,
                    status: CompactionJobStatusVariant::Update(commit_update),
                })
                .await;
        }
    }

    #[tokio::test]
    async fn test_status_update_upgrade() {
        test_helpers::maybe_start_logging();

        let (scheduler, existing_1, existing_2) = create_scheduler_with_partitions().await;
        let jobs = scheduler.get_jobs().await;
        let job = jobs
            .into_iter()
            .find(|job| job.partition_id == existing_1.partition.partition.id)
            .unwrap();

        let commit_update = CommitUpdate {
            partition_id: job.partition_id,
            delete: vec![],
            upgrade: vec![existing_1.into(), existing_2.into()],
            target_level: data_types::CompactionLevel::Final,
            create: vec![],
        };

        assert!(scheduler
            .job_status(CompactionJobStatus {
                job,
                status: CompactionJobStatusVariant::Update(commit_update),
            })
            .await
            .is_ok());
    }

    #[tokio::test]
    async fn test_status_update_can_replace_and_upgrade_at_once() {
        test_helpers::maybe_start_logging();

        let (scheduler, existing_1, existing_2) = create_scheduler_with_partitions().await;
        let jobs = scheduler.get_jobs().await;
        let job = jobs
            .into_iter()
            .find(|job| job.partition_id == existing_1.partition.partition.id)
            .unwrap();

        let created = ParquetFileBuilder::new(1002)
            .with_partition(job.partition_id.get())
            .build();

        let commit_update = CommitUpdate {
            partition_id: job.partition_id,
            delete: vec![existing_1.into()],
            upgrade: vec![existing_2.into()],
            target_level: data_types::CompactionLevel::Final,
            create: vec![created.into()],
        };

        assert!(scheduler
            .job_status(CompactionJobStatus {
                job,
                status: CompactionJobStatusVariant::Update(commit_update),
            })
            .await
            .is_ok());
    }

    #[tokio::test]
    async fn test_status_skip() {
        test_helpers::maybe_start_logging();

        let (scheduler, _, _) = create_scheduler_with_partitions().await;
        let jobs = scheduler.get_jobs().await;

        for job in jobs {
            assert!(scheduler
                .job_status(CompactionJobStatus {
                    job,
                    status: CompactionJobStatusVariant::RequestToSkip(SkipReason::CompactionError(
                        "some error".into()
                    )),
                })
                .await
                .is_ok());
        }
    }

    #[tokio::test]
    async fn test_status_error() {
        test_helpers::maybe_start_logging();

        let (scheduler, _, _) = create_scheduler_with_partitions().await;
        let jobs = scheduler.get_jobs().await;

        for job in jobs {
            assert!(scheduler
                .job_status(CompactionJobStatus {
                    job,
                    status: CompactionJobStatusVariant::Error(crate::ErrorKind::OutOfMemory),
                })
                .await
                .is_ok());
        }
    }

    #[tokio::test]
    async fn test_status_complete() {
        test_helpers::maybe_start_logging();

        let (scheduler, _, _) = create_scheduler_with_partitions().await;
        let jobs = scheduler.get_jobs().await;

        for job in jobs {
            assert!(scheduler
                .job_status(CompactionJobStatus {
                    job,
                    status: CompactionJobStatusVariant::Complete,
                })
                .await
                .is_ok());
        }
    }
}
