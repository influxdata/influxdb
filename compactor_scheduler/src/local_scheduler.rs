//! Internals used by [`LocalScheduler`].
pub(crate) mod id_only_partition_filter;
pub(crate) mod partitions_source;
pub(crate) mod partitions_source_config;
pub(crate) mod shard_config;

use std::sync::Arc;

use async_trait::async_trait;
use backoff::BackoffConfig;
use iox_catalog::interface::Catalog;
use iox_time::TimeProvider;
use observability_deps::tracing::info;

use crate::{
    CompactionJob, MockPartitionsSource, PartitionsSource, PartitionsSourceConfig, Scheduler,
    ShardConfig,
};

use self::{
    id_only_partition_filter::{
        and::AndIdOnlyPartitionFilter, shard::ShardPartitionFilter, IdOnlyPartitionFilter,
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
    /// The partitions source config used by the local sceduler.
    pub partitions_source_config: PartitionsSourceConfig,
    /// The shard config used by the local sceduler.
    pub shard_config: Option<ShardConfig>,
}

/// Implementation of the scheduler for local (per compactor) scheduling.
#[derive(Debug)]
pub(crate) struct LocalScheduler {
    /// The partitions source to use for scheduling.
    partitions_source: Arc<dyn PartitionsSource>,
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
    ) -> Self {
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
        let partitions_source: Arc<dyn PartitionsSource> =
            Arc::new(FilterPartitionsSourceWrapper::new(
                AndIdOnlyPartitionFilter::new(id_only_partition_filters),
                partitions_source,
            ));

        Self {
            partitions_source,
            shard_config,
        }
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
    use iox_tests::TestCatalog;
    use iox_time::{MockProvider, Time};

    use super::*;

    #[test]
    fn test_display() {
        let scheduler = LocalScheduler::new(
            LocalSchedulerConfig::default(),
            BackoffConfig::default(),
            TestCatalog::new().catalog(),
            Arc::new(MockProvider::new(Time::MIN)),
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
            partitions_source_config: PartitionsSourceConfig::default(),
            shard_config,
        };

        let scheduler = LocalScheduler::new(
            config,
            BackoffConfig::default(),
            TestCatalog::new().catalog(),
            Arc::new(MockProvider::new(Time::MIN)),
        );

        assert_eq!(
            scheduler.to_string(),
            "local_compaction_scheduler(shard_cfg(n_shards=2,shard_id=1))",
        );
    }
}
