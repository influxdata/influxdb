//! Internals used by [`LocalScheduler`].
pub(crate) mod id_only_partition_filter;
pub(crate) mod partitions_source;
pub(crate) mod partitions_source_config;
pub(crate) mod shard_config;

use std::sync::Arc;

use async_trait::async_trait;
use backoff::BackoffConfig;
use data_types::PartitionId;
use iox_catalog::interface::Catalog;
use iox_time::TimeProvider;
use observability_deps::tracing::info;

use crate::{MockPartitionsSource, PartitionsSource, PartitionsSourceConfig, ShardConfig};

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

/// Implementation of the scheduler for local (per compactor) scheduling.
#[derive(Debug)]
pub struct LocalScheduler {
    /// The partitions source to use for scheduling.
    partitions_source: Arc<dyn PartitionsSource>,
}

impl LocalScheduler {
    /// Create a new [`LocalScheduler`].
    pub fn new(
        config: PartitionsSourceConfig,
        shard_config: Option<ShardConfig>,
        backoff_config: BackoffConfig,
        catalog: Arc<dyn Catalog>,
        time_provider: Arc<dyn TimeProvider>,
    ) -> Self {
        let partitions_source: Arc<dyn PartitionsSource> = match &config {
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

        Self { partitions_source }
    }
}

#[async_trait]
impl PartitionsSource for LocalScheduler {
    // TODO: followon PR will replace with Arc<dyn Scheduler>.get_job()
    async fn fetch(&self) -> Vec<PartitionId> {
        self.partitions_source.fetch().await
    }
}

impl std::fmt::Display for LocalScheduler {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "local_compaction_scheduler")
    }
}
