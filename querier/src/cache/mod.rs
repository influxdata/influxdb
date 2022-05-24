//! Caches used by the querier.
use backoff::BackoffConfig;
use cache_system::backend::lru::ResourcePool;
use iox_catalog::interface::Catalog;
use iox_time::TimeProvider;
use std::sync::Arc;

use self::{
    namespace::NamespaceCache, partition::PartitionCache,
    processed_tombstones::ProcessedTombstonesCache, ram::RamSize, table::TableCache,
};

pub mod namespace;
pub mod parquet_file;
pub mod partition;
pub mod processed_tombstones;
mod ram;
pub mod table;
pub mod tombstones;

#[cfg(test)]
mod test_util;

/// Caches request to the [`Catalog`].
#[derive(Debug)]
pub struct CatalogCache {
    /// Catalog.
    catalog: Arc<dyn Catalog>,

    /// Partition cache.
    partition_cache: PartitionCache,

    /// Table cache.
    table_cache: TableCache,

    /// Namespace cache.
    namespace_cache: NamespaceCache,

    /// Processed tombstone cache.
    processed_tombstones: ProcessedTombstonesCache,

    /// Time provider.
    time_provider: Arc<dyn TimeProvider>,
}

impl CatalogCache {
    /// Create empty cache.
    pub fn new(
        catalog: Arc<dyn Catalog>,
        time_provider: Arc<dyn TimeProvider>,
        metric_registry: Arc<metric::Registry>,
        ram_pool_bytes: usize,
    ) -> Self {
        let backoff_config = BackoffConfig::default();
        let ram_pool = Arc::new(ResourcePool::new(
            "ram",
            RamSize(ram_pool_bytes),
            Arc::clone(&time_provider),
            Arc::clone(&metric_registry),
        ));

        let namespace_cache = NamespaceCache::new(
            Arc::clone(&catalog),
            backoff_config.clone(),
            Arc::clone(&time_provider),
            &metric_registry,
            Arc::clone(&ram_pool),
        );
        let table_cache = TableCache::new(
            Arc::clone(&catalog),
            backoff_config.clone(),
            Arc::clone(&time_provider),
            &metric_registry,
            Arc::clone(&ram_pool),
        );
        let partition_cache = PartitionCache::new(
            Arc::clone(&catalog),
            backoff_config.clone(),
            Arc::clone(&time_provider),
            &metric_registry,
            Arc::clone(&ram_pool),
        );
        let processed_tombstones = ProcessedTombstonesCache::new(
            Arc::clone(&catalog),
            backoff_config,
            Arc::clone(&time_provider),
            &metric_registry,
            Arc::clone(&ram_pool),
        );

        Self {
            catalog,
            partition_cache,
            table_cache,
            namespace_cache,
            processed_tombstones,
            time_provider,
        }
    }

    /// Get underlying catalog
    pub(crate) fn catalog(&self) -> Arc<dyn Catalog> {
        Arc::clone(&self.catalog)
    }

    /// Get underlying time provider
    pub(crate) fn time_provider(&self) -> Arc<dyn TimeProvider> {
        Arc::clone(&self.time_provider)
    }

    /// Namespace cache
    pub(crate) fn namespace(&self) -> &NamespaceCache {
        &self.namespace_cache
    }

    /// Table cache
    pub(crate) fn table(&self) -> &TableCache {
        &self.table_cache
    }

    /// Partition cache
    pub(crate) fn partition(&self) -> &PartitionCache {
        &self.partition_cache
    }

    /// Processed tombstone cache.
    pub(crate) fn processed_tombstones(&self) -> &ProcessedTombstonesCache {
        &self.processed_tombstones
    }
}
