//! Caches used by the querier.
use backoff::BackoffConfig;
use iox_catalog::interface::Catalog;
use std::sync::Arc;
use time::TimeProvider;

use self::{
    namespace::NamespaceCache, partition::PartitionCache,
    processed_tombstones::ProcessedTombstonesCache, table::TableCache,
};

pub mod namespace;
pub mod partition;
pub mod processed_tombstones;
pub mod table;

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
}

impl CatalogCache {
    /// Create empty cache.
    pub fn new(catalog: Arc<dyn Catalog>, time_provider: Arc<dyn TimeProvider>) -> Self {
        let backoff_config = BackoffConfig::default();

        let namespace_cache = NamespaceCache::new(Arc::clone(&catalog), backoff_config.clone());
        let table_cache = TableCache::new(
            Arc::clone(&catalog),
            backoff_config.clone(),
            Arc::clone(&time_provider),
        );
        let partition_cache = PartitionCache::new(Arc::clone(&catalog), backoff_config.clone());
        let processed_tombstones =
            ProcessedTombstonesCache::new(Arc::clone(&catalog), backoff_config, time_provider);

        Self {
            catalog,
            partition_cache,
            table_cache,
            namespace_cache,
            processed_tombstones,
        }
    }

    /// Get underlying catalog
    pub fn catalog(&self) -> Arc<dyn Catalog> {
        Arc::clone(&self.catalog)
    }

    /// Namespace cache
    pub fn namespace(&self) -> &NamespaceCache {
        &self.namespace_cache
    }

    /// Table cache
    pub fn table(&self) -> &TableCache {
        &self.table_cache
    }

    /// Partition cache
    pub fn partition(&self) -> &PartitionCache {
        &self.partition_cache
    }

    /// Processed tombstone cache.
    pub fn processed_tombstones(&self) -> &ProcessedTombstonesCache {
        &self.processed_tombstones
    }
}
