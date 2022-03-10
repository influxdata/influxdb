use backoff::BackoffConfig;
use iox_catalog::interface::Catalog;
use std::sync::Arc;

use self::{namespace::NamespaceCache, partition::PartitionCache, table::TableCache};

pub mod namespace;
pub mod partition;
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
}

impl CatalogCache {
    /// Create empty cache.
    pub fn new(catalog: Arc<dyn Catalog>) -> Self {
        let backoff_config = BackoffConfig::default();

        let namespace_cache = NamespaceCache::new(Arc::clone(&catalog), backoff_config.clone());
        let table_cache = TableCache::new(Arc::clone(&catalog), backoff_config.clone());
        let partition_cache = PartitionCache::new(Arc::clone(&catalog), backoff_config);

        Self {
            catalog,
            partition_cache,
            table_cache,
            namespace_cache,
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
}
