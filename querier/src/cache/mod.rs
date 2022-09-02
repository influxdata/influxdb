//! Caches used by the querier.
use ::object_store::ObjectStore;
use backoff::BackoffConfig;
use cache_system::backend::policy::lru::ResourcePool;
use iox_catalog::interface::Catalog;
use iox_time::TimeProvider;
use std::sync::Arc;
use tokio::runtime::Handle;

use self::{
    namespace::NamespaceCache, object_store::ObjectStoreCache, parquet_file::ParquetFileCache,
    partition::PartitionCache, processed_tombstones::ProcessedTombstonesCache,
    projected_schema::ProjectedSchemaCache, ram::RamSize, read_buffer::ReadBufferCache,
    tombstones::TombstoneCache,
};

pub mod namespace;
pub mod object_store;
pub mod parquet_file;
pub mod partition;
pub mod processed_tombstones;
pub mod projected_schema;
mod ram;
pub mod read_buffer;
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

    /// Namespace cache.
    namespace_cache: NamespaceCache,

    /// Processed tombstone cache.
    processed_tombstones_cache: ProcessedTombstonesCache,

    /// Parquet file cache
    parquet_file_cache: ParquetFileCache,

    /// tombstone cache
    tombstone_cache: TombstoneCache,

    /// Read buffer chunk cache
    read_buffer_cache: ReadBufferCache,

    /// Projected schema cache.
    projected_schema_cache: ProjectedSchemaCache,

    /// Object store cache.
    object_store_cache: ObjectStoreCache,

    /// Metric registry
    metric_registry: Arc<metric::Registry>,

    /// Time provider.
    time_provider: Arc<dyn TimeProvider>,
}

impl CatalogCache {
    /// Create empty cache.
    pub fn new(
        catalog: Arc<dyn Catalog>,
        time_provider: Arc<dyn TimeProvider>,
        metric_registry: Arc<metric::Registry>,
        object_store: Arc<dyn ObjectStore>,
        ram_pool_metadata_bytes: usize,
        ram_pool_data_bytes: usize,
        handle: &Handle,
    ) -> Self {
        Self::new_internal(
            catalog,
            time_provider,
            metric_registry,
            object_store,
            ram_pool_metadata_bytes,
            ram_pool_data_bytes,
            handle,
            false,
        )
    }

    /// Create empty cache for testing.
    ///
    /// This cache will have unlimited RAM pools.
    pub fn new_testing(
        catalog: Arc<dyn Catalog>,
        time_provider: Arc<dyn TimeProvider>,
        metric_registry: Arc<metric::Registry>,
        object_store: Arc<dyn ObjectStore>,
        handle: &Handle,
    ) -> Self {
        Self::new_internal(
            catalog,
            time_provider,
            metric_registry,
            object_store,
            usize::MAX,
            usize::MAX,
            handle,
            true,
        )
    }

    #[allow(clippy::too_many_arguments)]
    fn new_internal(
        catalog: Arc<dyn Catalog>,
        time_provider: Arc<dyn TimeProvider>,
        metric_registry: Arc<metric::Registry>,
        object_store: Arc<dyn ObjectStore>,
        ram_pool_metadata_bytes: usize,
        ram_pool_data_bytes: usize,
        handle: &Handle,
        testing: bool,
    ) -> Self {
        let backoff_config = BackoffConfig::default();

        let ram_pool_metadata = Arc::new(ResourcePool::new(
            "ram_metadata",
            RamSize(ram_pool_metadata_bytes),
            Arc::clone(&metric_registry),
        ));
        let ram_pool_data = Arc::new(ResourcePool::new(
            "ram_data",
            RamSize(ram_pool_data_bytes),
            Arc::clone(&metric_registry),
        ));

        let partition_cache = PartitionCache::new(
            Arc::clone(&catalog),
            backoff_config.clone(),
            Arc::clone(&time_provider),
            &metric_registry,
            Arc::clone(&ram_pool_metadata),
            testing,
        );
        let namespace_cache = NamespaceCache::new(
            Arc::clone(&catalog),
            backoff_config.clone(),
            Arc::clone(&time_provider),
            &metric_registry,
            Arc::clone(&ram_pool_metadata),
            handle,
            testing,
        );
        let processed_tombstones_cache = ProcessedTombstonesCache::new(
            Arc::clone(&catalog),
            backoff_config.clone(),
            Arc::clone(&time_provider),
            &metric_registry,
            Arc::clone(&ram_pool_metadata),
            testing,
        );
        let parquet_file_cache = ParquetFileCache::new(
            Arc::clone(&catalog),
            backoff_config.clone(),
            Arc::clone(&time_provider),
            &metric_registry,
            Arc::clone(&ram_pool_metadata),
            testing,
        );
        let tombstone_cache = TombstoneCache::new(
            Arc::clone(&catalog),
            backoff_config.clone(),
            Arc::clone(&time_provider),
            &metric_registry,
            Arc::clone(&ram_pool_metadata),
            testing,
        );
        let read_buffer_cache = ReadBufferCache::new(
            backoff_config.clone(),
            Arc::clone(&time_provider),
            Arc::clone(&metric_registry),
            Arc::clone(&ram_pool_data),
            testing,
        );
        let projected_schema_cache = ProjectedSchemaCache::new(
            Arc::clone(&time_provider),
            &metric_registry,
            Arc::clone(&ram_pool_metadata),
            testing,
        );
        let object_store_cache = ObjectStoreCache::new(
            backoff_config,
            object_store,
            Arc::clone(&time_provider),
            &metric_registry,
            Arc::clone(&ram_pool_data),
            testing,
        );

        Self {
            catalog,
            partition_cache,
            namespace_cache,
            processed_tombstones_cache,
            parquet_file_cache,
            tombstone_cache,
            read_buffer_cache,
            projected_schema_cache,
            object_store_cache,
            metric_registry,
            time_provider,
        }
    }

    /// Get underlying catalog
    pub(crate) fn catalog(&self) -> Arc<dyn Catalog> {
        Arc::clone(&self.catalog)
    }

    /// Get underlying metric registry.
    pub(crate) fn metric_registry(&self) -> Arc<metric::Registry> {
        Arc::clone(&self.metric_registry)
    }

    /// Get underlying time provider
    pub(crate) fn time_provider(&self) -> Arc<dyn TimeProvider> {
        Arc::clone(&self.time_provider)
    }

    /// Namespace cache
    pub(crate) fn namespace(&self) -> &NamespaceCache {
        &self.namespace_cache
    }

    /// Partition cache
    pub(crate) fn partition(&self) -> &PartitionCache {
        &self.partition_cache
    }

    /// Processed tombstone cache.
    pub(crate) fn processed_tombstones(&self) -> &ProcessedTombstonesCache {
        &self.processed_tombstones_cache
    }

    /// Parquet file cache.
    pub(crate) fn parquet_file(&self) -> &ParquetFileCache {
        &self.parquet_file_cache
    }

    /// Tombstone cache.
    pub(crate) fn tombstone(&self) -> &TombstoneCache {
        &self.tombstone_cache
    }

    /// Read buffer chunk cache.
    pub(crate) fn read_buffer(&self) -> &ReadBufferCache {
        &self.read_buffer_cache
    }

    /// Projected schema cache.
    pub(crate) fn projected_schema(&self) -> &ProjectedSchemaCache {
        &self.projected_schema_cache
    }

    /// Object store cache.
    #[allow(dead_code)]
    pub(crate) fn object_store(&self) -> &ObjectStoreCache {
        &self.object_store_cache
    }
}
