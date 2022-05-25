//! Cache Parquet file data in Read Buffer chunks.

use super::ram::RamSize;
use cache_system::{
    backend::{
        lru::{LruBackend, ResourcePool},
        resource_consumption::FunctionEstimator,
        shared::SharedBackend,
    },
    driver::Cache,
    loader::{metrics::MetricsLoader, FunctionLoader},
};
use data_types::ParquetFileId;
use iox_time::TimeProvider;
use read_buffer::RBChunk;
use std::{collections::HashMap, mem, sync::Arc};

const CACHE_ID: &str = "read_buffer";

/// Cache for parquet file data decoded into read buffer chunks
#[derive(Debug)]
pub struct ReadBufferCache {
    cache: Cache<ParquetFileId, Arc<RBChunk>>,

    /// Handle that allows clearing entries for existing cache entries
    _backend: SharedBackend<ParquetFileId, Arc<RBChunk>>,
}

impl ReadBufferCache {
    /// Create a new empty cache.
    pub fn new(
        time_provider: Arc<dyn TimeProvider>,
        metric_registry: &metric::Registry,
        ram_pool: Arc<ResourcePool<RamSize>>,
    ) -> Self {
        let loader = Box::new(FunctionLoader::new(
            move |_parquet_file_id: ParquetFileId| async move {
                unimplemented!();
            },
        ));

        let loader = Arc::new(MetricsLoader::new(
            loader,
            CACHE_ID,
            Arc::clone(&time_provider),
            metric_registry,
        ));

        // add to memory pool
        let backend = Box::new(LruBackend::new(
            Box::new(HashMap::new()),
            Arc::clone(&ram_pool),
            CACHE_ID,
            Arc::new(FunctionEstimator::new(
                |k: &ParquetFileId, v: &Arc<RBChunk>| {
                    RamSize(mem::size_of_val(k) + mem::size_of_val(v) + v.size())
                },
            )),
        ));

        // get a direct handle so we can clear out entries as needed
        let _backend = SharedBackend::new(backend);

        let cache = Cache::new(loader, Box::new(_backend.clone()));

        Self { cache, _backend }
    }

    /// Get read buffer chunks by Parquet file id
    pub async fn get(&self, parquet_file_id: ParquetFileId) -> Arc<RBChunk> {
        self.cache.get(parquet_file_id).await
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::cache::ram::test_util::test_ram_pool;
    use iox_tests::util::TestCatalog;

    fn make_cache(catalog: &TestCatalog) -> ReadBufferCache {
        ReadBufferCache::new(
            catalog.time_provider(),
            &catalog.metric_registry(),
            test_ram_pool(),
        )
    }

    async fn make_catalog() -> Arc<TestCatalog> {
        TestCatalog::new()
    }

    #[tokio::test]
    async fn test_rb_chunks() {
        let catalog = make_catalog().await;
        let _cache = make_cache(&catalog);
    }
}
