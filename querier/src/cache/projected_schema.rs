//! Cache for schema projections.
//!
//! While this is technically NOT caching catalog requests (i.e. CPU and IO work), it heavily reduced memory when
//! creating [`QuerierChunk`](crate::chunk::QuerierChunk)s.
use std::{
    collections::HashMap,
    mem::{size_of, size_of_val},
    sync::Arc,
};

use cache_system::{
    backend::policy::{
        lru::{LruPolicy, ResourcePool},
        PolicyBackend,
    },
    cache::{driver::CacheDriver, metrics::CacheWithMetrics, Cache},
    loader::{metrics::MetricsLoader, FunctionLoader},
    resource_consumption::FunctionEstimator,
};
use data_types::TableId;
use iox_time::TimeProvider;
use schema::Schema;
use trace::span::Span;

use super::ram::RamSize;

const CACHE_ID: &str = "projected_schema";

/// Cache key.
#[derive(Debug, Clone, Hash, PartialEq, Eq, PartialOrd, Ord)]
struct CacheKey {
    table_id: TableId,
    projection: Vec<String>,
}

impl CacheKey {
    /// Create new key.
    ///
    /// This normalizes `projection`.
    fn new(table_id: TableId, mut projection: Vec<String>) -> Self {
        // normalize column order
        projection.sort();

        // ensure that cache key is as small as possible
        projection.shrink_to_fit();

        Self {
            table_id,
            projection,
        }
    }

    /// Size in of key including `Self`.
    fn size(&self) -> usize {
        size_of_val(self)
            + self.projection.capacity() * size_of::<String>()
            + self.projection.iter().map(|s| s.capacity()).sum::<usize>()
    }
}

type CacheT = Box<
    dyn Cache<
        K = CacheKey,
        V = Arc<Schema>,
        GetExtra = (Arc<Schema>, Option<Span>),
        PeekExtra = ((), Option<Span>),
    >,
>;

/// Cache for projected schemas.
#[derive(Debug)]
pub struct ProjectedSchemaCache {
    cache: CacheT,
}

impl ProjectedSchemaCache {
    /// Create new empty cache.
    pub fn new(
        time_provider: Arc<dyn TimeProvider>,
        metric_registry: &metric::Registry,
        ram_pool: Arc<ResourcePool<RamSize>>,
        testing: bool,
    ) -> Self {
        let loader =
            FunctionLoader::new(move |key: CacheKey, table_schema: Arc<Schema>| async move {
                let projection: Vec<&str> = key.projection.iter().map(|s| s.as_str()).collect();
                Arc::new(
                    table_schema
                        .select_by_names(&projection)
                        .expect("Bug in schema projection"),
                )
            });
        let loader = Arc::new(MetricsLoader::new(
            loader,
            CACHE_ID,
            Arc::clone(&time_provider),
            metric_registry,
            testing,
        ));

        // add to memory pool
        let mut backend = PolicyBackend::new(Box::new(HashMap::new()), Arc::clone(&time_provider));
        backend.add_policy(LruPolicy::new(
            Arc::clone(&ram_pool),
            CACHE_ID,
            Arc::new(FunctionEstimator::new(|k: &CacheKey, v: &Arc<Schema>| {
                RamSize(k.size() + size_of_val(v) + v.estimate_size())
            })),
        ));

        let cache = CacheDriver::new(loader, backend);
        let cache = Box::new(CacheWithMetrics::new(
            cache,
            CACHE_ID,
            time_provider,
            metric_registry,
        ));

        Self { cache }
    }

    /// Get projected schema for given table.
    ///
    /// # Key
    /// The cache will is `table_id` combined with `projection`. The projection order is normalized.
    ///
    /// The `table_schema` is NOT part of the cache key. It is OK to update the table schema (i.e. add new columns)
    /// between requests. The caller however MUST ensure that the `table_id` is correct.
    ///
    /// # Panic
    /// Will panic if any column in `projection` is missing in `table_schema`.
    pub async fn get(
        &self,
        table_id: TableId,
        table_schema: Arc<Schema>,
        projection: Vec<String>,
        span: Option<Span>,
    ) -> Arc<Schema> {
        let key = CacheKey::new(table_id, projection);

        self.cache.get(key, (table_schema, span)).await
    }
}

#[cfg(test)]
mod tests {
    use iox_time::SystemProvider;
    use schema::builder::SchemaBuilder;

    use crate::cache::ram::test_util::test_ram_pool;

    use super::*;

    #[tokio::test]
    async fn test() {
        let cache = ProjectedSchemaCache::new(
            Arc::new(SystemProvider::new()),
            &metric::Registry::new(),
            test_ram_pool(),
            true,
        );

        let table_id_1 = TableId::new(1);
        let table_id_2 = TableId::new(2);
        let table_schema_1a = Arc::new(
            SchemaBuilder::new()
                .tag("t1")
                .tag("t2")
                .tag("t3")
                .timestamp()
                .build()
                .unwrap(),
        );
        let table_schema_1b = Arc::new(
            SchemaBuilder::new()
                .tag("t1")
                .tag("t2")
                .tag("t3")
                .tag("t4")
                .timestamp()
                .build()
                .unwrap(),
        );

        // initial request
        let expected = Arc::new(SchemaBuilder::new().tag("t1").tag("t2").build().unwrap());
        let projection_1 = cache
            .get(
                table_id_1,
                Arc::clone(&table_schema_1a),
                vec!["t1".to_owned(), "t2".to_owned()],
                None,
            )
            .await;
        assert_eq!(projection_1, expected);

        // same request
        let projection_2 = cache
            .get(
                table_id_1,
                Arc::clone(&table_schema_1a),
                vec!["t1".to_owned(), "t2".to_owned()],
                None,
            )
            .await;
        assert!(Arc::ptr_eq(&projection_1, &projection_2));

        // updated table schema
        let projection_3 = cache
            .get(
                table_id_1,
                Arc::clone(&table_schema_1b),
                vec!["t1".to_owned(), "t2".to_owned()],
                None,
            )
            .await;
        assert!(Arc::ptr_eq(&projection_1, &projection_3));

        // different column order
        let projection_4 = cache
            .get(
                table_id_1,
                Arc::clone(&table_schema_1a),
                vec!["t2".to_owned(), "t1".to_owned()],
                None,
            )
            .await;
        assert!(Arc::ptr_eq(&projection_1, &projection_4));

        // different columns set
        let expected = Arc::new(SchemaBuilder::new().tag("t1").tag("t3").build().unwrap());
        let projection_5 = cache
            .get(
                table_id_1,
                Arc::clone(&table_schema_1a),
                vec!["t1".to_owned(), "t3".to_owned()],
                None,
            )
            .await;
        assert_eq!(projection_5, expected);

        // different table ID
        let projection_6 = cache
            .get(
                table_id_2,
                Arc::clone(&table_schema_1a),
                vec!["t1".to_owned(), "t2".to_owned()],
                None,
            )
            .await;
        assert_eq!(projection_6, projection_1);
        assert!(!Arc::ptr_eq(&projection_1, &projection_6));

        // original data still present
        let projection_7 = cache
            .get(
                table_id_1,
                Arc::clone(&table_schema_1a),
                vec!["t1".to_owned(), "t2".to_owned()],
                None,
            )
            .await;
        assert!(Arc::ptr_eq(&projection_1, &projection_7));
    }
}
