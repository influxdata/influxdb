//! Cache for schema projections.
//!
//! While this is technically NOT caching catalog requests (i.e. CPU and IO work), it heavily reduced memory when
//! creating [`QuerierParquetChunk`](crate::parquet::QuerierParquetChunk)s.
use std::{
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
use data_types::{ColumnId, TableId};
use iox_time::TimeProvider;
use schema::Schema;
use trace::span::Span;

use super::{namespace::CachedTable, ram::RamSize};

const CACHE_ID: &str = "projected_schema";

/// Cache key.
#[derive(Debug, Clone, Hash, PartialEq, Eq, PartialOrd, Ord)]
struct CacheKey {
    table_id: TableId,
    projection: Box<[ColumnId]>,
}

impl CacheKey {
    /// Create new key.
    ///
    /// This normalizes `projection`.
    fn new(table_id: TableId, mut projection: Box<[ColumnId]>) -> Self {
        // normalize column order
        projection.sort();

        Self {
            table_id,
            projection,
        }
    }

    /// Size in of key including `Self`.
    fn size(&self) -> usize {
        size_of_val(self) + self.projection.len() * size_of::<ColumnId>()
    }
}

type CacheT = Box<
    dyn Cache<
        K = CacheKey,
        V = Schema,
        GetExtra = (Arc<CachedTable>, Option<Span>),
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
            FunctionLoader::new(move |key: CacheKey, table: Arc<CachedTable>| async move {
                assert_eq!(key.table_id, table.id);

                let mut projection: Vec<&str> = key
                    .projection
                    .iter()
                    .map(|id| {
                        table
                            .column_id_map
                            .get(id)
                            .expect("cache table complete")
                            .as_ref()
                    })
                    .collect();

                // order by name since IDs are rather arbitrary
                projection.sort();

                table
                    .schema
                    .select_by_names(&projection)
                    .expect("Bug in schema projection")
            });
        let loader = Arc::new(MetricsLoader::new(
            loader,
            CACHE_ID,
            Arc::clone(&time_provider),
            metric_registry,
            testing,
        ));

        // add to memory pool
        let mut backend = PolicyBackend::hashmap_backed(Arc::clone(&time_provider));
        backend.add_policy(LruPolicy::new(
            Arc::clone(&ram_pool),
            CACHE_ID,
            Arc::new(FunctionEstimator::new(|k: &CacheKey, v: &Schema| {
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
        table: Arc<CachedTable>,
        projection: Box<[ColumnId]>,
        span: Option<Span>,
    ) -> Schema {
        let key = CacheKey::new(table.id, projection);

        self.cache.get(key, (table, span)).await
    }
}

#[cfg(test)]
mod tests {
    use data_types::partition_template::TablePartitionTemplateOverride;
    use iox_time::SystemProvider;
    use schema::{builder::SchemaBuilder, TIME_COLUMN_NAME};
    use std::collections::HashMap;

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
        let table_schema_a = SchemaBuilder::new()
            .tag("t1")
            .tag("t2")
            .tag("t3")
            .timestamp()
            .build()
            .unwrap();
        let table_schema_b = SchemaBuilder::new()
            .tag("t1")
            .tag("t2")
            .tag("t3")
            .tag("t4")
            .timestamp()
            .build()
            .unwrap();
        let column_id_map_a = HashMap::from([
            (ColumnId::new(1), Arc::from("t1")),
            (ColumnId::new(2), Arc::from("t2")),
            (ColumnId::new(3), Arc::from("t3")),
            (ColumnId::new(4), Arc::from(TIME_COLUMN_NAME)),
        ]);
        let column_id_map_b = HashMap::from([
            (ColumnId::new(1), Arc::from("t1")),
            (ColumnId::new(2), Arc::from("t2")),
            (ColumnId::new(3), Arc::from("t3")),
            (ColumnId::new(5), Arc::from("t4")),
            (ColumnId::new(4), Arc::from(TIME_COLUMN_NAME)),
        ]);
        let table_1a = Arc::new(CachedTable {
            id: table_id_1,
            schema: table_schema_a.clone(),
            column_id_map: column_id_map_a.clone(),
            column_id_map_rev: reverse_map(&column_id_map_a),
            primary_key_column_ids: [
                ColumnId::new(1),
                ColumnId::new(2),
                ColumnId::new(3),
                ColumnId::new(4),
            ]
            .into(),
            partition_template: TablePartitionTemplateOverride::default(),
        });
        let table_1b = Arc::new(CachedTable {
            id: table_id_1,
            schema: table_schema_b.clone(),
            column_id_map: column_id_map_b.clone(),
            column_id_map_rev: reverse_map(&column_id_map_b),
            primary_key_column_ids: [
                ColumnId::new(1),
                ColumnId::new(2),
                ColumnId::new(3),
                ColumnId::new(4),
            ]
            .into(),
            partition_template: TablePartitionTemplateOverride::default(),
        });
        let table_2a = Arc::new(CachedTable {
            id: table_id_2,
            schema: table_schema_a.clone(),
            column_id_map: column_id_map_a.clone(),
            column_id_map_rev: reverse_map(&column_id_map_a),
            primary_key_column_ids: [
                ColumnId::new(1),
                ColumnId::new(2),
                ColumnId::new(3),
                ColumnId::new(4),
                ColumnId::new(5),
            ]
            .into(),
            partition_template: TablePartitionTemplateOverride::default(),
        });

        // initial request
        let expected = SchemaBuilder::new().tag("t1").tag("t2").build().unwrap();
        let projection_1 = cache
            .get(
                Arc::clone(&table_1a),
                [ColumnId::new(1), ColumnId::new(2)].into(),
                None,
            )
            .await;
        assert_eq!(projection_1, expected);

        // same request
        let projection_2 = cache
            .get(
                Arc::clone(&table_1a),
                [ColumnId::new(1), ColumnId::new(2)].into(),
                None,
            )
            .await;
        assert!(Arc::ptr_eq(projection_1.inner(), projection_2.inner()));

        // updated table schema
        let projection_3 = cache
            .get(
                Arc::clone(&table_1b),
                [ColumnId::new(1), ColumnId::new(2)].into(),
                None,
            )
            .await;
        assert!(Arc::ptr_eq(projection_1.inner(), projection_3.inner()));

        // different column order
        let projection_4 = cache
            .get(
                Arc::clone(&table_1a),
                [ColumnId::new(2), ColumnId::new(1)].into(),
                None,
            )
            .await;
        assert!(Arc::ptr_eq(projection_1.inner(), projection_4.inner()));

        // different columns set
        let expected = SchemaBuilder::new().tag("t1").tag("t3").build().unwrap();
        let projection_5 = cache
            .get(
                Arc::clone(&table_1a),
                [ColumnId::new(1), ColumnId::new(3)].into(),
                None,
            )
            .await;
        assert_eq!(projection_5, expected);

        // different table ID
        let projection_6 = cache
            .get(
                Arc::clone(&table_2a),
                [ColumnId::new(1), ColumnId::new(2)].into(),
                None,
            )
            .await;
        assert_eq!(projection_6, projection_1);
        assert!(!Arc::ptr_eq(projection_1.inner(), projection_6.inner()));

        // original data still present
        let projection_7 = cache
            .get(
                Arc::clone(&table_1a),
                [ColumnId::new(1), ColumnId::new(2)].into(),
                None,
            )
            .await;
        assert!(Arc::ptr_eq(projection_1.inner(), projection_7.inner()));
    }

    fn reverse_map<K, V>(map: &HashMap<K, V>) -> HashMap<V, K>
    where
        K: Clone,
        V: Clone + std::hash::Hash + Eq,
    {
        map.iter().map(|(k, v)| (v.clone(), k.clone())).collect()
    }
}
