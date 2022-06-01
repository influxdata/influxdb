//! Namespace cache.

use backoff::{Backoff, BackoffConfig};
use cache_system::{
    backend::{
        lru::{LruBackend, ResourcePool},
        resource_consumption::FunctionEstimator,
        ttl::{OptionalValueTtlProvider, TtlBackend},
    },
    driver::Cache,
    loader::{metrics::MetricsLoader, FunctionLoader},
};
use data_types::NamespaceSchema;
use iox_catalog::interface::{get_schema_by_name, Catalog};
use iox_time::TimeProvider;
use std::{collections::HashMap, mem::size_of_val, sync::Arc, time::Duration};

use super::ram::RamSize;

/// Duration to keep existing namespaces.
pub const TTL_EXISTING: Duration = Duration::from_secs(10);

/// Duration to keep non-existing namespaces.
pub const TTL_NON_EXISTING: Duration = Duration::from_secs(60);

const CACHE_ID: &str = "namespace";

type CacheT = Cache<Arc<str>, Option<Arc<CachedNamespace>>, ()>;

/// Cache for namespace-related attributes.
#[derive(Debug)]
pub struct NamespaceCache {
    cache: CacheT,
}

impl NamespaceCache {
    /// Create new empty cache.
    pub fn new(
        catalog: Arc<dyn Catalog>,
        backoff_config: BackoffConfig,
        time_provider: Arc<dyn TimeProvider>,
        metric_registry: &metric::Registry,
        ram_pool: Arc<ResourcePool<RamSize>>,
    ) -> Self {
        let loader = Box::new(FunctionLoader::new(
            move |namespace_name: Arc<str>, _extra: ()| {
                let catalog = Arc::clone(&catalog);
                let backoff_config = backoff_config.clone();

                async move {
                    let schema = Backoff::new(&backoff_config)
                        .retry_all_errors("get namespace schema", || async {
                            let mut repos = catalog.repositories().await;
                            match get_schema_by_name(&namespace_name, repos.as_mut()).await {
                                Ok(schema) => Ok(Some(schema)),
                                Err(iox_catalog::interface::Error::NamespaceNotFound {
                                    ..
                                }) => Ok(None),
                                Err(e) => Err(e),
                            }
                        })
                        .await
                        .expect("retry forever")?;

                    Some(Arc::new(CachedNamespace {
                        schema: Arc::new(schema),
                    }))
                }
            },
        ));
        let loader = Arc::new(MetricsLoader::new(
            loader,
            CACHE_ID,
            Arc::clone(&time_provider),
            metric_registry,
        ));

        let backend = Box::new(TtlBackend::new(
            Box::new(HashMap::new()),
            Arc::new(OptionalValueTtlProvider::new(
                Some(TTL_NON_EXISTING),
                Some(TTL_EXISTING),
            )),
            Arc::clone(&time_provider),
        ));

        // add to memory pool
        let backend = Box::new(LruBackend::new(
            backend as _,
            Arc::clone(&ram_pool),
            CACHE_ID,
            Arc::new(FunctionEstimator::new(
                |k: &Arc<str>, v: &Option<Arc<CachedNamespace>>| {
                    RamSize(
                        size_of_val(k)
                            + k.len()
                            + size_of_val(v)
                            + v.as_ref().map(|v| v.size()).unwrap_or_default(),
                    )
                },
            )),
        ));

        let cache = Cache::new(loader, backend);

        Self { cache }
    }

    /// Get namespace schema by name.
    pub async fn schema(&self, name: Arc<str>) -> Option<Arc<NamespaceSchema>> {
        self.cache
            .get(name, ())
            .await
            .map(|n| Arc::clone(&n.schema))
    }
}

#[derive(Debug, Clone)]
struct CachedNamespace {
    schema: Arc<NamespaceSchema>,
}

impl CachedNamespace {
    /// RAM-bytes EXCLUDING `self`.
    fn size(&self) -> usize {
        self.schema.size() - size_of_val(&self.schema)
    }
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;

    use crate::cache::{ram::test_util::test_ram_pool, test_util::assert_histogram_metric_count};
    use data_types::{ColumnSchema, ColumnType, TableSchema};
    use iox_tests::util::TestCatalog;

    use super::*;

    #[tokio::test]
    async fn test_schema() {
        let catalog = TestCatalog::new();

        let ns1 = catalog.create_namespace("ns1").await;
        let ns2 = catalog.create_namespace("ns2").await;
        assert_ne!(ns1.namespace.id, ns2.namespace.id);

        let table11 = ns1.create_table("table1").await;
        let table12 = ns1.create_table("table2").await;
        let table21 = ns2.create_table("table1").await;

        let col111 = table11.create_column("col1", ColumnType::I64).await;
        let col112 = table11.create_column("col2", ColumnType::Tag).await;
        let col113 = table11.create_column("col3", ColumnType::Time).await;
        let col121 = table12.create_column("col1", ColumnType::F64).await;
        let col122 = table12.create_column("col2", ColumnType::Time).await;
        let col211 = table21.create_column("col1", ColumnType::Time).await;

        let cache = NamespaceCache::new(
            catalog.catalog(),
            BackoffConfig::default(),
            catalog.time_provider(),
            &catalog.metric_registry(),
            test_ram_pool(),
        );

        let schema1_a = cache.schema(Arc::from(String::from("ns1"))).await.unwrap();
        let expected_schema_1 = NamespaceSchema {
            id: ns1.namespace.id,
            kafka_topic_id: ns1.namespace.kafka_topic_id,
            query_pool_id: ns1.namespace.query_pool_id,
            tables: BTreeMap::from([
                (
                    String::from("table1"),
                    TableSchema {
                        id: table11.table.id,
                        columns: BTreeMap::from([
                            (
                                String::from("col1"),
                                ColumnSchema {
                                    id: col111.column.id,
                                    column_type: ColumnType::I64,
                                },
                            ),
                            (
                                String::from("col2"),
                                ColumnSchema {
                                    id: col112.column.id,
                                    column_type: ColumnType::Tag,
                                },
                            ),
                            (
                                String::from("col3"),
                                ColumnSchema {
                                    id: col113.column.id,
                                    column_type: ColumnType::Time,
                                },
                            ),
                        ]),
                    },
                ),
                (
                    String::from("table2"),
                    TableSchema {
                        id: table12.table.id,
                        columns: BTreeMap::from([
                            (
                                String::from("col1"),
                                ColumnSchema {
                                    id: col121.column.id,
                                    column_type: ColumnType::F64,
                                },
                            ),
                            (
                                String::from("col2"),
                                ColumnSchema {
                                    id: col122.column.id,
                                    column_type: ColumnType::Time,
                                },
                            ),
                        ]),
                    },
                ),
            ]),
        };
        assert_eq!(schema1_a.as_ref(), &expected_schema_1);
        assert_histogram_metric_count(&catalog.metric_registry, "namespace_get_by_name", 1);

        let schema2 = cache.schema(Arc::from(String::from("ns2"))).await.unwrap();
        let expected_schema_2 = NamespaceSchema {
            id: ns2.namespace.id,
            kafka_topic_id: ns2.namespace.kafka_topic_id,
            query_pool_id: ns2.namespace.query_pool_id,
            tables: BTreeMap::from([(
                String::from("table1"),
                TableSchema {
                    id: table21.table.id,
                    columns: BTreeMap::from([(
                        String::from("col1"),
                        ColumnSchema {
                            id: col211.column.id,
                            column_type: ColumnType::Time,
                        },
                    )]),
                },
            )]),
        };
        assert_eq!(schema2.as_ref(), &expected_schema_2);
        assert_histogram_metric_count(&catalog.metric_registry, "namespace_get_by_name", 2);

        let schema1_b = cache.schema(Arc::from(String::from("ns1"))).await.unwrap();
        assert!(Arc::ptr_eq(&schema1_a, &schema1_b));
        assert_histogram_metric_count(&catalog.metric_registry, "namespace_get_by_name", 2);

        // cache timeout
        catalog.mock_time_provider().inc(TTL_EXISTING);

        let schema1_c = cache.schema(Arc::from(String::from("ns1"))).await.unwrap();
        assert_eq!(schema1_c.as_ref(), schema1_a.as_ref());
        assert!(!Arc::ptr_eq(&schema1_a, &schema1_c));
        assert_histogram_metric_count(&catalog.metric_registry, "namespace_get_by_name", 3);
    }

    #[tokio::test]
    async fn test_schema_non_existing() {
        let catalog = TestCatalog::new();

        let cache = NamespaceCache::new(
            catalog.catalog(),
            BackoffConfig::default(),
            catalog.time_provider(),
            &catalog.metric_registry(),
            test_ram_pool(),
        );

        let none = cache.schema(Arc::from(String::from("foo"))).await;
        assert!(none.is_none());
        assert_histogram_metric_count(&catalog.metric_registry, "namespace_get_by_name", 1);

        let none = cache.schema(Arc::from(String::from("foo"))).await;
        assert!(none.is_none());
        assert_histogram_metric_count(&catalog.metric_registry, "namespace_get_by_name", 1);

        // cache timeout
        catalog.mock_time_provider().inc(TTL_NON_EXISTING);

        let none = cache.schema(Arc::from(String::from("foo"))).await;
        assert!(none.is_none());
        assert_histogram_metric_count(&catalog.metric_registry, "namespace_get_by_name", 2);
    }
}
