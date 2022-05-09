//! Namespace cache.

use crate::cache_system::{
    backend::{
        dual::dual_backends,
        ttl::{OptionalValueTtlProvider, TtlBackend},
    },
    driver::Cache,
    loader::FunctionLoader,
};
use backoff::{Backoff, BackoffConfig};
use data_types::{NamespaceId, NamespaceSchema};
use iox_catalog::interface::{get_schema_by_name, Catalog};
use iox_time::TimeProvider;
use std::{collections::HashMap, sync::Arc, time::Duration};

/// Duration to keep existing namespaces.
pub const TTL_EXISTING: Duration = Duration::from_secs(10);

/// Duration to keep non-existing namespaces.
pub const TTL_NON_EXISTING: Duration = Duration::from_secs(60);

type CacheFromId = Cache<NamespaceId, Option<Arc<CachedNamespace>>>;
type CacheFromName = Cache<Arc<str>, Option<Arc<CachedNamespace>>>;

/// Cache for namespace-related attributes.
#[derive(Debug)]
pub struct NamespaceCache {
    cache_from_id: CacheFromId,
    cache_from_name: CacheFromName,
}

impl NamespaceCache {
    /// Create new empty cache.
    pub fn new(
        catalog: Arc<dyn Catalog>,
        backoff_config: BackoffConfig,
        time_provider: Arc<dyn TimeProvider>,
    ) -> Self {
        let catalog_captured = Arc::clone(&catalog);
        let backoff_config_captured = backoff_config.clone();
        let loader_from_id = Arc::new(FunctionLoader::new(move |namespace_id| {
            let catalog = Arc::clone(&catalog_captured);
            let backoff_config = backoff_config_captured.clone();

            async move {
                let namespace = Backoff::new(&backoff_config)
                    .retry_all_errors("get namespace name by ID", || async {
                        catalog
                            .repositories()
                            .await
                            .namespaces()
                            .get_by_id(namespace_id)
                            .await
                    })
                    .await
                    .expect("retry forever")?;

                let schema = Backoff::new(&backoff_config)
                    .retry_all_errors("get namespace schema", || async {
                        let mut repos = catalog.repositories().await;
                        match get_schema_by_name(&namespace.name, repos.as_mut()).await {
                            Ok(schema) => Ok(Some(schema)),
                            Err(iox_catalog::interface::Error::NamespaceNotFound { .. }) => {
                                Ok(None)
                            }
                            Err(e) => Err(e),
                        }
                    })
                    .await
                    .expect("retry forever")?;

                Some(Arc::new(CachedNamespace {
                    name: Arc::from(namespace.name),
                    schema: Arc::new(schema),
                }))
            }
        }));
        let backend_from_id = Box::new(TtlBackend::new(
            Box::new(HashMap::new()),
            Arc::new(OptionalValueTtlProvider::new(
                Some(TTL_NON_EXISTING),
                Some(TTL_EXISTING),
            )),
            Arc::clone(&time_provider),
        ));
        let mapper_from_id = |_k: &_, maybe_table: &Option<Arc<CachedNamespace>>| {
            maybe_table.as_ref().map(|n| Arc::clone(&n.name))
        };

        let loader_from_name = Arc::new(FunctionLoader::new(move |namespace_name: Arc<str>| {
            let catalog = Arc::clone(&catalog);
            let backoff_config = backoff_config.clone();

            async move {
                let schema = Backoff::new(&backoff_config)
                    .retry_all_errors("get namespace schema", || async {
                        let mut repos = catalog.repositories().await;
                        match get_schema_by_name(&namespace_name, repos.as_mut()).await {
                            Ok(schema) => Ok(Some(schema)),
                            Err(iox_catalog::interface::Error::NamespaceNotFound { .. }) => {
                                Ok(None)
                            }
                            Err(e) => Err(e),
                        }
                    })
                    .await
                    .expect("retry forever")?;

                Some(Arc::new(CachedNamespace {
                    name: namespace_name,
                    schema: Arc::new(schema),
                }))
            }
        }));
        let backend_from_name = Box::new(TtlBackend::new(
            Box::new(HashMap::new()),
            Arc::new(OptionalValueTtlProvider::new(
                Some(TTL_NON_EXISTING),
                Some(TTL_EXISTING),
            )),
            Arc::clone(&time_provider),
        ));
        let mapper_from_name = |_k: &_, maybe_table: &Option<Arc<CachedNamespace>>| {
            maybe_table.as_ref().map(|n| n.schema.id)
        };

        // cross backends
        let (backend_from_id, backend_from_name) = dual_backends(
            backend_from_id,
            mapper_from_id,
            backend_from_name,
            mapper_from_name,
        );

        let cache_from_id = Cache::new(loader_from_id, Box::new(backend_from_id));
        let cache_from_name = Cache::new(loader_from_name, Box::new(backend_from_name));

        Self {
            cache_from_id,
            cache_from_name,
        }
    }

    /// Get the namespace name for the given namespace ID.
    ///
    /// This either uses a cached value or -- if required -- fetches the mapping from the catalog.
    pub async fn name(&self, id: NamespaceId) -> Option<Arc<str>> {
        self.cache_from_id
            .get(id)
            .await
            .map(|n| Arc::clone(&n.name))
    }

    /// Get namespace schema by name.
    pub async fn schema(&self, name: Arc<str>) -> Option<Arc<NamespaceSchema>> {
        self.cache_from_name
            .get(name)
            .await
            .map(|n| Arc::clone(&n.schema))
    }
}

#[derive(Debug, Clone)]
struct CachedNamespace {
    name: Arc<str>,
    schema: Arc<NamespaceSchema>,
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;

    use crate::cache::test_util::assert_histogram_metric_count;
    use data_types::{ColumnSchema, ColumnType, TableSchema};
    use iox_tests::util::TestCatalog;

    use super::*;

    #[tokio::test]
    async fn test_name() {
        let catalog = TestCatalog::new();

        let ns1 = catalog.create_namespace("ns1").await.namespace.clone();
        let ns2 = catalog.create_namespace("ns2").await.namespace.clone();
        assert_ne!(ns1.id, ns2.id);

        let cache = NamespaceCache::new(
            catalog.catalog(),
            BackoffConfig::default(),
            catalog.time_provider(),
        );

        let name1_a = cache.name(ns1.id).await.unwrap();
        assert_eq!(name1_a.as_ref(), ns1.name.as_str());
        assert_histogram_metric_count(&catalog.metric_registry, "namespace_get_by_id", 1);

        let name2 = cache.name(ns2.id).await.unwrap();
        assert_eq!(name2.as_ref(), ns2.name.as_str());
        assert_histogram_metric_count(&catalog.metric_registry, "namespace_get_by_id", 2);

        let name1_b = cache.name(ns1.id).await.unwrap();
        assert!(Arc::ptr_eq(&name1_a, &name1_b));
        assert_histogram_metric_count(&catalog.metric_registry, "namespace_get_by_id", 2);

        // cache timeout
        catalog.mock_time_provider().inc(TTL_EXISTING);

        let name1_c = cache.name(ns1.id).await.unwrap();
        assert_eq!(name1_c.as_ref(), ns1.name.as_str());
        assert!(!Arc::ptr_eq(&name1_a, &name1_c));
        assert_histogram_metric_count(&catalog.metric_registry, "namespace_get_by_id", 3);
    }

    #[tokio::test]
    async fn test_name_non_existing() {
        let catalog = TestCatalog::new();

        let cache = NamespaceCache::new(
            catalog.catalog(),
            BackoffConfig::default(),
            catalog.time_provider(),
        );

        let none = cache.name(NamespaceId::new(i64::MAX)).await;
        assert!(none.is_none());
        assert_histogram_metric_count(&catalog.metric_registry, "namespace_get_by_id", 1);

        let none = cache.name(NamespaceId::new(i64::MAX)).await;
        assert!(none.is_none());
        assert_histogram_metric_count(&catalog.metric_registry, "namespace_get_by_id", 1);

        // cache timeout
        catalog.mock_time_provider().inc(TTL_NON_EXISTING);

        let none = cache.name(NamespaceId::new(i64::MAX)).await;
        assert!(none.is_none());
        assert_histogram_metric_count(&catalog.metric_registry, "namespace_get_by_id", 2);
    }

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

    #[tokio::test]
    async fn test_shared_cache() {
        let catalog = TestCatalog::new();

        let ns1 = catalog.create_namespace("ns1").await.namespace.clone();
        let ns2 = catalog.create_namespace("ns2").await.namespace.clone();
        assert_ne!(ns1.id, ns2.id);

        let cache = NamespaceCache::new(
            catalog.catalog(),
            BackoffConfig::default(),
            catalog.time_provider(),
        );

        cache.name(ns1.id).await.unwrap();
        assert_histogram_metric_count(&catalog.metric_registry, "namespace_get_by_id", 1);

        // the schema gathering queries the ID via the name again
        assert_histogram_metric_count(&catalog.metric_registry, "namespace_get_by_name", 1);

        cache.schema(Arc::from(String::from("ns2"))).await.unwrap();
        assert_histogram_metric_count(&catalog.metric_registry, "namespace_get_by_name", 2);

        // `name` and `schema` share the same entries
        cache.name(ns2.id).await.unwrap();
        cache.schema(Arc::from(String::from("ns1"))).await.unwrap();
        assert_histogram_metric_count(&catalog.metric_registry, "namespace_get_by_id", 1);
        assert_histogram_metric_count(&catalog.metric_registry, "namespace_get_by_name", 2);
    }
}
