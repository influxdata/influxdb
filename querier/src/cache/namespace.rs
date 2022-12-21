//! Namespace cache.

use backoff::{Backoff, BackoffConfig};
use cache_system::{
    backend::policy::{
        lru::{LruPolicy, ResourcePool},
        refresh::{OptionalValueRefreshDurationProvider, RefreshPolicy},
        remove_if::{RemoveIfHandle, RemoveIfPolicy},
        ttl::{OptionalValueTtlProvider, TtlPolicy},
        PolicyBackend,
    },
    cache::{driver::CacheDriver, metrics::CacheWithMetrics, Cache},
    loader::{metrics::MetricsLoader, FunctionLoader},
    resource_consumption::FunctionEstimator,
};
use data_types::{ColumnId, NamespaceId, NamespaceSchema, TableId, TableSchema};
use iox_catalog::interface::{get_schema_by_name, Catalog};
use iox_time::TimeProvider;
use schema::Schema;
use std::{
    collections::{HashMap, HashSet},
    mem::{size_of, size_of_val},
    sync::Arc,
    time::Duration,
};
use tokio::runtime::Handle;
use trace::span::Span;

use super::ram::RamSize;

/// Duration to keep existing namespaces.
pub const TTL_EXISTING: Duration = Duration::from_secs(300);

/// When to refresh an existing namespace.
///
/// This policy is chosen to:
/// 1. decorrelate refreshes which smooths out catalog load
/// 2. refresh commonly accessed keys less frequently
pub const REFRESH_EXISTING: BackoffConfig = BackoffConfig {
    init_backoff: Duration::from_secs(30),
    max_backoff: Duration::MAX,
    base: 2.0,
    deadline: None,
};

/// Duration to keep non-existing namespaces.
///
/// TODO(marco): Caching non-existing namespaces is virtually disabled until
///              <https://github.com/influxdata/influxdb_iox/issues/4617> is implemented because the flux integration
///              tests fail otherwise, see <https://github.com/influxdata/conductor/issues/997>.
///              The very short duration is only used so that tests can assert easily that non-existing entries have
///              SOME TTL mechanism attached.
///              The TTL is not relevant for prod at the moment because other layers should prevent/filter queries for
///              non-existing namespaces.
pub const TTL_NON_EXISTING: Duration = Duration::from_nanos(1);

const CACHE_ID: &str = "namespace";

type CacheT = Box<
    dyn Cache<
        K = Arc<str>,
        V = Option<Arc<CachedNamespace>>,
        GetExtra = ((), Option<Span>),
        PeekExtra = ((), Option<Span>),
    >,
>;

/// Cache for namespace-related attributes.
#[derive(Debug)]
pub struct NamespaceCache {
    cache: CacheT,
    remove_if_handle: RemoveIfHandle<Arc<str>, Option<Arc<CachedNamespace>>>,
}

impl NamespaceCache {
    /// Create new empty cache.
    pub fn new(
        catalog: Arc<dyn Catalog>,
        backoff_config: BackoffConfig,
        time_provider: Arc<dyn TimeProvider>,
        metric_registry: &metric::Registry,
        ram_pool: Arc<ResourcePool<RamSize>>,
        handle: &Handle,
        testing: bool,
    ) -> Self {
        let loader = FunctionLoader::new(move |namespace_name: Arc<str>, _extra: ()| {
            let catalog = Arc::clone(&catalog);
            let backoff_config = backoff_config.clone();

            async move {
                let schema = Backoff::new(&backoff_config)
                    .retry_all_errors("get namespace schema", || async {
                        let mut repos = catalog.repositories().await;
                        match get_schema_by_name(&namespace_name, repos.as_mut()).await {
                            Ok(schema) => Ok(Some(schema)),
                            Err(iox_catalog::interface::Error::NamespaceNotFoundByName {
                                ..
                            }) => Ok(None),
                            Err(e) => Err(e),
                        }
                    })
                    .await
                    .expect("retry forever")?;

                Some(Arc::new(schema.into()))
            }
        });
        let loader = Arc::new(MetricsLoader::new(
            loader,
            CACHE_ID,
            Arc::clone(&time_provider),
            metric_registry,
            testing,
        ));

        let mut backend = PolicyBackend::hashmap_backed(Arc::clone(&time_provider));
        backend.add_policy(TtlPolicy::new(
            Arc::new(OptionalValueTtlProvider::new(
                Some(TTL_NON_EXISTING),
                Some(TTL_EXISTING),
            )),
            CACHE_ID,
            metric_registry,
        ));
        backend.add_policy(RefreshPolicy::new(
            Arc::clone(&time_provider),
            Arc::new(OptionalValueRefreshDurationProvider::new(
                None,
                Some(REFRESH_EXISTING),
            )),
            Arc::clone(&loader) as _,
            CACHE_ID,
            metric_registry,
            handle,
        ));

        let (constructor, remove_if_handle) =
            RemoveIfPolicy::create_constructor_and_handle(CACHE_ID, metric_registry);
        backend.add_policy(constructor);
        backend.add_policy(LruPolicy::new(
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

        let cache = CacheDriver::new(loader, backend);
        let cache = Box::new(CacheWithMetrics::new(
            cache,
            CACHE_ID,
            time_provider,
            metric_registry,
        ));

        Self {
            cache,
            remove_if_handle,
        }
    }

    /// Get namespace schema by name.
    ///
    /// Expire namespace if the cached schema does NOT cover the given set of columns. The set is given as a list of
    /// pairs of table name and column set.
    pub async fn get(
        &self,
        name: Arc<str>,
        should_cover: &[(&str, &HashSet<ColumnId>)],
        span: Option<Span>,
    ) -> Option<Arc<CachedNamespace>> {
        self.remove_if_handle
            .remove_if_and_get(
                &self.cache,
                name,
                |cached_namespace| {
                    if let Some(namespace) = cached_namespace.as_ref() {
                        should_cover.iter().any(|(table_name, columns)| {
                            if let Some(table) = namespace.tables.get(*table_name) {
                                columns
                                    .iter()
                                    .any(|col| !table.column_id_map.contains_key(col))
                            } else {
                                // table unknown => need to update
                                true
                            }
                        })
                    } else {
                        // namespace unknown => need to update if should cover anything
                        !should_cover.is_empty()
                    }
                },
                ((), span),
            )
            .await
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CachedTable {
    pub id: TableId,
    pub schema: Arc<Schema>,
    pub column_id_map: HashMap<ColumnId, Arc<str>>,
    pub column_id_map_rev: HashMap<Arc<str>, ColumnId>,
    pub primary_key_column_ids: Vec<ColumnId>,
}

impl CachedTable {
    /// RAM-bytes EXCLUDING `self`.
    fn size(&self) -> usize {
        self.schema.estimate_size()
            + (self.column_id_map.capacity() * size_of::<(ColumnId, Arc<str>)>())
            + self
                .column_id_map
                .values()
                .map(|name| name.len())
                .sum::<usize>()
            + (self.column_id_map_rev.capacity() * size_of::<(Arc<str>, ColumnId)>())
            + self
                .column_id_map_rev
                .keys()
                .map(|name| name.len())
                .sum::<usize>()
            + (self.primary_key_column_ids.capacity() * size_of::<ColumnId>())
    }
}

impl From<TableSchema> for CachedTable {
    fn from(table: TableSchema) -> Self {
        let mut column_id_map: HashMap<ColumnId, Arc<str>> = table
            .columns
            .iter()
            .map(|(name, c)| (c.id, Arc::from(name.clone())))
            .collect();
        column_id_map.shrink_to_fit();

        let id = table.id;
        let schema: Arc<Schema> = Arc::new(table.try_into().expect("Catalog table schema broken"));

        let mut column_id_map_rev: HashMap<Arc<str>, ColumnId> = column_id_map
            .iter()
            .map(|(v, k)| (Arc::clone(k), *v))
            .collect();
        column_id_map_rev.shrink_to_fit();

        let mut primary_key_column_ids: Vec<ColumnId> = schema
            .primary_key()
            .into_iter()
            .map(|name| {
                *column_id_map_rev
                    .get(name)
                    .unwrap_or_else(|| panic!("primary key not known?!: {name}"))
            })
            .collect();
        primary_key_column_ids.shrink_to_fit();

        Self {
            id,
            schema,
            column_id_map,
            column_id_map_rev,
            primary_key_column_ids,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CachedNamespace {
    pub id: NamespaceId,
    pub retention_period: Option<Duration>,
    pub tables: HashMap<Arc<str>, Arc<CachedTable>>,
}

impl CachedNamespace {
    /// RAM-bytes EXCLUDING `self`.
    fn size(&self) -> usize {
        self.tables.capacity() * size_of::<(Arc<str>, Arc<CachedTable>)>()
            + self
                .tables
                .iter()
                .map(|(name, table)| name.len() + table.size())
                .sum::<usize>()
    }
}

impl From<NamespaceSchema> for CachedNamespace {
    fn from(ns: NamespaceSchema) -> Self {
        let mut tables: HashMap<Arc<str>, Arc<CachedTable>> = ns
            .tables
            .into_iter()
            .map(|(name, table)| {
                let table: CachedTable = table.into();
                (Arc::from(name), Arc::new(table))
            })
            .collect();
        tables.shrink_to_fit();

        let retention_period = ns
            .retention_period_ns
            .map(|retention| Duration::from_nanos(retention as u64));
        Self {
            id: ns.id,
            retention_period,
            tables,
        }
    }
}

#[cfg(test)]
mod tests {
    use crate::cache::{ram::test_util::test_ram_pool, test_util::assert_histogram_metric_count};
    use arrow::datatypes::DataType;
    use data_types::ColumnType;
    use iox_tests::util::TestCatalog;
    use schema::SchemaBuilder;

    use super::*;

    #[tokio::test]
    async fn test_schema() {
        let catalog = TestCatalog::new();

        let ns1 = catalog.create_namespace_1hr_retention("ns1").await;
        let ns2 = catalog.create_namespace_1hr_retention("ns2").await;
        assert_ne!(ns1.namespace.id, ns2.namespace.id);

        let table11 = ns1.create_table("table1").await;
        let table12 = ns1.create_table("table2").await;
        let table21 = ns2.create_table("table1").await;

        let col111 = table11.create_column("col1", ColumnType::I64).await;
        let col112 = table11.create_column("col2", ColumnType::Tag).await;
        let col113 = table11.create_column("time", ColumnType::Time).await;
        let col121 = table12.create_column("col1", ColumnType::F64).await;
        let col122 = table12.create_column("time", ColumnType::Time).await;
        let col211 = table21.create_column("time", ColumnType::Time).await;

        let cache = NamespaceCache::new(
            catalog.catalog(),
            BackoffConfig::default(),
            catalog.time_provider(),
            &catalog.metric_registry(),
            test_ram_pool(),
            &Handle::current(),
            true,
        );

        let actual_ns_1_a = cache
            .get(Arc::from(String::from("ns1")), &[], None)
            .await
            .unwrap();
        let retention_period = ns1
            .namespace
            .retention_period_ns
            .map(|retention| Duration::from_nanos(retention as u64));
        let expected_ns_1 = CachedNamespace {
            id: ns1.namespace.id,
            retention_period,
            tables: HashMap::from([
                (
                    Arc::from("table1"),
                    Arc::new(CachedTable {
                        id: table11.table.id,
                        schema: Arc::new(
                            SchemaBuilder::new()
                                .field("col1", DataType::Int64)
                                .unwrap()
                                .tag("col2")
                                .timestamp()
                                .build()
                                .unwrap(),
                        ),
                        column_id_map: HashMap::from([
                            (col111.column.id, Arc::from(col111.column.name.clone())),
                            (col112.column.id, Arc::from(col112.column.name.clone())),
                            (col113.column.id, Arc::from(col113.column.name.clone())),
                        ]),
                        column_id_map_rev: HashMap::from([
                            (Arc::from(col111.column.name.clone()), col111.column.id),
                            (Arc::from(col112.column.name.clone()), col112.column.id),
                            (Arc::from(col113.column.name.clone()), col113.column.id),
                        ]),
                        primary_key_column_ids: vec![col112.column.id, col113.column.id],
                    }),
                ),
                (
                    Arc::from("table2"),
                    Arc::new(CachedTable {
                        id: table12.table.id,
                        schema: Arc::new(
                            SchemaBuilder::new()
                                .field("col1", DataType::Float64)
                                .unwrap()
                                .timestamp()
                                .build()
                                .unwrap(),
                        ),
                        column_id_map: HashMap::from([
                            (col121.column.id, Arc::from(col121.column.name.clone())),
                            (col122.column.id, Arc::from(col122.column.name.clone())),
                        ]),
                        column_id_map_rev: HashMap::from([
                            (Arc::from(col121.column.name.clone()), col121.column.id),
                            (Arc::from(col122.column.name.clone()), col122.column.id),
                        ]),
                        primary_key_column_ids: vec![col122.column.id],
                    }),
                ),
            ]),
        };
        assert_eq!(actual_ns_1_a.as_ref(), &expected_ns_1);
        assert_histogram_metric_count(&catalog.metric_registry, "namespace_get_by_name", 1);

        let actual_ns_2 = cache
            .get(Arc::from(String::from("ns2")), &[], None)
            .await
            .unwrap();
        let retention_period = ns2
            .namespace
            .retention_period_ns
            .map(|retention| Duration::from_nanos(retention as u64));
        let expected_ns_2 = CachedNamespace {
            id: ns2.namespace.id,
            retention_period,
            tables: HashMap::from([(
                Arc::from("table1"),
                Arc::new(CachedTable {
                    id: table21.table.id,
                    schema: Arc::new(SchemaBuilder::new().timestamp().build().unwrap()),
                    column_id_map: HashMap::from([(
                        col211.column.id,
                        Arc::from(col211.column.name.clone()),
                    )]),
                    column_id_map_rev: HashMap::from([(
                        Arc::from(col211.column.name.clone()),
                        col211.column.id,
                    )]),
                    primary_key_column_ids: vec![col211.column.id],
                }),
            )]),
        };
        assert_eq!(actual_ns_2.as_ref(), &expected_ns_2);
        assert_histogram_metric_count(&catalog.metric_registry, "namespace_get_by_name", 2);

        let actual_ns_1_b = cache
            .get(Arc::from(String::from("ns1")), &[], None)
            .await
            .unwrap();
        assert!(Arc::ptr_eq(&actual_ns_1_a, &actual_ns_1_b));
        assert_histogram_metric_count(&catalog.metric_registry, "namespace_get_by_name", 2);
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
            &Handle::current(),
            true,
        );

        let none = cache.get(Arc::from(String::from("foo")), &[], None).await;
        assert!(none.is_none());
        assert_histogram_metric_count(&catalog.metric_registry, "namespace_get_by_name", 1);

        let none = cache.get(Arc::from(String::from("foo")), &[], None).await;
        assert!(none.is_none());
        assert_histogram_metric_count(&catalog.metric_registry, "namespace_get_by_name", 1);
    }

    #[tokio::test]
    async fn test_expiration() {
        let catalog = TestCatalog::new();

        let cache = NamespaceCache::new(
            catalog.catalog(),
            BackoffConfig::default(),
            catalog.time_provider(),
            &catalog.metric_registry(),
            test_ram_pool(),
            &Handle::current(),
            true,
        );

        // ========== namespace unknown ==========
        assert!(cache.get(Arc::from("ns1"), &[], None).await.is_none());
        assert_histogram_metric_count(&catalog.metric_registry, "namespace_get_by_name", 1);

        assert!(cache.get(Arc::from("ns1"), &[], None).await.is_none());
        assert_histogram_metric_count(&catalog.metric_registry, "namespace_get_by_name", 1);

        assert!(cache
            .get(Arc::from("ns1"), &[("t1", &HashSet::from([]))], None)
            .await
            .is_none());
        assert_histogram_metric_count(&catalog.metric_registry, "namespace_get_by_name", 2);

        // ========== table unknown ==========
        let ns1 = catalog.create_namespace_1hr_retention("ns1").await;

        assert!(cache
            .get(Arc::from("ns1"), &[("t1", &HashSet::from([]))], None)
            .await
            .is_some());
        assert_histogram_metric_count(&catalog.metric_registry, "namespace_get_by_name", 3);

        assert!(cache
            .get(Arc::from("ns1"), &[("t1", &HashSet::from([]))], None)
            .await
            .is_some());
        assert_histogram_metric_count(&catalog.metric_registry, "namespace_get_by_name", 4);

        // ========== no columns ==========
        let t1 = ns1.create_table("t1").await;

        assert!(cache
            .get(Arc::from("ns1"), &[("t1", &HashSet::from([]))], None)
            .await
            .is_some());
        assert_histogram_metric_count(&catalog.metric_registry, "namespace_get_by_name", 5);

        assert!(cache
            .get(Arc::from("ns1"), &[("t1", &HashSet::from([]))], None)
            .await
            .is_some());
        assert_histogram_metric_count(&catalog.metric_registry, "namespace_get_by_name", 5);

        // ========== some columns ==========
        let c1 = t1.create_column("c1", ColumnType::Bool).await;
        let c2 = t1.create_column("c2", ColumnType::Bool).await;

        assert!(cache
            .get(Arc::from("ns1"), &[("t1", &HashSet::from([]))], None)
            .await
            .is_some());
        assert_histogram_metric_count(&catalog.metric_registry, "namespace_get_by_name", 5);

        assert!(cache
            .get(
                Arc::from("ns1"),
                &[("t1", &HashSet::from([c1.column.id]))],
                None
            )
            .await
            .is_some());
        assert_histogram_metric_count(&catalog.metric_registry, "namespace_get_by_name", 6);

        assert!(cache
            .get(
                Arc::from("ns1"),
                &[("t1", &HashSet::from([c2.column.id]))],
                None
            )
            .await
            .is_some());
        assert_histogram_metric_count(&catalog.metric_registry, "namespace_get_by_name", 6);
    }
}
