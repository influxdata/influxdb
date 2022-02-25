//! Namespace within the whole database.

use crate::{cache::CatalogCache, chunk::ParquetChunkAdapter};
use async_trait::async_trait;
use backoff::{Backoff, BackoffConfig};
use data_types2::{ChunkSummary, NamespaceId, PartitionAddr};
use datafusion::catalog::{catalog::CatalogProvider, schema::SchemaProvider};
use db::{access::QueryCatalogAccess, catalog::Catalog as DbCatalog, chunk::DbChunk};
use iox_catalog::interface::{get_schema_by_name, Catalog};
use job_registry::JobRegistry;
use object_store::ObjectStore;
use observability_deps::tracing::{info, warn};
use parking_lot::lock_api::RwLockUpgradableReadGuard;
use predicate::{rpc_predicate::QueryDatabaseMeta, Predicate};
use query::{
    exec::{ExecutionContextProvider, Executor, ExecutorType, IOxExecutionContext},
    QueryCompletedToken, QueryDatabase, QueryText,
};
use schema::Schema;
use std::{any::Any, collections::HashSet, sync::Arc};
use time::TimeProvider;
use trace::ctx::SpanContext;

/// Maps a catalog namespace to all the in-memory resources and sync-state that the querier needs.
///
/// # Data Structures & Sync
/// The in-memory data structure that is used for queries is the [`DbCatalog`] (via [`QueryCatalogAccess`]). The
/// main (and currently only) cluster-wide data source is the [IOx Catalog](Catalog). The cluster-wide view and
/// in-memory data structure are synced regularly via [`sync`](Self::sync).
///
/// To speed up the sync process and reduce the load on the [IOx Catalog](Catalog) we try to use rather large-scoped
/// queries as well as a [`CatalogCache`].
#[derive(Debug)]
pub struct QuerierNamespace {
    /// Backoff config for IO operations.
    backoff_config: BackoffConfig,

    /// The catalog.
    catalog: Arc<dyn Catalog>,

    /// Old-gen DB catalog.
    db_catalog: Arc<DbCatalog>,

    /// Adapter to create old-gen chunks.
    chunk_adapter: ParquetChunkAdapter,

    /// ID of this namespace.
    id: NamespaceId,

    /// Name of this namespace.
    name: Arc<str>,

    /// Catalog interface for query
    catalog_access: Arc<QueryCatalogAccess>,

    /// Executor for queries.
    exec: Arc<Executor>,
}

impl QuerierNamespace {
    /// Create new, empty namespace.
    ///
    /// You may call [`sync`](Self::sync) to fill the namespace with chunks.
    pub fn new(
        catalog_cache: Arc<CatalogCache>,
        name: Arc<str>,
        id: NamespaceId,
        metric_registry: Arc<metric::Registry>,
        object_store: Arc<ObjectStore>,
        time_provider: Arc<dyn TimeProvider>,
        exec: Arc<Executor>,
    ) -> Self {
        let catalog = catalog_cache.catalog();
        let db_catalog = Arc::new(DbCatalog::new(
            Arc::clone(&name),
            Arc::clone(&metric_registry),
            Arc::clone(&time_provider),
        ));

        // no real job registration system
        let jobs = Arc::new(JobRegistry::new(
            Arc::clone(&metric_registry),
            Arc::clone(&time_provider),
        ));
        let catalog_access = Arc::new(QueryCatalogAccess::new(
            name.to_string(),
            Arc::clone(&db_catalog),
            jobs,
            Arc::clone(&time_provider),
            &metric_registry,
        ));

        Self {
            backoff_config: BackoffConfig::default(),
            catalog,
            db_catalog,
            chunk_adapter: ParquetChunkAdapter::new(
                catalog_cache,
                object_store,
                metric_registry,
                time_provider,
            ),
            id,
            name,
            catalog_access,
            exec,
        }
    }

    /// Namespace name.
    pub fn name(&self) -> Arc<str> {
        Arc::clone(&self.name)
    }

    /// Sync tables and chunks.
    ///
    /// Should be called regularly.
    pub async fn sync(&self) {
        let catalog_schema_desired = Backoff::new(&self.backoff_config)
            .retry_all_errors("get schema", || async {
                let mut repos = self.catalog.repositories().await;
                match get_schema_by_name(&self.name, repos.as_mut()).await {
                    Ok(schema) => Ok(Some(schema)),
                    Err(iox_catalog::interface::Error::NamespaceNotFound { .. }) => Ok(None),
                    Err(e) => Err(e),
                }
            })
            .await
            .expect("retry forever");
        let catalog_schema_desired = match catalog_schema_desired {
            Some(schema) => schema,
            None => {
                warn!(
                    namespace = self.name.as_ref(),
                    "Cannot sync namespace because it is gone",
                );
                return;
            }
        };

        let table_names_actual: HashSet<_> = self.db_catalog.table_names().into_iter().collect();
        let to_delete: Vec<_> = table_names_actual
            .iter()
            .filter_map(|table| {
                (!catalog_schema_desired.tables.contains_key(table)).then(|| table.clone())
            })
            .collect();
        let to_add: Vec<_> = catalog_schema_desired
            .tables
            .keys()
            .filter_map(|table| (!table_names_actual.contains(table)).then(|| table.clone()))
            .collect();
        info!(
            add = to_add.len(),
            delete = to_delete.len(),
            actual = table_names_actual.len(),
            desired = catalog_schema_desired.tables.len(),
            namespace = self.name.as_ref(),
            "Syncing tables",
        );

        for _name in to_delete {
            // TODO: implement and test table deletion
            unimplemented!("table deletion");
        }

        for name in to_add {
            // we don't need the returned lock so we immediately drop it (otherwise clippy will also complain)
            drop(self.db_catalog.get_or_create_table(name));
        }

        for (name, table_schema) in catalog_schema_desired.tables {
            let table = match self.db_catalog.table(&name) {
                Ok(table) => table,
                Err(e) => {
                    // this might happen if some other process (e.g. management API) just removed the table
                    warn!(
                        %e,
                        namespace = self.name.as_ref(),
                        table = name.as_str(),
                        "Cannot check table schema",
                    );
                    continue;
                }
            };

            let desired_schema = Schema::try_from(table_schema).expect("cannot build schema");

            let schema = table.schema();
            let schema = schema.upgradable_read();
            if schema.as_ref() != &desired_schema {
                let mut schema = RwLockUpgradableReadGuard::upgrade(schema);
                info!(
                    namespace = self.name.as_ref(),
                    table = name.as_str(),
                    "table schema update",
                );
                *schema = Arc::new(desired_schema);
            }
        }

        // TODO: sync chunks
    }
}

impl QueryDatabaseMeta for QuerierNamespace {
    fn table_names(&self) -> Vec<String> {
        self.catalog_access.table_names()
    }

    fn table_schema(&self, table_name: &str) -> Option<Arc<Schema>> {
        self.catalog_access.table_schema(table_name)
    }
}

#[async_trait]
impl QueryDatabase for QuerierNamespace {
    type Chunk = DbChunk;

    fn partition_addrs(&self) -> Vec<PartitionAddr> {
        self.catalog_access.partition_addrs()
    }

    fn chunks(&self, table_name: &str, predicate: &Predicate) -> Vec<Arc<Self::Chunk>> {
        self.catalog_access.chunks(table_name, predicate)
    }

    fn chunk_summaries(&self) -> Vec<ChunkSummary> {
        self.catalog_access.chunk_summaries()
    }

    fn record_query(
        &self,
        query_type: impl Into<String>,
        query_text: QueryText,
    ) -> QueryCompletedToken {
        self.catalog_access.record_query(query_type, query_text)
    }
}

impl CatalogProvider for QuerierNamespace {
    fn as_any(&self) -> &dyn Any {
        self as &dyn Any
    }

    fn schema_names(&self) -> Vec<String> {
        self.catalog_access.schema_names()
    }

    fn schema(&self, name: &str) -> Option<Arc<dyn SchemaProvider>> {
        self.catalog_access.schema(name)
    }
}

impl ExecutionContextProvider for QuerierNamespace {
    fn new_query_context(self: &Arc<Self>, span_ctx: Option<SpanContext>) -> IOxExecutionContext {
        self.exec
            .new_execution_config(ExecutorType::Query)
            .with_default_catalog(Arc::<Self>::clone(self))
            .with_span_context(span_ctx)
            .build()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::test_util::{TestCatalog, TestNamespace};
    use data_types2::ColumnType;
    use schema::{builder::SchemaBuilder, InfluxColumnType, InfluxFieldType};

    #[tokio::test]
    async fn test_sync_namespace_gone() {
        let catalog = TestCatalog::new();

        let querier_namespace = QuerierNamespace::new(
            Arc::new(CatalogCache::new(catalog.catalog())),
            "ns".into(),
            NamespaceId::new(1),
            catalog.metric_registry(),
            catalog.object_store(),
            catalog.time_provider(),
            catalog.exec(),
        );

        // The container (`QuerierDatabase`) should prune the namespace if it's gone, however the `sync` might still be
        // in-progress and must not block or panic.
        querier_namespace.sync().await;
    }

    #[tokio::test]
    async fn test_sync_tables() {
        let catalog = TestCatalog::new();

        let ns = catalog.create_namespace("ns").await;

        let querier_namespace = querier_namespace(&catalog, &ns);

        querier_namespace.sync().await;
        assert_eq!(
            querier_namespace.db_catalog.table_names(),
            Vec::<String>::new()
        );

        ns.create_table("table1").await;
        ns.create_table("table2").await;
        querier_namespace.sync().await;
        assert_eq!(
            sorted(querier_namespace.db_catalog.table_names()),
            vec![String::from("table1"), String::from("table2")]
        );

        ns.create_table("table3").await;
        querier_namespace.sync().await;
        assert_eq!(
            sorted(querier_namespace.db_catalog.table_names()),
            vec![
                String::from("table1"),
                String::from("table2"),
                String::from("table3")
            ]
        );
    }

    #[tokio::test]
    async fn test_sync_schemas() {
        let catalog = TestCatalog::new();

        let ns = catalog.create_namespace("ns").await;
        let table = ns.create_table("table").await;

        let querier_namespace = querier_namespace(&catalog, &ns);

        querier_namespace.sync().await;
        let expected_schema = SchemaBuilder::new().build().unwrap();
        let actual_schema = schema(&querier_namespace, "table");
        assert_eq!(actual_schema.as_ref(), &expected_schema,);

        table.create_column("col1", ColumnType::I64).await;
        table.create_column("col2", ColumnType::Bool).await;
        table.create_column("col3", ColumnType::Tag).await;
        querier_namespace.sync().await;
        let expected_schema = SchemaBuilder::new()
            .influx_column("col1", InfluxColumnType::Field(InfluxFieldType::Integer))
            .influx_column("col2", InfluxColumnType::Field(InfluxFieldType::Boolean))
            .influx_column("col3", InfluxColumnType::Tag)
            .build()
            .unwrap();
        let actual_schema = schema(&querier_namespace, "table");
        assert_eq!(actual_schema.as_ref(), &expected_schema,);

        table.create_column("col4", ColumnType::Tag).await;
        table.create_column("col5", ColumnType::Time).await;
        querier_namespace.sync().await;
        let expected_schema = SchemaBuilder::new()
            .influx_column("col1", InfluxColumnType::Field(InfluxFieldType::Integer))
            .influx_column("col2", InfluxColumnType::Field(InfluxFieldType::Boolean))
            .influx_column("col3", InfluxColumnType::Tag)
            .influx_column("col4", InfluxColumnType::Tag)
            .influx_column("col5", InfluxColumnType::Timestamp)
            .build()
            .unwrap();
        let actual_schema = schema(&querier_namespace, "table");
        assert_eq!(actual_schema.as_ref(), &expected_schema,);

        // schema not updated => Arc not changed
        querier_namespace.sync().await;
        let actual_schema2 = schema(&querier_namespace, "table");
        assert!(Arc::ptr_eq(&actual_schema, &actual_schema2));
    }

    fn querier_namespace(catalog: &Arc<TestCatalog>, ns: &Arc<TestNamespace>) -> QuerierNamespace {
        QuerierNamespace::new(
            Arc::new(CatalogCache::new(catalog.catalog())),
            ns.namespace.name.clone().into(),
            ns.namespace.id,
            catalog.metric_registry(),
            catalog.object_store(),
            catalog.time_provider(),
            catalog.exec(),
        )
    }

    fn sorted<T>(mut v: Vec<T>) -> Vec<T>
    where
        T: Ord,
    {
        v.sort();
        v
    }

    fn schema(querier_namespace: &QuerierNamespace, table: &str) -> Arc<Schema> {
        Arc::clone(
            &querier_namespace
                .db_catalog
                .table(table)
                .unwrap()
                .schema()
                .read(),
        )
    }
}
