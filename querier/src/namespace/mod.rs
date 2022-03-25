//! Namespace within the whole database.
use crate::{cache::CatalogCache, chunk::ParquetChunkAdapter, table::QuerierTable};
use backoff::{Backoff, BackoffConfig};
use data_types2::NamespaceId;
use iox_catalog::interface::{get_schema_by_name, Catalog};
use object_store::DynObjectStore;
use observability_deps::tracing::warn;
use parking_lot::RwLock;
use query::exec::Executor;
use schema::Schema;
use std::{collections::HashMap, sync::Arc};
use time::TimeProvider;

mod query_access;

#[cfg(test)]
mod test_util;

/// Maps a catalog namespace to all the in-memory resources and sync-state that the querier needs.
///
/// # Data Structures & Sync
/// Tables and schemas are synced ahead of usage (via [`sync`](Self::sync)) because DataFusion does not implement async
/// schema inspection. The actual payload (chunks and tombstones) are only queried on demand.
///
/// Most access to the [IOx Catalog](Catalog) are cached.
#[derive(Debug)]
pub struct QuerierNamespace {
    /// Backoff config for IO operations.
    backoff_config: BackoffConfig,

    /// The catalog.
    catalog: Arc<dyn Catalog>,

    /// Catalog IO cache.
    catalog_cache: Arc<CatalogCache>,

    /// Tables in this namespace.
    tables: RwLock<Arc<HashMap<Arc<str>, Arc<QuerierTable>>>>,

    /// Adapter to create chunks.
    chunk_adapter: Arc<ParquetChunkAdapter>,

    /// ID of this namespace.
    id: NamespaceId,

    /// Name of this namespace.
    name: Arc<str>,

    /// Executor for queries.
    exec: Arc<Executor>,
}

impl QuerierNamespace {
    /// Create new, empty namespace.
    ///
    /// You may call [`sync`](Self::sync) to fill the namespace with chunks.
    pub fn new(
        backoff_config: BackoffConfig,
        chunk_adapter: Arc<ParquetChunkAdapter>,
        name: Arc<str>,
        id: NamespaceId,
        exec: Arc<Executor>,
    ) -> Self {
        let catalog_cache = Arc::clone(chunk_adapter.catalog_cache());
        let catalog = catalog_cache.catalog();

        Self {
            backoff_config,
            catalog,
            catalog_cache: Arc::clone(&catalog_cache),
            tables: RwLock::new(Arc::new(HashMap::new())),
            chunk_adapter,
            id,
            name,
            exec,
        }
    }

    /// Create new empty namespace for testing.
    pub fn new_testing(
        catalog: Arc<dyn Catalog>,
        object_store: Arc<DynObjectStore>,
        metric_registry: Arc<metric::Registry>,
        time_provider: Arc<dyn TimeProvider>,
        name: Arc<str>,
        id: NamespaceId,
        exec: Arc<Executor>,
    ) -> Self {
        let catalog_cache = Arc::new(CatalogCache::new(catalog, Arc::clone(&time_provider)));
        let chunk_adapter = Arc::new(ParquetChunkAdapter::new(
            catalog_cache,
            object_store,
            metric_registry,
            time_provider,
        ));

        Self::new(BackoffConfig::default(), chunk_adapter, name, id, exec)
    }

    /// Namespace name.
    pub fn name(&self) -> Arc<str> {
        Arc::clone(&self.name)
    }

    /// Sync partial namespace state.
    ///
    /// This includes:
    /// - tables
    /// - schemas
    ///
    /// Chunks and tombstones are queried on-demand.
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

        let tables: HashMap<_, _> = catalog_schema_desired
            .tables
            .into_iter()
            .map(|(name, table_schema)| {
                let name = Arc::from(name);
                let id = table_schema.id;
                let schema = Schema::try_from(table_schema).expect("cannot build schema");

                let table = Arc::new(QuerierTable::new(
                    self.backoff_config.clone(),
                    id,
                    Arc::clone(&name),
                    Arc::new(schema),
                    Arc::clone(&self.chunk_adapter),
                ));

                (name, table)
            })
            .collect();

        *self.tables.write() = Arc::new(tables);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::namespace::test_util::querier_namespace;
    use data_types2::ColumnType;
    use iox_tests::util::TestCatalog;
    use schema::{builder::SchemaBuilder, InfluxColumnType, InfluxFieldType};

    #[tokio::test]
    async fn test_sync_namespace_gone() {
        let catalog = TestCatalog::new();

        let catalog_cache = Arc::new(CatalogCache::new(
            catalog.catalog(),
            catalog.time_provider(),
        ));
        let chunk_adapter = Arc::new(ParquetChunkAdapter::new(
            catalog_cache,
            catalog.object_store(),
            catalog.metric_registry(),
            catalog.time_provider(),
        ));
        let querier_namespace = QuerierNamespace::new(
            BackoffConfig::default(),
            chunk_adapter,
            "ns".into(),
            NamespaceId::new(1),
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

        let querier_namespace = querier_namespace(&ns);

        querier_namespace.sync().await;
        assert_eq!(tables(&querier_namespace), Vec::<String>::new());

        ns.create_table("table1").await;
        ns.create_table("table2").await;
        querier_namespace.sync().await;
        assert_eq!(
            tables(&querier_namespace),
            vec![String::from("table1"), String::from("table2")]
        );

        ns.create_table("table3").await;
        querier_namespace.sync().await;
        assert_eq!(
            tables(&querier_namespace),
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

        let querier_namespace = querier_namespace(&ns);

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
    }

    fn sorted<T>(mut v: Vec<T>) -> Vec<T>
    where
        T: Ord,
    {
        v.sort();
        v
    }

    fn tables(querier_namespace: &QuerierNamespace) -> Vec<String> {
        sorted(
            querier_namespace
                .tables
                .read()
                .keys()
                .map(|s| s.to_string())
                .collect(),
        )
    }

    fn schema(querier_namespace: &QuerierNamespace, table: &str) -> Arc<Schema> {
        Arc::clone(querier_namespace.tables.read().get(table).unwrap().schema())
    }
}
