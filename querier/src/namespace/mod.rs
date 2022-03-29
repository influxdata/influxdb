//! Namespace within the whole database.
use crate::{cache::CatalogCache, chunk::ParquetChunkAdapter, table::QuerierTable};
use backoff::BackoffConfig;
use data_types2::{NamespaceId, NamespaceSchema};
use iox_catalog::interface::Catalog;
use object_store::DynObjectStore;
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
/// Tables and schemas are created when [`QuerierNamespace`] is created because DataFusion does not implement async
/// schema inspection. The actual payload (chunks and tombstones) are only queried on demand.
///
/// Most access to the [IOx Catalog](Catalog) are cached.
#[derive(Debug)]
pub struct QuerierNamespace {
    /// ID of this namespace.
    id: NamespaceId,

    /// Name of this namespace.
    name: Arc<str>,

    /// Tables in this namespace.
    tables: Arc<HashMap<Arc<str>, Arc<QuerierTable>>>,

    /// Executor for queries.
    exec: Arc<Executor>,
}

impl QuerierNamespace {
    /// Create new namespace for given schema.
    pub fn new(
        backoff_config: BackoffConfig,
        chunk_adapter: Arc<ParquetChunkAdapter>,
        schema: Arc<NamespaceSchema>,
        name: Arc<str>,
        exec: Arc<Executor>,
    ) -> Self {
        let tables: HashMap<_, _> = schema
            .tables
            .iter()
            .map(|(name, table_schema)| {
                let name = Arc::from(name.clone());
                let id = table_schema.id;
                let schema = Schema::try_from(table_schema.clone()).expect("cannot build schema");

                let table = Arc::new(QuerierTable::new(
                    backoff_config.clone(),
                    id,
                    Arc::clone(&name),
                    Arc::new(schema),
                    Arc::clone(&chunk_adapter),
                ));

                (name, table)
            })
            .collect();

        let id = schema.id;

        Self {
            id,
            name,
            tables: Arc::new(tables),
            exec,
        }
    }

    /// Create new namespace for given schema, for testing.
    pub fn new_testing(
        catalog: Arc<dyn Catalog>,
        object_store: Arc<DynObjectStore>,
        metric_registry: Arc<metric::Registry>,
        time_provider: Arc<dyn TimeProvider>,
        name: Arc<str>,
        schema: Arc<NamespaceSchema>,
        exec: Arc<Executor>,
    ) -> Self {
        let catalog_cache = Arc::new(CatalogCache::new(catalog, Arc::clone(&time_provider)));
        let chunk_adapter = Arc::new(ParquetChunkAdapter::new(
            catalog_cache,
            object_store,
            metric_registry,
            time_provider,
        ));

        Self::new(BackoffConfig::default(), chunk_adapter, schema, name, exec)
    }

    /// Namespace name.
    pub fn name(&self) -> Arc<str> {
        Arc::clone(&self.name)
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
    async fn test_sync_tables() {
        let catalog = TestCatalog::new();

        let ns = catalog.create_namespace("ns").await;

        let qns = querier_namespace(&ns).await;
        assert_eq!(tables(&qns), Vec::<String>::new());

        ns.create_table("table1").await;
        ns.create_table("table2").await;
        let qns = querier_namespace(&ns).await;
        assert_eq!(
            tables(&qns),
            vec![String::from("table1"), String::from("table2")]
        );

        ns.create_table("table3").await;
        let qns = querier_namespace(&ns).await;
        assert_eq!(
            tables(&qns),
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

        let qns = querier_namespace(&ns).await;
        let expected_schema = SchemaBuilder::new().build().unwrap();
        let actual_schema = schema(&qns, "table");
        assert_eq!(actual_schema.as_ref(), &expected_schema,);

        table.create_column("col1", ColumnType::I64).await;
        table.create_column("col2", ColumnType::Bool).await;
        table.create_column("col3", ColumnType::Tag).await;
        let qns = querier_namespace(&ns).await;
        let expected_schema = SchemaBuilder::new()
            .influx_column("col1", InfluxColumnType::Field(InfluxFieldType::Integer))
            .influx_column("col2", InfluxColumnType::Field(InfluxFieldType::Boolean))
            .influx_column("col3", InfluxColumnType::Tag)
            .build()
            .unwrap();
        let actual_schema = schema(&qns, "table");
        assert_eq!(actual_schema.as_ref(), &expected_schema,);

        table.create_column("col4", ColumnType::Tag).await;
        table.create_column("col5", ColumnType::Time).await;
        let qns = querier_namespace(&ns).await;
        let expected_schema = SchemaBuilder::new()
            .influx_column("col1", InfluxColumnType::Field(InfluxFieldType::Integer))
            .influx_column("col2", InfluxColumnType::Field(InfluxFieldType::Boolean))
            .influx_column("col3", InfluxColumnType::Tag)
            .influx_column("col4", InfluxColumnType::Tag)
            .influx_column("col5", InfluxColumnType::Timestamp)
            .build()
            .unwrap();
        let actual_schema = schema(&qns, "table");
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
                .keys()
                .map(|s| s.to_string())
                .collect(),
        )
    }

    fn schema(querier_namespace: &QuerierNamespace, table: &str) -> Arc<Schema> {
        Arc::clone(querier_namespace.tables.get(table).unwrap().schema())
    }
}
