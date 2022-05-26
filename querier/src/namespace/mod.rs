//! Namespace within the whole database.

use crate::{
    cache::CatalogCache, chunk::ParquetChunkAdapter, ingester::IngesterConnection,
    query_log::QueryLog, table::QuerierTable,
};
use data_types::{NamespaceId, NamespaceSchema};
use iox_query::exec::Executor;
use parquet_file::storage::ParquetStorage;
use schema::Schema;
use std::{collections::HashMap, sync::Arc};

mod query_access;

#[cfg(test)]
mod test_util;

/// Maps a catalog namespace to all the in-memory resources and sync-state that the querier needs.
///
/// # Data Structures & Sync
/// Tables and schemas are created when [`QuerierNamespace`] is created because DataFusion does not implement async
/// schema inspection. The actual payload (chunks and tombstones) are only queried on demand.
///
/// Most access to the [IOx Catalog](iox_catalog::interface::Catalog) are cached via [`CatalogCache`].
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

    /// Catalog cache
    catalog_cache: Arc<CatalogCache>,

    /// Query log.
    query_log: Arc<QueryLog>,
}

impl QuerierNamespace {
    /// Create new namespace for given schema.
    pub fn new(
        chunk_adapter: Arc<ParquetChunkAdapter>,
        schema: Arc<NamespaceSchema>,
        name: Arc<str>,
        exec: Arc<Executor>,
        ingester_connection: Arc<dyn IngesterConnection>,
        query_log: Arc<QueryLog>,
    ) -> Self {
        let tables: HashMap<_, _> = schema
            .tables
            .iter()
            .map(|(table_name, table_schema)| {
                let table_name = Arc::from(table_name.clone());
                let id = table_schema.id;
                let schema = Schema::try_from(table_schema.clone()).expect("cannot build schema");

                let table = Arc::new(QuerierTable::new(
                    Arc::clone(&name),
                    id,
                    Arc::clone(&table_name),
                    Arc::new(schema),
                    Arc::clone(&ingester_connection),
                    Arc::clone(&chunk_adapter),
                ));

                (table_name, table)
            })
            .collect();

        let id = schema.id;

        Self {
            id,
            name,
            tables: Arc::new(tables),
            exec,
            catalog_cache: Arc::clone(chunk_adapter.catalog_cache()),
            query_log,
        }
    }

    /// Create new namespace for given schema, for testing.
    #[allow(clippy::too_many_arguments)]
    pub fn new_testing(
        catalog_cache: Arc<CatalogCache>,
        store: ParquetStorage,
        metric_registry: Arc<metric::Registry>,
        name: Arc<str>,
        schema: Arc<NamespaceSchema>,
        exec: Arc<Executor>,
        ingester_connection: Arc<dyn IngesterConnection>,
    ) -> Self {
        let time_provider = catalog_cache.time_provider();
        let chunk_adapter = Arc::new(ParquetChunkAdapter::new(
            catalog_cache,
            store,
            metric_registry,
            Arc::clone(&time_provider),
        ));
        let query_log = Arc::new(QueryLog::new(10, time_provider));

        Self::new(
            chunk_adapter,
            schema,
            name,
            exec,
            ingester_connection,
            query_log,
        )
    }

    /// Namespace name.
    pub fn name(&self) -> Arc<str> {
        Arc::clone(&self.name)
    }

    #[must_use]
    /// Return the underlying catalog cache
    pub fn catalog_cache(&self) -> &Arc<CatalogCache> {
        &self.catalog_cache
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::namespace::test_util::querier_namespace;
    use data_types::ColumnType;
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
