//! Namespace within the whole catalog.

use crate::{
    cache::{namespace::CachedNamespace, CatalogCache},
    ingester::IngesterConnection,
    parquet::ChunkAdapter,
    query_log::QueryLog,
    table::{PruneMetrics, QuerierTable, QuerierTableArgs},
};
use data_types::NamespaceId;
use iox_query::exec::Executor;
use std::{collections::HashMap, sync::Arc, time::Duration};

mod query_access;

#[cfg(test)]
mod test_util;

/// Arguments to create a [`QuerierNamespace`].
#[derive(Debug)]
pub struct QuerierNamespaceArgs {
    pub chunk_adapter: Arc<ChunkAdapter>,
    pub ns: Arc<CachedNamespace>,
    pub name: Arc<str>,
    pub exec: Arc<Executor>,
    pub ingester_connection: Option<Arc<dyn IngesterConnection>>,
    pub query_log: Arc<QueryLog>,
    pub prune_metrics: Arc<PruneMetrics>,
    pub datafusion_config: Arc<HashMap<String, String>>,
    pub include_debug_info_tables: bool,
}

/// Maps a catalog namespace to all the in-memory resources and sync-state that the querier needs.
///
/// # Data Structures & Sync
///
/// Tables and schemas are created when [`QuerierNamespace`] is created because DataFusion does not
/// implement async schema inspection. The actual payload (chunks) is only queried on demand.
///
/// Most accesses to the [IOx Catalog](iox_catalog::interface::Catalog) are cached via
/// [`CatalogCache`].
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

    /// Catalog cache.
    catalog_cache: Arc<CatalogCache>,

    /// Query log.
    query_log: Arc<QueryLog>,

    /// DataFusion config.
    datafusion_config: Arc<HashMap<String, String>>,

    /// Include debug info tables.
    include_debug_info_tables: bool,

    /// Retention period.
    retention_period: Option<Duration>,
}

impl QuerierNamespace {
    /// Create new namespace for given schema.
    pub fn new(args: QuerierNamespaceArgs) -> Self {
        let QuerierNamespaceArgs {
            chunk_adapter,
            ns,
            name,
            exec,
            ingester_connection,
            query_log,
            prune_metrics,
            datafusion_config,
            include_debug_info_tables,
        } = args;

        let tables: HashMap<_, _> = ns
            .tables
            .iter()
            .map(|(table_name, cached_table)| {
                let table = Arc::new(QuerierTable::new(QuerierTableArgs {
                    namespace_id: ns.id,
                    namespace_name: Arc::clone(&name),
                    namespace_retention_period: ns.retention_period,
                    table_id: cached_table.id,
                    table_name: Arc::clone(table_name),
                    schema: cached_table.schema.clone(),
                    ingester_connection: ingester_connection.clone(),
                    chunk_adapter: Arc::clone(&chunk_adapter),
                    prune_metrics: Arc::clone(&prune_metrics),
                }));

                (Arc::clone(table_name), table)
            })
            .collect();

        let id = ns.id;

        Self {
            id,
            name,
            tables: Arc::new(tables),
            exec,
            catalog_cache: Arc::clone(chunk_adapter.catalog_cache()),
            query_log,
            datafusion_config,
            include_debug_info_tables,
            retention_period: ns.retention_period,
        }
    }

    /// Create new namespace for given schema, for testing.
    pub fn new_testing(
        catalog_cache: Arc<CatalogCache>,
        metric_registry: Arc<metric::Registry>,
        name: Arc<str>,
        ns: Arc<CachedNamespace>,
        exec: Arc<Executor>,
        ingester_connection: Option<Arc<dyn IngesterConnection>>,
    ) -> Self {
        let time_provider = catalog_cache.time_provider();
        let chunk_adapter = Arc::new(ChunkAdapter::new(catalog_cache, metric_registry));
        let query_log = Arc::new(QueryLog::new(10, time_provider));
        let prune_metrics = Arc::new(PruneMetrics::new(&chunk_adapter.metric_registry()));

        Self::new(QuerierNamespaceArgs {
            chunk_adapter,
            ns,
            name,
            exec,
            ingester_connection,
            query_log,
            prune_metrics,
            datafusion_config: Default::default(),
            include_debug_info_tables: true,
        })
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
    use iox_tests::TestCatalog;
    use schema::{
        builder::SchemaBuilder, InfluxColumnType, InfluxFieldType, Schema, TIME_COLUMN_NAME,
    };

    #[tokio::test]
    async fn test_sync_tables() {
        let catalog = TestCatalog::new();

        let ns = catalog.create_namespace_1hr_retention("ns").await;

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

        let ns = catalog.create_namespace_1hr_retention("ns").await;
        let table = ns.create_table("table").await;

        let qns = querier_namespace(&ns).await;
        let expected_schema = SchemaBuilder::new().build().unwrap();
        let actual_schema = schema(&qns, "table");
        assert_eq!(actual_schema, &expected_schema,);

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
        assert_eq!(actual_schema, &expected_schema);

        table.create_column("col4", ColumnType::Tag).await;
        table
            .create_column(TIME_COLUMN_NAME, ColumnType::Time)
            .await;
        let qns = querier_namespace(&ns).await;
        let expected_schema = SchemaBuilder::new()
            .influx_column("col1", InfluxColumnType::Field(InfluxFieldType::Integer))
            .influx_column("col2", InfluxColumnType::Field(InfluxFieldType::Boolean))
            .influx_column("col3", InfluxColumnType::Tag)
            .influx_column("col4", InfluxColumnType::Tag)
            .influx_column(TIME_COLUMN_NAME, InfluxColumnType::Timestamp)
            .build()
            .unwrap();
        let actual_schema = schema(&qns, "table");
        assert_eq!(actual_schema, &expected_schema);
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

    fn schema<'a>(querier_namespace: &'a QuerierNamespace, table: &str) -> &'a Schema {
        querier_namespace.tables.get(table).unwrap().schema()
    }
}
