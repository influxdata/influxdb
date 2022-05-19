use std::sync::Arc;

use backoff::BackoffConfig;
use iox_catalog::interface::get_schema_by_name;
use iox_tests::util::{TestCatalog, TestTable};
use schema::Schema;

use crate::{
    cache::CatalogCache, chunk::ParquetChunkAdapter, create_ingester_connection_for_testing,
};

use super::QuerierTable;

/// Create a [`QuerierTable`] for testing.
pub async fn querier_table(catalog: &Arc<TestCatalog>, table: &Arc<TestTable>) -> QuerierTable {
    let catalog_cache = Arc::new(CatalogCache::new(
        catalog.catalog(),
        catalog.time_provider(),
        catalog.metric_registry(),
        usize::MAX,
    ));
    let chunk_adapter = Arc::new(ParquetChunkAdapter::new(
        catalog_cache,
        catalog.object_store(),
        catalog.metric_registry(),
        catalog.time_provider(),
    ));

    let mut repos = catalog.catalog.repositories().await;
    let mut catalog_schema = get_schema_by_name(&table.namespace.namespace.name, repos.as_mut())
        .await
        .unwrap();
    let schema = catalog_schema.tables.remove(&table.table.name).unwrap();
    let schema = Arc::new(Schema::try_from(schema).unwrap());

    let namespace_name = Arc::from(table.namespace.namespace.name.as_str());

    QuerierTable::new(
        namespace_name,
        BackoffConfig::default(),
        table.table.id,
        table.table.name.clone().into(),
        schema,
        create_ingester_connection_for_testing(),
        chunk_adapter,
    )
}
