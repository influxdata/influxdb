use std::sync::Arc;

use data_types::TableId;
use iox_catalog::interface::get_schema_by_name;
use iox_tests::util::TestNamespace;
use parquet_file::storage::ParquetStorage;

use crate::{create_ingester_connection_for_testing, QuerierCatalogCache};

use super::QuerierNamespace;

/// Create [`QuerierNamespace`] for testing.
pub async fn querier_namespace(ns: &Arc<TestNamespace>) -> QuerierNamespace {
    let mut repos = ns.catalog.catalog.repositories().await;
    let schema = Arc::new(
        get_schema_by_name(&ns.namespace.name, repos.as_mut())
            .await
            .unwrap(),
    );

    let catalog_cache = Arc::new(QuerierCatalogCache::new(
        ns.catalog.catalog(),
        ns.catalog.time_provider(),
        ns.catalog.metric_registry(),
        usize::MAX,
    ));
    QuerierNamespace::new_testing(
        catalog_cache,
        ParquetStorage::new(ns.catalog.object_store()),
        ns.catalog.metric_registry(),
        ns.namespace.name.clone().into(),
        schema,
        ns.catalog.exec(),
        create_ingester_connection_for_testing(),
    )
}

/// Given some tests create parquet files without an ingester to
/// signal the need for a cache refresh, this function, explictly
/// trigger the "refresh cache logic"
pub fn clear_parquet_cache(querier_namespace: &QuerierNamespace, table_id: TableId) {
    querier_namespace
        .catalog_cache()
        .parquet_file()
        .expire(table_id);
}
