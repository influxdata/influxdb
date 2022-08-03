use super::QuerierNamespace;
use crate::{create_ingester_connection_for_testing, QuerierCatalogCache};
use data_types::{KafkaPartition, TableId};
use iox_catalog::interface::get_schema_by_name;
use iox_tests::util::TestNamespace;
use parquet_file::storage::ParquetStorage;
use sharder::JumpHash;
use std::sync::Arc;

/// Create [`QuerierNamespace`] for testing.
pub async fn querier_namespace(ns: &Arc<TestNamespace>) -> QuerierNamespace {
    querier_namespace_with_limit(ns, usize::MAX).await
}

/// Create [`QuerierNamespace`] for testing with chunk limits.
pub async fn querier_namespace_with_limit(
    ns: &Arc<TestNamespace>,
    max_table_query_bytes: usize,
) -> QuerierNamespace {
    let mut repos = ns.catalog.catalog.repositories().await;
    let schema = Arc::new(
        get_schema_by_name(&ns.namespace.name, repos.as_mut())
            .await
            .unwrap(),
    );

    let catalog_cache = Arc::new(QuerierCatalogCache::new_testing(
        ns.catalog.catalog(),
        ns.catalog.time_provider(),
        ns.catalog.metric_registry(),
    ));

    let sharder = Arc::new(JumpHash::new((0..1).map(KafkaPartition::new).map(Arc::new)).unwrap());

    QuerierNamespace::new_testing(
        catalog_cache,
        ParquetStorage::new(ns.catalog.object_store()),
        ns.catalog.metric_registry(),
        ns.namespace.name.clone().into(),
        schema,
        ns.catalog.exec(),
        Some(create_ingester_connection_for_testing()),
        sharder,
        Default::default(),
        max_table_query_bytes,
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
