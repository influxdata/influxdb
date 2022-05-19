use std::sync::Arc;

use iox_catalog::interface::get_schema_by_name;
use iox_tests::util::TestNamespace;

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
        ns.catalog.object_store(),
        ns.catalog.metric_registry(),
        ns.namespace.name.clone().into(),
        schema,
        ns.catalog.exec(),
        create_ingester_connection_for_testing(),
    )
}
