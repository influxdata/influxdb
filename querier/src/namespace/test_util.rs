use std::sync::Arc;

use iox_catalog::interface::get_schema_by_name;
use iox_tests::util::TestNamespace;

use super::QuerierNamespace;

/// Create [`QuerierNamespace`] for testing.
pub async fn querier_namespace(ns: &Arc<TestNamespace>) -> QuerierNamespace {
    let mut repos = ns.catalog.catalog.repositories().await;
    let schema = Arc::new(
        get_schema_by_name(&ns.namespace.name, repos.as_mut())
            .await
            .unwrap(),
    );

    QuerierNamespace::new_testing(
        ns.catalog.catalog(),
        ns.catalog.object_store(),
        ns.catalog.metric_registry(),
        ns.catalog.time_provider(),
        ns.namespace.name.clone().into(),
        schema,
        ns.catalog.exec(),
    )
}
