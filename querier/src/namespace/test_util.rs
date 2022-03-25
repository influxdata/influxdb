use std::sync::Arc;

use iox_tests::util::TestNamespace;

use super::QuerierNamespace;

/// Create [`QuerierNamespace`] for testing.
pub fn querier_namespace(ns: &Arc<TestNamespace>) -> QuerierNamespace {
    QuerierNamespace::new_testing(
        ns.catalog.catalog(),
        ns.catalog.object_store(),
        ns.catalog.metric_registry(),
        ns.catalog.time_provider(),
        ns.namespace.name.clone().into(),
        ns.namespace.id,
        ns.catalog.exec(),
    )
}
