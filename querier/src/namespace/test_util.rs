use std::sync::Arc;

use crate::{
    cache::CatalogCache,
    test_util::{TestCatalog, TestNamespace},
};

use super::QuerierNamespace;

pub fn querier_namespace(catalog: &Arc<TestCatalog>, ns: &Arc<TestNamespace>) -> QuerierNamespace {
    QuerierNamespace::new(
        Arc::new(CatalogCache::new(catalog.catalog())),
        ns.namespace.name.clone().into(),
        ns.namespace.id,
        catalog.metric_registry(),
        catalog.object_store(),
        catalog.time_provider(),
        catalog.exec(),
    )
}
