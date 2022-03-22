//! Namespace cache.
use std::{collections::HashMap, sync::Arc};

use backoff::{Backoff, BackoffConfig};
use data_types2::NamespaceId;
use iox_catalog::interface::Catalog;

use crate::cache_system::{driver::Cache, loader::FunctionLoader};

/// Cache for namespace-related attributes.
#[derive(Debug)]
pub struct NamespaceCache {
    cache: Cache<NamespaceId, CachedNamespace>,
}

impl NamespaceCache {
    /// Create new empty cache.
    pub fn new(catalog: Arc<dyn Catalog>, backoff_config: BackoffConfig) -> Self {
        let loader = Arc::new(FunctionLoader::new(move |namespace_id| {
            let catalog = Arc::clone(&catalog);
            let backoff_config = backoff_config.clone();

            async move {
                let namespace = Backoff::new(&backoff_config)
                    .retry_all_errors("get namespace_name", || async {
                        catalog
                            .repositories()
                            .await
                            .namespaces()
                            .get_by_id(namespace_id)
                            .await
                    })
                    .await
                    .expect("retry forever")
                    .expect("namespace gone from catalog?!");

                CachedNamespace {
                    name: Arc::from(namespace.name),
                }
            }
        }));
        let backend = Box::new(HashMap::new());

        Self {
            cache: Cache::new(loader, backend),
        }
    }

    /// Get the namespace name for the given namespace ID.
    ///
    /// This either uses a cached value or -- if required -- fetches the mapping from the catalog.
    pub async fn name(&self, id: NamespaceId) -> Arc<str> {
        self.cache.get(id).await.name
    }
}

#[derive(Debug, Clone)]
struct CachedNamespace {
    name: Arc<str>,
}

#[cfg(test)]
mod tests {
    use crate::cache::test_util::assert_histogram_metric_count;
    use iox_tests::util::TestCatalog;

    use super::*;

    #[tokio::test]
    async fn test_name() {
        let catalog = TestCatalog::new();

        let ns1 = catalog.create_namespace("ns1").await.namespace.clone();
        let ns2 = catalog.create_namespace("ns2").await.namespace.clone();
        assert_ne!(ns1.id, ns2.id);

        let cache = NamespaceCache::new(catalog.catalog(), BackoffConfig::default());

        let name1_a = cache.name(ns1.id).await;
        assert_eq!(name1_a.as_ref(), ns1.name.as_str());
        assert_histogram_metric_count(&catalog.metric_registry, "namespace_get_by_id", 1);

        let name2 = cache.name(ns2.id).await;
        assert_eq!(name2.as_ref(), ns2.name.as_str());
        assert_histogram_metric_count(&catalog.metric_registry, "namespace_get_by_id", 2);

        let name1_b = cache.name(ns1.id).await;
        assert!(Arc::ptr_eq(&name1_a, &name1_b));
        assert_histogram_metric_count(&catalog.metric_registry, "namespace_get_by_id", 2);
    }
}
