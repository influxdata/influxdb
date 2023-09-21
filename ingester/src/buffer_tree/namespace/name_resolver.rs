use std::{sync::Arc, time::Duration};

use backoff::{Backoff, BackoffConfig};
use data_types::NamespaceId;
use iox_catalog::interface::{Catalog, SoftDeletedRows};

use super::NamespaceName;
use crate::deferred_load::DeferredLoad;

/// An abstract provider of a [`DeferredLoad`] configured to fetch the
/// [`NamespaceName`] of the specified [`NamespaceId`].
pub(crate) trait NamespaceNameProvider: Send + Sync + std::fmt::Debug {
    fn for_namespace(&self, id: NamespaceId) -> DeferredLoad<NamespaceName>;
}

#[derive(Debug)]
pub(crate) struct NamespaceNameResolver {
    max_smear: Duration,
    catalog: Arc<dyn Catalog>,
    backoff_config: BackoffConfig,
    metrics: Arc<metric::Registry>,
}

impl NamespaceNameResolver {
    pub(crate) fn new(
        max_smear: Duration,
        catalog: Arc<dyn Catalog>,
        backoff_config: BackoffConfig,
        metrics: Arc<metric::Registry>,
    ) -> Self {
        Self {
            max_smear,
            catalog,
            backoff_config,
            metrics,
        }
    }

    /// Fetch the [`NamespaceName`] from the [`Catalog`] for specified
    /// `namespace_id`, retrying endlessly when errors occur.
    pub(crate) async fn fetch(
        namespace_id: NamespaceId,
        catalog: Arc<dyn Catalog>,
        backoff_config: BackoffConfig,
    ) -> NamespaceName {
        Backoff::new(&backoff_config)
            .retry_all_errors("fetch namespace name", || async {
                let s = catalog
                    .repositories()
                    .await
                    .namespaces()
                    // Accept soft-deleted namespaces to enable any writes for
                    // it to flow through the system. Eventually the routers
                    // will prevent new writes to the deleted namespace.
                    .get_by_id(namespace_id, SoftDeletedRows::AllRows)
                    .await?
                    .unwrap_or_else(|| {
                        panic!(
                            "resolving namespace name for non-existent namespace id {namespace_id}"
                        )
                    })
                    .name
                    .into();

                Result::<_, iox_catalog::interface::Error>::Ok(s)
            })
            .await
            .expect("retry forever")
    }
}

impl NamespaceNameProvider for NamespaceNameResolver {
    fn for_namespace(&self, id: NamespaceId) -> DeferredLoad<NamespaceName> {
        DeferredLoad::new(
            self.max_smear,
            Self::fetch(id, Arc::clone(&self.catalog), self.backoff_config.clone()),
            &metric::Registry::default(),
        )
    }
}

#[cfg(test)]
pub(crate) mod mock {
    use super::*;

    #[derive(Debug)]
    pub(crate) struct MockNamespaceNameProvider {
        name: NamespaceName,
    }

    impl MockNamespaceNameProvider {
        pub(crate) fn new(name: impl Into<NamespaceName>) -> Self {
            Self { name: name.into() }
        }
    }

    impl NamespaceNameProvider for MockNamespaceNameProvider {
        fn for_namespace(&self, _id: NamespaceId) -> DeferredLoad<NamespaceName> {
            let name = self.name.clone();
            DeferredLoad::new(
                Duration::from_secs(1),
                async { name },
                &metric::Registry::default(),
            )
        }
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use test_helpers::timeout::FutureTimeout;

    use super::*;
    use crate::test_util::populate_catalog;

    const TABLE_NAME: &str = "bananas";
    const NAMESPACE_NAME: &str = "platanos";

    #[tokio::test]
    async fn test_fetch() {
        let metrics = Arc::new(metric::Registry::default());
        let backoff_config = BackoffConfig::default();
        let catalog: Arc<dyn Catalog> =
            Arc::new(iox_catalog::mem::MemCatalog::new(Arc::clone(&metrics)));

        // Populate the catalog with the namespace / table
        let (ns_id, _table_id) = populate_catalog(&*catalog, NAMESPACE_NAME, TABLE_NAME).await;

        let fetcher = Arc::new(NamespaceNameResolver::new(
            Duration::from_secs(10),
            Arc::clone(&catalog),
            backoff_config.clone(),
            metrics,
        ));

        let got = fetcher
            .for_namespace(ns_id)
            .get()
            .with_timeout_panic(Duration::from_secs(5))
            .await;
        assert_eq!(&**got, NAMESPACE_NAME);
    }
}
