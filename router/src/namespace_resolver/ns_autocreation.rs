use std::{fmt::Debug, sync::Arc};

use async_trait::async_trait;
use data_types::{NamespaceName, NamespaceSchema};
use iox_catalog::interface::Catalog;
use observability_deps::tracing::*;
use thiserror::Error;

use super::NamespaceResolver;
use crate::namespace_cache::NamespaceCache;

/// What to do when the namespace doesn't exist
#[derive(Debug, Copy, Clone)]
pub enum MissingNamespaceAction {
    /// Automatically create the namespace using the given retention period.
    AutoCreate(Option<i64>),

    /// Reject the write.
    Reject,
}

/// An error auto-creating the request namespace.
#[derive(Debug, Error)]
pub enum NamespaceCreationError {
    /// An error returned from a namespace creation request.
    #[error("failed to create namespace: {0}")]
    Create(iox_catalog::interface::Error),

    /// The write is to be rejected because the namespace doesn't exist and auto-creation is
    /// disabled.
    #[error("rejecting write due to non-existing namespace: {0}")]
    Reject(String),
}

/// A layer to populate the [`Catalog`] with all the namespaces the router
/// observes.
///
/// Uses a [`NamespaceCache`] to limit issuing create requests to namespaces the
/// router has not yet observed a schema for.
#[derive(Debug)]
pub struct NamespaceAutocreation<C, T> {
    inner: T,
    cache: C,
    catalog: Arc<dyn Catalog>,

    action: MissingNamespaceAction,
}

impl<C, T> NamespaceAutocreation<C, T> {
    /// Return a new [`NamespaceAutocreation`] layer that ensures a requested
    /// namespace exists in `catalog`.
    ///
    /// If the namespace does not exist, it is created with the specified
    /// `retention` policy.
    ///
    /// Namespaces are looked up in `cache`, skipping the creation request to
    /// the catalog if there's a hit.
    pub fn new(
        inner: T,
        cache: C,
        catalog: Arc<dyn Catalog>,
        action: MissingNamespaceAction,
    ) -> Self {
        Self {
            inner,
            cache,
            catalog,
            action,
        }
    }
}

#[async_trait]
impl<C, T> NamespaceResolver for NamespaceAutocreation<C, T>
where
    C: NamespaceCache<ReadError = iox_catalog::interface::Error>, // The resolver relies on the cache for read-through cache behaviour
    T: NamespaceResolver,
{
    /// Force the creation of `namespace` if it does not already exist in the
    /// cache, before passing the request through to the inner delegate.
    async fn get_namespace_schema(
        &self,
        namespace: &NamespaceName<'static>,
    ) -> Result<Arc<NamespaceSchema>, super::Error> {
        if self.cache.get_schema(namespace).await.is_err() {
            trace!(%namespace, "namespace not found in cache");

            match self.action {
                MissingNamespaceAction::Reject => {
                    // The namespace is not cached, but may exist in the
                    // catalog. Delegate discovery down to the inner handler,
                    // and map the lookup error to a reject error.
                    match self.inner.get_namespace_schema(namespace).await {
                        Ok(v) => return Ok(v),
                        Err(super::Error::Lookup(
                            iox_catalog::interface::Error::NamespaceNotFoundByName { .. },
                        )) => {
                            warn!(%namespace, "namespace not in catalog and auto-creation disabled");
                            return Err(NamespaceCreationError::Reject(namespace.into()).into());
                        }
                        Err(e) => return Err(e),
                    }
                }
                MissingNamespaceAction::AutoCreate(retention_period_ns) => {
                    match self
                        .catalog
                        .repositories()
                        .await
                        .namespaces()
                        .create(namespace, None, retention_period_ns, None)
                        .await
                    {
                        Ok(_) => {
                            debug!(%namespace, "created namespace");
                        }
                        Err(iox_catalog::interface::Error::NameExists { .. }) => {
                            // Either the cache has not yet converged to include this
                            // namespace, or another thread raced populating the catalog
                            // and beat this thread to it.
                            debug!(%namespace, "spurious namespace create failed");
                        }
                        Err(e) => {
                            error!(error=%e, %namespace, "failed to auto-create namespace");
                            return Err(NamespaceCreationError::Create(e).into());
                        }
                    }
                }
            }
        }

        self.inner.get_namespace_schema(namespace).await
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use assert_matches::assert_matches;
    use data_types::{Namespace, NamespaceId, NamespaceSchema};
    use iox_catalog::{interface::SoftDeletedRows, mem::MemCatalog};

    use super::*;
    use crate::{
        namespace_cache::{MemoryNamespaceCache, ReadThroughCache},
        namespace_resolver::{mock::MockNamespaceResolver, NamespaceSchemaResolver},
    };

    /// Common retention period value we'll use in tests
    const TEST_RETENTION_PERIOD_NS: Option<i64> = Some(3_600 * 1_000_000_000);

    #[tokio::test]
    async fn test_cache_hit() {
        const NAMESPACE_ID: NamespaceId = NamespaceId::new(42);

        let ns = NamespaceName::try_from("bananas").unwrap();

        let metrics = Arc::new(metric::Registry::new());
        let catalog: Arc<dyn Catalog> = Arc::new(MemCatalog::new(metrics));

        // Prep the cache before the test to cause a hit
        let cache = Arc::new(ReadThroughCache::new(
            Arc::new(MemoryNamespaceCache::default()),
            Arc::clone(&catalog),
        ));
        cache.put_schema(
            ns.clone(),
            NamespaceSchema {
                id: NAMESPACE_ID,
                tables: Default::default(),
                max_columns_per_table: 4,
                max_tables: 42,
                retention_period_ns: None,
                partition_template: Default::default(),
            },
        );

        let creator = NamespaceAutocreation::new(
            MockNamespaceResolver::default().with_mapping(ns.clone(), NAMESPACE_ID),
            cache,
            Arc::clone(&catalog),
            MissingNamespaceAction::AutoCreate(TEST_RETENTION_PERIOD_NS),
        );

        // Drive the code under test
        let got_namespace_schema = creator
            .get_namespace_schema(&ns)
            .await
            .expect("handler should succeed");
        assert_eq!(got_namespace_schema.id, NAMESPACE_ID);

        // The cache hit should mean the catalog SHOULD NOT see a create request
        // for the namespace.
        let mut repos = catalog.repositories().await;
        assert!(
            repos
                .namespaces()
                .get_by_name(ns.as_str(), SoftDeletedRows::ExcludeDeleted)
                .await
                .expect("lookup should not error")
                .is_none(),
            "expected no request to the catalog"
        );
    }

    #[tokio::test]
    async fn test_cache_miss() {
        let ns = NamespaceName::try_from("bananas").unwrap();

        let metrics = Arc::new(metric::Registry::new());
        let catalog: Arc<dyn Catalog> = Arc::new(MemCatalog::new(metrics));

        let cache = Arc::new(ReadThroughCache::new(
            Arc::new(MemoryNamespaceCache::default()),
            Arc::clone(&catalog),
        ));

        let creator = NamespaceAutocreation::new(
            MockNamespaceResolver::default().with_mapping(ns.clone(), NamespaceId::new(1)),
            cache,
            Arc::clone(&catalog),
            MissingNamespaceAction::AutoCreate(TEST_RETENTION_PERIOD_NS),
        );

        let created_namespace_schema = creator
            .get_namespace_schema(&ns)
            .await
            .expect("handler should succeed");

        // The cache miss should mean the catalog MUST see a create request for
        // the namespace.
        let mut repos = catalog.repositories().await;
        let got = repos
            .namespaces()
            .get_by_name(ns.as_str(), SoftDeletedRows::ExcludeDeleted)
            .await
            .expect("lookup should not error")
            .expect("creation request should be sent to catalog");

        assert_eq!(got.id, created_namespace_schema.id);
        assert_eq!(
            got,
            Namespace {
                id: NamespaceId::new(1),
                name: ns.to_string(),
                max_tables: iox_catalog::DEFAULT_MAX_TABLES,
                max_columns_per_table: iox_catalog::DEFAULT_MAX_COLUMNS_PER_TABLE,
                retention_period_ns: TEST_RETENTION_PERIOD_NS,
                deleted_at: None,
                partition_template: Default::default(),
            }
        );
    }

    #[tokio::test]
    async fn test_reject() {
        let ns = NamespaceName::try_from("bananas").unwrap();

        let metrics = Arc::new(metric::Registry::new());
        let catalog: Arc<dyn Catalog> = Arc::new(MemCatalog::new(metrics));
        let cache = Arc::new(ReadThroughCache::new(
            Arc::new(MemoryNamespaceCache::default()),
            Arc::clone(&catalog),
        ));

        let creator = NamespaceAutocreation::new(
            MockNamespaceResolver::default(),
            cache,
            Arc::clone(&catalog),
            MissingNamespaceAction::Reject,
        );

        // It should not autocreate because we specified "rejection" behaviour, above
        assert_matches!(
            creator.get_namespace_schema(&ns).await,
            Err(crate::namespace_resolver::Error::Create(
                NamespaceCreationError::Reject(_ns)
            ))
        );

        // Make double-sure it wasn't created in the catalog
        let mut repos = catalog.repositories().await;
        assert_matches!(
            repos
                .namespaces()
                .get_by_name(ns.as_str(), SoftDeletedRows::ExcludeDeleted)
                .await,
            Ok(None)
        );
    }

    #[tokio::test]
    async fn test_reject_exists_in_catalog() {
        let ns = NamespaceName::try_from("bananas").unwrap();

        let metrics = Arc::new(metric::Registry::new());
        let catalog: Arc<dyn Catalog> = Arc::new(MemCatalog::new(metrics));
        let cache = Arc::new(ReadThroughCache::new(
            Arc::new(MemoryNamespaceCache::default()),
            Arc::clone(&catalog),
        ));

        // First drive the population of the catalog
        let creator = NamespaceAutocreation::new(
            NamespaceSchemaResolver::new(Arc::clone(&cache)),
            Arc::clone(&cache),
            Arc::clone(&catalog),
            MissingNamespaceAction::AutoCreate(TEST_RETENTION_PERIOD_NS),
        );

        let created_id = creator
            .get_namespace_schema(&ns)
            .await
            .expect("handler should succeed");

        // Now try in "reject" mode.
        let creator = NamespaceAutocreation::new(
            NamespaceSchemaResolver::new(Arc::clone(&cache)),
            cache,
            Arc::clone(&catalog),
            MissingNamespaceAction::Reject,
        );

        // It should not autocreate because we specified "rejection" behaviour, above
        let id = creator
            .get_namespace_schema(&ns)
            .await
            .expect("should allow existing namespace from catalog");
        assert_eq!(created_id, id);
    }
}
