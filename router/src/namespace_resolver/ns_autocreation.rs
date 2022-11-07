use std::{fmt::Debug, sync::Arc};

use async_trait::async_trait;
use data_types::{DatabaseName, NamespaceId, QueryPoolId, TopicId};
use iox_catalog::interface::Catalog;
use observability_deps::tracing::*;
use thiserror::Error;

use super::NamespaceResolver;
use crate::namespace_cache::NamespaceCache;

/// An error auto-creating the request namespace.
#[derive(Debug, Error)]
pub enum NamespaceCreationError {
    /// An error returned from a namespace creation request.
    #[error("failed to create namespace: {0}")]
    Create(iox_catalog::interface::Error),
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

    topic_id: TopicId,
    query_id: QueryPoolId,
    retention: String,
}

impl<C, T> NamespaceAutocreation<C, T> {
    /// Return a new [`NamespaceAutocreation`] layer that ensures a requested
    /// namespace exists in `catalog`.
    ///
    /// If the namespace does not exist, it is created with the specified
    /// `topic_id`, `query_id` and `retention` policy.
    ///
    /// Namespaces are looked up in `cache`, skipping the creation request to
    /// the catalog if there's a hit.
    pub fn new(
        inner: T,
        cache: C,
        catalog: Arc<dyn Catalog>,
        topic_id: TopicId,
        query_id: QueryPoolId,
        retention: String,
    ) -> Self {
        Self {
            inner,
            cache,
            catalog,
            topic_id,
            query_id,
            retention,
        }
    }
}

#[async_trait]
impl<C, T> NamespaceResolver for NamespaceAutocreation<C, T>
where
    C: NamespaceCache,
    T: NamespaceResolver,
{
    /// Force the creation of `namespace` if it does not already exist in the
    /// cache, before passing the request through to the inner delegate.
    async fn get_namespace_id(
        &self,
        namespace: &DatabaseName<'static>,
    ) -> Result<NamespaceId, super::Error> {
        if self.cache.get_schema(namespace).is_none() {
            trace!(%namespace, "namespace auto-create cache miss");

            let mut repos = self.catalog.repositories().await;

            match repos
                .namespaces()
                .create(
                    namespace.as_str(),
                    &self.retention,
                    self.topic_id,
                    self.query_id,
                )
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

        self.inner.get_namespace_id(namespace).await
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use data_types::{Namespace, NamespaceId, NamespaceSchema};
    use iox_catalog::mem::MemCatalog;

    use super::*;
    use crate::{
        namespace_cache::MemoryNamespaceCache, namespace_resolver::mock::MockNamespaceResolver,
    };

    #[tokio::test]
    async fn test_cache_hit() {
        const NAMESPACE_ID: NamespaceId = NamespaceId::new(42);

        let ns = DatabaseName::try_from("bananas").unwrap();

        // Prep the cache before the test to cause a hit
        let cache = Arc::new(MemoryNamespaceCache::default());
        cache.put_schema(
            ns.clone(),
            NamespaceSchema {
                id: NAMESPACE_ID,
                topic_id: TopicId::new(2),
                query_pool_id: QueryPoolId::new(3),
                tables: Default::default(),
                max_columns_per_table: 4,
            },
        );

        let metrics = Arc::new(metric::Registry::new());
        let catalog: Arc<dyn Catalog> = Arc::new(MemCatalog::new(metrics));

        let creator = NamespaceAutocreation::new(
            MockNamespaceResolver::default().with_mapping(ns.clone(), NAMESPACE_ID),
            cache,
            Arc::clone(&catalog),
            TopicId::new(42),
            QueryPoolId::new(42),
            "inf".to_owned(),
        );

        // Drive the code under test
        let got = creator
            .get_namespace_id(&ns)
            .await
            .expect("handler should succeed");
        assert_eq!(got, NAMESPACE_ID);

        // The cache hit should mean the catalog SHOULD NOT see a create request
        // for the namespace.
        let mut repos = catalog.repositories().await;
        assert!(
            repos
                .namespaces()
                .get_by_name(ns.as_str())
                .await
                .expect("lookup should not error")
                .is_none(),
            "expected no request to the catalog"
        );
    }

    #[tokio::test]
    async fn test_cache_miss() {
        let ns = DatabaseName::try_from("bananas").unwrap();

        let cache = Arc::new(MemoryNamespaceCache::default());
        let metrics = Arc::new(metric::Registry::new());
        let catalog: Arc<dyn Catalog> = Arc::new(MemCatalog::new(metrics));

        let creator = NamespaceAutocreation::new(
            MockNamespaceResolver::default().with_mapping(ns.clone(), NamespaceId::new(1)),
            cache,
            Arc::clone(&catalog),
            TopicId::new(42),
            QueryPoolId::new(42),
            "inf".to_owned(),
        );

        let created_id = creator
            .get_namespace_id(&ns)
            .await
            .expect("handler should succeed");

        // The cache miss should mean the catalog MUST see a create request for
        // the namespace.
        let mut repos = catalog.repositories().await;
        let got = repos
            .namespaces()
            .get_by_name(ns.as_str())
            .await
            .expect("lookup should not error")
            .expect("creation request should be sent to catalog");

        assert_eq!(got.id, created_id);
        assert_eq!(
            got,
            Namespace {
                id: NamespaceId::new(1),
                name: ns.to_string(),
                retention_duration: Some("inf".to_owned()),
                topic_id: TopicId::new(42),
                query_pool_id: QueryPoolId::new(42),
                max_tables: iox_catalog::DEFAULT_MAX_TABLES,
                max_columns_per_table: iox_catalog::DEFAULT_MAX_COLUMNS_PER_TABLE,
                retention_period_ns: iox_catalog::DEFAULT_RETENTION_PERIOD,
            }
        );
    }
}
