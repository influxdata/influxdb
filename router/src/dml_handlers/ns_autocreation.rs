use super::DmlHandler;
use crate::namespace_cache::NamespaceCache;
use async_trait::async_trait;
use data_types::{DatabaseName, DeletePredicate, QueryPoolId, TopicId};
use iox_catalog::interface::Catalog;
use observability_deps::tracing::*;
use std::{fmt::Debug, marker::PhantomData, sync::Arc};
use thiserror::Error;
use trace::ctx::SpanContext;

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
    catalog: Arc<dyn Catalog>,
    cache: C,

    topic_id: TopicId,
    query_id: QueryPoolId,
    retention: String,
    _input: PhantomData<T>,
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
        catalog: Arc<dyn Catalog>,
        cache: C,
        topic_id: TopicId,
        query_id: QueryPoolId,
        retention: String,
    ) -> Self {
        Self {
            catalog,
            cache,
            topic_id,
            query_id,
            retention,
            _input: Default::default(),
        }
    }
}

#[async_trait]
impl<C, T> DmlHandler for NamespaceAutocreation<C, T>
where
    C: NamespaceCache,
    T: Debug + Send + Sync,
{
    type WriteError = NamespaceCreationError;
    type DeleteError = NamespaceCreationError;

    // This handler accepts any write input type, returning it to the caller
    // unmodified.
    type WriteInput = T;
    type WriteOutput = T;

    /// Write `batches` to `namespace`.
    async fn write(
        &self,
        namespace: &'_ DatabaseName<'static>,
        batches: Self::WriteInput,
        _span_ctx: Option<SpanContext>,
    ) -> Result<Self::WriteOutput, Self::WriteError> {
        // If the namespace does not exist in the schema cache (populated by the
        // schema validator) request an (idempotent) creation.
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
                    return Err(NamespaceCreationError::Create(e));
                }
            }
        }

        Ok(batches)
    }

    /// Delete the data specified in `delete`.
    async fn delete(
        &self,
        _namespace: &DatabaseName<'static>,
        _table_name: &str,
        _predicate: &DeletePredicate,
        _span_ctx: Option<SpanContext>,
    ) -> Result<(), Self::DeleteError> {
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::namespace_cache::MemoryNamespaceCache;
    use data_types::{Namespace, NamespaceId, NamespaceSchema};
    use iox_catalog::mem::MemCatalog;
    use std::sync::Arc;

    #[tokio::test]
    async fn test_cache_hit() {
        let ns = DatabaseName::try_from("bananas").unwrap();

        // Prep the cache before the test to cause a hit
        let cache = Arc::new(MemoryNamespaceCache::default());
        cache.put_schema(
            ns.clone(),
            NamespaceSchema {
                id: NamespaceId::new(1),
                topic_id: TopicId::new(2),
                query_pool_id: QueryPoolId::new(3),
                tables: Default::default(),
            },
        );

        let metrics = Arc::new(metric::Registry::new());
        let catalog: Arc<dyn Catalog> = Arc::new(MemCatalog::new(metrics));

        let creator = NamespaceAutocreation::new(
            Arc::clone(&catalog),
            cache,
            TopicId::new(42),
            QueryPoolId::new(42),
            "inf".to_owned(),
        );

        // Drive the code under test
        creator
            .write(&ns, (), None)
            .await
            .expect("handler should succeed");

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
            Arc::clone(&catalog),
            cache,
            TopicId::new(42),
            QueryPoolId::new(42),
            "inf".to_owned(),
        );

        creator
            .write(&ns, (), None)
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

        assert_eq!(
            got,
            Namespace {
                id: NamespaceId::new(1),
                name: ns.to_string(),
                retention_duration: Some("inf".to_owned()),
                topic_id: TopicId::new(42),
                query_pool_id: QueryPoolId::new(42),
                max_tables: 10000,
                max_columns_per_table: 1000,
            }
        );
    }
}
