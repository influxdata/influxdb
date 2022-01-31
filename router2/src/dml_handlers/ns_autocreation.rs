use std::sync::Arc;

use async_trait::async_trait;
use data_types::{delete_predicate::DeletePredicate, DatabaseName};
use hashbrown::HashMap;
use iox_catalog::interface::{Catalog, KafkaTopicId, QueryPoolId};
use mutable_batch::MutableBatch;
use observability_deps::tracing::*;
use thiserror::Error;
use trace::ctx::SpanContext;

use crate::namespace_cache::NamespaceCache;

use super::{DmlError, DmlHandler};

/// An error auto-creating the request namespace.
#[derive(Debug, Error)]
pub enum NamespaceCreationError {
    /// An error returned from a namespace creation request.
    #[error("failed to create namespace: {0}")]
    Create(iox_catalog::interface::Error),

    /// The inner DML handler returned an error.
    #[error(transparent)]
    Inner(Box<DmlError>),
}

/// A layer to populate the [`Catalog`] with all the namespaces the router
/// observes.
///
/// Uses a [`NamespaceCache`] to limit issuing create requests to namespaces the
/// router has not yet observed a schema for.
#[derive(Debug)]
pub struct NamespaceAutocreation<D, C> {
    catalog: Arc<dyn Catalog>,
    cache: C,

    topic_id: KafkaTopicId,
    query_id: QueryPoolId,
    retention: String,

    inner: D,
}

impl<D, C> NamespaceAutocreation<D, C> {
    /// Return a new [`NamespaceAutocreation`] layer that ensures a requested
    /// namespace exists in `catalog` before passing the request to `inner`.
    ///
    /// If the namespace does not exist, it is created with the specified
    /// `topic_id`, `query_id` and `retention` policy.
    ///
    /// Namespaces are looked up in `cache`, skipping the creation request to
    /// the catalog if there's a hit.
    pub fn new(
        catalog: Arc<dyn Catalog>,
        cache: C,
        topic_id: KafkaTopicId,
        query_id: QueryPoolId,
        retention: String,
        inner: D,
    ) -> Self {
        Self {
            catalog,
            cache,
            topic_id,
            query_id,
            retention,
            inner,
        }
    }
}

#[async_trait]
impl<D, C> DmlHandler for NamespaceAutocreation<D, C>
where
    D: DmlHandler,
    C: NamespaceCache,
{
    type WriteError = NamespaceCreationError;
    type DeleteError = NamespaceCreationError;

    /// Write `batches` to `namespace`.
    async fn write(
        &self,
        namespace: DatabaseName<'static>,
        batches: HashMap<String, MutableBatch>,
        span_ctx: Option<SpanContext>,
    ) -> Result<(), Self::WriteError> {
        // If the namespace does not exist in the schema cache (populated by the
        // schema validator) request an (idempotent) creation.
        if self.cache.get_schema(&namespace).is_none() {
            trace!(%namespace, "namespace auto-create cache miss");

            match self
                .catalog
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

        self.inner
            .write(namespace, batches, span_ctx)
            .await
            .map_err(|e| NamespaceCreationError::Inner(Box::new(e.into())))
    }

    /// Delete the data specified in `delete`.
    async fn delete<'a>(
        &self,
        namespace: DatabaseName<'static>,
        table_name: impl Into<String> + Send + Sync + 'a,
        predicate: DeletePredicate,
        span_ctx: Option<SpanContext>,
    ) -> Result<(), Self::DeleteError> {
        self.inner
            .delete(namespace, table_name, predicate, span_ctx)
            .await
            .map_err(|e| NamespaceCreationError::Inner(Box::new(e.into())))
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use assert_matches::assert_matches;
    use iox_catalog::{
        interface::{Namespace, NamespaceId, NamespaceSchema},
        mem::MemCatalog,
    };

    use crate::{
        dml_handlers::mock::{MockDmlHandler, MockDmlHandlerCall},
        namespace_cache::MemoryNamespaceCache,
    };

    use super::*;

    #[tokio::test]
    async fn test_cache_hit() {
        let ns = DatabaseName::try_from("bananas").unwrap();

        let cache = Arc::new(MemoryNamespaceCache::default());
        cache.put_schema(
            ns.clone(),
            NamespaceSchema {
                id: NamespaceId::new(1),
                kafka_topic_id: KafkaTopicId::new(2),
                query_pool_id: QueryPoolId::new(3),
                tables: Default::default(),
            },
        );

        let catalog: Arc<dyn Catalog> = Arc::new(MemCatalog::default());
        let mock_handler = Arc::new(MockDmlHandler::default().with_write_return([Ok(())]));

        let creator = NamespaceAutocreation::new(
            Arc::clone(&catalog),
            cache,
            KafkaTopicId::new(42),
            QueryPoolId::new(42),
            "inf".to_owned(),
            Arc::clone(&mock_handler),
        );

        creator
            .write(ns.clone(), Default::default(), None)
            .await
            .expect("handler should succeed");

        // The cache hit should mean the catalog SHOULD NOT see a create request
        // for the namespace.
        assert!(
            catalog
                .namespaces()
                .get_by_name(ns.as_str())
                .await
                .expect("lookup should not error")
                .is_none(),
            "expected no request to the catalog"
        );

        // And the DML handler must be called.
        assert_matches!(mock_handler.calls().as_slice(), [MockDmlHandlerCall::Write { namespace, .. }] => {
            assert_eq!(*namespace, *ns);
        });
    }

    #[tokio::test]
    async fn test_cache_miss() {
        let ns = DatabaseName::try_from("bananas").unwrap();

        let cache = Arc::new(MemoryNamespaceCache::default());
        let catalog: Arc<dyn Catalog> = Arc::new(MemCatalog::default());
        let mock_handler = Arc::new(MockDmlHandler::default().with_write_return([Ok(())]));

        let creator = NamespaceAutocreation::new(
            Arc::clone(&catalog),
            cache,
            KafkaTopicId::new(42),
            QueryPoolId::new(42),
            "inf".to_owned(),
            Arc::clone(&mock_handler),
        );

        creator
            .write(ns.clone(), Default::default(), None)
            .await
            .expect("handler should succeed");

        // The cache miss should mean the catalog MUST see a create request for
        // the namespace.
        let got = catalog
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
                kafka_topic_id: KafkaTopicId::new(42),
                query_pool_id: QueryPoolId::new(42),
            }
        );

        // And the DML handler must be called.
        assert_matches!(mock_handler.calls().as_slice(), [MockDmlHandlerCall::Write { namespace, .. }] => {
            assert_eq!(*namespace, *ns);
        });
    }
}
