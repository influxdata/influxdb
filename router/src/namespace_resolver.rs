//! An trait to abstract resolving a[`NamespaceName`] to [`NamespaceSchema`], and a
//! collection of composable implementations.
use async_trait::async_trait;
use data_types::{NamespaceName, NamespaceSchema};
use observability_deps::tracing::*;
use std::sync::Arc;
use thiserror::Error;

use crate::namespace_cache::NamespaceCache;

pub mod mock;
pub(crate) mod ns_autocreation;
pub use ns_autocreation::*;

/// Error states encountered during [`NamespaceSchema`] lookup.
#[derive(Debug, Error)]
pub enum Error {
    /// An error occured when attempting to fetch the namespace ID.
    #[error("failed to resolve namespace ID: {0}")]
    Lookup(iox_catalog::interface::Error),

    /// An error state for errors returned by [`NamespaceAutocreation`].
    #[error(transparent)]
    Create(#[from] NamespaceCreationError),
}

/// An abstract resolver of [`NamespaceName`] to [`NamespaceSchema`].
#[async_trait]
pub trait NamespaceResolver: std::fmt::Debug + Send + Sync {
    /// Return the [`NamespaceSchema`] for the given [`NamespaceName`].
    async fn get_namespace_schema(
        &self,
        namespace: &NamespaceName<'static>,
    ) -> Result<Arc<NamespaceSchema>, Error>;
}

/// An implementation of [`NamespaceResolver`] that resolves the [`NamespaceSchema`]
/// for a given name through a [`NamespaceCache`].
#[derive(Debug)]
pub struct NamespaceSchemaResolver<C> {
    cache: C,
}

impl<C> NamespaceSchemaResolver<C> {
    /// Construct a new [`NamespaceSchemaResolver`] that resolves namespace schemas
    /// using `cache`.
    pub fn new(cache: C) -> Self {
        Self { cache }
    }
}

#[async_trait]
impl<C> NamespaceResolver for NamespaceSchemaResolver<C>
where
    C: NamespaceCache<ReadError = iox_catalog::interface::Error>,
{
    async fn get_namespace_schema(
        &self,
        namespace: &NamespaceName<'static>,
    ) -> Result<Arc<NamespaceSchema>, Error> {
        // Load the namespace schema from the cache.
        match self.cache.get_schema(namespace).await {
            Ok(v) => Ok(v),
            Err(e) => return Err(Error::Lookup(e)),
        }
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use assert_matches::assert_matches;
    use data_types::{NamespaceId, NamespaceSchema};
    use iox_catalog::{
        interface::{Catalog, SoftDeletedRows},
        mem::MemCatalog,
    };

    use super::*;
    use crate::namespace_cache::{MemoryNamespaceCache, ReadThroughCache};

    #[tokio::test]
    async fn test_cache_hit() {
        let ns = NamespaceName::try_from("bananas").unwrap();

        let metrics = Arc::new(metric::Registry::new());
        let catalog: Arc<dyn Catalog> = Arc::new(MemCatalog::new(metrics));

        // Prep the cache before the test to cause a hit
        let cache = Arc::new(ReadThroughCache::new(
            MemoryNamespaceCache::default(),
            Arc::clone(&catalog),
        ));
        cache.put_schema(
            ns.clone(),
            NamespaceSchema {
                id: NamespaceId::new(42),
                tables: Default::default(),
                max_columns_per_table: 4,
                max_tables: 42,
                retention_period_ns: None,
                partition_template: Default::default(),
            },
        );

        let resolver = NamespaceSchemaResolver::new(Arc::clone(&cache));

        // Drive the code under test
        resolver
            .get_namespace_schema(&ns)
            .await
            .expect("lookup should succeed");

        assert!(cache.get_schema(&ns).await.is_ok());

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
            MemoryNamespaceCache::default(),
            Arc::clone(&catalog),
        ));

        // Create the namespace in the catalog
        {
            let mut repos = catalog.repositories().await;
            repos
                .namespaces()
                .create(&ns, None, None, None)
                .await
                .expect("failed to setup catalog state");
        }

        let resolver = NamespaceSchemaResolver::new(Arc::clone(&cache));

        resolver
            .get_namespace_schema(&ns)
            .await
            .expect("lookup should succeed");

        // The cache should be populated as a result of the lookup.
        assert!(cache.get_schema(&ns).await.is_ok());
    }

    #[tokio::test]
    async fn test_cache_miss_soft_deleted() {
        let ns = NamespaceName::try_from("bananas").unwrap();

        let metrics = Arc::new(metric::Registry::new());
        let catalog: Arc<dyn Catalog> = Arc::new(MemCatalog::new(metrics));
        let cache = Arc::new(ReadThroughCache::new(
            MemoryNamespaceCache::default(),
            Arc::clone(&catalog),
        ));

        // Create the namespace in the catalog and mark it as deleted
        {
            let mut repos = catalog.repositories().await;
            repos
                .namespaces()
                .create(&ns, None, None, None)
                .await
                .expect("failed to setup catalog state");
            repos
                .namespaces()
                .soft_delete(&ns)
                .await
                .expect("failed to setup catalog state");
        }

        let resolver = NamespaceSchemaResolver::new(Arc::clone(&cache));

        let err = resolver
            .get_namespace_schema(&ns)
            .await
            .expect_err("lookup should succeed");
        assert_matches!(
            err,
            Error::Lookup(iox_catalog::interface::Error::NamespaceNotFoundByName { .. })
        );

        // The cache should NOT be populated as a result of the lookup.
        assert!(cache.get_schema(&ns).await.is_err());
    }

    #[tokio::test]
    async fn test_cache_miss_does_not_exist() {
        let ns = NamespaceName::try_from("bananas").unwrap();

        let metrics = Arc::new(metric::Registry::new());
        let catalog: Arc<dyn Catalog> = Arc::new(MemCatalog::new(metrics));
        let cache = Arc::new(ReadThroughCache::new(
            MemoryNamespaceCache::default(),
            Arc::clone(&catalog),
        ));

        let resolver = NamespaceSchemaResolver::new(Arc::clone(&cache));

        let err = resolver
            .get_namespace_schema(&ns)
            .await
            .expect_err("lookup should error");

        assert_matches!(err, Error::Lookup(_));
        assert!(cache.get_schema(&ns).await.is_err());
    }
}
