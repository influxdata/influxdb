//! Read-through caching behaviour for a [`NamespaceCache`] implementation

use std::{ops::DerefMut, sync::Arc};

use async_trait::async_trait;
use data_types::{NamespaceName, NamespaceSchema};
use iox_catalog::interface::{get_schema_by_name, Catalog, SoftDeletedRows};
use observability_deps::tracing::*;

use super::memory::CacheMissErr;
use super::{ChangeStats, NamespaceCache};

/// A [`ReadThroughCache`] decorates a [`NamespaceCache`] with read-through
/// caching behaviour on calls to `self.get_schema()` when contained in an
/// [`Arc`], resolving cache misses with the provided [`Catalog`].
///
/// Filters out all soft-deleted namespaces when resolving.
///
/// No attempt to serialise cache misses for a particular namespace is made -
/// `N` concurrent calls for a missing namespace will cause `N` concurrent
/// catalog queries, and `N` [`NamespaceSchema`] instances replacing each other
/// in the cache before converging on a single instance (last resolved wins).
/// Subsequent queries will return the currently cached instance.
#[derive(Debug)]
pub struct ReadThroughCache<T> {
    inner_cache: T,
    catalog: Arc<dyn Catalog>,
}

impl<T> ReadThroughCache<T> {
    /// Decorates `inner_cache` with read-through caching behaviour, looking
    /// up schema from `catalog` when not present in the underlying cache.
    pub fn new(inner_cache: T, catalog: Arc<dyn Catalog>) -> Self {
        Self {
            inner_cache,
            catalog,
        }
    }
}

#[async_trait]
impl<T> NamespaceCache for ReadThroughCache<T>
where
    T: NamespaceCache<ReadError = CacheMissErr>,
{
    type ReadError = iox_catalog::interface::Error;
    /// Fetch the schema for `namespace` directly from the inner cache if
    /// present, pullng from the catalog if not.
    async fn get_schema(
        &self,
        namespace: &NamespaceName<'static>,
    ) -> Result<Arc<NamespaceSchema>, Self::ReadError> {
        match self.inner_cache.get_schema(namespace).await {
            Ok(v) => Ok(v),
            Err(CacheMissErr {
                namespace: cache_ns,
            }) => {
                // Invariant: the cache should not return misses for a different
                // namespace name.
                assert_eq!(cache_ns, *namespace);
                let mut repos = self.catalog.repositories().await;

                let schema = match get_schema_by_name(
                    namespace,
                    repos.deref_mut(),
                    SoftDeletedRows::ExcludeDeleted,
                )
                .await
                {
                    Ok(v) => v,
                    Err(e) => {
                        warn!(
                            error = %e,
                            %namespace,
                            "failed to retrieve namespace schema"
                        );
                        return Err(e);
                    }
                };

                let (new_schema, _) = self.put_schema(namespace.clone(), schema);

                trace!(%namespace, "schema cache populated");
                Ok(new_schema)
            }
        }
    }

    fn put_schema(
        &self,
        namespace: NamespaceName<'static>,
        schema: NamespaceSchema,
    ) -> (Arc<NamespaceSchema>, ChangeStats) {
        self.inner_cache.put_schema(namespace, schema)
    }
}

#[cfg(test)]
mod tests {
    use assert_matches::assert_matches;
    use data_types::NamespaceId;
    use iox_catalog::mem::MemCatalog;

    use super::*;
    use crate::namespace_cache::memory::MemoryNamespaceCache;

    #[tokio::test]
    async fn test_put_get() {
        let ns = NamespaceName::try_from("arán").expect("namespace name should be valid");

        let inner = MemoryNamespaceCache::default();
        let metrics = Arc::new(metric::Registry::new());
        let catalog = Arc::new(MemCatalog::new(metrics));

        let cache = ReadThroughCache::new(inner, catalog);

        // Pre-condition: Namespace not in cache or catalog.
        assert_matches!(cache.get_schema(&ns).await, Err(_));

        // Place a schema in the cache for that name
        let schema1 = NamespaceSchema {
            id: NamespaceId::new(1),
            tables: Default::default(),
            max_columns_per_table: iox_catalog::DEFAULT_MAX_COLUMNS_PER_TABLE as usize,
            max_tables: iox_catalog::DEFAULT_MAX_TABLES as usize,
            retention_period_ns: iox_catalog::DEFAULT_RETENTION_PERIOD,
            partition_template: Default::default(),
        };
        assert_matches!(cache.put_schema(ns.clone(), schema1.clone()), (result, _) => {
            assert_eq!(*result, schema1);
        });

        // Ensure it is present
        assert_eq!(
            *cache
                .get_schema(&ns)
                .await
                .expect("schema should be present in cache"),
            schema1
        );
    }

    #[tokio::test]
    async fn test_get_cache_miss_catalog_fetch_ok() {
        let ns = NamespaceName::try_from("arán").expect("namespace name should be valid");

        let inner = MemoryNamespaceCache::default();
        let metrics = Arc::new(metric::Registry::new());
        let catalog: Arc<dyn Catalog> = Arc::new(MemCatalog::new(metrics));

        let cache = ReadThroughCache::new(inner, Arc::clone(&catalog));

        // Pre-condition: Namespace not in cache or catalog.
        assert_matches!(cache.get_schema(&ns).await, Err(_));

        // Place a schema in the catalog for that name
        let schema1 = NamespaceSchema {
            id: NamespaceId::new(1),
            tables: Default::default(),
            max_columns_per_table: iox_catalog::DEFAULT_MAX_COLUMNS_PER_TABLE as usize,
            max_tables: iox_catalog::DEFAULT_MAX_TABLES as usize,
            retention_period_ns: iox_catalog::DEFAULT_RETENTION_PERIOD,
            partition_template: Default::default(),
        };

        assert_matches!(
            catalog
                .repositories()
                .await
                .namespaces()
                .create(&ns, None, iox_catalog::DEFAULT_RETENTION_PERIOD, None)
                .await,
            Ok(_)
        );

        // Query the cache again, should return the above schema after missing the cache.
        assert_matches!(cache.get_schema(&ns).await, Ok(v) => {
            assert_eq!(*v, schema1);
        })
    }
}
