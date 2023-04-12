use std::sync::Arc;

use async_trait::async_trait;
use data_types::{NamespaceName, NamespaceSchema};
use hashbrown::HashMap;
use parking_lot::RwLock;
use thiserror::Error;

use super::NamespaceCache;

/// An error type indicating that `namespace` is not present in the cache.
#[derive(Debug, Error)]
#[error("namespace {namespace} not found in cache")]
pub struct CacheMissErr {
    pub(super) namespace: NamespaceName<'static>,
}

/// An in-memory cache of [`NamespaceSchema`] backed by a hashmap protected with
/// a read-write mutex.
#[derive(Debug, Default)]
pub struct MemoryNamespaceCache {
    cache: RwLock<HashMap<NamespaceName<'static>, Arc<NamespaceSchema>>>,
}

#[async_trait]
impl NamespaceCache for Arc<MemoryNamespaceCache> {
    type ReadError = CacheMissErr;

    async fn get_schema(
        &self,
        namespace: &NamespaceName<'static>,
    ) -> Result<Arc<NamespaceSchema>, Self::ReadError> {
        self.cache
            .read()
            .get(namespace)
            .ok_or(CacheMissErr {
                namespace: namespace.clone(),
            })
            .map(Arc::clone)
    }

    fn put_schema(
        &self,
        namespace: NamespaceName<'static>,
        schema: impl Into<Arc<NamespaceSchema>>,
    ) -> Option<Arc<NamespaceSchema>> {
        self.cache.write().insert(namespace, schema.into())
    }
}

#[cfg(test)]
mod tests {
    use assert_matches::assert_matches;
    use data_types::{NamespaceId, QueryPoolId, TopicId};

    use super::*;

    #[tokio::test]
    async fn test_put_get() {
        let ns = NamespaceName::new("test").expect("namespace name is valid");
        let cache = Arc::new(MemoryNamespaceCache::default());

        assert_matches!(
            cache.get_schema(&ns).await,
            Err(CacheMissErr { namespace: got_ns }) => {
                assert_eq!(got_ns, ns);
            }
        );

        let schema1 = NamespaceSchema {
            id: NamespaceId::new(42),
            topic_id: TopicId::new(24),
            query_pool_id: QueryPoolId::new(1234),
            tables: Default::default(),
            max_columns_per_table: 50,
            max_tables: 24,
            retention_period_ns: Some(876),
        };
        assert!(cache.put_schema(ns.clone(), schema1.clone()).is_none());
        assert_eq!(
            *cache.get_schema(&ns).await.expect("lookup failure"),
            schema1
        );

        let schema2 = NamespaceSchema {
            id: NamespaceId::new(2),
            topic_id: TopicId::new(2),
            query_pool_id: QueryPoolId::new(2),
            tables: Default::default(),
            max_columns_per_table: 10,
            max_tables: 42,
            retention_period_ns: Some(876),
        };

        assert_eq!(
            *cache
                .put_schema(ns.clone(), schema2.clone())
                .expect("should have existing schema"),
            schema1
        );
        assert_eq!(
            *cache.get_schema(&ns).await.expect("lookup failure"),
            schema2
        );
    }
}
