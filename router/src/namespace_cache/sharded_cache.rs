use std::sync::Arc;

use data_types::{NamespaceName, NamespaceSchema};
use sharder::JumpHash;

use super::NamespaceCache;

/// A decorator sharding the [`NamespaceCache`] keyspace into a set of `T`.
#[derive(Debug)]
pub struct ShardedCache<T> {
    shards: JumpHash<T>,
}

impl<T> ShardedCache<T> {
    /// initialise a [`ShardedCache`] splitting the keyspace over the given
    /// instances of `T`.
    pub fn new(shards: impl IntoIterator<Item = T>) -> Self {
        Self {
            shards: JumpHash::new(shards),
        }
    }
}

impl<T> NamespaceCache for Arc<ShardedCache<T>>
where
    T: NamespaceCache,
{
    fn get_schema(&self, namespace: &NamespaceName<'_>) -> Option<Arc<NamespaceSchema>> {
        self.shards.hash(namespace).get_schema(namespace)
    }

    fn put_schema(
        &self,
        namespace: NamespaceName<'static>,
        schema: impl Into<Arc<NamespaceSchema>>,
    ) -> Option<Arc<NamespaceSchema>> {
        self.shards.hash(&namespace).put_schema(namespace, schema)
    }
}

#[cfg(test)]
mod tests {
    use std::{collections::HashMap, iter};

    use data_types::{NamespaceId, QueryPoolId, TopicId};
    use rand::{distributions::Alphanumeric, thread_rng, Rng};

    use super::*;
    use crate::namespace_cache::MemoryNamespaceCache;

    fn rand_namespace() -> NamespaceName<'static> {
        thread_rng()
            .sample_iter(&Alphanumeric)
            .take(10)
            .map(char::from)
            .collect::<String>()
            .try_into()
            .expect("generated invalid random namespace name")
    }

    fn schema_with_id(id: i64) -> NamespaceSchema {
        NamespaceSchema {
            id: NamespaceId::new(id),
            topic_id: TopicId::new(1),
            query_pool_id: QueryPoolId::new(1),
            tables: Default::default(),
            max_columns_per_table: 7,
            retention_period_ns: None,
        }
    }

    #[test]
    fn test_stable_cache_sharding() {
        // The number of namespaces to test with.
        const N: usize = 100;

        // The number of shards to hash into.
        const SHARDS: usize = 10;

        let cache = Arc::new(ShardedCache::new(
            iter::repeat_with(|| Arc::new(MemoryNamespaceCache::default())).take(SHARDS),
        ));

        // Build a set of namespace -> unique integer to validate the shard
        // mapping later.
        let names = (0..N)
            .cycle()
            .take(N)
            .map(|id| (rand_namespace(), id))
            .collect::<HashMap<_, _>>();

        // The cache should be empty.
        for name in names.keys() {
            assert!(cache.get_schema(name).is_none());
        }

        // Populate the cache
        for (name, id) in &names {
            let schema = schema_with_id(*id as _);
            assert!(cache.put_schema(name.clone(), schema).is_none());
        }

        // The mapping should be stable
        for (name, id) in names {
            let want = schema_with_id(id as _);
            assert_eq!(cache.get_schema(&name), Some(Arc::new(want)));
        }
    }
}
