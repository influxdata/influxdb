use std::{collections::HashMap, sync::Arc};

use async_trait::async_trait;
use data_types::{
    NamespaceId, Partition, PartitionId, PartitionKey, SequenceNumber, ShardId, TableId,
};
use observability_deps::tracing::debug;
use parking_lot::Mutex;

use crate::data::partition::PartitionData;

use super::r#trait::PartitionProvider;

/// The data-carrying value of a `(shard_id, table_id, partition_key)` lookup.
#[derive(Debug)]
struct Entry {
    partition_id: PartitionId,
    max_sequence_number: Option<SequenceNumber>,
}

/// A read-through cache mapping `(shard_id, table_id, partition_key)` tuples to
/// `(partition_id, max_sequence_number)`.
///
/// This data is safe to cache as only one ingester is ever responsible for a
/// given table partition, and this amortises partition persist marker discovery
/// queries during startup, eliminating them from the ingest hot path in the
/// common startup case (an ingester restart with no new partitions to add).
///
/// # Memory Overhead
///
/// Excluding map overhead, and assuming partition keys in the form
/// "YYYY-MM-DD", each entry takes:
///
///   - Partition key: String (8 len + 8 cap + 8 ptr + data len) = 34 bytes
///   - ShardId: 8 bytes
///   - TableId: 8 bytes
///   - PartitionId: 8 bytes
///   - Optional sequence number: 16 bytes
///
/// For a total of 74 bytes per entry - approx 1,690 entries can be held in 1Mb
/// of memory.
///
/// Each cache hit _removes_ the entry from the cache - this eliminates the
/// memory overhead for items that were hit. This is the expected (only valid!)
/// usage pattern.
#[derive(Debug)]
pub(crate) struct PartitionCache<T> {
    // The inner delegate called for a cache miss.
    inner: T,

    /// Cached entries.
    ///
    /// First lookup level is the shared partition keys - this eliminates
    /// needing to share the string key per shard_id/table_id in memory which
    /// would be the case if using either as the first level lookup. This is
    /// cheaper (memory) than using Arc refs to share the keys.
    ///
    /// It's also likely a smaller N (more tables than partition keys) making it
    /// a faster search for cache misses.
    #[allow(clippy::type_complexity)]
    entries: Mutex<HashMap<PartitionKey, HashMap<ShardId, HashMap<TableId, Entry>>>>,
}

impl<T> PartitionCache<T> {
    /// Initialise a [`PartitionCache`] containing the specified partitions.
    ///
    /// Any cache miss is passed through to `inner`.
    pub(crate) fn new<P>(inner: T, partitions: P) -> Self
    where
        P: IntoIterator<Item = Partition>,
    {
        let mut entries = HashMap::<PartitionKey, HashMap<ShardId, HashMap<TableId, Entry>>>::new();
        for p in partitions.into_iter() {
            entries
                .entry(p.partition_key)
                .or_default()
                .entry(p.shard_id)
                .or_default()
                .insert(
                    p.table_id,
                    Entry {
                        partition_id: p.id,
                        max_sequence_number: p.persisted_sequence_number,
                    },
                );
        }

        // Minimise the overhead of the maps.
        for shards in entries.values_mut() {
            for tables in shards.values_mut() {
                tables.shrink_to_fit();
            }
            shards.shrink_to_fit();
        }
        entries.shrink_to_fit();

        Self {
            entries: Mutex::new(entries),
            inner,
        }
    }

    /// Search for an cached entry matching the `(partition_key, shard_id, table_id)`
    /// tuple.
    fn find(
        &self,
        shard_id: ShardId,
        table_id: TableId,
        partition_key: &PartitionKey,
    ) -> Option<(PartitionKey, Entry)> {
        let mut entries = self.entries.lock();

        // Look up the partition key provided by the caller.
        //
        // If the partition key is a hit, clone the key from the map and return
        // it instead of using the caller-provided partition key - this allows
        // effective reuse of the same partition key str across all hits for it
        // and is more memory efficient than using the caller-provided partition
        // key in the PartitionData.
        let key = entries.get_key_value(partition_key)?.0.clone();
        let partition = entries.get_mut(partition_key).unwrap();
        let shard = partition.get_mut(&shard_id)?;

        let e = shard.remove(&table_id)?;

        // As an item was just removed from the shard map, check if it is now
        // empty.
        if shard.is_empty() {
            // Remove the shard from the partition map to reclaim the memory it
            // was using.
            partition.remove(&shard_id);

            // As a shard was removed, likewise the partition may now be empty!
            if partition.is_empty() {
                entries.remove(partition_key);
                entries.shrink_to_fit();
            } else {
                partition.shrink_to_fit();
            }
        }

        Some((key, e))
    }
}

#[async_trait]
impl<T> PartitionProvider for PartitionCache<T>
where
    T: PartitionProvider,
{
    async fn get_partition(
        &self,
        partition_key: PartitionKey,
        shard_id: ShardId,
        namespace_id: NamespaceId,
        table_id: TableId,
        table_name: Arc<str>,
    ) -> PartitionData {
        // Use the cached PartitionKey instead of the caller's partition_key,
        // instead preferring to reuse the already-shared Arc<str> in the cache.

        if let Some((key, cached)) = self.find(shard_id, table_id, &partition_key) {
            debug!(%table_id, %partition_key, "partition cache hit");
            // Use the returned partition key instead of the callers - this
            // allows the backing str memory to be reused across all partitions
            // using the same key!
            return PartitionData::new(
                cached.partition_id,
                key,
                shard_id,
                namespace_id,
                table_id,
                table_name,
                cached.max_sequence_number,
            );
        }

        debug!(%table_id, %partition_key, "partition cache miss");

        // Otherwise delegate to the catalog / inner impl.
        self.inner
            .get_partition(partition_key, shard_id, namespace_id, table_id, table_name)
            .await
    }
}

#[cfg(test)]
mod tests {
    use crate::data::partition::resolver::MockPartitionProvider;

    use super::*;

    const PARTITION_KEY: &str = "bananas";
    const PARTITION_ID: PartitionId = PartitionId::new(42);
    const SHARD_ID: ShardId = ShardId::new(1);
    const NAMESPACE_ID: NamespaceId = NamespaceId::new(2);
    const TABLE_ID: TableId = TableId::new(3);
    const TABLE_NAME: &str = "platanos";

    #[tokio::test]
    async fn test_miss() {
        let data = PartitionData::new(
            PARTITION_ID,
            PARTITION_KEY.into(),
            SHARD_ID,
            NAMESPACE_ID,
            TABLE_ID,
            TABLE_NAME.into(),
            None,
        );
        let inner = MockPartitionProvider::default().with_partition(data);

        let cache = PartitionCache::new(inner, []);
        let got = cache
            .get_partition(
                PARTITION_KEY.into(),
                SHARD_ID,
                NAMESPACE_ID,
                TABLE_ID,
                TABLE_NAME.into(),
            )
            .await;

        assert_eq!(got.partition_id(), PARTITION_ID);
        assert_eq!(got.shard_id(), SHARD_ID);
        assert_eq!(got.table_id(), TABLE_ID);
        assert_eq!(got.table_name(), TABLE_NAME);
        assert!(cache.inner.is_empty());
    }

    #[tokio::test]
    async fn test_hit() {
        let inner = MockPartitionProvider::default();

        let stored_partition_key = PartitionKey::from(PARTITION_KEY);
        let partition = Partition {
            id: PARTITION_ID,
            shard_id: SHARD_ID,
            table_id: TABLE_ID,
            partition_key: stored_partition_key.clone(),
            sort_key: Default::default(),
            persisted_sequence_number: Default::default(),
        };

        let cache = PartitionCache::new(inner, [partition]);

        let callers_partition_key = PartitionKey::from(PARTITION_KEY);
        let got = cache
            .get_partition(
                callers_partition_key.clone(),
                SHARD_ID,
                NAMESPACE_ID,
                TABLE_ID,
                TABLE_NAME.into(),
            )
            .await;

        assert_eq!(got.partition_id(), PARTITION_ID);
        assert_eq!(got.shard_id(), SHARD_ID);
        assert_eq!(got.table_id(), TABLE_ID);
        assert_eq!(got.table_name(), TABLE_NAME);
        assert_eq!(*got.partition_key(), PartitionKey::from(PARTITION_KEY));

        // The cache should have been cleaned up as it was consumed.
        assert!(cache.entries.lock().is_empty());

        // Assert the partition key from the cache was used for the lifetime of
        // the partition, so that it is shared with the cache + other partitions
        // that share the same partition key across all tables.
        assert!(got.partition_key().ptr_eq(&stored_partition_key));
        // It does not use the short-lived caller's partition key (derived from
        // the DML op it is processing).
        assert!(!got.partition_key().ptr_eq(&callers_partition_key));
    }

    #[tokio::test]
    async fn test_miss_partition_jey() {
        let other_key = PartitionKey::from("test");
        let other_key_id = PartitionId::new(99);
        let inner = MockPartitionProvider::default().with_partition(PartitionData::new(
            other_key_id,
            other_key.clone(),
            SHARD_ID,
            NAMESPACE_ID,
            TABLE_ID,
            TABLE_NAME.into(),
            None,
        ));

        let partition = Partition {
            id: PARTITION_ID,
            shard_id: SHARD_ID,
            table_id: TABLE_ID,
            partition_key: PARTITION_KEY.into(),
            sort_key: Default::default(),
            persisted_sequence_number: Default::default(),
        };

        let cache = PartitionCache::new(inner, [partition]);
        let got = cache
            .get_partition(
                other_key.clone(),
                SHARD_ID,
                NAMESPACE_ID,
                TABLE_ID,
                TABLE_NAME.into(),
            )
            .await;

        assert_eq!(got.partition_id(), other_key_id);
        assert_eq!(got.shard_id(), SHARD_ID);
        assert_eq!(got.table_id(), TABLE_ID);
        assert_eq!(got.table_name(), TABLE_NAME);
    }

    #[tokio::test]
    async fn test_miss_table_id() {
        let other_table = TableId::new(1234);
        let inner = MockPartitionProvider::default().with_partition(PartitionData::new(
            PARTITION_ID,
            PARTITION_KEY.into(),
            SHARD_ID,
            NAMESPACE_ID,
            other_table,
            TABLE_NAME.into(),
            None,
        ));

        let partition = Partition {
            id: PARTITION_ID,
            shard_id: SHARD_ID,
            table_id: TABLE_ID,
            partition_key: PARTITION_KEY.into(),
            sort_key: Default::default(),
            persisted_sequence_number: Default::default(),
        };

        let cache = PartitionCache::new(inner, [partition]);
        let got = cache
            .get_partition(
                PARTITION_KEY.into(),
                SHARD_ID,
                NAMESPACE_ID,
                other_table,
                TABLE_NAME.into(),
            )
            .await;

        assert_eq!(got.partition_id(), PARTITION_ID);
        assert_eq!(got.shard_id(), SHARD_ID);
        assert_eq!(got.table_id(), other_table);
        assert_eq!(got.table_name(), TABLE_NAME);
    }

    #[tokio::test]
    async fn test_miss_shard_id() {
        let other_shard = ShardId::new(1234);
        let inner = MockPartitionProvider::default().with_partition(PartitionData::new(
            PARTITION_ID,
            PARTITION_KEY.into(),
            other_shard,
            NAMESPACE_ID,
            TABLE_ID,
            TABLE_NAME.into(),
            None,
        ));

        let partition = Partition {
            id: PARTITION_ID,
            shard_id: SHARD_ID,
            table_id: TABLE_ID,
            partition_key: PARTITION_KEY.into(),
            sort_key: Default::default(),
            persisted_sequence_number: Default::default(),
        };

        let cache = PartitionCache::new(inner, [partition]);
        let got = cache
            .get_partition(
                PARTITION_KEY.into(),
                other_shard,
                NAMESPACE_ID,
                TABLE_ID,
                TABLE_NAME.into(),
            )
            .await;

        assert_eq!(got.partition_id(), PARTITION_ID);
        assert_eq!(got.shard_id(), other_shard);
        assert_eq!(got.table_id(), TABLE_ID);
        assert_eq!(got.table_name(), TABLE_NAME);
    }
}
