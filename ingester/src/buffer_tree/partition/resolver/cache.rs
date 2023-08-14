use std::{collections::HashMap, sync::Arc, time::Duration};

use async_trait::async_trait;
use backoff::BackoffConfig;
use data_types::{NamespaceId, Partition, PartitionKey, TableId, TransitionPartitionId};
use iox_catalog::interface::Catalog;
use observability_deps::tracing::debug;
use parking_lot::Mutex;

use super::r#trait::PartitionProvider;
use crate::{
    buffer_tree::{
        namespace::NamespaceName,
        partition::{resolver::SortKeyResolver, PartitionData, SortKeyState},
        table::TableMetadata,
    },
    deferred_load::DeferredLoad,
};

/// A read-through cache mapping `(table_id, partition_key)` tuples to
/// `partition_id`.
///
/// This data is safe to cache as only one ingester is ever responsible for a
/// given table partition, and this amortises partition discovery
/// queries during startup, eliminating them from the ingest hot path in the
/// common startup case (an ingester restart with no new partitions to add).
///
/// # Memory Overhead
///
/// Excluding map overhead, and assuming partition keys in the form
/// "YYYY-MM-DD", each entry takes:
///
///   - `PartitionKey`: String (8 len + 8 cap + 8 ptr + data len) = 34 bytes
///   - `TableId`: 8 bytes
///   - `TransitionPartitionId`: 8 bytes
///
/// For a total of 50 bytes per entry - approx 20,971 entries can be held in
/// 1MiB of memory.
///
/// Each cache hit _removes_ the entry from the cache - this eliminates the
/// memory overhead for items that were hit. This is the expected (only valid!)
/// usage pattern.
///
/// # Deferred Sort Key Loading
///
/// This cache does NOT cache the [`SortKey`] for each [`PartitionData`], as the
/// sort key can be large and is likely unique per table, and thus not
/// share-able across instances / prohibitively expensive to cache.
///
/// Instead cached instances are returned with a deferred sort key resolver
/// which attempts to fetch the sort key in the background some time after
/// construction.
///
/// [`SortKey`]: schema::sort::SortKey
#[derive(Debug)]
pub(crate) struct PartitionCache<T> {
    // The inner delegate called for a cache miss.
    inner: T,

    /// Cached entries.
    ///
    /// First lookup level is the shared partition keys - this eliminates
    /// needing to share the string key per table_id in memory which would be
    /// the case if inverted. This is cheaper (memory) than using Arc refs to
    /// share the keys.
    ///
    /// It's also likely a smaller N (more tables than partition keys) making it
    /// a faster search for cache misses.
    #[allow(clippy::type_complexity)]
    entries: Mutex<HashMap<PartitionKey, HashMap<TableId, TransitionPartitionId>>>,

    /// Data needed to construct the [`SortKeyResolver`] for cached entries.
    catalog: Arc<dyn Catalog>,
    backoff_config: BackoffConfig,
    /// The maximum amount of time a [`SortKeyResolver`] may wait until
    /// pre-fetching the sort key in the background.
    max_smear: Duration,

    metrics: Arc<metric::Registry>,
}

impl<T> PartitionCache<T> {
    /// Initialise a [`PartitionCache`] containing the specified partitions.
    ///
    /// Any cache miss is passed through to `inner`.
    ///
    /// Any cache hit returns a [`PartitionData`] configured with a
    /// [`SortKeyState::Deferred`] for deferred key loading in the background.
    /// The [`SortKeyResolver`] is initialised with the given `catalog`,
    /// `backoff_config`, and `max_smear` maximal load wait duration.
    pub(crate) fn new<P>(
        inner: T,
        partitions: P,
        max_smear: Duration,
        catalog: Arc<dyn Catalog>,
        backoff_config: BackoffConfig,
        metrics: Arc<metric::Registry>,
    ) -> Self
    where
        P: IntoIterator<Item = Partition>,
    {
        let mut entries = HashMap::<PartitionKey, HashMap<TableId, TransitionPartitionId>>::new();
        for p in partitions.into_iter() {
            entries
                .entry(p.partition_key.clone())
                .or_default()
                .insert(p.table_id, p.transition_partition_id());
        }

        // Minimise the overhead of the maps.
        for tables in entries.values_mut() {
            tables.shrink_to_fit();
        }
        entries.shrink_to_fit();

        Self {
            entries: Mutex::new(entries),
            inner,
            catalog,
            backoff_config,
            max_smear,
            metrics,
        }
    }

    /// Search for a cached entry matching the `(partition_key, table_id)`
    /// tuple.
    fn find(
        &self,
        table_id: TableId,
        partition_key: &PartitionKey,
    ) -> Option<(PartitionKey, TransitionPartitionId)> {
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

        let partition_id = partition.remove(&table_id)?;

        // As a entry was removed, check if it is now empty.
        if partition.is_empty() {
            entries.remove(partition_key);
            entries.shrink_to_fit();
        } else {
            partition.shrink_to_fit();
        }

        Some((key, partition_id))
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
        namespace_id: NamespaceId,
        namespace_name: Arc<DeferredLoad<NamespaceName>>,
        table_id: TableId,
        table: Arc<DeferredLoad<TableMetadata>>,
    ) -> Arc<Mutex<PartitionData>> {
        // Use the cached PartitionKey instead of the caller's partition_key,
        // instead preferring to reuse the already-shared Arc<str> in the cache.

        if let Some((key, partition_id)) = self.find(table_id, &partition_key) {
            debug!(%table_id, %partition_key, "partition cache hit");

            // Initialise a deferred resolver for the sort key.
            let sort_key_resolver = DeferredLoad::new(
                self.max_smear,
                SortKeyResolver::new(
                    partition_id.clone(),
                    Arc::clone(&__self.catalog),
                    self.backoff_config.clone(),
                )
                .fetch(),
                &self.metrics,
            );

            // Use the returned partition key instead of the callers - this
            // allows the backing str memory to be reused across all partitions
            // using the same key!
            return Arc::new(Mutex::new(PartitionData::new(
                partition_id,
                key,
                namespace_id,
                namespace_name,
                table_id,
                table,
                SortKeyState::Deferred(Arc::new(sort_key_resolver)),
            )));
        }

        debug!(%table_id, %partition_key, "partition cache miss");

        // Otherwise delegate to the catalog / inner impl.
        self.inner
            .get_partition(partition_key, namespace_id, namespace_name, table_id, table)
            .await
    }
}

#[cfg(test)]
mod tests {
    // Harmless in tests - saves a bunch of extra vars.
    #![allow(clippy::await_holding_lock)]

    use iox_catalog::mem::MemCatalog;

    use super::*;
    use crate::{
        buffer_tree::partition::resolver::mock::MockPartitionProvider,
        test_util::{
            defer_namespace_name_1_sec, defer_table_metadata_1_sec, PartitionDataBuilder,
            ARBITRARY_CATALOG_PARTITION_ID, ARBITRARY_NAMESPACE_ID, ARBITRARY_NAMESPACE_NAME,
            ARBITRARY_PARTITION_KEY, ARBITRARY_PARTITION_KEY_STR, ARBITRARY_TABLE_ID,
            ARBITRARY_TABLE_NAME, ARBITRARY_TRANSITION_PARTITION_ID,
        },
    };

    fn new_cache<P>(
        inner: MockPartitionProvider,
        partitions: P,
    ) -> PartitionCache<MockPartitionProvider>
    where
        P: IntoIterator<Item = Partition>,
    {
        PartitionCache::new(
            inner,
            partitions,
            Duration::from_secs(10_000_000),
            Arc::new(MemCatalog::new(Arc::new(metric::Registry::default()))),
            BackoffConfig::default(),
            Arc::new(metric::Registry::default()),
        )
    }

    #[tokio::test]
    async fn test_miss() {
        let data = PartitionDataBuilder::new().build();
        let inner = MockPartitionProvider::default().with_partition(data);

        let cache = new_cache(inner, []);
        let got = cache
            .get_partition(
                ARBITRARY_PARTITION_KEY.clone(),
                ARBITRARY_NAMESPACE_ID,
                defer_namespace_name_1_sec(),
                ARBITRARY_TABLE_ID,
                defer_table_metadata_1_sec(),
            )
            .await;

        assert_eq!(
            got.lock().partition_id(),
            &*ARBITRARY_TRANSITION_PARTITION_ID
        );
        assert_eq!(got.lock().table_id(), ARBITRARY_TABLE_ID);
        assert_eq!(
            &**got.lock().table().get().await.name(),
            &**ARBITRARY_TABLE_NAME
        );
        assert_eq!(
            &**got.lock().namespace_name().get().await,
            &***ARBITRARY_NAMESPACE_NAME
        );
        assert!(cache.inner.is_empty());
    }

    #[tokio::test]
    async fn test_hit() {
        let inner = MockPartitionProvider::default();

        let stored_partition_key = PartitionKey::from(ARBITRARY_PARTITION_KEY_STR);
        let partition = Partition::new_in_memory_only(
            ARBITRARY_CATALOG_PARTITION_ID,
            ARBITRARY_TABLE_ID,
            stored_partition_key.clone(),
            vec!["dos".to_string(), "bananas".to_string()],
            Default::default(),
        );

        let cache = new_cache(inner, [partition]);

        let callers_partition_key = PartitionKey::from(ARBITRARY_PARTITION_KEY_STR);
        let got = cache
            .get_partition(
                callers_partition_key.clone(),
                ARBITRARY_NAMESPACE_ID,
                defer_namespace_name_1_sec(),
                ARBITRARY_TABLE_ID,
                defer_table_metadata_1_sec(),
            )
            .await;

        assert_eq!(
            got.lock().partition_id(),
            &*ARBITRARY_TRANSITION_PARTITION_ID
        );
        assert_eq!(got.lock().table_id(), ARBITRARY_TABLE_ID);
        assert_eq!(
            &**got.lock().table().get().await.name(),
            &**ARBITRARY_TABLE_NAME
        );
        assert_eq!(
            &**got.lock().namespace_name().get().await,
            &***ARBITRARY_NAMESPACE_NAME
        );
        assert_eq!(
            *got.lock().partition_key(),
            PartitionKey::from(ARBITRARY_PARTITION_KEY_STR)
        );

        // The cache should have been cleaned up as it was consumed.
        assert!(cache.entries.lock().is_empty());

        // Assert the partition key from the cache was used for the lifetime of
        // the partition, so that it is shared with the cache + other partitions
        // that share the same partition key across all tables.
        assert!(got.lock().partition_key().ptr_eq(&stored_partition_key));
        // It does not use the short-lived caller's partition key (derived from
        // the DML op it is processing).
        assert!(!got.lock().partition_key().ptr_eq(&callers_partition_key));
    }

    #[tokio::test]
    async fn test_miss_partition_key() {
        let other_key = PartitionKey::from("test");
        let other_partition_id = TransitionPartitionId::new(ARBITRARY_TABLE_ID, &other_key);
        let inner = MockPartitionProvider::default().with_partition(
            PartitionDataBuilder::new()
                .with_partition_key(other_key.clone())
                .build(),
        );

        let partition = Partition::new_in_memory_only(
            ARBITRARY_CATALOG_PARTITION_ID,
            ARBITRARY_TABLE_ID,
            ARBITRARY_PARTITION_KEY.clone(),
            Default::default(),
            Default::default(),
        );

        let cache = new_cache(inner, [partition]);
        let got = cache
            .get_partition(
                other_key,
                ARBITRARY_NAMESPACE_ID,
                defer_namespace_name_1_sec(),
                ARBITRARY_TABLE_ID,
                defer_table_metadata_1_sec(),
            )
            .await;

        assert_eq!(got.lock().partition_id(), &other_partition_id);
        assert_eq!(got.lock().table_id(), ARBITRARY_TABLE_ID);
        assert_eq!(
            &**got.lock().table().get().await.name(),
            &**ARBITRARY_TABLE_NAME
        );
    }

    #[tokio::test]
    async fn test_miss_table_id() {
        let other_table = TableId::new(1234);
        let other_partition_id = TransitionPartitionId::new(other_table, &ARBITRARY_PARTITION_KEY);
        let inner = MockPartitionProvider::default().with_partition(
            PartitionDataBuilder::new()
                .with_table_id(other_table)
                .build(),
        );

        let partition = Partition::new_in_memory_only(
            ARBITRARY_CATALOG_PARTITION_ID,
            ARBITRARY_TABLE_ID,
            ARBITRARY_PARTITION_KEY.clone(),
            Default::default(),
            Default::default(),
        );

        let cache = new_cache(inner, [partition]);
        let got = cache
            .get_partition(
                ARBITRARY_PARTITION_KEY.clone(),
                ARBITRARY_NAMESPACE_ID,
                defer_namespace_name_1_sec(),
                other_table,
                defer_table_metadata_1_sec(),
            )
            .await;

        assert_eq!(got.lock().partition_id(), &other_partition_id);
        assert_eq!(got.lock().table_id(), other_table);
        assert_eq!(
            &**got.lock().table().get().await.name(),
            &**ARBITRARY_TABLE_NAME
        );
    }
}
