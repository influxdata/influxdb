use std::{collections::hash_map::RandomState, sync::Arc, time::Duration};

use async_trait::async_trait;
use backoff::BackoffConfig;
use bloom2::CompressedBitmap;
use data_types::{
    NamespaceId, Partition, PartitionHashId, PartitionKey, TableId, TransitionPartitionId,
};
use iox_catalog::interface::Catalog;
use observability_deps::tracing::{debug, info};
use parking_lot::Mutex;

use super::PartitionProvider;
use crate::{
    buffer_tree::{
        namespace::NamespaceName,
        partition::{resolver::SortKeyResolver, PartitionData, SortKeyState},
        table::TableMetadata,
    },
    deferred_load::DeferredLoad,
};

/// A probabilistic filter minimising queries against the catalog for new-style
/// partitions which use deterministic hash IDs.
///
/// Prior to <https://github.com/influxdata/influxdb_iox/pull/7963>, partitions
/// were identified by their catalog row ID. Since this PR was merged,
/// partitions are now addressed by a deterministic ID generated from the
/// partition's `(table_id, partition_key)` tuple as described in [Deterministic
/// Partition ID Generation] and implemented within the [`PartitionHashId`]
/// type.
///
/// In order for IOx to correctly derive the object store path, the appropriate
/// ID must be used: row IDs for old partitions, and hash IDs for new
/// partitions. The ingester informs the queriers of the appropriate ID during
/// query execution, so in order to provide the correct row-based ID for
/// old-style partitions, a query must be performed in the write path (as it
/// always has) to resolve it prior to executing any queries against the
/// partition.
///
/// As an optimisation, and for reasons outlined in the referenced document,
/// these queries should be minimised.
///
/// By building a bloom filter containing the set of all old-style partitions at
/// startup, the number of hot-path queries can be reduced to ~0 for new
/// partitions by not performing a query if the bloom filter does not contain
/// the requested partition. This eliminates queries in the hot path for a large
/// majority of the workload (all non-backfill workloads).
///
/// [Deterministic Partition ID Generation]:
///     https://docs.google.com/document/d/1YWjjnPPEdeTVX88nV5kvGRyRjXzSILjyWwa9GR9Z3bU/edit?usp=sharing
#[derive(Debug)]
pub struct OldPartitionBloomFilter<T> {
    /// The inner [`PartitionProvider`] delegate to resolve partitions that pass
    /// through the filter.
    inner: T,

    /// A bloom filter with low false-positive rates, containing the set of all
    /// [`PartitionHashId`] generated for all old-style, row-ID-addressed
    /// partitions.
    ///
    /// If this filter indicates a hash ID is present in this filter, a query
    /// must be performed to resolve it (which may turn out to be either an old
    /// or new-style partition).
    ///
    /// If this filter indicates a hash ID is not present in this filter, it is
    /// *definitely* a hash-ID-addressed, new-style partition.
    ///
    /// Because of the compressed nature of this filter, it becomes more space
    /// efficient as more old-style partitions age out, requiring less entries
    /// to be included in this filter.
    ///
    /// A [`RandomState`] uses random per-process secret keying, ensuring that
    /// external users cannot easily construct collisions to bypass this filter.
    filter: bloom2::Bloom2<RandomState, CompressedBitmap, PartitionHashId>,

    /// Data needed to construct the [`SortKeyResolver`] for cached entries.
    catalog: Arc<dyn Catalog>,
    backoff_config: BackoffConfig,
    /// The maximum amount of time a [`SortKeyResolver`] may wait until
    /// pre-fetching the sort key in the background.
    max_smear: Duration,

    metrics: Arc<metric::Registry>,
}

impl<T> OldPartitionBloomFilter<T> {
    pub fn new(
        inner: T,
        catalog: Arc<dyn Catalog>,
        backoff_config: BackoffConfig,
        max_smear: Duration,
        metrics: Arc<metric::Registry>,
        old_partitions: impl IntoIterator<Item = Partition>,
    ) -> Self {
        let old = old_partitions.into_iter();

        // A filter of this size can hold approximately 10,300,768 in a maximum
        // of 2MiB of memory, with a false-positive probability of ~50%.
        //
        // At 883,829 entries, this filter has a false-positive probability of
        // 1%, eliminating 99% of queries for new-style partitions.
        //
        // At 269,556 entries, this filter has a false-positive probability of
        // 0.1%, eliminating 99.9% of queries for new-style partitions.
        let mut b = bloom2::BloomFilterBuilder::default()
            .size(bloom2::FilterSize::KeyBytes3)
            .build();

        let mut n_partitions = 0_usize;
        for v in old {
            assert!(v.hash_id().is_none());
            let id = PartitionHashId::new(v.table_id, &v.partition_key);
            b.insert(&id);
            n_partitions += 1;
        }

        // Minimise the capacity of internal buffers now nothing else will be
        // added to this filter.
        b.shrink_to_fit();

        info!(
            %n_partitions,
            filter_byte_size=%b.byte_size(),
            "initialised row-addressed partition filter"
        );

        Self {
            inner,
            filter: b,
            catalog,
            backoff_config,
            max_smear,
            metrics,
        }
    }
}

#[async_trait]
impl<T> PartitionProvider for OldPartitionBloomFilter<T>
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
        let hash_id = PartitionHashId::new(table_id, &partition_key);

        if !self.filter.contains(&hash_id) {
            debug!(
                %table_id,
                %namespace_id,
                %partition_key,
                "identified as hash-ID addressed partition"
            );

            // This partition definitely is NOT an old-style / row-ID-addressed
            // partition.
            //
            // This partition definitely does not exist in the set, so it MUST
            // be a new-style, hash-ID-addressed partition and can be
            // initialised without the need to perform a catalog query.

            let partition_id = TransitionPartitionId::new(table_id, &partition_key);

            // Initialise a deferred resolver for the sort key.
            let sort_key_resolver = DeferredLoad::new(
                self.max_smear,
                SortKeyResolver::new(
                    partition_id.clone(),
                    Arc::clone(&self.catalog),
                    self.backoff_config.clone(),
                )
                .fetch(),
                &self.metrics,
            );

            return Arc::new(Mutex::new(PartitionData::new(
                partition_id,
                partition_key,
                namespace_id,
                namespace_name,
                table_id,
                table,
                SortKeyState::Deferred(Arc::new(sort_key_resolver)),
            )));
        }

        debug!(
            %table_id,
            %namespace_id,
            %partition_key,
            "identified as likely a row-ID addressed partition"
        );

        // This partition MAY be an old-style / row-ID-addressed partition
        // that needs querying for.
        self.inner
            .get_partition(partition_key, namespace_id, namespace_name, table_id, table)
            .await
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use data_types::PartitionId;
    use hashbrown::HashMap;
    use iox_catalog::mem::MemCatalog;
    use proptest::{prelude::*, prop_compose};

    use super::*;
    use crate::test_util::{
        defer_namespace_name_1_sec, defer_table_metadata_1_sec, PartitionDataBuilder,
        ARBITRARY_NAMESPACE_ID, ARBITRARY_PARTITION_HASH_ID, ARBITRARY_PARTITION_KEY,
        ARBITRARY_TABLE_ID,
    };

    #[derive(Debug, Default)]
    struct MockCatalogProvider {
        partitions: HashMap<(TableId, PartitionKey), Partition>,
    }

    impl MockCatalogProvider {
        fn new(v: impl IntoIterator<Item = Partition>) -> Self {
            Self {
                partitions: v
                    .into_iter()
                    .map(|v| ((v.table_id, v.partition_key.clone()), v))
                    .collect(),
            }
        }
    }

    #[async_trait]
    impl PartitionProvider for MockCatalogProvider {
        async fn get_partition(
            &self,
            partition_key: PartitionKey,
            _namespace_id: NamespaceId,
            _namespace_name: Arc<DeferredLoad<NamespaceName>>,
            table_id: TableId,
            _table: Arc<DeferredLoad<TableMetadata>>,
        ) -> Arc<Mutex<PartitionData>> {
            let mut builder = PartitionDataBuilder::default();

            let got = self
                .partitions
                .get(&(table_id, partition_key.clone()))
                .unwrap();

            if got.hash_id().is_none() {
                builder = builder.with_deprecated_partition_id(got.id);
            }

            Arc::new(Mutex::new(
                builder
                    .with_partition_key(partition_key)
                    .with_table_id(table_id)
                    .build(),
            ))
        }
    }

    prop_compose! {
        fn arbitrary_table_id()(id in any::<i64>()) -> TableId {
            TableId::new(id)
        }
    }

    prop_compose! {
        fn arbitrary_row_id()(id in any::<i64>()) -> PartitionId {
            PartitionId::new(id)
        }
    }

    prop_compose! {
        fn arbitrary_partition_key()(v in ".+") -> PartitionKey {
            PartitionKey::from(v)
        }
    }

    prop_compose! {
        fn arbitrary_partition()(
            table_id in arbitrary_table_id(),
            partition_key in arbitrary_partition_key(),
            row_id in arbitrary_row_id(),
            has_hash_id in any::<bool>(),
        ) -> Partition {
            let hash_id = match has_hash_id {
                true => Some(PartitionHashId::new(table_id, &partition_key)),
                false => None,
            };

            Partition::new_with_hash_id_from_sqlite_catalog_only(
                row_id,
                hash_id,
                table_id,
                partition_key,
                vec![],
                None,
            )
        }
    }

    proptest! {
        /// A property test that asserts the following invariants:
        ///
        ///     - Given a bloom filter initialised with a set of input
        ///       partitions, queries for each partition in the set correctly
        ///       resolves to a PartitionData using the expected row-based or
        ///       hash-based ID.
        ///
        ///     - Given a set of partitions that were not placed in the filter,
        ///       all correctly resolve when passing through the filter.
        ///
        /// The former ensures this filter doesn't return incorrect results,
        /// regardless of filtering.
        ///
        /// The latter ensures that the filter doesn't have any negative effect
        /// on partitions that it is unaware of, regardless of addressing
        /// scheme.
        #[test]
        fn prop_bloom_filter(
            // The set of partitions that will be queried for.
            partitions in prop::collection::hash_set(arbitrary_partition(), 0..100),
        ) {
            let _rt = tokio::runtime::Runtime::new().unwrap().enter();

            // Configure the catalog with the full set of partitions that will
            // be queried for.
            let catalog = MockCatalogProvider::new(partitions.clone());

            // Init the filter.
            let metrics = Arc::new(metric::Registry::default());
            let filter = OldPartitionBloomFilter::new(
                catalog,
                Arc::new(MemCatalog::new(Arc::clone(&metrics))),
                BackoffConfig::default(),
                Duration::MAX,
                metrics,
                // Configure the filter with the full set of old-style
                // partitions.
                partitions.clone()
                    .into_iter()
                    .filter(|v| matches!(v.transition_partition_id(), TransitionPartitionId::Deprecated(_))),
            );

            // Now query for all partitions.
            //
            // The returned partition must use the same ID addressing scheme as
            // the input.
            for p in partitions {
                let want_id = p.transition_partition_id().clone();

                let got = futures::executor::block_on(filter.get_partition(
                    p.partition_key,
                    ARBITRARY_NAMESPACE_ID,
                    defer_namespace_name_1_sec(),
                    p.table_id,
                    defer_table_metadata_1_sec()
                ));

                let got_id = got.lock().partition_id().clone();
                assert_eq!(got_id, want_id);
            }
        }
    }

    /// Assert that some requests are satisfied without a query to the
    /// underlying implementation / catalog.
    #[tokio::test]
    async fn test_cache_hit() {
        // The mock panics if it isn't configured with the partition being asked
        // for.
        let catalog = MockCatalogProvider::default();

        let p = Partition::new_with_hash_id_from_sqlite_catalog_only(
            PartitionId::new(42),
            Some(ARBITRARY_PARTITION_HASH_ID.clone()),
            ARBITRARY_TABLE_ID,
            ARBITRARY_PARTITION_KEY.clone(),
            vec![],
            None,
        );
        let want_id = p.transition_partition_id().clone();

        // Initialise a filter not containing the target partition.
        let metrics = Arc::new(metric::Registry::default());
        let filter = OldPartitionBloomFilter::new(
            catalog,
            Arc::new(MemCatalog::new(Arc::clone(&metrics))),
            BackoffConfig::default(),
            Duration::MAX,
            metrics,
            std::iter::empty(),
        );

        // Ask the filter for the partition.
        //
        // If the filter is successful in determining this is a hash ID, no
        // panic will occur. If not, the mock will be asked for the partition it
        // doesn't know about, and it'll panic.
        let got = filter
            .get_partition(
                ARBITRARY_PARTITION_KEY.clone(),
                ARBITRARY_NAMESPACE_ID,
                defer_namespace_name_1_sec(),
                ARBITRARY_TABLE_ID,
                defer_table_metadata_1_sec(),
            )
            .await;

        let got_id = got.lock().partition_id().clone();
        assert_eq!(got_id, want_id);
    }
}
