//! Partition cache.

use backoff::{Backoff, BackoffConfig};
use cache_system::{
    backend::policy::{
        lru::{LruPolicy, ResourcePool},
        remove_if::{RemoveIfHandle, RemoveIfPolicy},
        PolicyBackend,
    },
    cache::{driver::CacheDriver, metrics::CacheWithMetrics, Cache},
    loader::{metrics::MetricsLoader, FunctionLoader},
    resource_consumption::FunctionEstimator,
};
use data_types::{ColumnId, PartitionId, ShardId};
use iox_catalog::interface::Catalog;
use iox_time::TimeProvider;
use schema::sort::SortKey;
use std::{
    collections::{HashMap, HashSet},
    mem::{size_of, size_of_val},
    sync::Arc,
};
use trace::span::Span;

use super::{namespace::CachedTable, ram::RamSize};

const CACHE_ID: &str = "partition";

type CacheT = Box<
    dyn Cache<
        K = PartitionId,
        V = Option<CachedPartition>,
        GetExtra = (Arc<CachedTable>, Option<Span>),
        PeekExtra = ((), Option<Span>),
    >,
>;

/// Cache for partition-related attributes.
#[derive(Debug)]
pub struct PartitionCache {
    cache: CacheT,
    remove_if_handle: RemoveIfHandle<PartitionId, Option<CachedPartition>>,
}

impl PartitionCache {
    /// Create new empty cache.
    pub fn new(
        catalog: Arc<dyn Catalog>,
        backoff_config: BackoffConfig,
        time_provider: Arc<dyn TimeProvider>,
        metric_registry: &metric::Registry,
        ram_pool: Arc<ResourcePool<RamSize>>,
        testing: bool,
    ) -> Self {
        let loader =
            FunctionLoader::new(move |partition_id: PartitionId, extra: Arc<CachedTable>| {
                let catalog = Arc::clone(&catalog);
                let backoff_config = backoff_config.clone();

                async move {
                    let partition = Backoff::new(&backoff_config)
                        .retry_all_errors("get partition_key", || async {
                            catalog
                                .repositories()
                                .await
                                .partitions()
                                .get_by_id(partition_id)
                                .await
                        })
                        .await
                        .expect("retry forever")?;

                    let sort_key = partition.sort_key().map(|sort_key| {
                        Arc::new(PartitionSortKey::new(sort_key, &extra.column_id_map_rev))
                    });

                    Some(CachedPartition {
                        shard_id: partition.shard_id,
                        sort_key,
                    })
                }
            });
        let loader = Arc::new(MetricsLoader::new(
            loader,
            CACHE_ID,
            Arc::clone(&time_provider),
            metric_registry,
            testing,
        ));

        let mut backend = PolicyBackend::hashmap_backed(Arc::clone(&time_provider));
        let (policy_constructor, remove_if_handle) =
            RemoveIfPolicy::create_constructor_and_handle(CACHE_ID, metric_registry);
        backend.add_policy(policy_constructor);
        backend.add_policy(LruPolicy::new(
            ram_pool,
            CACHE_ID,
            Arc::new(FunctionEstimator::new(|k, v: &Option<CachedPartition>| {
                RamSize(
                    size_of_val(k)
                        + size_of_val(v)
                        + v.as_ref().map(|v| v.size()).unwrap_or_default(),
                )
            })),
        ));

        let cache = CacheDriver::new(loader, backend);
        let cache = Box::new(CacheWithMetrics::new(
            cache,
            CACHE_ID,
            time_provider,
            metric_registry,
        ));

        Self {
            cache,
            remove_if_handle,
        }
    }

    /// Get shard ID.
    pub async fn shard_id(
        &self,
        cached_table: Arc<CachedTable>,
        partition_id: PartitionId,
        span: Option<Span>,
    ) -> Option<ShardId> {
        self.cache
            .get(partition_id, (cached_table, span))
            .await
            .map(|p| p.shard_id)
    }

    /// Get sort key
    ///
    /// Expire partition if the cached sort key does NOT cover the given set of columns.
    pub async fn sort_key(
        &self,
        cached_table: Arc<CachedTable>,
        partition_id: PartitionId,
        should_cover: &[ColumnId],
        span: Option<Span>,
    ) -> Option<Arc<PartitionSortKey>> {
        self.remove_if_handle
            .remove_if_and_get(
                &self.cache,
                partition_id,
                |cached_partition| {
                    if let Some(sort_key) = &cached_partition.and_then(|p| p.sort_key) {
                        should_cover
                            .iter()
                            .any(|col| !sort_key.column_set.contains(col))
                    } else {
                        // no sort key at all => need to update if there is anything to cover
                        !should_cover.is_empty()
                    }
                },
                (cached_table, span),
            )
            .await
            .and_then(|p| p.sort_key)
    }
}

#[derive(Debug, Clone)]
struct CachedPartition {
    shard_id: ShardId,
    sort_key: Option<Arc<PartitionSortKey>>,
}

impl CachedPartition {
    /// RAM-bytes EXCLUDING `self`.
    fn size(&self) -> usize {
        // Arc content
        self.sort_key
            .as_ref()
            .map(|sk| sk.size())
            .unwrap_or_default()
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PartitionSortKey {
    pub sort_key: Arc<SortKey>,
    pub column_set: HashSet<ColumnId>,
    pub column_order: Vec<ColumnId>,
}

impl PartitionSortKey {
    fn new(sort_key: SortKey, column_id_map_rev: &HashMap<Arc<str>, ColumnId>) -> Self {
        let sort_key = Arc::new(sort_key);

        let mut column_order: Vec<ColumnId> = sort_key
            .iter()
            .map(|(name, _opts)| {
                *column_id_map_rev
                    .get(name.as_ref())
                    .unwrap_or_else(|| panic!("column_id_map_rev misses data: {name}"))
            })
            .collect();
        column_order.shrink_to_fit();

        let mut column_set: HashSet<ColumnId> = column_order.iter().copied().collect();
        column_set.shrink_to_fit();

        Self {
            sort_key,
            column_set,
            column_order,
        }
    }

    /// Size of this object in bytes, including `self`.
    fn size(&self) -> usize {
        size_of_val(self)
            + self.sort_key.as_ref().size()
            + (self.column_set.capacity() * size_of::<ColumnId>())
            + (self.column_order.capacity() * size_of::<ColumnId>())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::cache::{ram::test_util::test_ram_pool, test_util::assert_histogram_metric_count};
    use data_types::ColumnType;
    use iox_tests::util::TestCatalog;
    use schema::{Schema, SchemaBuilder};

    #[tokio::test]
    async fn test_shard_id() {
        let catalog = TestCatalog::new();

        let ns = catalog.create_namespace_1hr_retention("ns").await;
        let t = ns.create_table("table").await;
        let s1 = ns.create_shard(1).await;
        let s2 = ns.create_shard(2).await;
        let p1 = t
            .with_shard(&s1)
            .create_partition("k1")
            .await
            .partition
            .clone();
        let p2 = t
            .with_shard(&s2)
            .create_partition("k2")
            .await
            .partition
            .clone();
        let cached_table = Arc::new(CachedTable {
            id: t.table.id,
            schema: schema(),
            column_id_map: HashMap::default(),
            column_id_map_rev: HashMap::default(),
            primary_key_column_ids: vec![],
        });

        let cache = PartitionCache::new(
            catalog.catalog(),
            BackoffConfig::default(),
            catalog.time_provider(),
            &catalog.metric_registry(),
            test_ram_pool(),
            true,
        );

        let id1 = cache
            .shard_id(Arc::clone(&cached_table), p1.id, None)
            .await
            .unwrap();
        assert_eq!(id1, s1.shard.id);
        assert_histogram_metric_count(&catalog.metric_registry, "partition_get_by_id", 1);

        let id2 = cache
            .shard_id(Arc::clone(&cached_table), p2.id, None)
            .await
            .unwrap();
        assert_eq!(id2, s2.shard.id);
        assert_histogram_metric_count(&catalog.metric_registry, "partition_get_by_id", 2);

        let id1 = cache
            .shard_id(Arc::clone(&cached_table), p1.id, None)
            .await
            .unwrap();
        assert_eq!(id1, s1.shard.id);
        assert_histogram_metric_count(&catalog.metric_registry, "partition_get_by_id", 2);

        // non-existing partition
        for _ in 0..2 {
            let res = cache
                .shard_id(Arc::clone(&cached_table), PartitionId::new(i64::MAX), None)
                .await;
            assert_eq!(res, None);
            assert_histogram_metric_count(&catalog.metric_registry, "partition_get_by_id", 3);
        }
    }

    #[tokio::test]
    async fn test_sort_key() {
        let catalog = TestCatalog::new();

        let ns = catalog.create_namespace_1hr_retention("ns").await;
        let t = ns.create_table("table").await;
        let c1 = t.create_column("tag", ColumnType::Tag).await;
        let c2 = t.create_column("time", ColumnType::Time).await;
        let s1 = ns.create_shard(1).await;
        let s2 = ns.create_shard(2).await;
        let p1 = t
            .with_shard(&s1)
            .create_partition_with_sort_key("k1", &["tag", "time"])
            .await
            .partition
            .clone();
        let p2 = t
            .with_shard(&s2)
            .create_partition("k2") // no sort key
            .await
            .partition
            .clone();
        let cached_table = Arc::new(CachedTable {
            id: t.table.id,
            schema: schema(),
            column_id_map: HashMap::from([
                (c1.column.id, Arc::from(c1.column.name.clone())),
                (c2.column.id, Arc::from(c2.column.name.clone())),
            ]),
            column_id_map_rev: HashMap::from([
                (Arc::from(c1.column.name.clone()), c1.column.id),
                (Arc::from(c2.column.name.clone()), c2.column.id),
            ]),
            primary_key_column_ids: vec![c1.column.id, c2.column.id],
        });

        let cache = PartitionCache::new(
            catalog.catalog(),
            BackoffConfig::default(),
            catalog.time_provider(),
            &catalog.metric_registry(),
            test_ram_pool(),
            true,
        );

        let sort_key1a = cache
            .sort_key(Arc::clone(&cached_table), p1.id, &Vec::new(), None)
            .await;
        assert_eq!(
            sort_key1a.as_ref().unwrap().as_ref(),
            &PartitionSortKey {
                sort_key: Arc::new(p1.sort_key().unwrap()),
                column_set: HashSet::from([c1.column.id, c2.column.id]),
                column_order: vec![c1.column.id, c2.column.id],
            }
        );
        assert_histogram_metric_count(&catalog.metric_registry, "partition_get_by_id", 1);

        let sort_key2 = cache
            .sort_key(Arc::clone(&cached_table), p2.id, &Vec::new(), None)
            .await;
        assert_eq!(sort_key2, None);
        assert_histogram_metric_count(&catalog.metric_registry, "partition_get_by_id", 2);

        let sort_key1b = cache
            .sort_key(Arc::clone(&cached_table), p1.id, &Vec::new(), None)
            .await;
        assert!(Arc::ptr_eq(
            sort_key1a.as_ref().unwrap(),
            sort_key1b.as_ref().unwrap()
        ));
        assert_histogram_metric_count(&catalog.metric_registry, "partition_get_by_id", 2);

        // non-existing partition
        for _ in 0..2 {
            let res = cache
                .sort_key(
                    Arc::clone(&cached_table),
                    PartitionId::new(i64::MAX),
                    &Vec::new(),
                    None,
                )
                .await;
            assert_eq!(res, None);
            assert_histogram_metric_count(&catalog.metric_registry, "partition_get_by_id", 3);
        }
    }

    #[tokio::test]
    async fn test_cache_sharing() {
        let catalog = TestCatalog::new();

        let ns = catalog.create_namespace_1hr_retention("ns").await;
        let t = ns.create_table("table").await;
        let c1 = t.create_column("tag", ColumnType::Tag).await;
        let c2 = t.create_column("time", ColumnType::Time).await;
        let s1 = ns.create_shard(1).await;
        let s2 = ns.create_shard(2).await;
        let p1 = t
            .with_shard(&s1)
            .create_partition_with_sort_key("k1", &["tag", "time"])
            .await
            .partition
            .clone();
        let p2 = t
            .with_shard(&s2)
            .create_partition("k2")
            .await
            .partition
            .clone();
        let p3 = t
            .with_shard(&s2)
            .create_partition("k3")
            .await
            .partition
            .clone();
        let cached_table = Arc::new(CachedTable {
            id: t.table.id,
            schema: schema(),
            column_id_map: HashMap::from([
                (c1.column.id, Arc::from(c1.column.name.clone())),
                (c2.column.id, Arc::from(c2.column.name.clone())),
            ]),
            column_id_map_rev: HashMap::from([
                (Arc::from(c1.column.name.clone()), c1.column.id),
                (Arc::from(c2.column.name.clone()), c2.column.id),
            ]),
            primary_key_column_ids: vec![c1.column.id, c2.column.id],
        });

        let cache = PartitionCache::new(
            catalog.catalog(),
            BackoffConfig::default(),
            catalog.time_provider(),
            &catalog.metric_registry(),
            test_ram_pool(),
            true,
        );

        cache.shard_id(Arc::clone(&cached_table), p2.id, None).await;
        cache
            .sort_key(Arc::clone(&cached_table), p3.id, &Vec::new(), None)
            .await;
        assert_histogram_metric_count(&catalog.metric_registry, "partition_get_by_id", 2);

        cache.shard_id(Arc::clone(&cached_table), p1.id, None).await;
        cache
            .sort_key(Arc::clone(&cached_table), p2.id, &Vec::new(), None)
            .await;
        assert_histogram_metric_count(&catalog.metric_registry, "partition_get_by_id", 3);

        cache
            .sort_key(Arc::clone(&cached_table), p1.id, &Vec::new(), None)
            .await;
        cache.shard_id(Arc::clone(&cached_table), p2.id, None).await;
        assert_histogram_metric_count(&catalog.metric_registry, "partition_get_by_id", 3);
    }

    #[tokio::test]
    async fn test_expiration() {
        let catalog = TestCatalog::new();

        let ns = catalog.create_namespace_1hr_retention("ns").await;
        let t = ns.create_table("table").await;
        let c1 = t.create_column("foo", ColumnType::Tag).await;
        let c2 = t.create_column("time", ColumnType::Time).await;
        let s = ns.create_shard(1).await;
        let p = t.with_shard(&s).create_partition("k1").await;
        let p_id = p.partition.id;
        let p_sort_key = p.partition.sort_key();
        let cached_table = Arc::new(CachedTable {
            id: t.table.id,
            schema: schema(),
            column_id_map: HashMap::from([
                (c1.column.id, Arc::from(c1.column.name.clone())),
                (c2.column.id, Arc::from(c2.column.name.clone())),
            ]),
            column_id_map_rev: HashMap::from([
                (Arc::from(c1.column.name.clone()), c1.column.id),
                (Arc::from(c2.column.name.clone()), c2.column.id),
            ]),
            primary_key_column_ids: vec![c1.column.id, c2.column.id],
        });

        let cache = PartitionCache::new(
            catalog.catalog(),
            BackoffConfig::default(),
            catalog.time_provider(),
            &catalog.metric_registry(),
            test_ram_pool(),
            true,
        );

        let sort_key = cache
            .sort_key(Arc::clone(&cached_table), p_id, &Vec::new(), None)
            .await;
        assert_eq!(sort_key, None,);
        assert_histogram_metric_count(&catalog.metric_registry, "partition_get_by_id", 1);

        // requesting nother will not expire
        assert!(p_sort_key.is_none());
        let sort_key = cache
            .sort_key(Arc::clone(&cached_table), p_id, &Vec::new(), None)
            .await;
        assert_eq!(sort_key, None,);
        assert_histogram_metric_count(&catalog.metric_registry, "partition_get_by_id", 1);

        // but requesting something will expire
        let sort_key = cache
            .sort_key(Arc::clone(&cached_table), p_id, &[c1.column.id], None)
            .await;
        assert_eq!(sort_key, None,);
        assert_histogram_metric_count(&catalog.metric_registry, "partition_get_by_id", 2);

        // set sort key
        let p = p
            .update_sort_key(SortKey::from_columns([
                c1.column.name.as_str(),
                c2.column.name.as_str(),
            ]))
            .await;

        // expire & fetch
        let p_sort_key = p.partition.sort_key();
        let sort_key = cache
            .sort_key(Arc::clone(&cached_table), p_id, &[c1.column.id], None)
            .await;
        assert_eq!(
            sort_key.as_ref().unwrap().as_ref(),
            &PartitionSortKey {
                sort_key: Arc::new(p_sort_key.clone().unwrap()),
                column_set: HashSet::from([c1.column.id, c2.column.id]),
                column_order: vec![c1.column.id, c2.column.id],
            }
        );
        assert_histogram_metric_count(&catalog.metric_registry, "partition_get_by_id", 4);

        // subsets and the full key don't expire
        for should_cover in [
            Vec::new(),
            vec![c1.column.id],
            vec![c2.column.id],
            vec![c1.column.id, c2.column.id],
        ] {
            let sort_key_2 = cache
                .sort_key(Arc::clone(&cached_table), p_id, &should_cover, None)
                .await;
            assert!(Arc::ptr_eq(
                sort_key.as_ref().unwrap(),
                sort_key_2.as_ref().unwrap()
            ));
            assert_histogram_metric_count(&catalog.metric_registry, "partition_get_by_id", 4);
        }

        // unknown columns expire
        let c3 = t.create_column("x", ColumnType::Tag).await;
        let sort_key_2 = cache
            .sort_key(
                Arc::clone(&cached_table),
                p_id,
                &[c1.column.id, c3.column.id],
                None,
            )
            .await;
        assert!(!Arc::ptr_eq(
            sort_key.as_ref().unwrap(),
            sort_key_2.as_ref().unwrap()
        ));
        assert_eq!(sort_key, sort_key_2);
        assert_histogram_metric_count(&catalog.metric_registry, "partition_get_by_id", 5);
    }

    fn schema() -> Schema {
        SchemaBuilder::new().build().unwrap()
    }
}
