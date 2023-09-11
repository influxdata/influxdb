//! Partition cache.

use backoff::{Backoff, BackoffConfig};
use cache_system::{
    backend::policy::{
        lru::{LruPolicy, ResourcePool},
        remove_if::{RemoveIfHandle, RemoveIfPolicy},
        PolicyBackend,
    },
    cache::{driver::CacheDriver, metrics::CacheWithMetrics, Cache, CacheGetStatus},
    loader::{
        batch::{BatchLoader, BatchLoaderFlusher, BatchLoaderFlusherExt},
        metrics::MetricsLoader,
        FunctionLoader,
    },
    resource_consumption::FunctionEstimator,
};
use data_types::{
    partition_template::{build_column_values, ColumnValue},
    ColumnId, Partition, SortedColumnSet, TransitionPartitionId,
};
use datafusion::scalar::ScalarValue;
use iox_catalog::{interface::Catalog, partition_lookup_batch};
use iox_query::chunk_statistics::{ColumnRange, ColumnRanges};
use iox_time::TimeProvider;
use observability_deps::tracing::debug;
use schema::sort::SortKey;
use std::{
    collections::{hash_map::Entry, HashMap, HashSet},
    mem::{size_of, size_of_val},
    sync::Arc,
};
use trace::span::{Span, SpanRecorder};

use super::{namespace::CachedTable, ram::RamSize};

const CACHE_ID: &str = "partition";

type CacheT = Box<
    dyn Cache<
        K = TransitionPartitionId,
        V = Option<Arc<CachedPartition>>,
        GetExtra = (Arc<CachedTable>, Option<Span>),
        PeekExtra = ((), Option<Span>),
    >,
>;

/// Cache for partition-related attributes.
#[derive(Debug)]
pub struct PartitionCache {
    cache: CacheT,
    remove_if_handle: RemoveIfHandle<TransitionPartitionId, Option<Arc<CachedPartition>>>,
    flusher: Arc<dyn BatchLoaderFlusher>,
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
        let loader = FunctionLoader::new(
            move |partition_ids: Vec<TransitionPartitionId>,
                  cached_tables: Vec<Arc<CachedTable>>| {
                // sanity checks
                assert_eq!(partition_ids.len(), cached_tables.len());

                let catalog = Arc::clone(&catalog);
                let backoff_config = backoff_config.clone();

                async move {
                    // prepare output buffer
                    let mut out = (0..partition_ids.len()).map(|_| None).collect::<Vec<_>>();
                    let mut out_map =
                        HashMap::<TransitionPartitionId, usize>::with_capacity(partition_ids.len());
                    for (idx, id) in partition_ids.iter().enumerate() {
                        match out_map.entry(id.clone()) {
                            Entry::Occupied(_) => unreachable!(
                                "cache system requested same partition from loader concurrently, \
                                this should have been prevented by the CacheDriver"
                            ),
                            Entry::Vacant(v) => {
                                v.insert(idx);
                            }
                        }
                    }

                    let ids: Vec<&TransitionPartitionId> = partition_ids.iter().collect();

                    // fetch catalog data
                    let partitions = Backoff::new(&backoff_config)
                        .retry_all_errors("get partition_key", || async {
                            let mut repos = catalog.repositories().await;
                            partition_lookup_batch(repos.as_mut(), &ids).await
                        })
                        .await
                        .expect("retry forever");

                    // build output
                    for p in partitions {
                        let idx = out_map[&p.transition_partition_id()];
                        let cached_table = &cached_tables[idx];
                        let p = Arc::new(CachedPartition::new(p, cached_table));
                        out[idx] = Some(p);
                    }

                    out
                }
            },
        );
        let loader = Arc::new(BatchLoader::new(loader));
        let flusher = Arc::clone(&loader);
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
            Arc::new(FunctionEstimator::new(
                |k, v: &Option<Arc<CachedPartition>>| {
                    RamSize(
                        size_of_val(k)
                            + size_of_val(v)
                            + v.as_ref().map(|v| v.size()).unwrap_or_default(),
                    )
                },
            )),
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
            flusher,
        }
    }

    /// Get cached partition.
    ///
    /// The result only contains existing partitions. The order is undefined.
    ///
    /// Expire partition if the cached sort key does NOT cover the given set of columns.
    pub async fn get(
        &self,
        cached_table: Arc<CachedTable>,
        partitions: Vec<PartitionRequest>,
        span: Option<Span>,
    ) -> Vec<Arc<CachedPartition>> {
        let mut span_recorder = SpanRecorder::new(span);

        let futures = partitions
            .into_iter()
            .map(
                |PartitionRequest {
                     partition_id,
                     sort_key_should_cover,
                 }| {
                    let cached_table = Arc::clone(&cached_table);

                    // Do NOT create a span per partition because that floods the tracing system. Just pass `None`
                    // instead. The metric wrappers will still fill emit aggregated metrics which is good enough.
                    let span = None;

                    self.remove_if_handle.remove_if_and_get_with_status(
                        &self.cache,
                        partition_id.clone(),
                        move |cached_partition| {
                            let invalidates = if let Some(sort_key) =
                                &cached_partition.and_then(|p| p.sort_key.clone())
                            {
                                sort_key_should_cover
                                    .iter()
                                    .any(|col| !sort_key.column_set.contains(col))
                            } else {
                                // no sort key at all => need to update if there is anything to cover
                                !sort_key_should_cover.is_empty()
                            };

                            if invalidates {
                                debug!(
                                    %partition_id,
                                    "invalidate partition cache",
                                );
                            }

                            invalidates
                        },
                        (cached_table, span),
                    )
                },
            )
            .collect();

        let res = self.flusher.auto_flush(futures).await;

        let mut n_miss = 0u64;
        let mut n_hit = 0u64;
        let mut n_miss_already_loading = 0u64;
        let out = res
            .into_iter()
            .filter_map(|(p, status)| {
                match status {
                    CacheGetStatus::Miss => n_miss += 1,
                    CacheGetStatus::Hit => n_hit += 1,
                    CacheGetStatus::MissAlreadyLoading => n_miss_already_loading += 1,
                }
                p
            })
            .collect();

        span_recorder.set_metadata("n_miss", n_miss.to_string());
        span_recorder.set_metadata("n_hit", n_hit.to_string());
        span_recorder.set_metadata("n_miss_already_loading", n_miss_already_loading.to_string());
        span_recorder.ok("done");

        out
    }
}

/// Request for [`PartitionCache::get`].
#[derive(Debug)]
pub struct PartitionRequest {
    pub partition_id: TransitionPartitionId,
    pub sort_key_should_cover: Vec<ColumnId>,
}

#[derive(Debug, PartialEq, Eq)]
pub struct CachedPartition {
    pub id: TransitionPartitionId,
    pub sort_key: Option<Arc<PartitionSortKey>>,
    pub column_ranges: ColumnRanges,
}

impl CachedPartition {
    fn new(partition: Partition, table: &CachedTable) -> Self {
        // build sort_key from the partition's sort_key_ids and table columns
        let sort_key = partition.sort_key_ids_none_if_empty().map(|sort_key_ids| {
            Arc::new(PartitionSortKey::new(sort_key_ids, &table.column_id_map))
        });

        // This is here to catch bugs if any while mapping sort_key_ids to column names
        // This wil be removed once sort_key is removed from partition
        let p_sort_key = partition.sort_key();
        assert_eq!(
            sort_key.as_ref().map(|sk| sk.sort_key.as_ref()),
            p_sort_key.as_ref()
        );

        let mut column_ranges =
            build_column_values(&table.partition_template, partition.partition_key.inner())
                .filter_map(|(col, val)| {
                    // resolve column name to already existing Arc for cheaper storage
                    let col = Arc::clone(table.column_id_map_rev.get_key_value(col)?.0);

                    let range = match val {
                        ColumnValue::Identity(s) => {
                            let s = Arc::new(ScalarValue::from(s.as_ref()));
                            ColumnRange {
                                min_value: Arc::clone(&s),
                                max_value: s,
                            }
                        }
                        ColumnValue::Prefix(p) => {
                            if p.is_empty() {
                                // full range => value is useless
                                return None;
                            }

                            // If the partition only has a prefix of the tag value (it was truncated) then form a conservative
                            // range:
                            //
                            //
                            // # Minimum
                            // Use the prefix itself.
                            //
                            // Note that the minimum is inclusive.
                            //
                            // All values in the partition are either:
                            // - identical to the prefix, in which case they are included by the inclusive minimum
                            // - have the form `"<prefix><s>"`, and it holds that `"<prefix><s>" > "<prefix>"` for all
                            //   strings `"<s>"`.
                            //
                            //
                            // # Maximum
                            // Use `"<prefix_excluding_last_char><char::max>"`.
                            //
                            // Note that the maximum is inclusive.
                            //
                            // All strings in this partition must be smaller than this constructed maximum, because
                            // string comparison is front-to-back and the `"<prefix_excluding_last_char><char::max>" > "<prefix>"`.

                            let min_value = Arc::new(ScalarValue::from(p.as_ref()));

                            let mut chars = p.as_ref().chars().collect::<Vec<_>>();
                            *chars.last_mut().expect("checked that prefix is not empty") =
                                std::char::MAX;
                            let max_value = Arc::new(ScalarValue::from(
                                chars.into_iter().collect::<String>().as_str(),
                            ));

                            ColumnRange {
                                min_value,
                                max_value,
                            }
                        }
                    };

                    Some((col, range))
                })
                .collect::<HashMap<_, _>>();
        column_ranges.shrink_to_fit();

        Self {
            id: partition.transition_partition_id(),
            sort_key,
            column_ranges: Arc::new(column_ranges),
        }
    }

    /// RAM-bytes INCLUDING `self`.
    fn size(&self) -> usize {
        let id = self.id.size() - std::mem::size_of_val(&self.id);

        // Arc content
        let sort_key = self
            .sort_key
            .as_ref()
            .map(|sk| sk.size())
            .unwrap_or_default();

        // Arc content
        let column_ranges = std::mem::size_of::<HashMap<Arc<str>, ColumnRange>>()
            + (self.column_ranges.capacity() * std::mem::size_of::<(Arc<str>, ColumnRange)>())
            + self
                .column_ranges
                .iter()
                .map(|(col, range)| col.len() + range.min_value.size() + range.max_value.size())
                .sum::<usize>();

        std::mem::size_of_val(self) + id + sort_key + column_ranges
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PartitionSortKey {
    pub sort_key: Arc<SortKey>,
    pub column_set: HashSet<ColumnId>,
    pub column_order: Box<[ColumnId]>,
}

impl PartitionSortKey {
    fn new(sort_key_ids: SortedColumnSet, column_id_map: &HashMap<ColumnId, Arc<str>>) -> Self {
        let column_order: Box<[ColumnId]> = sort_key_ids.iter().copied().collect();

        // build sort_key from the column order
        let sort_key = SortKey::from_columns(
            column_order
                .iter()
                .map(|c_id| {
                    column_id_map.get(c_id).unwrap_or_else(|| {
                        panic!("column_id_map misses data for column id: {}", c_id.get())
                    })
                })
                .cloned(),
        );

        let column_set: HashSet<ColumnId> = column_order.iter().copied().collect();

        Self {
            sort_key: Arc::new(sort_key),
            column_set,
            column_order,
        }
    }

    /// Size of this object in bytes, including `self`.
    fn size(&self) -> usize {
        size_of_val(self)
            + self.sort_key.as_ref().size()
            + (self.column_set.capacity() * size_of::<ColumnId>())
            + (self.column_order.len() * size_of::<ColumnId>())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::cache::{
        ram::test_util::test_ram_pool, test_util::assert_catalog_access_metric_count,
    };
    use async_trait::async_trait;
    use data_types::{
        partition_template::TablePartitionTemplateOverride, ColumnType, PartitionId, PartitionKey,
        SortedColumnSet, TableId,
    };
    use futures::StreamExt;
    use generated_types::influxdata::iox::partition_template::v1::{
        template_part::Part, PartitionTemplate, TemplatePart,
    };
    use iox_tests::{TestCatalog, TestNamespace};
    use schema::{Schema, SchemaBuilder};
    use tokio::sync::Barrier;

    #[tokio::test]
    async fn test_sort_key() {
        let catalog = TestCatalog::new();

        let ns = catalog.create_namespace_1hr_retention("ns").await;
        let t = ns.create_table("table").await;
        let c1 = t.create_column("tag", ColumnType::Tag).await;
        let c2 = t.create_column("time", ColumnType::Time).await;
        let p1 = t
            .create_partition_with_sort_key("k1", &["tag", "time"], &[c1.id(), c2.id()])
            .await
            .partition
            .clone();
        let p2 = t
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
            primary_key_column_ids: [c1.column.id, c2.column.id].into(),
            partition_template: TablePartitionTemplateOverride::default(),
        });

        let cache = PartitionCache::new(
            catalog.catalog(),
            BackoffConfig::default(),
            catalog.time_provider(),
            &catalog.metric_registry(),
            test_ram_pool(),
            true,
        );

        let p1_id = p1.transition_partition_id();
        let p2_id = p2.transition_partition_id();

        let sort_key1a = cache
            .get_one(Arc::clone(&cached_table), &p1_id, &Vec::new(), None)
            .await
            .unwrap()
            .sort_key
            .clone();
        assert_eq!(
            sort_key1a.as_ref().unwrap().as_ref(),
            &PartitionSortKey {
                sort_key: Arc::new(p1.sort_key().unwrap()),
                column_set: HashSet::from([c1.column.id, c2.column.id]),
                column_order: [c1.column.id, c2.column.id].into(),
            }
        );
        assert_catalog_access_metric_count(
            &catalog.metric_registry,
            "partition_get_by_hash_id_batch",
            1,
        );

        let sort_key2 = cache
            .get_one(Arc::clone(&cached_table), &p2_id, &Vec::new(), None)
            .await
            .unwrap()
            .sort_key
            .clone();
        assert_eq!(sort_key2, None);
        assert_catalog_access_metric_count(
            &catalog.metric_registry,
            "partition_get_by_hash_id_batch",
            2,
        );

        let sort_key1b = cache
            .get_one(Arc::clone(&cached_table), &p1_id, &Vec::new(), None)
            .await
            .unwrap()
            .sort_key
            .clone();
        assert!(Arc::ptr_eq(
            sort_key1a.as_ref().unwrap(),
            sort_key1b.as_ref().unwrap()
        ));
        assert_catalog_access_metric_count(
            &catalog.metric_registry,
            "partition_get_by_hash_id_batch",
            2,
        );

        // non-existing partition
        for _ in 0..2 {
            // Non-existing partition identified by partition hash ID
            let res = cache
                .get_one(
                    Arc::clone(&cached_table),
                    &TransitionPartitionId::new(
                        TableId::new(i64::MAX),
                        &PartitionKey::from("bananas_not_found"),
                    ),
                    &[],
                    None,
                )
                .await;
            assert_eq!(res, None);
            assert_catalog_access_metric_count(
                &catalog.metric_registry,
                "partition_get_by_hash_id_batch",
                3,
            );

            // Non-existing partition identified by deprecated catalog IDs; this part can be
            // removed when partition identification is fully transitioned to partition hash IDs
            let res = cache
                .get_one(
                    Arc::clone(&cached_table),
                    &TransitionPartitionId::Deprecated(PartitionId::new(i64::MAX)),
                    &[],
                    None,
                )
                .await;
            assert_eq!(res, None);
            assert_catalog_access_metric_count(
                &catalog.metric_registry,
                "partition_get_by_id_batch",
                1,
            );
        }
    }

    #[tokio::test]
    async fn test_column_ranges() {
        let catalog = TestCatalog::new();

        let ns = catalog.create_namespace_1hr_retention("ns").await;
        let t = ns
            .create_table_with_partition_template(
                "table",
                Some(PartitionTemplate {
                    parts: vec![
                        TemplatePart {
                            part: Some(Part::TagValue(String::from("tag2"))),
                        },
                        TemplatePart {
                            part: Some(Part::TagValue(String::from("tag1"))),
                        },
                    ],
                }),
            )
            .await;
        let c1 = t.create_column("tag1", ColumnType::Tag).await;
        let c2 = t.create_column("tag2", ColumnType::Tag).await;
        let c3 = t.create_column("tag3", ColumnType::Tag).await;
        let c4 = t.create_column("time", ColumnType::Time).await;

        // See `data_types::partition_template` for the template language.
        // Two normal values.
        let p1 = t.create_partition("v1|v2").await.partition.clone();
        // 2nd part is NULL
        let p2 = t.create_partition("v1|!").await.partition.clone();
        // 2nd part is empty
        let p3 = t.create_partition("v1|^").await.partition.clone();
        // 2nd part is truncated (i.e. the original value was longer)
        let p4 = t.create_partition("v1|v2#").await.partition.clone();
        // 2nd part is truncated to empty string
        let p5 = t.create_partition("v1|#").await.partition.clone();
        let cached_table = Arc::new(CachedTable {
            id: t.table.id,
            schema: schema(),
            column_id_map: HashMap::from([
                (c1.column.id, Arc::from(c1.column.name.clone())),
                (c2.column.id, Arc::from(c2.column.name.clone())),
                (c3.column.id, Arc::from(c3.column.name.clone())),
                (c4.column.id, Arc::from(c4.column.name.clone())),
            ]),
            column_id_map_rev: HashMap::from([
                (Arc::from(c1.column.name.clone()), c1.column.id),
                (Arc::from(c2.column.name.clone()), c2.column.id),
                (Arc::from(c3.column.name.clone()), c3.column.id),
                (Arc::from(c4.column.name.clone()), c4.column.id),
            ]),
            primary_key_column_ids: [c1.column.id, c2.column.id, c3.column.id, c4.column.id].into(),
            partition_template: t.table.partition_template.clone(),
        });

        let cache = PartitionCache::new(
            catalog.catalog(),
            BackoffConfig::default(),
            catalog.time_provider(),
            &catalog.metric_registry(),
            test_ram_pool(),
            true,
        );

        let p1_id = p1.transition_partition_id();
        let p2_id = p2.transition_partition_id();
        let p3_id = p3.transition_partition_id();
        let p4_id = p4.transition_partition_id();
        let p5_id = p5.transition_partition_id();

        let ranges1a = &cache
            .get_one(Arc::clone(&cached_table), &p1_id, &[], None)
            .await
            .unwrap()
            .column_ranges;
        assert_eq!(
            ranges1a.as_ref(),
            &HashMap::from([
                (
                    Arc::from("tag1"),
                    ColumnRange {
                        min_value: Arc::new(ScalarValue::from("v2")),
                        max_value: Arc::new(ScalarValue::from("v2"))
                    }
                ),
                (
                    Arc::from("tag2"),
                    ColumnRange {
                        min_value: Arc::new(ScalarValue::from("v1")),
                        max_value: Arc::new(ScalarValue::from("v1"))
                    }
                ),
            ]),
        );
        assert!(Arc::ptr_eq(
            &ranges1a.get("tag1").unwrap().min_value,
            &ranges1a.get("tag1").unwrap().max_value,
        ));
        assert_catalog_access_metric_count(
            &catalog.metric_registry,
            "partition_get_by_hash_id_batch",
            1,
        );

        let ranges2 = &cache
            .get_one(Arc::clone(&cached_table), &p2_id, &[], None)
            .await
            .unwrap()
            .column_ranges;
        assert_eq!(
            ranges2.as_ref(),
            &HashMap::from([(
                Arc::from("tag2"),
                ColumnRange {
                    min_value: Arc::new(ScalarValue::from("v1")),
                    max_value: Arc::new(ScalarValue::from("v1"))
                }
            ),]),
        );
        assert_catalog_access_metric_count(
            &catalog.metric_registry,
            "partition_get_by_hash_id_batch",
            2,
        );

        let ranges3 = &cache
            .get_one(Arc::clone(&cached_table), &p3_id, &[], None)
            .await
            .unwrap()
            .column_ranges;
        assert_eq!(
            ranges3.as_ref(),
            &HashMap::from([
                (
                    Arc::from("tag1"),
                    ColumnRange {
                        min_value: Arc::new(ScalarValue::from("")),
                        max_value: Arc::new(ScalarValue::from(""))
                    }
                ),
                (
                    Arc::from("tag2"),
                    ColumnRange {
                        min_value: Arc::new(ScalarValue::from("v1")),
                        max_value: Arc::new(ScalarValue::from("v1"))
                    }
                ),
            ]),
        );
        assert_catalog_access_metric_count(
            &catalog.metric_registry,
            "partition_get_by_hash_id_batch",
            3,
        );

        let ranges4 = &cache
            .get_one(Arc::clone(&cached_table), &p4_id, &[], None)
            .await
            .unwrap()
            .column_ranges;
        assert_eq!(
            ranges4.as_ref(),
            &HashMap::from([
                (
                    Arc::from("tag1"),
                    ColumnRange {
                        min_value: Arc::new(ScalarValue::from("v2")),
                        max_value: Arc::new(ScalarValue::from("v\u{10FFFF}"))
                    }
                ),
                (
                    Arc::from("tag2"),
                    ColumnRange {
                        min_value: Arc::new(ScalarValue::from("v1")),
                        max_value: Arc::new(ScalarValue::from("v1"))
                    }
                ),
            ]),
        );
        assert_catalog_access_metric_count(
            &catalog.metric_registry,
            "partition_get_by_hash_id_batch",
            4,
        );

        let ranges5 = &cache
            .get_one(Arc::clone(&cached_table), &p5_id, &[], None)
            .await
            .unwrap()
            .column_ranges;
        assert_eq!(
            ranges5.as_ref(),
            &HashMap::from([(
                Arc::from("tag2"),
                ColumnRange {
                    min_value: Arc::new(ScalarValue::from("v1")),
                    max_value: Arc::new(ScalarValue::from("v1"))
                }
            ),]),
        );
        assert_catalog_access_metric_count(
            &catalog.metric_registry,
            "partition_get_by_hash_id_batch",
            5,
        );

        let ranges1b = &cache
            .get_one(Arc::clone(&cached_table), &p1_id, &[], None)
            .await
            .unwrap()
            .column_ranges;
        assert!(Arc::ptr_eq(ranges1a, ranges1b));
        assert_catalog_access_metric_count(
            &catalog.metric_registry,
            "partition_get_by_hash_id_batch",
            5,
        );

        for _ in 0..2 {
            // Non-existing partition identified by partition hash ID
            let res = cache
                .get_one(
                    Arc::clone(&cached_table),
                    &TransitionPartitionId::new(
                        TableId::new(i64::MAX),
                        &PartitionKey::from("bananas_not_found"),
                    ),
                    &[],
                    None,
                )
                .await;
            assert_eq!(res, None);
            assert_catalog_access_metric_count(
                &catalog.metric_registry,
                "partition_get_by_hash_id_batch",
                6,
            );

            // Non-existing partition identified by deprecated catalog IDs; this part can be
            // removed when partition identification is fully transitioned to partition hash IDs
            let res = cache
                .get_one(
                    Arc::clone(&cached_table),
                    &TransitionPartitionId::Deprecated(PartitionId::new(i64::MAX)),
                    &[],
                    None,
                )
                .await;
            assert_eq!(res, None);
            assert_catalog_access_metric_count(
                &catalog.metric_registry,
                "partition_get_by_id_batch",
                1,
            );
        }
    }

    #[tokio::test]
    async fn test_expiration() {
        let catalog = TestCatalog::new();

        let ns = catalog.create_namespace_1hr_retention("ns").await;
        let t = ns.create_table("table").await;
        let c1 = t.create_column("foo", ColumnType::Tag).await;
        let c2 = t.create_column("time", ColumnType::Time).await;
        let p = t.create_partition("k1").await;
        let p_id = p.partition.transition_partition_id();
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
            primary_key_column_ids: [c1.column.id, c2.column.id].into(),
            partition_template: TablePartitionTemplateOverride::default(),
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
            .get_one(Arc::clone(&cached_table), &p_id, &[], None)
            .await
            .unwrap()
            .sort_key
            .clone();
        assert_eq!(sort_key, None,);
        assert_catalog_access_metric_count(
            &catalog.metric_registry,
            "partition_get_by_hash_id_batch",
            1,
        );

        // requesting nother will not expire
        assert!(p_sort_key.is_none());
        let sort_key = cache
            .get_one(Arc::clone(&cached_table), &p_id, &[], None)
            .await
            .unwrap()
            .sort_key
            .clone();
        assert_eq!(sort_key, None,);
        assert_catalog_access_metric_count(
            &catalog.metric_registry,
            "partition_get_by_hash_id_batch",
            1,
        );

        // but requesting something will expire
        let sort_key = cache
            .get_one(Arc::clone(&cached_table), &p_id, &[c1.column.id], None)
            .await
            .unwrap()
            .sort_key
            .clone();
        assert_eq!(sort_key, None,);
        assert_catalog_access_metric_count(
            &catalog.metric_registry,
            "partition_get_by_hash_id_batch",
            2,
        );

        // set sort key
        let p = p
            .update_sort_key(
                SortKey::from_columns([c1.column.name.as_str(), c2.column.name.as_str()]),
                &SortedColumnSet::from([c1.column.id.get(), c2.column.id.get()]),
            )
            .await;
        assert_catalog_access_metric_count(&catalog.metric_registry, "partition_get_by_hash_id", 1);

        // expire & fetch
        let p_sort_key = p.partition.sort_key();
        let sort_key = cache
            .get_one(Arc::clone(&cached_table), &p_id, &[c1.column.id], None)
            .await
            .unwrap()
            .sort_key
            .clone();
        assert_eq!(
            sort_key.as_ref().unwrap().as_ref(),
            &PartitionSortKey {
                sort_key: Arc::new(p_sort_key.clone().unwrap()),
                column_set: HashSet::from([c1.column.id, c2.column.id]),
                column_order: [c1.column.id, c2.column.id].into(),
            }
        );
        assert_catalog_access_metric_count(
            &catalog.metric_registry,
            "partition_get_by_hash_id_batch",
            3,
        );

        // subsets and the full key don't expire
        for should_cover in [
            Vec::new(),
            vec![c1.column.id],
            vec![c2.column.id],
            vec![c1.column.id, c2.column.id],
        ] {
            let sort_key_2 = cache
                .get_one(Arc::clone(&cached_table), &p_id, &should_cover, None)
                .await
                .unwrap()
                .sort_key
                .clone();
            assert!(Arc::ptr_eq(
                sort_key.as_ref().unwrap(),
                sort_key_2.as_ref().unwrap()
            ));
            assert_catalog_access_metric_count(
                &catalog.metric_registry,
                "partition_get_by_hash_id_batch",
                3,
            );
        }

        // unknown columns expire
        let c3 = t.create_column("x", ColumnType::Tag).await;
        let sort_key_2 = cache
            .get_one(
                Arc::clone(&cached_table),
                &p_id,
                &[c1.column.id, c3.column.id],
                None,
            )
            .await
            .unwrap()
            .sort_key
            .clone();
        assert!(!Arc::ptr_eq(
            sort_key.as_ref().unwrap(),
            sort_key_2.as_ref().unwrap()
        ));
        assert_eq!(sort_key, sort_key_2);
        assert_catalog_access_metric_count(
            &catalog.metric_registry,
            "partition_get_by_hash_id_batch",
            4,
        );
    }

    #[tokio::test]
    async fn test_multi_get() {
        let catalog = TestCatalog::new();

        let ns = catalog.create_namespace_1hr_retention("ns").await;
        let t = ns.create_table("table").await;
        let p1 = t.create_partition("k1").await.partition.clone();
        let p2 = t.create_partition("k2").await.partition.clone();
        let cached_table = Arc::new(CachedTable {
            id: t.table.id,
            schema: schema(),
            column_id_map: HashMap::default(),
            column_id_map_rev: HashMap::default(),
            primary_key_column_ids: [].into(),
            partition_template: TablePartitionTemplateOverride::default(),
        });

        let cache = PartitionCache::new(
            catalog.catalog(),
            BackoffConfig::default(),
            catalog.time_provider(),
            &catalog.metric_registry(),
            test_ram_pool(),
            true,
        );

        let p1_id = p1.transition_partition_id();
        let p2_id = p2.transition_partition_id();

        let mut res = cache
            .get(
                Arc::clone(&cached_table),
                vec![
                    PartitionRequest {
                        partition_id: p1_id.clone(),
                        sort_key_should_cover: vec![],
                    },
                    PartitionRequest {
                        partition_id: p2_id.clone(),
                        sort_key_should_cover: vec![],
                    },
                    PartitionRequest {
                        partition_id: p1_id.clone(),
                        sort_key_should_cover: vec![],
                    },
                    // requesting non-existing partitions is fine, they just don't appear in
                    // the output
                    PartitionRequest {
                        partition_id: TransitionPartitionId::Deprecated(PartitionId::new(i64::MAX)),
                        sort_key_should_cover: vec![],
                    },
                    PartitionRequest {
                        partition_id: TransitionPartitionId::new(
                            TableId::new(i64::MAX),
                            &PartitionKey::from("bananas_not_found"),
                        ),
                        sort_key_should_cover: vec![],
                    },
                ],
                None,
            )
            .await;
        res.sort_by(|a, b| a.id.cmp(&b.id));
        let ids = res.into_iter().map(|p| p.id.clone()).collect::<Vec<_>>();
        assert_eq!(ids, vec![p1_id.clone(), p1_id, p2_id]);
        assert_catalog_access_metric_count(
            &catalog.metric_registry,
            "partition_get_by_id_batch",
            1,
        );

        // empty get
        let res = cache.get(Arc::clone(&cached_table), vec![], None).await;
        assert_eq!(res, vec![]);
    }

    /// This is a regression test for <https://github.com/influxdata/influxdb_iox/issues/8286>.
    ///
    /// The issue happened when requests for multiple (different) tables were made concurrently. The root cause was the
    /// wrong assumption that when flushing the batched up requests, there would only be a single table in the flushed set.
    ///
    /// To trigger this, we need at least 2 tokio threads.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn test_multi_table_concurrent_get() {
        // In most cases, the issue triggers on the first run. However let's be sure and try multiple times.
        for _ in 0..10 {
            test_multi_table_concurrent_get_inner().await;
        }
    }

    #[test]
    fn test_partition_sort_key() {
        // map column id -> name
        let column_id_map = HashMap::from([
            (ColumnId::new(1), Arc::from("tag1")),
            (ColumnId::new(2), Arc::from("tag2")),
            (ColumnId::new(3), Arc::from("tag3")),
            (ColumnId::new(4), Arc::from("time")),
            (ColumnId::new(5), Arc::from("field1")),
            (ColumnId::new(6), Arc::from("field2")),
        ]);

        // same order of the columns
        let sort_key_ids = SortedColumnSet::from(vec![1, 2, 3, 4]);
        let p_sort_key = PartitionSortKey::new(sort_key_ids, &column_id_map);
        assert_eq!(
            p_sort_key,
            PartitionSortKey {
                sort_key: Arc::new(SortKey::from_columns(vec!["tag1", "tag2", "tag3", "time"])),
                column_set: HashSet::from([
                    ColumnId::new(1),
                    ColumnId::new(2),
                    ColumnId::new(3),
                    ColumnId::new(4)
                ]),
                column_order: [
                    ColumnId::new(1),
                    ColumnId::new(2),
                    ColumnId::new(3),
                    ColumnId::new(4)
                ]
                .into(),
            }
        );

        // different order
        let sort_key_ids = SortedColumnSet::from(vec![3, 1, 4]);
        let p_sort_key = PartitionSortKey::new(sort_key_ids, &column_id_map);
        assert_eq!(
            p_sort_key,
            PartitionSortKey {
                sort_key: Arc::new(SortKey::from_columns(vec!["tag3", "tag1", "time"])),
                column_set: HashSet::from([ColumnId::new(3), ColumnId::new(1), ColumnId::new(4)]),
                column_order: [ColumnId::new(3), ColumnId::new(1), ColumnId::new(4)].into(),
            }
        );

        // only time
        let sort_key_ids = SortedColumnSet::from(vec![4]);
        let p_sort_key = PartitionSortKey::new(sort_key_ids, &column_id_map);
        assert_eq!(
            p_sort_key,
            PartitionSortKey {
                sort_key: Arc::new(SortKey::from_columns(vec!["time"])),
                column_set: HashSet::from([ColumnId::new(4)]),
                column_order: [ColumnId::new(4)].into(),
            }
        );
    }

    #[test]
    #[should_panic(expected = "column_id_map misses data for column id: 9")]
    fn test_partition_sort_key_panic() {
        // map column id -> name
        let column_id_map = HashMap::from([
            (ColumnId::new(1), Arc::from("tag1")),
            (ColumnId::new(2), Arc::from("tag2")),
            (ColumnId::new(3), Arc::from("tag3")),
            (ColumnId::new(4), Arc::from("time")),
            (ColumnId::new(5), Arc::from("field1")),
            (ColumnId::new(6), Arc::from("field2")),
        ]);

        // same order of the columns
        let sort_key_ids = SortedColumnSet::from(vec![1, 9]);
        let _sort_key = PartitionSortKey::new(sort_key_ids, &column_id_map);
    }

    /// Actually implementation of [`test_multi_table_concurrent_get`] that is tried multiple times.
    async fn test_multi_table_concurrent_get_inner() {
        let catalog = TestCatalog::new();

        // prepare catalog state for two tables
        let ns = catalog.create_namespace_1hr_retention("ns").await;
        let state_1 = ConcurrencyTestState::prepare(&ns, "t1").await;
        let state_2 = ConcurrencyTestState::prepare(&ns, "t2").await;

        // sanity checks for test setup
        assert!(!Arc::ptr_eq(&state_1.cached_table, &state_2.cached_table));
        assert_ne!(state_1.cached_table.id, state_2.cached_table.id);
        assert_ne!(state_1.c_id, state_2.c_id);
        assert_ne!(state_1.partitions, state_2.partitions);

        let cache = Arc::new(PartitionCache::new(
            catalog.catalog(),
            BackoffConfig::default(),
            catalog.time_provider(),
            &catalog.metric_registry(),
            test_ram_pool(),
            true,
        ));

        // use a barrier to make sure that both tokio tasks are running at the same time
        let barrier = Arc::new(Barrier::new(2));

        // set up first tokio task
        let barrier_captured = Arc::clone(&barrier);
        let cache_captured = Arc::clone(&cache);
        let handle_1 = tokio::spawn(async move {
            barrier_captured.wait().await;

            // When running quickly, both tasks will end up on the same tokio worker and will run in sequence. It seems
            // that tokio tries to avoid costly work-stealing. However we can trick tokio into actually running both
            // task concurrently with a bit more async work: a simple sleep.
            tokio::time::sleep(std::time::Duration::from_millis(10)).await;

            state_1.run(cache_captured).await;
        });

        // set up 2nd tokio tasks in a same manner as the first one (but for the other table)
        let barrier_captured = Arc::clone(&barrier);
        let cache_captured = Arc::clone(&cache);
        let handle_2 = tokio::spawn(async move {
            barrier_captured.wait().await;
            tokio::time::sleep(std::time::Duration::from_millis(10)).await;
            state_2.run(cache_captured).await;
        });

        handle_1.await.unwrap();
        handle_2.await.unwrap();
    }

    /// Building block for a single table within the [`test_multi_table_concurrent_get`] test.
    struct ConcurrencyTestState {
        /// Cached table that is used for [`PartitionCache::get`].
        cached_table: Arc<CachedTable>,

        /// ID of the only column within that table.
        c_id: ColumnId,

        /// Partitions within that table.
        partitions: Vec<TransitionPartitionId>,
    }

    impl ConcurrencyTestState {
        /// Prepare catalog state.
        async fn prepare(ns: &Arc<TestNamespace>, name: &str) -> Self {
            let t = ns.create_table(name).await;
            let c = t.create_column("time", ColumnType::Time).await;
            let cached_table = Arc::new(CachedTable {
                id: t.table.id,
                schema: schema(),
                column_id_map: HashMap::from([(c.column.id, Arc::from(c.column.name.clone()))]),
                column_id_map_rev: HashMap::from([(Arc::from(c.column.name.clone()), c.column.id)]),
                primary_key_column_ids: [c.column.id].into(),
                partition_template: TablePartitionTemplateOverride::default(),
            });
            const N_PARTITIONS: usize = 20;
            let c_id = c.column.id.get();
            let mut partitions = futures::stream::iter(0..N_PARTITIONS)
                .then(|i| {
                    let t = Arc::clone(&t);
                    async move {
                        t.create_partition_with_sort_key(&format!("p{i}"), &["time"], &[c_id])
                            .await
                            .partition
                            .transition_partition_id()
                    }
                })
                .collect::<Vec<_>>()
                .await;
            partitions.sort();

            Self {
                cached_table,
                c_id: c.column.id,
                partitions,
            }
        }

        /// Perform the actual [`PartitionCache::get`] call and run some basic sanity checks on the
        /// result.
        async fn run(self, cache: Arc<PartitionCache>) {
            let Self {
                cached_table,
                c_id,
                partitions,
            } = self;

            let mut results = cache
                .get(
                    cached_table,
                    partitions
                        .iter()
                        .map(|p| PartitionRequest {
                            partition_id: p.clone(),
                            sort_key_should_cover: vec![],
                        })
                        .collect(),
                    None,
                )
                .await;
            results.sort_by(|a, b| a.id.cmp(&b.id));
            let partitions_res = results.iter().map(|p| p.id.clone()).collect::<Vec<_>>();
            assert_eq!(partitions, partitions_res);
            assert!(results
                .iter()
                .all(|p| p.sort_key.as_ref().unwrap().column_set == HashSet::from([c_id])));
        }
    }

    fn schema() -> Schema {
        SchemaBuilder::new().build().unwrap()
    }

    /// Extension methods for simpler testing.
    #[async_trait]
    trait PartitionCacheExt {
        async fn get_one(
            &self,
            cached_table: Arc<CachedTable>,
            partition_id: &TransitionPartitionId,
            sort_key_should_cover: &[ColumnId],
            span: Option<Span>,
        ) -> Option<Arc<CachedPartition>>;
    }

    #[async_trait]
    impl PartitionCacheExt for PartitionCache {
        async fn get_one(
            &self,
            cached_table: Arc<CachedTable>,
            partition_id: &TransitionPartitionId,
            sort_key_should_cover: &[ColumnId],
            span: Option<Span>,
        ) -> Option<Arc<CachedPartition>> {
            self.get(
                cached_table,
                vec![PartitionRequest {
                    partition_id: partition_id.clone(),
                    sort_key_should_cover: sort_key_should_cover.to_vec(),
                }],
                span,
            )
            .await
            .into_iter()
            .next()
        }
    }
}
