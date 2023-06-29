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
use data_types::{
    partition_template::{build_column_values, ColumnValue},
    ColumnId, Partition, PartitionId,
};
use datafusion::scalar::ScalarValue;
use iox_catalog::interface::Catalog;
use iox_time::TimeProvider;
use observability_deps::tracing::debug;
use schema::sort::SortKey;
use std::{
    collections::{HashMap, HashSet},
    mem::{size_of, size_of_val},
    sync::Arc,
};
use trace::span::Span;

use crate::df_stats::{ColumnRange, ColumnRanges};

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

                    Some(CachedPartition::new(partition, &extra))
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

    /// Get cached partition.
    ///
    /// Expire partition if the cached sort key does NOT cover the given set of columns.
    pub async fn get(
        &self,
        cached_table: Arc<CachedTable>,
        partition_id: PartitionId,
        sort_key_should_cover: &[ColumnId],
        span: Option<Span>,
    ) -> Option<CachedPartition> {
        self.remove_if_handle
            .remove_if_and_get(
                &self.cache,
                partition_id,
                |cached_partition| {
                    let invalidates =
                        if let Some(sort_key) = &cached_partition.and_then(|p| p.sort_key) {
                            sort_key_should_cover
                                .iter()
                                .any(|col| !sort_key.column_set.contains(col))
                        } else {
                            // no sort key at all => need to update if there is anything to cover
                            !sort_key_should_cover.is_empty()
                        };

                    if invalidates {
                        debug!(
                            partition_id = partition_id.get(),
                            "invalidate partition cache",
                        );
                    }

                    invalidates
                },
                (cached_table, span),
            )
            .await
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CachedPartition {
    pub sort_key: Option<Arc<PartitionSortKey>>,
    pub column_ranges: ColumnRanges,
}

impl CachedPartition {
    fn new(partition: Partition, table: &CachedTable) -> Self {
        let sort_key = partition
            .sort_key()
            .map(|sort_key| Arc::new(PartitionSortKey::new(sort_key, &table.column_id_map_rev)));

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
            sort_key,
            column_ranges: Arc::new(column_ranges),
        }
    }

    /// RAM-bytes EXCLUDING `self`.
    fn size(&self) -> usize {
        // Arc content
        self.sort_key
            .as_ref()
            .map(|sk| sk.size())
            .unwrap_or_default()
            + std::mem::size_of::<HashMap<Arc<str>, ColumnRange>>()
            + (self.column_ranges.capacity() * std::mem::size_of::<(Arc<str>, ColumnRange)>())
            + self
                .column_ranges
                .iter()
                .map(|(col, range)| col.len() + range.min_value.size() + range.max_value.size())
                .sum::<usize>()
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PartitionSortKey {
    pub sort_key: Arc<SortKey>,
    pub column_set: HashSet<ColumnId>,
    pub column_order: Box<[ColumnId]>,
}

impl PartitionSortKey {
    fn new(sort_key: SortKey, column_id_map_rev: &HashMap<Arc<str>, ColumnId>) -> Self {
        let sort_key = Arc::new(sort_key);

        let column_order: Box<[ColumnId]> = sort_key
            .iter()
            .map(|(name, _opts)| {
                *column_id_map_rev
                    .get(name.as_ref())
                    .unwrap_or_else(|| panic!("column_id_map_rev misses data: {name}"))
            })
            .collect();

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
            + (self.column_order.len() * size_of::<ColumnId>())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::cache::{
        ram::test_util::test_ram_pool, test_util::assert_catalog_access_metric_count,
    };
    use data_types::{partition_template::TablePartitionTemplateOverride, ColumnType};
    use generated_types::influxdata::iox::partition_template::v1::{
        template_part::Part, PartitionTemplate, TemplatePart,
    };
    use iox_tests::TestCatalog;
    use schema::{Schema, SchemaBuilder};

    #[tokio::test]
    async fn test_sort_key() {
        let catalog = TestCatalog::new();

        let ns = catalog.create_namespace_1hr_retention("ns").await;
        let t = ns.create_table("table").await;
        let c1 = t.create_column("tag", ColumnType::Tag).await;
        let c2 = t.create_column("time", ColumnType::Time).await;
        let p1 = t
            .create_partition_with_sort_key("k1", &["tag", "time"])
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

        let sort_key1a = cache
            .get(Arc::clone(&cached_table), p1.id, &Vec::new(), None)
            .await
            .unwrap()
            .sort_key;
        assert_eq!(
            sort_key1a.as_ref().unwrap().as_ref(),
            &PartitionSortKey {
                sort_key: Arc::new(p1.sort_key().unwrap()),
                column_set: HashSet::from([c1.column.id, c2.column.id]),
                column_order: [c1.column.id, c2.column.id].into(),
            }
        );
        assert_catalog_access_metric_count(&catalog.metric_registry, "partition_get_by_id", 1);

        let sort_key2 = cache
            .get(Arc::clone(&cached_table), p2.id, &Vec::new(), None)
            .await
            .unwrap()
            .sort_key;
        assert_eq!(sort_key2, None);
        assert_catalog_access_metric_count(&catalog.metric_registry, "partition_get_by_id", 2);

        let sort_key1b = cache
            .get(Arc::clone(&cached_table), p1.id, &Vec::new(), None)
            .await
            .unwrap()
            .sort_key;
        assert!(Arc::ptr_eq(
            sort_key1a.as_ref().unwrap(),
            sort_key1b.as_ref().unwrap()
        ));
        assert_catalog_access_metric_count(&catalog.metric_registry, "partition_get_by_id", 2);

        // non-existing partition
        for _ in 0..2 {
            let res = cache
                .get(
                    Arc::clone(&cached_table),
                    PartitionId::new(i64::MAX),
                    &Vec::new(),
                    None,
                )
                .await;
            assert_eq!(res, None);
            assert_catalog_access_metric_count(&catalog.metric_registry, "partition_get_by_id", 3);
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

        let ranges1a = cache
            .get(Arc::clone(&cached_table), p1.id, &[], None)
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
        assert_catalog_access_metric_count(&catalog.metric_registry, "partition_get_by_id", 1);

        let ranges2 = cache
            .get(Arc::clone(&cached_table), p2.id, &[], None)
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
        assert_catalog_access_metric_count(&catalog.metric_registry, "partition_get_by_id", 2);

        let ranges3 = cache
            .get(Arc::clone(&cached_table), p3.id, &[], None)
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
        assert_catalog_access_metric_count(&catalog.metric_registry, "partition_get_by_id", 3);

        let ranges4 = cache
            .get(Arc::clone(&cached_table), p4.id, &[], None)
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
        assert_catalog_access_metric_count(&catalog.metric_registry, "partition_get_by_id", 4);

        let ranges5 = cache
            .get(Arc::clone(&cached_table), p5.id, &[], None)
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
        assert_catalog_access_metric_count(&catalog.metric_registry, "partition_get_by_id", 5);

        let ranges1b = cache
            .get(Arc::clone(&cached_table), p1.id, &[], None)
            .await
            .unwrap()
            .column_ranges;
        assert!(Arc::ptr_eq(&ranges1a, &ranges1b));
        assert_catalog_access_metric_count(&catalog.metric_registry, "partition_get_by_id", 5);

        // non-existing partition
        for _ in 0..2 {
            let res = cache
                .get(
                    Arc::clone(&cached_table),
                    PartitionId::new(i64::MAX),
                    &[],
                    None,
                )
                .await;
            assert_eq!(res, None);
            assert_catalog_access_metric_count(&catalog.metric_registry, "partition_get_by_id", 6);
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
            .get(Arc::clone(&cached_table), p_id, &[], None)
            .await
            .unwrap()
            .sort_key;
        assert_eq!(sort_key, None,);
        assert_catalog_access_metric_count(&catalog.metric_registry, "partition_get_by_id", 1);

        // requesting nother will not expire
        assert!(p_sort_key.is_none());
        let sort_key = cache
            .get(Arc::clone(&cached_table), p_id, &[], None)
            .await
            .unwrap()
            .sort_key;
        assert_eq!(sort_key, None,);
        assert_catalog_access_metric_count(&catalog.metric_registry, "partition_get_by_id", 1);

        // but requesting something will expire
        let sort_key = cache
            .get(Arc::clone(&cached_table), p_id, &[c1.column.id], None)
            .await
            .unwrap()
            .sort_key;
        assert_eq!(sort_key, None,);
        assert_catalog_access_metric_count(&catalog.metric_registry, "partition_get_by_id", 2);

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
            .get(Arc::clone(&cached_table), p_id, &[c1.column.id], None)
            .await
            .unwrap()
            .sort_key;
        assert_eq!(
            sort_key.as_ref().unwrap().as_ref(),
            &PartitionSortKey {
                sort_key: Arc::new(p_sort_key.clone().unwrap()),
                column_set: HashSet::from([c1.column.id, c2.column.id]),
                column_order: [c1.column.id, c2.column.id].into(),
            }
        );
        assert_catalog_access_metric_count(&catalog.metric_registry, "partition_get_by_id", 4);

        // subsets and the full key don't expire
        for should_cover in [
            Vec::new(),
            vec![c1.column.id],
            vec![c2.column.id],
            vec![c1.column.id, c2.column.id],
        ] {
            let sort_key_2 = cache
                .get(Arc::clone(&cached_table), p_id, &should_cover, None)
                .await
                .unwrap()
                .sort_key;
            assert!(Arc::ptr_eq(
                sort_key.as_ref().unwrap(),
                sort_key_2.as_ref().unwrap()
            ));
            assert_catalog_access_metric_count(&catalog.metric_registry, "partition_get_by_id", 4);
        }

        // unknown columns expire
        let c3 = t.create_column("x", ColumnType::Tag).await;
        let sort_key_2 = cache
            .get(
                Arc::clone(&cached_table),
                p_id,
                &[c1.column.id, c3.column.id],
                None,
            )
            .await
            .unwrap()
            .sort_key;
        assert!(!Arc::ptr_eq(
            sort_key.as_ref().unwrap(),
            sort_key_2.as_ref().unwrap()
        ));
        assert_eq!(sort_key, sort_key_2);
        assert_catalog_access_metric_count(&catalog.metric_registry, "partition_get_by_id", 5);
    }

    fn schema() -> Schema {
        SchemaBuilder::new().build().unwrap()
    }
}
