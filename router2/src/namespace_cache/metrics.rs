//! Metric instrumentation for a [`NamespaceCache`] implementation.

use std::sync::Arc;

use data_types::DatabaseName;
use iox_catalog::interface::NamespaceSchema;
use metric::{Metric, U64Counter, U64Gauge};

use super::NamespaceCache;

/// An [`InstrumentedCache`] decorates a [`NamespaceCache`] with cache read
/// hit/miss and cache put insert/update metrics.
#[derive(Debug)]
pub struct InstrumentedCache<T> {
    inner: T,

    /// Metrics derived from the [`NamespaceSchema`] held within the cache.
    table_count: U64Gauge,
    column_count: U64Gauge,

    /// A cache read hit
    get_hit_counter: U64Counter,
    /// A cache read miss
    get_miss_counter: U64Counter,

    /// A cache put for a namespace that did not previously exist.
    put_insert_counter: U64Counter,
    /// A cache put replacing a namespace that previously had a cache entry.
    put_update_counter: U64Counter,
}

impl<T> InstrumentedCache<T> {
    /// Instrument `T`, recording cache operations to `registry`.
    pub fn new(inner: T, registry: &metric::Registry) -> Self {
        let get_counter: Metric<U64Counter> =
            registry.register_metric("namespace_cache_get_count", "cache read requests");
        let get_hit_counter = get_counter.recorder(&[("result", "hit")]);
        let get_miss_counter = get_counter.recorder(&[("result", "miss")]);

        let put_counter: Metric<U64Counter> =
            registry.register_metric("namespace_cache_put_count", "cache put requests");
        let put_insert_counter = put_counter.recorder(&[("op", "insert")]);
        let put_update_counter = put_counter.recorder(&[("op", "update")]);

        let table_count = registry
            .register_metric::<U64Gauge>(
                "namespace_cache_table_count",
                "number of tables in the cache across all namespaces",
            )
            .recorder([]);
        let column_count = registry
            .register_metric::<U64Gauge>(
                "namespace_cache_column_count",
                "number of columns in the cache across all tables and namespaces",
            )
            .recorder([]);

        Self {
            inner,
            table_count,
            column_count,
            get_hit_counter,
            get_miss_counter,
            put_insert_counter,
            put_update_counter,
        }
    }
}

impl<T> NamespaceCache for Arc<InstrumentedCache<T>>
where
    T: NamespaceCache,
{
    fn get_schema(&self, namespace: &DatabaseName<'_>) -> Option<Arc<NamespaceSchema>> {
        match self.inner.get_schema(namespace) {
            Some(v) => {
                self.get_hit_counter.inc(1);
                Some(v)
            }
            None => {
                self.get_miss_counter.inc(1);
                None
            }
        }
    }

    fn put_schema(
        &self,
        namespace: DatabaseName<'static>,
        schema: impl Into<Arc<NamespaceSchema>>,
    ) -> Option<Arc<NamespaceSchema>> {
        let schema = schema.into();
        let stats = NamespaceStats::new(&*schema);

        match self.inner.put_schema(namespace, schema) {
            Some(v) => {
                self.put_update_counter.inc(1);

                // Figure out the difference between the new namespace and the
                // evicted old namespace
                let old_stats = NamespaceStats::new(&*v);
                let table_count_diff = stats.table_count as i64 - old_stats.table_count as i64;
                let column_count_diff = stats.column_count as i64 - old_stats.column_count as i64;

                // Adjust the metrics to reflect the change
                self.table_count.delta(table_count_diff);
                self.column_count.delta(column_count_diff);

                Some(v)
            }
            None => {
                self.put_insert_counter.inc(1);

                // Add the new namespace stats to the counts.
                self.table_count.inc(stats.table_count);
                self.column_count.inc(stats.column_count);

                None
            }
        }
    }
}

#[derive(Debug)]
struct NamespaceStats {
    table_count: u64,
    column_count: u64,
}

impl NamespaceStats {
    fn new(ns: &NamespaceSchema) -> Self {
        let table_count = ns.tables.len() as _;
        let column_count = ns.tables.values().fold(0, |acc, t| acc + t.columns.len()) as _;
        Self {
            table_count,
            column_count,
        }
    }
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;

    use iox_catalog::interface::{
        ColumnId, ColumnSchema, ColumnType, KafkaTopicId, NamespaceId, QueryPoolId, TableId,
        TableSchema,
    };
    use metric::{MetricObserver, Observation};

    use crate::namespace_cache::MemoryNamespaceCache;

    use super::*;

    /// Deterministically generate a schema containing tables with the specified
    /// column cardinality.
    fn new_schema(tables: &[usize]) -> NamespaceSchema {
        let tables = tables
            .iter()
            .enumerate()
            .map(|(i, &n)| {
                let columns = (0..n)
                    .enumerate()
                    .map(|(i, _)| {
                        (
                            i.to_string(),
                            ColumnSchema {
                                id: ColumnId::new(i as _),
                                column_type: ColumnType::Bool,
                            },
                        )
                    })
                    .collect::<BTreeMap<String, ColumnSchema>>();

                (
                    i.to_string(),
                    TableSchema {
                        id: TableId::new(i as _),
                        columns,
                    },
                )
            })
            .collect();

        NamespaceSchema {
            id: NamespaceId::new(42),
            kafka_topic_id: KafkaTopicId::new(24),
            query_pool_id: QueryPoolId::new(1234),
            tables,
        }
    }

    #[test]
    fn test_put() {
        let ns = DatabaseName::new("test").expect("database name is valid");
        let registry = metric::Registry::default();
        let cache = Arc::new(MemoryNamespaceCache::default());
        let cache = Arc::new(InstrumentedCache::new(cache, &registry));

        // No tables
        let schema = new_schema(&[]);
        assert!(cache.put_schema(ns.clone(), schema).is_none());
        assert_eq!(
            cache.put_insert_counter.observe(),
            Observation::U64Counter(1)
        );
        assert_eq!(
            cache.put_update_counter.observe(),
            Observation::U64Counter(0)
        );
        assert_eq!(cache.table_count.observe(), Observation::U64Gauge(0));
        assert_eq!(cache.column_count.observe(), Observation::U64Gauge(0));

        // Add a table with 1 column
        let schema = new_schema(&[1]);
        assert!(cache.put_schema(ns.clone(), schema).is_some());
        assert_eq!(
            cache.put_insert_counter.observe(),
            Observation::U64Counter(1)
        ); // Unchanged
        assert_eq!(
            cache.put_update_counter.observe(),
            Observation::U64Counter(1)
        );
        assert_eq!(cache.table_count.observe(), Observation::U64Gauge(1));
        assert_eq!(cache.column_count.observe(), Observation::U64Gauge(1));

        // Increase the number of columns in this one table
        let schema = new_schema(&[5]);
        assert!(cache.put_schema(ns.clone(), schema).is_some());
        assert_eq!(
            cache.put_insert_counter.observe(),
            Observation::U64Counter(1)
        ); // Unchanged
        assert_eq!(
            cache.put_update_counter.observe(),
            Observation::U64Counter(2)
        );
        assert_eq!(cache.table_count.observe(), Observation::U64Gauge(1));
        assert_eq!(cache.column_count.observe(), Observation::U64Gauge(5));

        // Decrease the number of columns
        let schema = new_schema(&[2]);
        assert!(cache.put_schema(ns.clone(), schema).is_some());
        assert_eq!(
            cache.put_insert_counter.observe(),
            Observation::U64Counter(1)
        ); // Unchanged
        assert_eq!(
            cache.put_update_counter.observe(),
            Observation::U64Counter(3)
        );
        assert_eq!(cache.table_count.observe(), Observation::U64Gauge(1));
        assert_eq!(cache.column_count.observe(), Observation::U64Gauge(2));

        // Add another table
        let schema = new_schema(&[2, 5]);
        assert!(cache.put_schema(ns.clone(), schema).is_some());
        assert_eq!(
            cache.put_insert_counter.observe(),
            Observation::U64Counter(1)
        ); // Unchanged
        assert_eq!(
            cache.put_update_counter.observe(),
            Observation::U64Counter(4)
        );
        assert_eq!(cache.table_count.observe(), Observation::U64Gauge(2));
        assert_eq!(cache.column_count.observe(), Observation::U64Gauge(7));

        // Add another table and adjust the existing tables (one up, one down)
        let schema = new_schema(&[1, 10, 4]);
        assert!(cache.put_schema(ns.clone(), schema).is_some());
        assert_eq!(
            cache.put_insert_counter.observe(),
            Observation::U64Counter(1)
        ); // Unchanged
        assert_eq!(
            cache.put_update_counter.observe(),
            Observation::U64Counter(5)
        );
        assert_eq!(cache.table_count.observe(), Observation::U64Gauge(3));
        assert_eq!(cache.column_count.observe(), Observation::U64Gauge(15));

        // Remove a table
        let schema = new_schema(&[1, 10]);
        assert!(cache.put_schema(ns, schema).is_some());
        assert_eq!(
            cache.put_insert_counter.observe(),
            Observation::U64Counter(1)
        ); // Unchanged
        assert_eq!(
            cache.put_update_counter.observe(),
            Observation::U64Counter(6)
        );
        assert_eq!(cache.table_count.observe(), Observation::U64Gauge(2));
        assert_eq!(cache.column_count.observe(), Observation::U64Gauge(11));

        // Add a new namespace
        let ns = DatabaseName::new("another").expect("database name is valid");
        let schema = new_schema(&[10, 12, 9]);
        assert!(cache.put_schema(ns, schema).is_none());
        assert_eq!(
            cache.put_insert_counter.observe(),
            Observation::U64Counter(2)
        );
        assert_eq!(
            cache.put_update_counter.observe(),
            Observation::U64Counter(6)
        );
        assert_eq!(cache.table_count.observe(), Observation::U64Gauge(5));
        assert_eq!(cache.column_count.observe(), Observation::U64Gauge(42));
    }
}
