//! Metric instrumentation for a [`NamespaceCache`] implementation.

use std::sync::Arc;

use data_types::{NamespaceName, NamespaceSchema};
use iox_time::{SystemProvider, TimeProvider};
use metric::{DurationHistogram, Metric, U64Gauge};

use super::NamespaceCache;

/// An [`InstrumentedCache`] decorates a [`NamespaceCache`] with cache read
/// hit/miss and cache put insert/update metrics.
#[derive(Debug)]
pub struct InstrumentedCache<T, P = SystemProvider> {
    inner: T,
    time_provider: P,

    /// Metrics derived from the [`NamespaceSchema`] held within the cache.
    table_count: U64Gauge,
    column_count: U64Gauge,

    /// A cache read hit
    get_hit: DurationHistogram,
    /// A cache read miss
    get_miss: DurationHistogram,

    /// A cache put for a namespace that did not previously exist.
    put_insert: DurationHistogram,
    /// A cache put replacing a namespace that previously had a cache entry.
    put_update: DurationHistogram,
}

impl<T> InstrumentedCache<T> {
    /// Instrument `T`, recording cache operations to `registry`.
    pub fn new(inner: T, registry: &metric::Registry) -> Self {
        let get_counter: Metric<DurationHistogram> =
            registry.register_metric("namespace_cache_get_duration", "cache read call duration");
        let get_hit = get_counter.recorder(&[("result", "hit")]);
        let get_miss = get_counter.recorder(&[("result", "miss")]);

        let put_counter: Metric<DurationHistogram> =
            registry.register_metric("namespace_cache_put_duration", "cache put call duration");
        let put_insert = put_counter.recorder(&[("op", "insert")]);
        let put_update = put_counter.recorder(&[("op", "update")]);

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
            time_provider: Default::default(),
            table_count,
            column_count,
            get_hit,
            get_miss,
            put_insert,
            put_update,
        }
    }
}

impl<T, P> NamespaceCache for Arc<InstrumentedCache<T, P>>
where
    T: NamespaceCache,
    P: TimeProvider,
{
    fn get_schema(&self, namespace: &NamespaceName<'_>) -> Option<Arc<NamespaceSchema>> {
        let t = self.time_provider.now();
        let res = self.inner.get_schema(namespace);

        // Avoid exploding if time goes backwards - simply drop the measurement
        // if it happens.
        if let Some(delta) = self.time_provider.now().checked_duration_since(t) {
            match &res {
                Some(_) => self.get_hit.record(delta),
                None => self.get_miss.record(delta),
            };
        }

        res
    }

    fn put_schema(
        &self,
        namespace: NamespaceName<'static>,
        schema: impl Into<Arc<NamespaceSchema>>,
    ) -> Option<Arc<NamespaceSchema>> {
        let schema = schema.into();
        let stats = NamespaceStats::new(&schema);

        let t = self.time_provider.now();
        let res = self.inner.put_schema(namespace, schema);

        match res {
            Some(v) => {
                if let Some(delta) = self.time_provider.now().checked_duration_since(t) {
                    self.put_update.record(delta);
                }

                // Figure out the difference between the new namespace and the
                // evicted old namespace
                let old_stats = NamespaceStats::new(&v);
                let table_count_diff = stats.table_count as i64 - old_stats.table_count as i64;
                let column_count_diff = stats.column_count as i64 - old_stats.column_count as i64;

                // Adjust the metrics to reflect the change
                self.table_count.delta(table_count_diff);
                self.column_count.delta(column_count_diff);

                Some(v)
            }
            None => {
                if let Some(delta) = self.time_provider.now().checked_duration_since(t) {
                    self.put_insert.record(delta);
                }

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

    use data_types::{
        ColumnId, ColumnSchema, ColumnType, NamespaceId, QueryPoolId, TableId, TableSchema, TopicId,
    };
    use metric::{Attributes, MetricObserver, Observation};

    use super::*;
    use crate::namespace_cache::MemoryNamespaceCache;

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
            topic_id: TopicId::new(24),
            query_pool_id: QueryPoolId::new(1234),
            tables,
            max_columns_per_table: 100,
            max_tables: 42,
            retention_period_ns: None,
        }
    }

    fn assert_histogram_hit(
        metrics: &metric::Registry,
        metric_name: &'static str,
        attr: (&'static str, &'static str),
        count: u64,
    ) {
        let histogram = metrics
            .get_instrument::<Metric<DurationHistogram>>(metric_name)
            .expect("failed to read metric")
            .get_observer(&Attributes::from(&[attr]))
            .expect("failed to get observer")
            .fetch();

        let hit_count = histogram.sample_count();
        assert_eq!(
            hit_count, count,
            "metric did not record correct number of calls"
        );
    }

    #[test]
    fn test_put() {
        let ns = NamespaceName::new("test").expect("namespace name is valid");
        let registry = metric::Registry::default();
        let cache = Arc::new(MemoryNamespaceCache::default());
        let cache = Arc::new(InstrumentedCache::new(cache, &registry));

        // No tables
        let schema = new_schema(&[]);
        assert!(cache.put_schema(ns.clone(), schema).is_none());
        assert_histogram_hit(
            &registry,
            "namespace_cache_put_duration",
            ("op", "insert"),
            1,
        );
        assert_histogram_hit(
            &registry,
            "namespace_cache_put_duration",
            ("op", "update"),
            0,
        );
        assert_eq!(cache.table_count.observe(), Observation::U64Gauge(0));
        assert_eq!(cache.column_count.observe(), Observation::U64Gauge(0));

        // Add a table with 1 column
        let schema = new_schema(&[1]);
        assert!(cache.put_schema(ns.clone(), schema).is_some());
        assert_histogram_hit(
            &registry,
            "namespace_cache_put_duration",
            ("op", "insert"),
            1,
        ); // Unchanged
        assert_histogram_hit(
            &registry,
            "namespace_cache_put_duration",
            ("op", "update"),
            1,
        );
        assert_eq!(cache.table_count.observe(), Observation::U64Gauge(1));
        assert_eq!(cache.column_count.observe(), Observation::U64Gauge(1));

        // Increase the number of columns in this one table
        let schema = new_schema(&[5]);
        assert!(cache.put_schema(ns.clone(), schema).is_some());
        assert_histogram_hit(
            &registry,
            "namespace_cache_put_duration",
            ("op", "insert"),
            1,
        ); // Unchanged
        assert_histogram_hit(
            &registry,
            "namespace_cache_put_duration",
            ("op", "update"),
            2,
        );
        assert_eq!(cache.table_count.observe(), Observation::U64Gauge(1));
        assert_eq!(cache.column_count.observe(), Observation::U64Gauge(5));

        // Decrease the number of columns
        let schema = new_schema(&[2]);
        assert!(cache.put_schema(ns.clone(), schema).is_some());
        assert_histogram_hit(
            &registry,
            "namespace_cache_put_duration",
            ("op", "insert"),
            1,
        ); // Unchanged
        assert_histogram_hit(
            &registry,
            "namespace_cache_put_duration",
            ("op", "update"),
            3,
        );
        assert_eq!(cache.table_count.observe(), Observation::U64Gauge(1));
        assert_eq!(cache.column_count.observe(), Observation::U64Gauge(2));

        // Add another table
        let schema = new_schema(&[2, 5]);
        assert!(cache.put_schema(ns.clone(), schema).is_some());
        assert_histogram_hit(
            &registry,
            "namespace_cache_put_duration",
            ("op", "insert"),
            1,
        ); // Unchanged
        assert_histogram_hit(
            &registry,
            "namespace_cache_put_duration",
            ("op", "update"),
            4,
        );
        assert_eq!(cache.table_count.observe(), Observation::U64Gauge(2));
        assert_eq!(cache.column_count.observe(), Observation::U64Gauge(7));

        // Add another table and adjust the existing tables (one up, one down)
        let schema = new_schema(&[1, 10, 4]);
        assert!(cache.put_schema(ns.clone(), schema).is_some());
        assert_histogram_hit(
            &registry,
            "namespace_cache_put_duration",
            ("op", "insert"),
            1,
        ); // Unchanged
        assert_histogram_hit(
            &registry,
            "namespace_cache_put_duration",
            ("op", "update"),
            5,
        );
        assert_eq!(cache.table_count.observe(), Observation::U64Gauge(3));
        assert_eq!(cache.column_count.observe(), Observation::U64Gauge(15));

        // Remove a table
        let schema = new_schema(&[1, 10]);
        assert!(cache.put_schema(ns, schema).is_some());
        assert_histogram_hit(
            &registry,
            "namespace_cache_put_duration",
            ("op", "insert"),
            1,
        ); // Unchanged
        assert_histogram_hit(
            &registry,
            "namespace_cache_put_duration",
            ("op", "update"),
            6,
        );
        assert_eq!(cache.table_count.observe(), Observation::U64Gauge(2));
        assert_eq!(cache.column_count.observe(), Observation::U64Gauge(11));

        // Add a new namespace
        let ns = NamespaceName::new("another").expect("namespace name is valid");
        let schema = new_schema(&[10, 12, 9]);
        assert!(cache.put_schema(ns.clone(), schema).is_none());
        assert_histogram_hit(
            &registry,
            "namespace_cache_put_duration",
            ("op", "insert"),
            2,
        );
        assert_histogram_hit(
            &registry,
            "namespace_cache_put_duration",
            ("op", "update"),
            6,
        );
        assert_eq!(cache.table_count.observe(), Observation::U64Gauge(5));
        assert_eq!(cache.column_count.observe(), Observation::U64Gauge(42));

        let _got = cache.get_schema(&ns).expect("should exist");
        assert_histogram_hit(
            &registry,
            "namespace_cache_get_duration",
            ("result", "hit"),
            1,
        );
    }
}
