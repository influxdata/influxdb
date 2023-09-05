//! Metric instrumentation for a [`NamespaceCache`] implementation.

use std::sync::Arc;

use async_trait::async_trait;
use data_types::{NamespaceName, NamespaceSchema};
use iox_time::{SystemProvider, TimeProvider};
use metric::{DurationHistogram, Metric, U64Gauge};

use super::{ChangeStats, NamespaceCache};

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

    /// A cache put for a namespace that did not previously exist in the cache
    put_insert: DurationHistogram,
    /// A cache put for a namespace that previously had a cache entry
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

#[async_trait]
impl<T, P> NamespaceCache for InstrumentedCache<T, P>
where
    T: NamespaceCache,
    P: TimeProvider,
{
    type ReadError = T::ReadError;

    async fn get_schema(
        &self,
        namespace: &NamespaceName<'static>,
    ) -> Result<Arc<NamespaceSchema>, Self::ReadError> {
        let t = self.time_provider.now();
        let res = self.inner.get_schema(namespace).await;

        // Avoid exploding if time goes backwards - simply drop the measurement
        // if it happens.
        if let Some(delta) = self.time_provider.now().checked_duration_since(t) {
            match &res {
                Ok(_) => self.get_hit.record(delta),
                Err(_) => self.get_miss.record(delta),
            };
        }

        res
    }

    fn put_schema(
        &self,
        namespace: NamespaceName<'static>,
        schema: NamespaceSchema,
    ) -> (Arc<NamespaceSchema>, ChangeStats) {
        let t = self.time_provider.now();
        let (result, change_stats) = self.inner.put_schema(namespace, schema);

        if let Some(delta) = self.time_provider.now().checked_duration_since(t) {
            if change_stats.did_update {
                self.put_update.record(delta);
            } else {
                self.put_insert.record(delta)
            };
        }

        // Adjust the metrics to reflect the change in table and column counts
        self.table_count.inc(change_stats.new_tables.len() as u64);
        self.column_count.inc(change_stats.num_new_columns as u64);

        (result, change_stats)
    }
}

#[cfg(test)]
mod tests {
    use assert_matches::assert_matches;
    use data_types::{
        Column, ColumnId, ColumnType, ColumnsByName, NamespaceId, TableId, TableSchema,
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
                    .map(|(i, _)| Column {
                        id: ColumnId::new(i as _),
                        column_type: ColumnType::Bool,
                        name: i.to_string(),
                        table_id: TableId::new(i as _),
                    })
                    .collect::<Vec<_>>();

                (
                    i.to_string(),
                    TableSchema {
                        id: TableId::new(i as _),
                        partition_template: Default::default(),
                        columns: ColumnsByName::new(columns),
                    },
                )
            })
            .collect();

        NamespaceSchema {
            id: NamespaceId::new(42),
            tables,
            max_columns_per_table: 100,
            max_tables: 42,
            retention_period_ns: None,
            partition_template: Default::default(),
        }
    }

    fn assert_histogram_hit(
        metrics: &metric::Registry,
        metric_name: &'static str,
        attr: impl Into<Attributes>,
        count: u64,
    ) {
        let histogram = metrics
            .get_instrument::<Metric<DurationHistogram>>(metric_name)
            .expect("failed to read metric")
            .get_observer(&attr.into())
            .expect("failed to get observer")
            .fetch();

        let hit_count = histogram.sample_count();
        assert_eq!(
            hit_count, count,
            "metric did not record correct number of calls"
        );
    }

    #[tokio::test]
    async fn test_put() {
        let ns = NamespaceName::new("test").expect("namespace name is valid");
        let registry = metric::Registry::default();
        let cache = MemoryNamespaceCache::default();
        let cache = InstrumentedCache::new(cache, &registry);

        // No tables
        let schema = new_schema(&[]);
        cache.put_schema(ns.clone(), schema.to_owned());
        assert_histogram_hit(
            &registry,
            "namespace_cache_put_duration",
            &[("op", "insert")],
            1,
        );
        assert_histogram_hit(
            &registry,
            "namespace_cache_put_duration",
            &[("op", "update")],
            0,
        );
        assert_eq!(cache.table_count.observe(), Observation::U64Gauge(0));
        assert_eq!(cache.column_count.observe(), Observation::U64Gauge(0));

        // Add a table with 1 column
        let schema = new_schema(&[1]);
        assert_matches!(cache.put_schema(ns.clone(), schema.to_owned()), (result, _) => {
            assert_eq!(*result, schema);
        });
        assert_histogram_hit(
            &registry,
            "namespace_cache_put_duration",
            &[("op", "insert")],
            1,
        );
        assert_histogram_hit(
            &registry,
            "namespace_cache_put_duration",
            &[("op", "update")],
            1,
        );
        assert_eq!(cache.table_count.observe(), Observation::U64Gauge(1));
        assert_eq!(cache.column_count.observe(), Observation::U64Gauge(1));

        // Increase the number of columns in this one table
        let schema = new_schema(&[5]);
        cache.put_schema(ns.clone(), schema);
        assert_histogram_hit(
            &registry,
            "namespace_cache_put_duration",
            &[("op", "insert")],
            1,
        );
        assert_histogram_hit(
            &registry,
            "namespace_cache_put_duration",
            &[("op", "update")],
            2,
        );
        assert_eq!(cache.table_count.observe(), Observation::U64Gauge(1));
        assert_eq!(cache.column_count.observe(), Observation::U64Gauge(5));

        // Add another table
        let schema = new_schema(&[5, 5]);
        cache.put_schema(ns.clone(), schema);
        assert_histogram_hit(
            &registry,
            "namespace_cache_put_duration",
            &[("op", "insert")],
            1,
        );
        assert_histogram_hit(
            &registry,
            "namespace_cache_put_duration",
            &[("op", "update")],
            3,
        );
        assert_eq!(cache.table_count.observe(), Observation::U64Gauge(2));
        assert_eq!(cache.column_count.observe(), Observation::U64Gauge(10));

        // Add another table and adjust an existing table (increased column count)
        let schema = new_schema(&[5, 10, 4]);
        cache.put_schema(ns.clone(), schema);
        assert_histogram_hit(
            &registry,
            "namespace_cache_put_duration",
            &[("op", "insert")],
            1,
        );
        assert_histogram_hit(
            &registry,
            "namespace_cache_put_duration",
            &[("op", "update")],
            4,
        );
        assert_eq!(cache.table_count.observe(), Observation::U64Gauge(3));
        assert_eq!(cache.column_count.observe(), Observation::U64Gauge(19));

        // Add a new namespace
        let ns = NamespaceName::new("another").expect("namespace name is valid");
        let schema = new_schema(&[10, 12, 9]);
        cache.put_schema(ns.clone(), schema);
        assert_histogram_hit(
            &registry,
            "namespace_cache_put_duration",
            &[("op", "insert")],
            2,
        );
        assert_histogram_hit(
            &registry,
            "namespace_cache_put_duration",
            &[("op", "update")],
            4,
        );

        assert_eq!(cache.table_count.observe(), Observation::U64Gauge(6));
        assert_eq!(cache.column_count.observe(), Observation::U64Gauge(50)); // 15 + new columns (31)

        let _got = cache.get_schema(&ns).await.expect("should exist");
        assert_histogram_hit(
            &registry,
            "namespace_cache_get_duration",
            &[("result", "hit")],
            1,
        );
    }
}
