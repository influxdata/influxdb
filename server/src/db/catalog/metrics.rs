use crate::db::catalog::chunk::ChunkMetrics;
use data_types::write_summary::TimestampSummary;
use metrics::{Counter, Gauge, GaugeValue, Histogram, KeyValue, MetricObserverBuilder};
use parking_lot::Mutex;
use std::sync::Arc;
use tracker::{LockTracker, RwLock};

const TIMESTAMP_METRICS_ENABLE_ENV: &str = "INFLUXDB_IOX_ROW_TIMESTAMP_METRICS";
fn report_timestamp_metrics(table_name: &str) -> bool {
    std::env::var(TIMESTAMP_METRICS_ENABLE_ENV)
        .ok()
        .map(|x| x.split(',').any(|x| x == table_name))
        .unwrap_or(false)
}

#[derive(Debug)]
pub struct CatalogMetrics {
    /// Metrics domain
    metrics_domain: Arc<metrics::Domain>,

    /// Catalog memory metrics
    memory_metrics: StorageGauge,
}

impl CatalogMetrics {
    pub fn new(metrics_domain: metrics::Domain) -> Self {
        let chunks_mem_usage = metrics_domain.register_gauge_metric(
            "chunks_mem_usage",
            Some("bytes"),
            "Memory usage by catalog chunks",
        );

        Self {
            memory_metrics: StorageGauge::new(&chunks_mem_usage),
            metrics_domain: Arc::new(metrics_domain),
        }
    }

    /// Returns the memory metrics for the catalog
    pub fn memory(&self) -> &StorageGauge {
        &self.memory_metrics
    }

    pub(super) fn new_table_metrics(&self, table_name: &str) -> TableMetrics {
        let table_lock_tracker = Default::default();
        self.metrics_domain.register_observer(
            None,
            &[
                KeyValue::new("lock", "table"),
                KeyValue::new("table", table_name.to_string()),
            ],
            &table_lock_tracker,
        );

        let partition_lock_tracker = Default::default();
        self.metrics_domain.register_observer(
            None,
            &[
                KeyValue::new("lock", "partition"),
                KeyValue::new("table", table_name.to_string()),
            ],
            &partition_lock_tracker,
        );

        let chunk_lock_tracker = Default::default();
        self.metrics_domain.register_observer(
            None,
            &[
                KeyValue::new("lock", "chunk"),
                KeyValue::new("table", table_name.to_string()),
            ],
            &chunk_lock_tracker,
        );

        let storage_gauge = self.metrics_domain.register_gauge_metric_with_attributes(
            "loaded",
            Some("chunks"),
            "The number of chunks loaded in a each chunk storage location",
            &[KeyValue::new("table", table_name.to_string())],
        );

        let row_gauge = self.metrics_domain.register_gauge_metric_with_attributes(
            "loaded",
            Some("rows"),
            "The number of rows loaded in each chunk storage location",
            &[KeyValue::new("table", table_name.to_string())],
        );

        let timestamp_histogram = Default::default();
        if report_timestamp_metrics(table_name) {
            self.metrics_domain.register_observer(
                None,
                &[KeyValue::new("table", table_name.to_string())],
                &timestamp_histogram,
            );
        }

        TableMetrics {
            metrics_domain: Arc::clone(&self.metrics_domain),
            chunk_storage: StorageGauge::new(&storage_gauge),
            row_count: StorageGauge::new(&row_gauge),
            memory_metrics: self.memory_metrics.clone_empty(),
            table_lock_tracker,
            partition_lock_tracker,
            chunk_lock_tracker,
            timestamp_histogram,
        }
    }
}

#[derive(Debug)]
pub struct TableMetrics {
    /// Metrics domain
    metrics_domain: Arc<metrics::Domain>,

    /// Chunk storage metrics
    chunk_storage: StorageGauge,

    /// Chunk row count metrics
    row_count: StorageGauge,

    /// Catalog memory metrics
    memory_metrics: StorageGauge,

    /// Lock tracker for table-level locks
    table_lock_tracker: LockTracker,

    /// Lock tracker for partition-level locks
    partition_lock_tracker: LockTracker,

    /// Lock tracker for chunk-level locks
    chunk_lock_tracker: LockTracker,

    /// Track ingested timestamps
    timestamp_histogram: TimestampHistogram,
}

impl TableMetrics {
    pub(super) fn new_table_lock<T>(&self, t: T) -> RwLock<T> {
        self.table_lock_tracker.new_lock(t)
    }

    pub(super) fn new_partition_lock<T>(&self, t: T) -> RwLock<T> {
        self.partition_lock_tracker.new_lock(t)
    }

    pub(super) fn new_partition_metrics(&self) -> PartitionMetrics {
        // Lock tracker for chunk-level locks
        PartitionMetrics {
            chunk_storage: self.chunk_storage.clone_empty(),
            row_count: self.row_count.clone_empty(),
            memory_metrics: self.memory_metrics.clone_empty(),
            chunk_state: self.metrics_domain.register_counter_metric_with_attributes(
                "chunks",
                None,
                "In-memory chunks created in various life-cycle stages",
                vec![],
            ),
            immutable_chunk_size: self
                .metrics_domain
                .register_histogram_metric(
                    "chunk_creation",
                    "size",
                    "bytes",
                    "The new size of an immutable chunk",
                )
                .init(),
            chunk_lock_tracker: self.chunk_lock_tracker.clone(),
            timestamp_histogram: self.timestamp_histogram.clone(),
        }
    }
}

#[derive(Debug)]
pub struct PartitionMetrics {
    /// Chunk storage metrics
    chunk_storage: StorageGauge,

    /// Chunk row count metrics
    row_count: StorageGauge,

    /// Catalog memory metrics
    memory_metrics: StorageGauge,

    chunk_state: Counter,

    immutable_chunk_size: Histogram,

    /// Lock Tracker for chunk-level locks
    chunk_lock_tracker: LockTracker,

    /// Track ingested timestamps
    timestamp_histogram: TimestampHistogram,
}

impl PartitionMetrics {
    pub(super) fn new_chunk_lock<T>(&self, t: T) -> RwLock<T> {
        self.chunk_lock_tracker.new_lock(t)
    }

    pub(super) fn new_chunk_metrics(&self) -> ChunkMetrics {
        ChunkMetrics {
            timestamp_histogram: self.timestamp_histogram.clone(),
            state: self.chunk_state.clone(),
            immutable_chunk_size: self.immutable_chunk_size.clone(),
            chunk_storage: self.chunk_storage.clone_empty(),
            row_count: self.row_count.clone_empty(),
            memory_metrics: self.memory_metrics.clone_empty(),
        }
    }
}

/// Created from a `metrics::Gauge` and extracts a `GaugeValue` for each chunk storage
///
/// This can then be used within each `CatalogChunk` to record its observations for
/// the different storages
pub struct StorageGauge {
    inner: Mutex<StorageGaugeInner>,
}

impl std::fmt::Debug for StorageGauge {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("StorageGauge").finish_non_exhaustive()
    }
}

struct StorageGaugeInner {
    mutable_buffer: GaugeValue,
    read_buffer: GaugeValue,
    object_store: GaugeValue,
}

impl StorageGauge {
    pub(super) fn new_unregistered() -> Self {
        let inner = StorageGaugeInner {
            mutable_buffer: GaugeValue::new_unregistered(),
            read_buffer: GaugeValue::new_unregistered(),
            object_store: GaugeValue::new_unregistered(),
        };
        Self {
            inner: Mutex::new(inner),
        }
    }

    pub(super) fn new(gauge: &Gauge) -> Self {
        let inner = StorageGaugeInner {
            mutable_buffer: gauge.gauge_value(&[KeyValue::new("location", "mutable_buffer")]),
            read_buffer: gauge.gauge_value(&[KeyValue::new("location", "read_buffer")]),
            object_store: gauge.gauge_value(&[KeyValue::new("location", "object_store")]),
        };
        Self {
            inner: Mutex::new(inner),
        }
    }

    pub(super) fn set_mub_only(&self, value: usize) {
        let mut guard = self.inner.lock();

        guard.mutable_buffer.set(value);
        guard.read_buffer.set(0);
        guard.object_store.set(0);
    }

    pub(super) fn set_rub_only(&self, value: usize) {
        let mut guard = self.inner.lock();

        guard.mutable_buffer.set(0);
        guard.read_buffer.set(value);
        guard.object_store.set(0);
    }

    pub(super) fn set_rub_and_object_store_only(&self, rub: usize, parquet: usize) {
        let mut guard = self.inner.lock();

        guard.mutable_buffer.set(0);
        guard.read_buffer.set(rub);
        guard.object_store.set(parquet);
    }

    pub(super) fn set_object_store_only(&self, value: usize) {
        let mut guard = self.inner.lock();

        guard.mutable_buffer.set(0);
        guard.read_buffer.set(0);
        guard.object_store.set(value);
    }

    pub(super) fn set_to_zero(&self) {
        let mut guard = self.inner.lock();

        guard.mutable_buffer.set(0);
        guard.read_buffer.set(0);
        guard.object_store.set(0);
    }

    fn clone_empty(&self) -> Self {
        let guard = self.inner.lock();

        let inner = StorageGaugeInner {
            mutable_buffer: guard.mutable_buffer.clone_empty(),
            read_buffer: guard.read_buffer.clone_empty(),
            object_store: guard.object_store.clone_empty(),
        };
        Self {
            inner: Mutex::new(inner),
        }
    }

    /// Returns the total for the mutable buffer
    pub fn mutable_buffer(&self) -> usize {
        let guard = self.inner.lock();
        guard.mutable_buffer.get_total()
    }

    /// Returns the total for the read buffer
    pub fn read_buffer(&self) -> usize {
        let guard = self.inner.lock();
        guard.read_buffer.get_total()
    }

    /// Returns the total for object storage
    pub fn object_store(&self) -> usize {
        let guard = self.inner.lock();
        guard.object_store.get_total()
    }

    /// Returns the total over all storages
    pub fn total(&self) -> usize {
        let guard = self.inner.lock();
        guard.mutable_buffer.get_total()
            + guard.read_buffer.get_total()
            + guard.object_store.get_total()
    }
}

/// A Histogram-inspired metric for reporting `TimestampSummary`
///
/// This is partly to workaround limitations defining custom Histogram bucketing in OTEL
/// and also because it can be implemented more efficiently as the set of values is fixed
///
/// Like `TimestampSummary`, this is bucketed based on minute within the hour
/// It will therefore wrap around on the hour
#[derive(Debug, Clone, Default)]
pub(super) struct TimestampHistogram {
    inner: Arc<Mutex<TimestampSummary>>,
}

impl TimestampHistogram {
    pub(super) fn add(&self, summary: &TimestampSummary) {
        self.inner.lock().merge(summary)
    }
}

impl metrics::MetricObserver for &TimestampHistogram {
    fn register(self, builder: MetricObserverBuilder<'_>) {
        let inner = Arc::clone(&self.inner);
        builder.register_histogram_bucket(
            "row_time",
            Some("seconds"),
            "The cumulative distribution of the ingested row timestamps",
            move |result| {
                let inner = inner.lock();
                for (min, total) in inner.cumulative_counts() {
                    result.observe(total, &[KeyValue::new("le", (min * 60).to_string())])
                }
            },
        )
    }
}
