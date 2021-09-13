use std::sync::Arc;
use std::time::Duration;

use crate::db::catalog::chunk::ChunkMetrics;
use data_types::write_summary::TimestampSummary;
use metric::{
    Attributes, CumulativeGauge, CumulativeRecorder, DurationHistogram, DurationHistogramOptions,
    Metric, MetricObserver,
};
use tracker::{LockMetrics, RwLock};

const TIMESTAMP_METRICS_ENABLE_ENV: &str = "INFLUXDB_IOX_ROW_TIMESTAMP_METRICS";
fn report_timestamp_metrics(table_name: &str) -> bool {
    std::env::var(TIMESTAMP_METRICS_ENABLE_ENV)
        .ok()
        .map(|x| x.split(',').any(|x| x == table_name))
        .unwrap_or(false)
}

#[derive(Debug)]
pub struct CatalogMetrics {
    /// Name of the database
    db_name: Arc<str>,

    /// Metrics registry
    metrics_registry: Arc<metric::Registry>,
    /// Catalog memory metrics
    memory_metrics: StorageGauge,
}

impl CatalogMetrics {
    pub fn new(db_name: Arc<str>, metrics_registry: Arc<metric::Registry>) -> Self {
        let chunks_mem_usage = metrics_registry.register_metric(
            "catalog_chunks_mem_usage_bytes",
            "Memory usage by catalog chunks",
        );

        let memory_metrics =
            StorageGauge::new(&chunks_mem_usage, [("db_name", db_name.to_string().into())]);

        Self {
            db_name,
            metrics_registry,
            memory_metrics,
        }
    }

    /// Returns the memory metrics for the catalog
    pub fn memory(&self) -> &StorageGauge {
        &self.memory_metrics
    }

    pub(super) fn new_table_metrics(self: &Arc<Self>, table_name: &str) -> TableMetrics {
        let base_attributes = metric::Attributes::from([
            ("db_name", self.db_name.to_string().into()),
            ("table", table_name.to_string().into()),
        ]);

        let mut lock_attributes = base_attributes.clone();
        lock_attributes.insert("lock", "table");
        let table_lock_metrics = Arc::new(LockMetrics::new(
            self.metrics_registry.as_ref(),
            lock_attributes.clone(),
        ));

        lock_attributes.insert("lock", "partition");
        let partition_lock_metrics = Arc::new(LockMetrics::new(
            self.metrics_registry.as_ref(),
            lock_attributes.clone(),
        ));

        lock_attributes.insert("lock", "chunk");
        let chunk_lock_metrics = Arc::new(LockMetrics::new(
            self.metrics_registry.as_ref(),
            lock_attributes,
        ));

        let storage_gauge = self.metrics_registry.register_metric(
            "catalog_loaded_chunks",
            "The number of chunks loaded in a each chunk storage location",
        );

        let row_gauge = self.metrics_registry.register_metric(
            "catalog_loaded_rows",
            "The number of rows loaded in each chunk storage location",
        );

        let timestamp_histogram = report_timestamp_metrics(table_name).then(|| {
            TimestampHistogram::new(self.metrics_registry.as_ref(), base_attributes.clone())
        });

        let chunk_storage = StorageGauge::new(&storage_gauge, base_attributes.clone());
        let row_count = StorageGauge::new(&row_gauge, base_attributes);

        TableMetrics {
            catalog_metrics: Arc::clone(self),
            chunk_storage,
            row_count,
            table_lock_metrics,
            partition_lock_metrics,
            chunk_lock_metrics,
            timestamp_histogram,
        }
    }
}

/// Metrics that are collected and aggregated per-table within a database
#[derive(Debug)]
pub struct TableMetrics {
    catalog_metrics: Arc<CatalogMetrics>,

    /// Chunk storage metrics
    chunk_storage: StorageGauge,

    /// Chunk row count metrics
    row_count: StorageGauge,

    /// Lock metrics for table-level locks
    table_lock_metrics: Arc<LockMetrics>,

    /// Lock metrics for partition-level locks
    partition_lock_metrics: Arc<LockMetrics>,

    /// Lock metrics for chunk-level locks
    chunk_lock_metrics: Arc<LockMetrics>,

    /// Track ingested timestamps
    timestamp_histogram: Option<TimestampHistogram>,
}

impl TableMetrics {
    pub(super) fn new_table_lock<T>(&self, t: T) -> RwLock<T> {
        self.table_lock_metrics.new_lock(t)
    }

    pub(super) fn new_partition_lock<T>(&self, t: T) -> RwLock<T> {
        self.partition_lock_metrics.new_lock(t)
    }

    pub(super) fn new_partition_metrics(self: &Arc<Self>) -> PartitionMetrics {
        // Lock metrics for chunk-level locks
        PartitionMetrics {
            table_metrics: Arc::clone(self),
        }
    }
}

#[derive(Debug)]
pub struct PartitionMetrics {
    table_metrics: Arc<TableMetrics>,
}

impl PartitionMetrics {
    pub(super) fn new_chunk_lock<T>(&self, t: T) -> RwLock<T> {
        self.table_metrics.chunk_lock_metrics.new_lock(t)
    }

    pub(super) fn new_chunk_metrics(&self) -> ChunkMetrics {
        ChunkMetrics {
            timestamp_histogram: self.table_metrics.timestamp_histogram.clone(),
            chunk_storage: self.table_metrics.chunk_storage.recorder(),
            row_count: self.table_metrics.row_count.recorder(),
            memory_metrics: self.table_metrics.catalog_metrics.memory_metrics.recorder(),
        }
    }
}

/// Created from a `Metric<CumulativeGauge>` and extracts a `CumulativeRecorder` for each chunk storage
///
/// This can then be used within each `CatalogChunk` to record its observations for
/// the different storages
#[derive(Debug, Clone, Default)]
pub struct StorageGauge {
    mutable_buffer: CumulativeGauge,
    read_buffer: CumulativeGauge,
    object_store: CumulativeGauge,
}

impl StorageGauge {
    pub(super) fn new(metric: &Metric<CumulativeGauge>, attributes: impl Into<Attributes>) -> Self {
        let mut attributes = attributes.into();

        attributes.insert("location", "mutable_buffer");
        let mutable_buffer = metric.observer(attributes.clone()).clone();

        attributes.insert("location", "read_buffer");
        let read_buffer = metric.observer(attributes.clone()).clone();

        attributes.insert("location", "object_store");
        let object_store = metric.observer(attributes).clone();

        Self {
            mutable_buffer,
            read_buffer,
            object_store,
        }
    }

    pub fn recorder(&self) -> StorageRecorder {
        StorageRecorder {
            mutable_buffer: self.mutable_buffer.recorder(),
            read_buffer: self.read_buffer.recorder(),
            object_store: self.object_store.recorder(),
        }
    }

    /// Returns the total for the mutable buffer
    pub fn mutable_buffer(&self) -> usize {
        self.mutable_buffer.fetch() as usize
    }

    /// Returns the total for the read buffer
    pub fn read_buffer(&self) -> usize {
        self.read_buffer.fetch() as usize
    }

    /// Returns the total for object storage
    pub fn object_store(&self) -> usize {
        self.object_store.fetch() as usize
    }

    /// Returns the total over all storages
    pub fn total(&self) -> usize {
        (self.mutable_buffer() + self.read_buffer() + self.object_store()) as usize
    }
}

#[derive(Debug)]
pub struct StorageRecorder {
    mutable_buffer: CumulativeRecorder,
    read_buffer: CumulativeRecorder,
    object_store: CumulativeRecorder,
}

impl StorageRecorder {
    pub(super) fn new_unregistered() -> Self {
        Self {
            mutable_buffer: CumulativeRecorder::new_unregistered(),
            read_buffer: CumulativeRecorder::new_unregistered(),
            object_store: CumulativeRecorder::new_unregistered(),
        }
    }

    #[allow(dead_code)]
    pub(super) fn reporter(&self) -> StorageGauge {
        StorageGauge {
            mutable_buffer: self.mutable_buffer.reporter(),
            read_buffer: self.read_buffer.reporter(),
            object_store: self.object_store.reporter(),
        }
    }

    pub(super) fn set_mub_only(&mut self, value: usize) {
        self.mutable_buffer.set(value as u64);
        self.read_buffer.set(0);
        self.object_store.set(0);
    }

    pub(super) fn set_rub_only(&mut self, value: usize) {
        self.mutable_buffer.set(0);
        self.read_buffer.set(value as u64);
        self.object_store.set(0);
    }

    pub(super) fn set_rub_and_object_store_only(&mut self, rub: usize, parquet: usize) {
        self.mutable_buffer.set(0);
        self.read_buffer.set(rub as u64);
        self.object_store.set(parquet as u64);
    }

    pub(super) fn set_object_store_only(&mut self, value: usize) {
        self.mutable_buffer.set(0);
        self.read_buffer.set(0);
        self.object_store.set(value as u64);
    }

    pub(super) fn set_to_zero(&mut self) {
        self.mutable_buffer.set(0);
        self.read_buffer.set(0);
        self.object_store.set(0);
    }
}

/// A Histogram-inspired metric for reporting `TimestampSummary`
///
/// Like `TimestampSummary`, this is bucketed based on minute within the hour
/// It will therefore wrap around on the hour
#[derive(Debug, Clone)]
pub(super) struct TimestampHistogram {
    inner: metric::DurationHistogram,
}

impl TimestampHistogram {
    fn new(registry: &metric::Registry, attributes: impl Into<Attributes>) -> Self {
        let histogram: metric::Metric<DurationHistogram> = registry.register_metric_with_options(
            "catalog_row_time",
            "The cumulative distribution of the ingested row timestamps",
            || {
                DurationHistogramOptions::new(
                    (0..60).map(|minute| Duration::from_secs(minute * 60)),
                )
            },
        );

        Self {
            inner: histogram.recorder(attributes),
        }
    }

    pub(super) fn add(&self, summary: &TimestampSummary) {
        for (minute, count) in summary.counts.iter().enumerate() {
            self.inner
                .record_multiple(Duration::from_secs(minute as u64 * 60), *count as u64)
        }
    }
}
