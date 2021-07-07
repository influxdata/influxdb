use crate::db::catalog::chunk::ChunkMetrics;
use metrics::{Counter, GaugeValue, Histogram, KeyValue};
use std::sync::Arc;
use tracker::{LockTracker, RwLock};

#[derive(Debug)]
pub struct CatalogMetrics {
    /// Metrics domain
    metrics_domain: Arc<metrics::Domain>,

    /// Memory registries
    memory_metrics: MemoryMetrics,
}

impl CatalogMetrics {
    pub fn new(metrics_domain: metrics::Domain) -> Self {
        Self {
            memory_metrics: MemoryMetrics::new(&metrics_domain),
            metrics_domain: Arc::new(metrics_domain),
        }
    }

    /// Returns the memory metrics for the catalog
    pub fn memory(&self) -> &MemoryMetrics {
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

        TableMetrics {
            metrics_domain: Arc::clone(&self.metrics_domain),
            table_lock_tracker,
            partition_lock_tracker,
            chunk_lock_tracker,
        }
    }
}

#[derive(Debug)]
pub struct TableMetrics {
    /// Metrics domain
    metrics_domain: Arc<metrics::Domain>,

    /// Lock tracker for table-level locks
    table_lock_tracker: LockTracker,

    /// Lock tracker for partition-level locks
    partition_lock_tracker: LockTracker,

    /// Lock tracker for chunk-level locks
    chunk_lock_tracker: LockTracker,
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
            chunk_state: self.metrics_domain.register_counter_metric_with_labels(
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
        }
    }
}

#[derive(Debug)]
pub struct PartitionMetrics {
    chunk_state: Counter,

    immutable_chunk_size: Histogram,

    /// Lock Tracker for chunk-level locks
    chunk_lock_tracker: LockTracker,
}

impl PartitionMetrics {
    pub(super) fn new_chunk_lock<T>(&self, t: T) -> RwLock<T> {
        self.chunk_lock_tracker.new_lock(t)
    }

    pub(super) fn new_chunk_metrics(&self) -> ChunkMetrics {
        ChunkMetrics {
            state: self.chunk_state.clone(),
            immutable_chunk_size: self.immutable_chunk_size.clone(),
        }
    }
}

#[derive(Debug)]
pub struct MemoryMetrics {
    mutable_buffer: GaugeValue,
    read_buffer: GaugeValue,
    parquet: GaugeValue,
}

impl MemoryMetrics {
    fn new(metrics_domain: &metrics::Domain) -> Self {
        let gauge = metrics_domain.register_gauge_metric(
            "chunks_mem_usage",
            Some("bytes"),
            "Memory usage by catalog chunks",
        );

        Self {
            mutable_buffer: gauge.gauge_value(&[KeyValue::new("source", "mutable_buffer")]),
            read_buffer: gauge.gauge_value(&[KeyValue::new("source", "read_buffer")]),
            parquet: gauge.gauge_value(&[KeyValue::new("source", "parquet")]),
        }
    }

    /// Returns the size of the mutable buffer
    pub fn mutable_buffer(&self) -> GaugeValue {
        self.mutable_buffer.clone_empty()
    }

    /// Returns the size of the mutable buffer
    pub fn read_buffer(&self) -> GaugeValue {
        self.read_buffer.clone_empty()
    }

    /// Returns the amount of data in parquet
    pub fn parquet(&self) -> GaugeValue {
        self.parquet.clone_empty()
    }

    /// Total bytes over all registries.
    pub fn total(&self) -> usize {
        self.mutable_buffer.get_total() + self.read_buffer.get_total() + self.parquet.get_total()
    }
}
