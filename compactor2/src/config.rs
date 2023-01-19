//! Config-related stuff.
use std::{num::NonZeroUsize, sync::Arc};

use backoff::BackoffConfig;
use iox_catalog::interface::Catalog;
use iox_query::exec::Executor;
use iox_time::TimeProvider;
use parquet_file::storage::ParquetStorage;

/// Config to set up a compactor.
#[derive(Debug, Clone)]
pub struct Config {
    /// Metric registry.
    pub metric_registry: Arc<metric::Registry>,

    /// Central catalog.
    pub catalog: Arc<dyn Catalog>,

    /// Store holding the parquet files.
    pub parquet_store: ParquetStorage,

    /// Executor.
    pub exec: Arc<Executor>,

    /// Time provider.
    pub time_provider: Arc<dyn TimeProvider>,

    /// Backoff config
    pub backoff_config: BackoffConfig,

    /// Number of partitions that should be compacted in parallel.
    pub partition_concurrency: NonZeroUsize,

    /// Partitions with recent created files these last minutes are selected for compaction.
    pub partition_minute_threshold: u64,
}
