//! Config-related stuff.
use std::{num::NonZeroUsize, sync::Arc, time::Duration};

use backoff::BackoffConfig;
use compactor_scheduler::SchedulerConfig;
use iox_catalog::interface::Catalog;
use iox_query::exec::Executor;
use iox_time::TimeProvider;
use parquet_file::storage::ParquetStorage;

use crate::components::parquet_files_sink::ParquetFilesSink;

/// Multiple from `max_desired_file_size_bytes` to compute the minimum value for
/// `max_compact_size_bytes`. Since `max_desired_file_size_bytes` is softly enforced, actual file
/// sizes can exceed it. A single compaction job must be able to compact > 1 max sized file, so the
/// multiple should be at least 3.
const MIN_COMPACT_SIZE_MULTIPLE: usize = 3;

/// Config to set up a compactor.
#[derive(Debug, Clone)]
pub struct Config {
    /// Metric registry.
    pub metric_registry: Arc<metric::Registry>,

    /// trace collector
    pub trace_collector: Option<Arc<dyn trace::TraceCollector>>,

    /// Central catalog.
    pub catalog: Arc<dyn Catalog>,

    /// Scheduler configuration.
    pub scheduler_config: SchedulerConfig,

    /// Store holding the actual parquet files.
    pub parquet_store_real: ParquetStorage,

    /// Store holding temporary files.
    pub parquet_store_scratchpad: ParquetStorage,

    /// Executor.
    pub exec: Arc<Executor>,

    /// Time provider.
    pub time_provider: Arc<dyn TimeProvider>,

    /// Backoff config
    pub backoff_config: BackoffConfig,

    /// Number of partitions that should be compacted in parallel.
    ///
    /// This should usually be larger than the compaction job concurrency since one partition can spawn multiple
    /// compaction jobs.
    pub partition_concurrency: NonZeroUsize,

    /// Number of compaction jobs concurrently scheduled to DataFusion.
    ///
    /// This should usually be smaller than the partition concurrency since one partition can spawn multiple compaction
    /// jobs.
    pub df_concurrency: NonZeroUsize,

    /// Number of jobs PER PARTITION that move files in and out of the scratchpad.
    pub partition_scratchpad_concurrency: NonZeroUsize,

    /// Desired max size of compacted parquet files
    /// It is a target desired value than a guarantee
    pub max_desired_file_size_bytes: u64,

    /// Percentage of desired max file size.
    /// If the estimated compacted result is too small, no need to split it.
    /// This percentage is to determine how small it is:
    ///    < percentage_max_file_size * max_desired_file_size_bytes:
    /// This value must be between (0, 100)
    pub percentage_max_file_size: u16,

    /// Split file percentage
    /// If the estimated compacted result is neither too small nor too large, it will be split
    /// into 2 files determined by this percentage.
    ///    . Too small means: < percentage_max_file_size * max_desired_file_size_bytes
    ///    . Too large means: > max_desired_file_size_bytes
    ///    . Any size in the middle will be considered neither too small nor too large
    /// This value must be between (0, 100)
    pub split_percentage: u16,

    /// Maximum duration of the per-partition compaction task.
    pub partition_timeout: Duration,

    /// Shadow mode.
    ///
    /// This will NOT write / commit any output to the object store or catalog.
    ///
    /// This is mostly useful for debugging.
    pub shadow_mode: bool,

    /// Enable Scratchpad
    ///
    /// Enabled by default, if this is set to false, the compactor will not use the scratchpad
    ///
    /// This is useful for disabling the scratchpad in production to evaluate the performance & memory impacts.
    pub enable_scratchpad: bool,

    /// Minimum number of L1 files to compact to L2
    /// This is to prevent too many small files
    pub min_num_l1_files_to_compact: usize,

    /// Only process all discovered partitions once.
    pub process_once: bool,

    /// Simulate compactor w/o any object store interaction. No parquet
    /// files will be read or written.
    ///
    /// This will still use the catalog
    ///
    /// This is useful for testing.
    pub simulate_without_object_store: bool,

    /// Use the provided [`ParquetFilesSink`] to create parquet files
    /// (used for testing)
    pub parquet_files_sink_override: Option<Arc<dyn ParquetFilesSink>>,

    /// Ensure that ALL errors (including object store errors) result in "skipped" partitions.
    ///
    /// This is mostly useful for testing.
    pub all_errors_are_fatal: bool,

    /// Maximum number of columns in the table of a partition that will be considered get comapcted
    /// If there are more columns, the partition will be skipped
    /// This is to prevent too many columns in a table
    pub max_num_columns_per_table: usize,

    /// max number of files per compaction plan
    pub max_num_files_per_plan: usize,

    /// Limit the number of partition fetch queries to at most the specified
    /// number of queries per second.
    ///
    /// Queries are smoothed over the full second.
    pub max_partition_fetch_queries_per_second: Option<usize>,
}

impl Config {
    /// Maximum input bytes (from parquet files) per compaction. If there is more data, we ignore
    /// the partition (for now) as a self-protection mechanism.
    pub fn max_compact_size_bytes(&self) -> usize {
        self.max_desired_file_size_bytes as usize * MIN_COMPACT_SIZE_MULTIPLE
    }
}
