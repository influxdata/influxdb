//! CLI config for compactor2-related commands

use std::{fmt::Display, num::NonZeroUsize};

use clap::ValueEnum;

/// Algorithm version used by the compactor
#[derive(Copy, Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Default, ValueEnum)]
pub enum CompactorAlgoVersion {
    // Note: clap only keeps the first line for the help text, so try to be brief.
    /// Compacts all files of a partition in single DataFusion job, prone to reject "too large" partitions. Default.
    #[default]
    AllAtOnce,

    /// Repeat to compact to higher level until reaching the highest level. NOT yet ready for production
    TargetLevel,
}

impl Display for CompactorAlgoVersion {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{self:?}")
    }
}

/// CLI config for compactor2
#[derive(Debug, Clone, clap::Parser)]
pub struct Compactor2Config {
    /// Number of partitions that should be compacted in parallel.
    ///
    /// This should usually be larger than the compaction job
    /// concurrency since one partition can spawn multiple compaction
    /// jobs.
    #[clap(
        long = "compaction-partition-concurrency",
        env = "INFLUXDB_IOX_COMPACTION_PARTITION_CONCURRENCY",
        default_value = "100",
        action
    )]
    pub compaction_partition_concurrency: NonZeroUsize,

    /// Number of concurrent compaction jobs.
    ///
    /// This should usually be smaller than the partition concurrency
    /// since one partition can spawn multiple compaction jobs.
    #[clap(
        long = "compaction-job-concurrency",
        env = "INFLUXDB_IOX_COMPACTION_JOB_CONCURRENCY",
        default_value = "10",
        action
    )]
    pub compaction_job_concurrency: NonZeroUsize,

    /// Number of jobs PER PARTITION that move files in and out of the
    /// scratchpad.
    #[clap(
        long = "compaction-partition-scratchpad-concurrency",
        env = "INFLUXDB_IOX_COMPACTION_PARTITION_SCRATCHPAD_CONCURRENCY",
        default_value = "10",
        action
    )]
    pub compaction_partition_scratchpad_concurrency: NonZeroUsize,

    /// The compactor will only consider compacting partitions that
    /// have new parquet files created within this many minutes.
    #[clap(
        long = "compaction_partition_minute_threshold",
        env = "INFLUXDB_IOX_COMPACTION_PARTITION_MINUTE_THRESHOLD",
        default_value = "10",
        action
    )]
    pub compaction_partition_minute_threshold: u64,

    /// Number of threads to use for the compactor query execution,
    /// compaction and persistence.
    #[clap(
        long = "query-exec-thread-count",
        env = "INFLUXDB_IOX_QUERY_EXEC_THREAD_COUNT",
        default_value = "4",
        action
    )]
    pub query_exec_thread_count: usize,

    /// Size of memory pool used during compaction plan execution, in
    /// bytes.
    ///
    /// If compaction plans attempt to allocate more than this many
    /// bytes during execution, they will error with
    /// "ResourcesExhausted".
    #[clap(
        long = "exec-mem-pool-bytes",
        env = "INFLUXDB_IOX_EXEC_MEM_POOL_BYTES",
        default_value = "8589934592",  // 8GB
        action
    )]
    pub exec_mem_pool_bytes: usize,

    /// Desired max size of compacted parquet files.
    ///
    /// Note this is a target desired value, rather than a guarantee.
    /// 1024 * 1024 * 100 =  104,857,600
    #[clap(
        long = "compaction-max-desired-size-bytes",
        env = "INFLUXDB_IOX_COMPACTION_MAX_DESIRED_FILE_SIZE_BYTES",
        default_value = "104857600",
        action
    )]
    pub max_desired_file_size_bytes: u64,

    /// Percentage of desired max file size.
    ///
    /// If the estimated compacted result is too small, no need to split it.
    /// This percentage is to determine how small it is:
    ///    < percentage_max_file_size * max_desired_file_size_bytes:
    /// This value must be between (0, 100)
    /// Default is 20
    #[clap(
        long = "compaction-percentage-max-file_size",
        env = "INFLUXDB_IOX_COMPACTION_PERCENTAGE_MAX_FILE_SIZE",
        default_value = "20",
        action
    )]
    pub percentage_max_file_size: u16,

    /// Split file percentage
    /// If the estimated compacted result is neither too small nor too large, it will be
    /// split into 2 files determined by this percentage.
    ///    . Too small means: < percentage_max_file_size * max_desired_file_size_bytes
    ///    . Too large means: > max_desired_file_size_bytes
    ///    . Any size in the middle will be considered neither too small nor too large
    ///
    /// This value must be between (0, 100)
    /// Default is 80
    #[clap(
        long = "compaction-split-percentage",
        env = "INFLUXDB_IOX_COMPACTION_SPLIT_PERCENTAGE",
        default_value = "80",
        action
    )]
    pub split_percentage: u16,

    /// Maximum duration of the per-partition compaction task in seconds.
    #[clap(
        long = "compaction-partition-timeout-secs",
        env = "INFLUXDB_IOX_COMPACTION_PARTITION_TIMEOUT_SECS",
        default_value = "1800",
        action
    )]
    pub partition_timeout_secs: u64,

    /// Filter partitions to the given set of IDs.
    ///
    /// This is mostly useful for debugging.
    #[clap(
        long = "compaction-partition-filter",
        env = "INFLUXDB_IOX_COMPACTION_PARTITION_FILTER",
        action
    )]
    pub partition_filter: Option<Vec<i64>>,

    /// Shadow mode.
    ///
    /// This will NOT write / commit any output to the object store or catalog.
    ///
    /// This is mostly useful for debugging.
    #[clap(
        long = "compaction-shadow-mode",
        env = "INFLUXDB_IOX_COMPACTION_SHADOW_MODE",
        action
    )]
    pub shadow_mode: bool,

    /// Ignores "partition marked w/ error and shall be skipped" entries in the catalog.
    ///
    /// This is mostly useful for debugging.
    #[clap(
        long = "compaction-ignore-partition-skip-marker",
        env = "INFLUXDB_IOX_COMPACTION_IGNORE_PARTITION_SKIP_MARKER",
        action
    )]
    pub ignore_partition_skip_marker: bool,

    /// Maximum number of files that the compactor will try and
    /// compact in a single plan.
    ///
    /// The higher this setting is the fewer compactor plans are run
    /// and thus fewer resources over time are consumed by the
    /// compactor. Increasing this setting also increases the peak
    /// memory used for each compaction plan, and thus if it is set
    /// too high, the compactor plans may exceed available memory.
    #[clap(
        long = "compaction-max-num-files-per-plan",
        env = "INFLUXDB_IOX_COMPACTION_MAX_NUM_FILES_PER_PLAN",
        default_value = "200",
        action
    )]
    pub max_num_files_per_plan: usize,

    /// Maximum input bytes (in parquet) per partition that the
    /// compactor will attempt to compact in any one plan.
    ///
    /// In the worst case, if the sum of the sizes of all parquet
    /// files in a partition is greater than this value, the compactor
    /// may not try to compact this partition. Under normal operation,
    /// the compactor compacts a subset of files in a partition but in
    /// some cases it may need to compact them all.
    ///
    /// This setting is a self protection mechanism, and it is
    /// expected to be removed in future versions
    #[clap(
        long = "compaction-max-input-parquet-bytes-per-partition",
        env = "INFLUXDB_IOX_COMPACTION_MAX_INPUT_PARQUET_BYTES_PER_PARTITION",
        default_value = "268435456",  // 256MB
        action
    )]
    pub max_input_parquet_bytes_per_partition: usize,

    /// Number of shards.
    ///
    /// If this is set then the shard ID MUST also be set. If both are not provided, sharding is disabled.
    #[clap(
        long = "compaction-shard-count",
        env = "INFLUXDB_IOX_COMPACTION_SHARD_COUNT",
        action
    )]
    pub shard_count: Option<usize>,

    /// Shard ID.
    ///
    /// Starts at 0, must be smaller than the number of shard.
    ///
    /// If this is set then the shard count MUST also be set. If both are not provided, sharding is disabled.
    #[clap(
        long = "compaction-shard-id",
        env = "INFLUXDB_IOX_COMPACTION_SHARD_ID",
        action
    )]
    pub shard_id: Option<usize>,

    /// Version of the compaction algorithm.
    #[clap(
        long = "compaction-compact-version",
        env = "INFLUXDB_IOX_COMPACTION_COMPACT_VERSION",
        default_value_t = CompactorAlgoVersion::default(),
        value_enum
    )]
    pub compact_version: CompactorAlgoVersion,

    /// Minimum number of L1 files to compact to L2.
    ///
    /// If there are more than this many L1 (by definition non
    /// overlapping) files in a partition, the compactor will compact
    /// them together into one or more larger L2 files.
    ///
    /// Setting this value higher in general results in fewer overall
    /// resources spent on compaction but more files per partition (and
    /// thus less optimal compression and query performance).
    #[clap(
        long = "compaction-min-num-l1-files-to-compact",
        env = "INFLUXDB_IOX_COMPACTION_MIN_NUM_L1_FILES_TO_COMPACT",
        default_value = "10",
        action
    )]
    pub min_num_l1_files_to_compact: usize,

    /// Only process all discovered partitions once.
    ///
    /// By default the compactor will continuously loop over all
    /// partitions looking for work. Setting this option results in
    /// exiting the loop after the one iteration.
    #[clap(
        long = "compaction-process-once",
        env = "INFLUXDB_IOX_COMPACTION_PROCESS_ONCE",
        action
    )]
    pub process_once: bool,

    /// Compact all partitions found in the catalog, no matter if/when
    /// they received writes.
    #[clap(
        long = "compaction-process-all-partitions",
        env = "INFLUXDB_IOX_COMPACTION_PROCESS_ALL_PARTITIONS",
        action
    )]
    pub process_all_partitions: bool,

    /// Maximum number of columns in a table of a partition that
    /// will be able to considered to get compacted
    ///
    /// If a table has more than this many columns, the compactor will
    /// not compact it, to avoid large memory use.
    #[clap(
        long = "compaction-max-num-columns-per-table",
        env = "INFLUXDB_IOX_COMPACTION_MAX_NUM_COLUMNS_PER_TABLE",
        default_value = "10000",
        action
    )]
    pub max_num_columns_per_table: usize,
}
