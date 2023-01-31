//! CLI config for compactor2-related commands

use std::num::NonZeroUsize;

/// CLI config for compactor2
#[derive(Debug, Clone, clap::Parser)]
pub struct Compactor2Config {
    /// Number of partitions that should be compacted in parallel.
    ///
    /// This should usually be larger than the compaction job concurrency since one partition can spawn multiple compaction jobs.
    #[clap(
        long = "compaction-partition-concurrency",
        env = "INFLUXDB_IOX_COMPACTION_PARTITION_CONCURRENCY",
        default_value = "100",
        action
    )]
    pub compaction_partition_concurrency: NonZeroUsize,

    /// Number of concurrent compaction jobs.
    ///
    /// This should usually be smaller than the partition concurrency since one partition can spawn multiple compaction jobs.
    #[clap(
        long = "compaction-job-concurrency",
        env = "INFLUXDB_IOX_COMPACTION_JOB_CONCURRENCY",
        default_value = "10",
        action
    )]
    pub compaction_job_concurrency: NonZeroUsize,

    /// Number of jobs PER PARTITION that move files in and out of the scratchpad.
    #[clap(
        long = "compaction-partition-scratchpad-concurrency",
        env = "INFLUXDB_IOX_COMPACTION_PARTITION_SCRATCHPAD_CONCURRENCY",
        default_value = "10",
        action
    )]
    pub compaction_partition_scratchpad_concurrency: NonZeroUsize,

    /// Partitions with recent created files these last minutes are selected for compaction.
    #[clap(
        long = "compaction_partition_minute_threshold",
        env = "INFLUXDB_IOX_COMPACTION_PARTITION_MINUTE_THRESHOLD",
        default_value = "10",
        action
    )]
    pub compaction_partition_minute_threshold: u64,

    /// Number of threads to use for the compactor query execution, compaction and persistence.
    #[clap(
        long = "query-exec-thread-count",
        env = "INFLUXDB_IOX_QUERY_EXEC_THREAD_COUNT",
        default_value = "4",
        action
    )]
    pub query_exec_thread_count: usize,

    /// Size of memory pool used during query exec, in bytes.
    ///
    /// If queries attempt to allocate more than this many bytes
    /// during execution, they will error with "ResourcesExhausted".
    #[clap(
        long = "exec-mem-pool-bytes",
        env = "INFLUXDB_IOX_EXEC_MEM_POOL_BYTES",
        default_value = "8589934592",  // 8GB
        action
    )]
    pub exec_mem_pool_bytes: usize,

    /// Desired max size of compacted parquet files.
    /// It is a target desired value, rather than a guarantee.
    /// 1024 * 1024 * 25 =  26,214,400 (25MB)
    #[clap(
        long = "compaction-max-desired-size-bytes",
        env = "INFLUXDB_IOX_COMPACTION_MAX_DESIRED_FILE_SIZE_BYTES",
        default_value = "26214400",
        action
    )]
    pub max_desired_file_size_bytes: u64,

    /// Percentage of desired max file size.
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

    /// Maximum number of input files per partition. If there are more files, we ignore the partition (for now) as a
    /// self-protection mechanism.
    #[clap(
        long = "compaction-max-input-files-per-partition",
        env = "INFLUXDB_IOX_COMPACTION_MAX_INPUT_FILES_PER_PARTITION",
        default_value = "200",
        action
    )]
    pub max_input_files_per_partition: usize,

    /// Maximum input bytes (in parquet) per partition. If there is more data, we ignore the partition (for now) as a
    /// self-protection mechanism.
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
}
