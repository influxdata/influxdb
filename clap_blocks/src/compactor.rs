//! CLI config for compactor-related commands

use std::num::NonZeroUsize;

use crate::{gossip::GossipConfig, memory_size::MemorySize};

use super::compactor_scheduler::CompactorSchedulerConfig;

/// CLI config for compactor
#[derive(Debug, Clone, clap::Parser)]
pub struct CompactorConfig {
    /// Gossip config.
    #[clap(flatten)]
    pub gossip_config: GossipConfig,

    /// Configuration for the compactor scheduler
    #[clap(flatten)]
    pub compactor_scheduler_config: CompactorSchedulerConfig,

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

    /// Number of concurrent compaction jobs scheduled to DataFusion.
    ///
    /// This should usually be smaller than the partition concurrency
    /// since one partition can spawn multiple DF compaction jobs.
    #[clap(
        long = "compaction-df-concurrency",
        env = "INFLUXDB_IOX_COMPACTION_DF_CONCURRENCY",
        default_value = "10",
        action
    )]
    pub compaction_df_concurrency: NonZeroUsize,

    /// Number of jobs PER PARTITION that move files in and out of the
    /// scratchpad.
    #[clap(
        long = "compaction-partition-scratchpad-concurrency",
        env = "INFLUXDB_IOX_COMPACTION_PARTITION_SCRATCHPAD_CONCURRENCY",
        default_value = "10",
        action
    )]
    pub compaction_partition_scratchpad_concurrency: NonZeroUsize,

    /// Number of threads to use for the compactor query execution,
    /// compaction and persistence.
    /// If not specified, defaults to one less than the number of cores on the system
    #[clap(
        long = "query-exec-thread-count",
        env = "INFLUXDB_IOX_QUERY_EXEC_THREAD_COUNT",
        action
    )]
    pub query_exec_thread_count: Option<NonZeroUsize>,

    /// Size of memory pool used during compaction plan execution, in
    /// bytes.
    ///
    /// If compaction plans attempt to allocate more than this many
    /// bytes during execution, they will error with
    /// "ResourcesExhausted".
    ///
    /// Can be given as absolute value or in percentage of the total available memory (e.g. `10%`).
    #[clap(
        long = "exec-mem-pool-bytes",
        env = "INFLUXDB_IOX_EXEC_MEM_POOL_BYTES",
        default_value = "17179869184",  // 16GB
        action
    )]
    pub exec_mem_pool_bytes: MemorySize,

    /// Overrides INFLUXDB_IOX_EXEC_MEM_POOL_BYTES to set the size of memory pool
    /// used during compaction DF plan execution.  This value is expressed as a percent
    /// of the memory limit for the cgroup (e.g. 70 = 70% of the cgroup memory limit).
    /// This is converted to a byte limit as the compactor starts.
    ///
    /// Extreme values (<20% or >90%) are ignored and INFLUXDB_IOX_EXEC_MEM_POOL_BYTES
    /// is used.  It will also use INFLUXDB_IOX_EXEC_MEM_POOL_BYTES if we fail to read
    /// the cgroup limit, or it doesn't parse to a sane value.
    ///
    /// If compaction plans attempt to allocate more than the computed byte limit
    /// during execution, they will error with "ResourcesExhausted".
    #[clap(
        long = "exec-mem-pool-percent",
        env = "INFLUXDB_IOX_EXEC_MEM_POOL_PERCENT",
        default_value = "70",
        action
    )]
    pub exec_mem_pool_percent: u64,

    /// Maximum duration of the per-partition compaction task in seconds.
    #[clap(
        long = "compaction-partition-timeout-secs",
        env = "INFLUXDB_IOX_COMPACTION_PARTITION_TIMEOUT_SECS",
        default_value = "1800",
        action
    )]
    pub partition_timeout_secs: u64,

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

    /// Enable scratchpad.
    ///
    /// This allows disabling the scratchpad in production.
    ///
    /// Disabling this is useful for testing performance and memory consequences of the scratchpad.
    #[clap(
        long = "compaction-enable-scratchpad",
        env = "INFLUXDB_IOX_COMPACTION_ENABLE_SCRATCHPAD",
        default_value = "true",
        action
    )]
    pub enable_scratchpad: bool,

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

    /// Limit the number of partition fetch queries to at most the specified
    /// number of queries per second.
    ///
    /// Queries are smoothed over the full second.
    #[clap(
        long = "max-partition-fetch-queries-per-second",
        env = "INFLUXDB_IOX_MAX_PARTITION_FETCH_QUERIES_PER_SECOND",
        action
    )]
    pub max_partition_fetch_queries_per_second: Option<usize>,
}
