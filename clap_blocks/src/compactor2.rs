//! CLI config for compactor2-related commands

use std::num::NonZeroUsize;

/// CLI config for compactor2
#[derive(Debug, Clone, Copy, clap::Parser)]
pub struct Compactor2Config {
    /// Number of partitions that should be compacted in parallel.
    #[clap(
        long = "compaction-partition-concurrency",
        env = "INFLUXDB_IOX_COMPACTION_PARTITION_CONCURRENCY",
        default_value = "10",
        action
    )]
    pub compaction_partition_concurrency: NonZeroUsize,

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
}
