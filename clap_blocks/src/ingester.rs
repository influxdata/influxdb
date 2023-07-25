//! CLI config for the ingester using the RPC write path

use std::path::PathBuf;

use crate::gossip::GossipConfig;

/// CLI config for the ingester using the RPC write path
#[derive(Debug, Clone, clap::Parser)]
#[allow(missing_copy_implementations)]
pub struct IngesterConfig {
    /// Gossip config.
    #[clap(flatten)]
    pub gossip_config: GossipConfig,

    /// Where this ingester instance should store its write-ahead log files. Each ingester instance
    /// must have its own directory.
    #[clap(long = "wal-directory", env = "INFLUXDB_IOX_WAL_DIRECTORY", action)]
    pub wal_directory: PathBuf,

    /// Specify the maximum allowed incoming RPC write message size sent by the
    /// Router.
    #[clap(
        long = "rpc-write-max-incoming-bytes",
        env = "INFLUXDB_IOX_RPC_WRITE_MAX_INCOMING_BYTES",
        default_value = "104857600", // 100MiB
    )]
    pub rpc_write_max_incoming_bytes: usize,

    /// The number of seconds between WAL file rotations.
    #[clap(
        long = "wal-rotation-period-seconds",
        env = "INFLUXDB_IOX_WAL_ROTATION_PERIOD_SECONDS",
        default_value = "300",
        action
    )]
    pub wal_rotation_period_seconds: u64,

    /// Sets how many queries the ingester will handle simultaneously before
    /// rejecting further incoming requests.
    #[clap(
        long = "concurrent-query-limit",
        env = "INFLUXDB_IOX_CONCURRENT_QUERY_LIMIT",
        default_value = "20",
        action
    )]
    pub concurrent_query_limit: usize,

    /// The maximum number of persist tasks that can run simultaneously.
    #[clap(
        long = "persist-max-parallelism",
        env = "INFLUXDB_IOX_PERSIST_MAX_PARALLELISM",
        default_value = "5",
        action
    )]
    pub persist_max_parallelism: usize,

    /// The maximum number of persist tasks that can be queued at any one time.
    ///
    /// Once this limit is reached, ingest is blocked until the persist backlog
    /// is reduced.
    #[clap(
        long = "persist-queue-depth",
        env = "INFLUXDB_IOX_PERSIST_QUEUE_DEPTH",
        default_value = "250",
        action
    )]
    pub persist_queue_depth: usize,

    /// The limit at which a partition's estimated persistence cost causes it to
    /// be queued for persistence.
    #[clap(
        long = "persist-hot-partition-cost",
        env = "INFLUXDB_IOX_PERSIST_HOT_PARTITION_COST",
        default_value = "20000000", // 20,000,000
        action
    )]
    pub persist_hot_partition_cost: usize,
}
