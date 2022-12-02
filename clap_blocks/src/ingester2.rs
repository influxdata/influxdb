//! CLI config for the ingester using the RPC write path

use std::path::PathBuf;

/// CLI config for the ingester using the RPC write path
#[derive(Debug, Clone, clap::Parser)]
#[allow(missing_copy_implementations)]
pub struct Ingester2Config {
    /// Where this ingester instance should store its write-ahead log files. Each ingester instance
    /// must have its own directory.
    #[clap(long = "wal-directory", env = "INFLUXDB_IOX_WAL_DIRECTORY", action)]
    pub wal_directory: PathBuf,

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
}
