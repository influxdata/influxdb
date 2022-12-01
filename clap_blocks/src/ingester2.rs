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

    /// Sets how many concurrent requests the ingester will handle before rejecting
    /// incoming requests.
    #[clap(
        long = "concurrent-request-limit",
        env = "INFLUXDB_IOX_CONCURRENT_REQUEST_LIMIT",
        default_value = "20",
        action
    )]
    pub concurrent_request_limit: usize,
}
