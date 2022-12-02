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

    /// The maximum number of persist tasks that can run simultaneously.
    #[clap(
        long = "persist-max-parallelism",
        env = "INFLUXDB_IOX_PERSIST_MAX_PARALLELISM",
        default_value = "5",
        action
    )]
    pub persist_max_parallelism: usize,

    /// The maximum number of persist tasks that can be queued for each worker.
    ///
    /// Note that each partition is consistently hashed to the same worker -
    /// this can cause uneven distribution of persist tasks across workers in
    /// workloads with skewed / hot partitions.
    #[clap(
        long = "persist-worker-queue-depth",
        env = "INFLUXDB_IOX_PERSIST_WORKER_QUEUE_DEPTH",
        default_value = "10",
        action
    )]
    pub persist_worker_queue_depth: usize,

    /// The maximum number of persist tasks queued in the shared submission
    /// queue. This is an advanced option, users should prefer
    /// "--persist-worker-queue-depth".
    ///
    /// This queue provides a buffer for persist tasks before they are hashed to
    /// a worker and enqueued for the worker to process.
    #[clap(
        long = "persist-submission-queue-depth",
        env = "INFLUXDB_IOX_PERSIST_SUBMISSION_QUEUE_DEPTH",
        default_value = "5",
        action
    )]
    pub persist_submission_queue_depth: usize,
}
