//! Querier-related config for the RPC write path.

/// CLI config for the querier using the RPC write path
#[derive(Debug, Clone, PartialEq, Eq, clap::Parser)]
pub struct QuerierRpcWriteConfig {
    /// The number of threads to use for queries.
    ///
    /// If not specified, defaults to the number of cores on the system
    #[clap(
        long = "num-query-threads",
        env = "INFLUXDB_IOX_NUM_QUERY_THREADS",
        action
    )]
    pub num_query_threads: Option<usize>,

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

    /// gRPC address for the router to talk with the ingesters. For
    /// example:
    ///
    /// "http://127.0.0.1:8083"
    ///
    /// or
    ///
    /// "http://10.10.10.1:8083,http://10.10.10.2:8083"
    ///
    /// for multiple addresses.
    #[clap(
        long = "ingester-addresses",
        env = "INFLUXDB_IOX_INGESTER_ADDRESSES",
        required = true
    )]
    pub ingester_addresses: Vec<String>,

    /// Size of the RAM cache used to store catalog metadata information in bytes.
    #[clap(
        long = "ram-pool-metadata-bytes",
        env = "INFLUXDB_IOX_RAM_POOL_METADATA_BYTES",
        default_value = "134217728",  // 128MB
        action
    )]
    pub ram_pool_metadata_bytes: usize,

    /// Size of the RAM cache used to store data in bytes.
    #[clap(
        long = "ram-pool-data-bytes",
        env = "INFLUXDB_IOX_RAM_POOL_DATA_BYTES",
        default_value = "1073741824",  // 1GB
        action
    )]
    pub ram_pool_data_bytes: usize,

    /// Limit the number of concurrent queries.
    #[clap(
        long = "max-concurrent-queries",
        env = "INFLUXDB_IOX_MAX_CONCURRENT_QUERIES",
        default_value = "10",
        action
    )]
    pub max_concurrent_queries: usize,

    /// After how many ingester query errors should the querier enter circuit breaker mode?
    ///
    /// The querier normally contacts the ingester for any unpersisted data during query planning.
    /// However, when the ingester can not be contacted for some reason, the querier will begin
    /// returning results that do not include unpersisted data and enter "circuit breaker mode"
    /// to avoid continually retrying the failing connection on subsequent queries.
    ///
    /// If circuits are open, the querier will NOT contact the ingester and no unpersisted data
    /// will be presented to the user.
    ///
    /// Circuits will switch to "half open" after some jittered timeout and the querier will try to
    /// use the ingester in question again. If this succeeds, we are back to normal, otherwise it
    /// will back off exponentially before trying again (and again ...).
    ///
    /// In a production environment the `ingester_circuit_state` metric should be monitored.
    #[clap(
        long = "ingester-circuit-breaker-threshold",
        env = "INFLUXDB_IOX_INGESTER_CIRCUIT_BREAKER_THRESHOLD",
        default_value = "10",
        action
    )]
    pub ingester_circuit_breaker_threshold: u64,
}
