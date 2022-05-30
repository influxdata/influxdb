/// CLI config for catalog ingest lifecycle
#[derive(Debug, Clone, clap::Parser)]
pub struct IngesterConfig {
    /// Write buffer partition number to start (inclusive) range with
    #[clap(
        long = "--write-buffer-partition-range-start",
        env = "INFLUXDB_IOX_WRITE_BUFFER_PARTITION_RANGE_START"
    )]
    pub write_buffer_partition_range_start: i32,

    /// Write buffer partition number to end (inclusive) range with
    #[clap(
        long = "--write-buffer-partition-range-end",
        env = "INFLUXDB_IOX_WRITE_BUFFER_PARTITION_RANGE_END"
    )]
    pub write_buffer_partition_range_end: i32,

    /// The ingester will continue to pull data and buffer it from the write buffer as long as the
    /// ingester buffer is below this size. If the ingester buffer hits this size, ingest from the
    /// write buffer will pause until the ingester buffer goes below this threshold.
    #[clap(
        long = "--pause-ingest-size-bytes",
        env = "INFLUXDB_IOX_PAUSE_INGEST_SIZE_BYTES"
    )]
    pub pause_ingest_size_bytes: usize,

    /// Once the ingester crosses this threshold of data buffered across all sequencers, it will
    /// pick the largest partitions and persist them until it falls below this threshold. An
    /// ingester running in a steady state is expected to take up this much memory.
    #[clap(
        long = "--persist-memory-threshold-bytes",
        env = "INFLUXDB_IOX_PERSIST_MEMORY_THRESHOLD_BYTES"
    )]
    pub persist_memory_threshold_bytes: usize,

    /// If an individual partition crosses this size threshold, it will be persisted.
    /// The default value is 300MB (in bytes).
    #[clap(
        long = "--persist-partition-size-threshold-bytes",
        env = "INFLUXDB_IOX_PERSIST_PARTITION_SIZE_THRESHOLD_BYTES",
        default_value = "314572800"
    )]
    pub persist_partition_size_threshold_bytes: usize,

    /// If a partition has had data buffered for longer than this period of time, it will be
    /// persisted. This puts an upper bound on how far back the ingester may need to read from the
    /// write buffer on restart or recovery. The default value is 30 minutes (in seconds).
    #[clap(
        long = "--persist-partition-age-threshold-seconds",
        env = "INFLUXDB_IOX_PERSIST_PARTITION_AGE_THRESHOLD_SECONDS",
        default_value = "1800"
    )]
    pub persist_partition_age_threshold_seconds: u64,

    /// If a partition has had data buffered and hasn't received a write for this
    /// period of time, it will be persisted. The default value is 300 seconds (5 minutes).
    #[clap(
        long = "--persist-partition-cold-threshold-seconds",
        env = "INFLUXDB_IOX_PERSIST_PARTITION_COLD_THRESHOLD_SECONDS",
        default_value = "300"
    )]
    pub persist_partition_cold_threshold_seconds: u64,

    /// If the catalog's max sequence number for the partition is no longer available in the write
    /// buffer due to the retention policy, by default the ingester will panic. If this flag is
    /// specified, the ingester will skip any sequence numbers that have not been retained in the
    /// write buffer and will start up successfully with the oldest available data.
    #[clap(
        long = "--skip-to-oldest-available",
        env = "INFLUXDB_IOX_SKIP_TO_OLDEST_AVAILABLE"
    )]
    pub skip_to_oldest_available: bool,

    /// Sets how often `do_get` flight requests should panic for testing purposes.
    ///
    /// The first N requests will panic. Requests after this will just pass.
    #[clap(
        long = "--test-flight-do-get-panic",
        env = "INFLUXDB_IOX_FLIGHT_DO_GET_PANIC",
        default_value = "0"
    )]
    pub test_flight_do_get_panic: u64,

    /// Sets how many concurrent requests the ingester will handle before rejecting
    /// incoming requests.
    #[clap(
        long = "--concurrent-request-limit",
        env = "INFLUXDB_IOX_CONCURRENT_REQEST_LIMIT",
        default_value = "20"
    )]
    pub concurrent_request_limit: usize,
}
