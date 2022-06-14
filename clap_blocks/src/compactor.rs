/// CLI config for compactor
#[derive(Debug, Clone, clap::Parser)]
pub struct CompactorConfig {
    /// Write buffer topic/database that the compactor will be compacting files for. It won't
    /// connect to Kafka, but uses this to get the sequencers out of the catalog.
    #[clap(
        long = "--write-buffer-topic",
        env = "INFLUXDB_IOX_WRITE_BUFFER_TOPIC",
        default_value = "iox-shared",
        action
    )]
    pub topic: String,

    /// Write buffer partition number to start (inclusive) range with
    #[clap(
        long = "--write-buffer-partition-range-start",
        env = "INFLUXDB_IOX_WRITE_BUFFER_PARTITION_RANGE_START",
        action
    )]
    pub write_buffer_partition_range_start: i32,

    /// Write buffer partition number to end (inclusive) range with
    #[clap(
        long = "--write-buffer-partition-range-end",
        env = "INFLUXDB_IOX_WRITE_BUFFER_PARTITION_RANGE_END",
        action
    )]
    pub write_buffer_partition_range_end: i32,

    /// Percentage of least recent data we want to split to reduce compacting non-overlapped data
    /// Must be between 0 and 100. Default is 100, which won't split the resulting file.
    #[clap(
        long = "--compaction-split-percentage",
        env = "INFLUXDB_IOX_COMPACTION_SPLIT_PERCENTAGE",
        default_value = "100",
        action
    )]
    pub split_percentage: i64,

    /// The compactor will limit the number of simultaneous compaction jobs based on the
    /// size of the input files to be compacted.  This number should be less than 1/10th
    /// of the available memory to ensure compactions have
    /// enough space to run. Default is 1,000,000,000 (1GB ).
    #[clap(
        long = "--compaction-concurrent-size-bytes",
        env = "INFLUXDB_IOX_COMPACTION_CONCURRENT_SIZE_BYTES",
        default_value = "1000000000",
        action
    )]
    pub max_concurrent_compaction_size_bytes: i64,

    /// The compactor will compact overlapped files with non-overlapped and contiguous files
    /// a larger file of max size defined by the config value.
    /// Default is 100,000,000 (100MB)
    #[clap(
        long = "--compaction-max-size-bytes",
        env = "INFLUXDB_IOX_COMPACTION_MAX_SIZE_BYTES",
        default_value = "100000000",
        action
    )]
    pub compaction_max_size_bytes: i64,

    /// Limit the number of files per compaction
    /// Default is 100
    #[clap(
        long = "--compaction-max-file-count",
        env = "INFLUXDB_IOX_COMPACTION_MAX_FILE_COUNT",
        default_value = "100",
        action
    )]
    pub compaction_max_file_count: i64,
}
