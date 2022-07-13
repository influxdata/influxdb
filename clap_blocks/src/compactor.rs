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

    /// Max number of level-0 files (written by ingester) we want to compact with level-1 each time
    /// Default: 3
    #[clap(
        // TODO: verify the theory that if the streaming works  as expected,
        // we do not need to limit this number
        long = "--compaction-max-number-level-0-files",
        env = "INFLUXDB_IOX_COMPACTION_MAX_NUMBER_LEVEL_0_FILES",
        default_value = "3",
        action
    )]
    pub compaction_max_number_level_0_files: i32,

    /// Desired max size of compacted parquet files
    /// It is a target desired value than a guarantee
    /// Default is 100,000,000 (100MB)
    #[clap(
        long = "--compaction-max-desired-size-bytes",
        env = "INFLUXDB_IOX_COMPACTION_MAX_DESIRED_FILE_SIZE_BYTES",
        default_value = "100000000",
        action
    )]
    pub compaction_max_desired_file_size_bytes: i64,

    /// Percentage of desired max file size.
    /// If the estimated compacted result is too small, no need to split it.
    /// This percentage is to determine how small it is:
    ///    < compaction_percentage_max_file_size * compaction_max_desired_file_size_bytes:
    /// This value must be between (0, 100)
    /// Default is 30
    #[clap(
        long = "--compaction-percentage-max-file_size",
        env = "INFLUXDB_IOX_COMPACTION_PERCENTAGE_MAX_FILE_SIZE",
        default_value = "30",
        action
    )]
    pub compaction_percentage_max_file_size: i16,

    /// Split file percentage
    /// If the estimated compacted result is neither too small nor too large, it will be split
    /// into 2 files determined by this percentage.
    ///    . Too small means: < compaction_percentage_max_file_size * compaction_max_desired_file_size_bytes
    ///    . Too large means: > compaction_max_desired_file_size_bytes
    ///    . Any size in the middle will be considered neither too small nor too large
    /// This value must be between (0, 100)
    /// Default is 80
    #[clap(
        long = "--compaction-split-percentage",
        env = "INFLUXDB_IOX_COMPACTION_SPLIT_PERCENTAGE",
        default_value = "80",
        action
    )]
    pub compaction_split_percentage: i16,

    /// The compactor will limit the number of simultaneous compaction jobs based on the
    /// size of the input files to be compacted.  This number should be less than 1/10th
    /// of the available memory to ensure compactions have
    /// enough space to run. Default is 1,000,000,000 (1GB ).
    #[clap(
        long = "--compaction-concurrent-size-bytes",
        env = "INFLUXDB_IOX_COMPACTION_CONCURRENT_SIZE_BYTES",
        default_value = "100000000",
        action
    )]
    pub max_concurrent_compaction_size_bytes: i64,
}
