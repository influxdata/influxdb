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

    /// Desired max size of compacted parquet files.
    /// It is a target desired value, rather than a guarantee.
    /// 1024 * 1024 * 25 =  26,214,400 (25MB)
    #[clap(
        long = "--compaction-max-desired-size-bytes",
        env = "INFLUXDB_IOX_COMPACTION_MAX_DESIRED_FILE_SIZE_BYTES",
        default_value = "26214400",
        action
    )]
    pub max_desired_file_size_bytes: u64,

    /// Percentage of desired max file size.
    /// If the estimated compacted result is too small, no need to split it.
    /// This percentage is to determine how small it is:
    ///    < percentage_max_file_size * max_desired_file_size_bytes:
    /// This value must be between (0, 100)
    /// Default is 80
    #[clap(
        long = "--compaction-percentage-max-file_size",
        env = "INFLUXDB_IOX_COMPACTION_PERCENTAGE_MAX_FILE_SIZE",
        default_value = "80",
        action
    )]
    pub percentage_max_file_size: u16,

    /// Split file percentage
    /// If the estimated compacted result is neither too small nor too large, it will be split
    /// into 2 files determined by this percentage.
    ///    . Too small means: < percentage_max_file_size * max_desired_file_size_bytes
    ///    . Too large means: > max_desired_file_size_bytes
    ///    . Any size in the middle will be considered neither too small nor too large
    ///
    /// This value must be between (0, 100)
    /// Default is 80
    #[clap(
        long = "--compaction-split-percentage",
        env = "INFLUXDB_IOX_COMPACTION_SPLIT_PERCENTAGE",
        default_value = "80",
        action
    )]
    pub split_percentage: u16,

    /// The compactor will limit the number of simultaneous hot partition compaction jobs based on
    /// the size of the input files to be compacted. This number should be less than 1/10th of the
    /// available memory to ensure compactions have enough space to run.
    ///
    /// Default is 1024 * 1024 * 1024 = 1,073,741,824 bytes (1GB).
    //
    // The number of compact_hot_partititons run in parallel is determined by:
    //    max_concurrent_size_bytes/input_size_threshold_bytes
    #[clap(
        long = "--compaction-concurrent-size-bytes",
        env = "INFLUXDB_IOX_COMPACTION_CONCURRENT_SIZE_BYTES",
        default_value = "1073741824",
        action
    )]
    pub max_concurrent_size_bytes: u64,

    /// The compactor will limit the number of simultaneous cold partition compaction jobs based on
    /// the size of the input files to be compacted. This number should be less than 1/10th of the
    /// available memory to ensure compactions have enough space to run.
    ///
    /// Default is 1024 * 1024 * 900 = 943,718,400 bytes (900MB).
    //
    // The number of compact_cold_partititons run in parallel is determined by:
    //    max_cold_concurrent_size_bytes/cold_input_size_threshold_bytes
    #[clap(
        long = "--compaction-cold-concurrent-size-bytes",
        env = "INFLUXDB_IOX_COMPACTION_COLD_CONCURRENT_SIZE_BYTES",
        default_value = "943718400",
        action
    )]
    pub max_cold_concurrent_size_bytes: u64,

    /// Max number of partitions per sequencer we want to compact per cycle
    /// Default: 1
    #[clap(
        long = "--compaction-max-number-partitions-per-sequencer",
        env = "INFLUXDB_IOX_COMPACTION_MAX_NUMBER_PARTITIONS_PER_SEQUENCER",
        default_value = "1",
        action
    )]
    pub max_number_partitions_per_sequencer: usize,

    /// Min number of recent ingested files a partition needs to be considered for compacting
    /// Default: 1
    #[clap(
        long = "--compaction-min-number-recent-ingested-files-per-partition",
        env = "INFLUXDB_IOX_COMPACTION_MIN_NUMBER_RECENT_INGESTED_FILES_PER_PARTITION",
        default_value = "1",
        action
    )]
    pub min_number_recent_ingested_files_per_partition: usize,

    /// A compaction operation for hot partitions will gather as many L0 files with their
    /// overlapping L1 files to compact together until the total size of input files crosses this
    /// threshold. Later compactions will pick up the remaining L0 files.
    ///
    /// A compaction operation will be limited by this or by the file count threshold, whichever is
    /// hit first.
    ///
    /// Default is 1024 * 1024 * 100 = 100,048,576 bytes (100MB).
    #[clap(
        long = "--compaction-input-size-threshold-bytes",
        env = "INFLUXDB_IOX_COMPACTION_INPUT_SIZE_THRESHOLD_BYTES",
        default_value = "100048576",
        action
    )]
    pub input_size_threshold_bytes: u64,

    /// A compaction operation for cold partitions will gather as many L0 files with their
    /// overlapping L1 files to compact together until the total size of input files crosses this
    /// threshold. Later compactions will pick up the remaining L0 files.
    ///
    /// Default is 1024 * 1024 * 600 = 629,145,600 bytes (600MB).
    #[clap(
        long = "--compaction-cold-input-size-threshold-bytes",
        env = "INFLUXDB_IOX_COMPACTION_COLD_INPUT_SIZE_THRESHOLD_BYTES",
        default_value = "629145600",
        action
    )]
    pub cold_input_size_threshold_bytes: u64,

    /// A compaction operation will gather as many L0 files with their overlapping L1 files to
    /// compact together until the total number of L0 + L1 files crosses this threshold. Later
    /// compactions will pick up the remaining L0 files.
    ///
    /// A compaction operation will be limited by this or by the input size threshold, whichever is
    /// hit first.
    ///
    /// Default is 50.
    #[clap(
        long = "--compaction-input-file-count-threshold",
        env = "INFLUXDB_IOX_COMPACTION_INPUT_FILE_COUNT_THRESHOLD",
        default_value = "50",
        action
    )]
    pub input_file_count_threshold: usize,

    /// The multiple of times that compacting hot partitions should run for every one time that
    /// compacting cold partitions runs. Set to 1 to compact hot partitions and cold partitions
    /// equally.
    ///
    /// Default is 4.
    #[clap(
        long = "--compaction-hot-multiple",
        env = "INFLUXDB_IOX_COMPACTION_HOT_MULTIPLE",
        default_value = "4",
        action
    )]
    pub hot_multiple: usize,
}
