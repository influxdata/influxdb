//! CLI config for compactor-related commands

#![cfg_attr(rustfmt, rustfmt_skip)] // https://github.com/rust-lang/rustfmt/issues/5489

/// Create compactor configuration that can have different defaults. The `run compactor`
/// server/service needs different defaults than the `compactor run-once` command, and this macro
/// enables sharing of the parts of the configs that are the same without duplicating the code.
macro_rules! gen_compactor_config {
    (
        $name:ident,
        // hot_multiple is currently the only flag that has a differing default. Add more macro
        // arguments similar to this one if more flags need different defaults.
        hot_multiple_default = $hot_multiple_default:literal
        $(,)?
    ) => {
        /// CLI config for compactor
        #[derive(Debug, Clone, clap::Parser)]
        pub struct $name {
            /// Write buffer topic/database that the compactor will be compacting files for. It
            /// won't connect to Kafka, but uses this to get the shards out of the catalog.
            #[clap(
                long = "--write-buffer-topic",
                env = "INFLUXDB_IOX_WRITE_BUFFER_TOPIC",
                default_value = "iox-shared",
                action
            )]
            pub topic: String,

            /// Write buffer shard index to start (inclusive) range with
            #[clap(
                long = "--shard-index-range-start",
                env = "INFLUXDB_IOX_SHARD_INDEX_RANGE_START",
                action
            )]
            pub shard_index_range_start: i32,

            /// Write buffer shard index to end (inclusive) range with
            #[clap(
                long = "--shard-index-range-end",
                env = "INFLUXDB_IOX_SHARD_INDEX_RANGE_END",
                action
            )]
            pub shard_index_range_end: i32,

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
            /// If the estimated compacted result is neither too small nor too large, it will be
            /// split into 2 files determined by this percentage.
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

            /// The compactor will limit the number of simultaneous cold partition compaction jobs
            /// based on the size of the input files to be compacted. This number should be less
            /// than 1/10th of the available memory to ensure compactions have enough space to run.
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

            /// Max number of partitions per shard we want to compact per cycle
            /// Default: 1
            #[clap(
                long = "--compaction-max-number-partitions-per-shard",
                env = "INFLUXDB_IOX_COMPACTION_MAX_NUMBER_PARTITIONS_PER_SHARD",
                default_value = "1",
                action
            )]
            pub max_number_partitions_per_shard: usize,

            /// Min number of recent ingested files a partition needs to be considered for
            /// compacting
            ///
            /// Default: 1
            #[clap(
                long = "--compaction-min-number-recent-ingested-files-per-partition",
                env = "INFLUXDB_IOX_COMPACTION_MIN_NUMBER_RECENT_INGESTED_FILES_PER_PARTITION",
                default_value = "1",
                action
            )]
            pub min_number_recent_ingested_files_per_partition: usize,

            /// A compaction operation for cold partitions will gather as many L0 files with their
            /// overlapping L1 files to compact together until the total size of input files
            /// crosses this threshold. Later compactions will pick up the remaining L0 files.
            ///
            /// Default is 1024 * 1024 * 600 = 629,145,600 bytes (600MB).
            #[clap(
                long = "--compaction-cold-input-size-threshold-bytes",
                env = "INFLUXDB_IOX_COMPACTION_COLD_INPUT_SIZE_THRESHOLD_BYTES",
                default_value = "629145600",
                action
            )]
            pub cold_input_size_threshold_bytes: u64,

            /// A compaction operation for cold partitions  will gather as many L0 files with their 
            /// overlapping L1 files to compact together until the total number of L0 + L1 files 
            /// crosses this threshold.
            /// Later compactions will pick up the remaining L0 files.
            ///
            /// A compaction operation will be limited by this or by the cold input size threshold,
            /// whichever is hit first.
            #[clap(
                long = "--compaction-cold-input-file-count-threshold",
                env = "INFLUXDB_IOX_COMPACTION_COLD_INPUT_FILE_COUNT_THRESHOLD",
                default_value = "50",
                action
            )]
            pub cold_input_file_count_threshold: usize,

            /// The multiple of times that compacting hot partitions should run for every one time
            /// that compacting cold partitions runs. Set to 1 to compact hot partitions and cold
            /// partitions equally.
            ///
            /// Default is
            #[doc = $hot_multiple_default]
            #[clap(
                long = "--compaction-hot-multiple",
                env = "INFLUXDB_IOX_COMPACTION_HOT_MULTIPLE",
                default_value = $hot_multiple_default,
                action
            )]
            pub hot_multiple: usize,

            /// The memory budget asigned to this compactor.
            /// For each partition candidate, we will esimate the memory needed to compact each file
            /// and only add more files if their needed estimated memory is below this memory budget.
            /// Since we must compact L1 files that overlapped with L0 files, if their total estimated 
            /// memory do not allow us to compact a part of a partition at all, we will not compact 
            /// it and will log the partition and its related information in a table in our catalog for
            /// further diagnosis of the issue.
            /// How many candidates compacted concurrently are also decided using this estimation and
            /// budget.
            /// Default is 30 * 1024 * 1024 * 1024 = 32,212,254,720 bytes (30GB).
            #[clap(
                long = "--compaction-memory-budget-bytes",
                env = "INFLUXDB_IOX_COMPACTION_MEMORY_BUDGET_BYTES",
                default_value = "32212254720",
                action
            )]
            pub memory_budget_bytes: u64,
        }
    };
}

gen_compactor_config!(CompactorConfig, hot_multiple_default = "4");

gen_compactor_config!(CompactorOnceConfig, hot_multiple_default = "1");

impl CompactorOnceConfig {
    /// Convert the configuration for `compactor run-once` into the configuration for `run
    /// compactor` so that run-once can reuse some of the code that the compactor server uses.
    pub fn into_compactor_config(self) -> CompactorConfig {
        CompactorConfig {
            topic: self.topic,
            shard_index_range_start: self.shard_index_range_start,
            shard_index_range_end: self.shard_index_range_end,
            max_desired_file_size_bytes: self.max_desired_file_size_bytes,
            percentage_max_file_size: self.percentage_max_file_size,
            split_percentage: self.split_percentage,
            max_cold_concurrent_size_bytes: self.max_cold_concurrent_size_bytes,
            max_number_partitions_per_shard: self.max_number_partitions_per_shard,
            min_number_recent_ingested_files_per_partition: self
                .min_number_recent_ingested_files_per_partition,
            cold_input_size_threshold_bytes: self.cold_input_size_threshold_bytes,
            cold_input_file_count_threshold: self.cold_input_file_count_threshold,
            hot_multiple: self.hot_multiple,
            memory_budget_bytes: self.memory_budget_bytes,
        }
    }
}
