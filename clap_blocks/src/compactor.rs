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
                long = "write-buffer-topic",
                env = "INFLUXDB_IOX_WRITE_BUFFER_TOPIC",
                default_value = "iox-shared",
                action
            )]
            pub topic: String,

            /// Write buffer shard index to start (inclusive) range with
            #[clap(
                long = "shard-index-range-start",
                env = "INFLUXDB_IOX_SHARD_INDEX_RANGE_START",
                action
            )]
            pub shard_index_range_start: i32,

            /// Write buffer shard index to end (inclusive) range with
            #[clap(
                long = "shard-index-range-end",
                env = "INFLUXDB_IOX_SHARD_INDEX_RANGE_END",
                action
            )]
            pub shard_index_range_end: i32,

            /// Desired max size of compacted parquet files.
            /// It is a target desired value, rather than a guarantee.
            /// 1024 * 1024 * 25 =  26,214,400 (25MB)
            #[clap(
                long = "compaction-max-desired-size-bytes",
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
                long = "compaction-percentage-max-file_size",
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
                long = "compaction-split-percentage",
                env = "INFLUXDB_IOX_COMPACTION_SPLIT_PERCENTAGE",
                default_value = "80",
                action
            )]
            pub split_percentage: u16,

            /// Max number of partitions per shard we want to compact per cycle
            /// Default: 1
            #[clap(
                long = "compaction-max-number-partitions-per-shard",
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
                long = "compaction-min-number-recent-ingested-files-per-partition",
                env = "INFLUXDB_IOX_COMPACTION_MIN_NUMBER_RECENT_INGESTED_FILES_PER_PARTITION",
                default_value = "1",
                action
            )]
            pub min_number_recent_ingested_files_per_partition: usize,

            /// The multiple of times that compacting hot partitions should run for every one time
            /// that compacting cold partitions runs. Set to 1 to compact hot partitions and cold
            /// partitions equally.
            ///
            /// Default is
            #[doc = $hot_multiple_default]
            #[clap(
                long = "compaction-hot-multiple",
                env = "INFLUXDB_IOX_COMPACTION_HOT_MULTIPLE",
                default_value = $hot_multiple_default,
                action
            )]
            pub hot_multiple: usize,

            /// The multiple of times that compacting warm partitions should run for every one time
            /// that compacting cold partitions runs. Set to 1 to compact warm partitions and cold
            /// partitions equally.
            ///
            /// Default is 1
            #[clap(
                long = "compaction-warm-multiple",
                env = "INFLUXDB_IOX_COMPACTION_WARM_MULTIPLE",
                default_value = "1",
                action
            )]
            pub warm_multiple: usize,

            /// The memory budget assigned to this compactor.
            ///
            /// For each partition candidate, we will estimate the memory needed to compact each
            /// file and only add more files if their needed estimated memory is below this memory
            /// budget. Since we must compact L1 files that overlapped with L0 files, if their
            /// total estimated memory does not allow us to compact a part of a partition at all,
            /// we will not compact it and will log the partition and its related information in a
            /// table in our catalog for further diagnosis of the issue.
            ///
            /// The number of candidates compacted concurrently is also decided using this
            /// estimation and budget.
            ///
            /// Default is 30 * 1024 * 1024 * 1024 = 32,212,254,720 bytes (30GB).
            #[clap(
                long = "compaction-memory-budget-bytes",
                env = "INFLUXDB_IOX_COMPACTION_MEMORY_BUDGET_BYTES",
                default_value = "32212254720",
                action
            )]
            pub memory_budget_bytes: u64,

            /// Minimum number of rows allocated for each record batch fed into DataFusion plan
            /// 
            /// We will use max(parquet_file's row_count, min_num_rows_allocated_per_record_batch_to_datafusion_plan)
            /// to estimate number of rows allocated for each record batch fed into DataFusion plan.
            /// 
            #[clap(
                long = "compaction-min-rows-allocated-per-record-batch-to-plan",
                env = "INFLUXDB_IOX_COMPACTION_MIN_ROWS_PER_RECORD_BATCH_TO_PLAN",
                default_value = "8192",
                action
            )]
            pub min_num_rows_allocated_per_record_batch_to_datafusion_plan: u64,

            /// Max number of files to compact per partition
            /// 
            /// Due to limitations of the implementation of the compaction plans
            /// there is a hard maximum on the number of files that can be compacted
            /// at once. This avoids a wide fan-in multi-way merge in the DataFusion plan
            #[clap(
                long = "compaction-max-num-compacting-files",
                env = "INFLUXDB_IOX_COMPACTION_MAX_COMPACTING_FILES",
                default_value = "20",
                action
            )]
            pub max_num_compacting_files: usize,

            /// Max number of files to compact for a partition in which the first file and its
            /// overlaps push the file count limit over `max_num_compacting_files`.
            /// It's a special case of `max_num_compacting_files` that's higher just for the first
            /// file in a partition
            #[clap(
                long = "compaction-max-num-compacting-files-first-in-partition",
                env = "INFLUXDB_IOX_COMPACTION_MAX_COMPACTING_FILES_FIRST_IN_PARTITION",
                default_value = "40",
                action
            )]
            pub max_num_compacting_files_first_in_partition: usize,

            /// Number of minutes without a write to a partition before it is considered cold
            /// and thus a candidate for compaction
            #[clap(
                long = "compaction-minutes-without-new-writes-to-be-cold",
                env = "INFLUXDB_IOX_COMPACTION_MINUTES_WITHOUT_NEW_WRITE_TO_BE_COLD",
                default_value = "480",
                action
            )]
            pub minutes_without_new_writes_to_be_cold: u64,

            /// When select cold partition candidates, partitions with new created files (any level) after 
            /// this threshold will be considered a candidate. However, only partitions without new writes
            /// after this minutes_without_new_writes_to_be_cold will get compacted
            #[clap(
                long = "compaction-cold-partition_candidate-hours-threshold",
                env = "INFLUXDB_IOX_COMPACTION_COLD_PARTITION_CANDIDATES_HOURS_THRESHOLD",
                default_value = "24",
                action
            )]
            pub cold_partition_candidates_hours_threshold: u64,

            /// When querying for partitions with data for hot compaction, how many hours to look
            /// back for a first pass.
            #[clap(
                long = "compaction-hot-partition-hours-threshold-1",
                env = "INFLUXDB_IOX_COMPACTION_HOT_PARTITION_HOURS_THRESHOLD_1",
                default_value = "4",
                action
            )]
            pub hot_compaction_hours_threshold_1: u64,

            /// When querying for partitions with data for hot compaction, how many hours to look
            /// back for a second pass if we found nothing in the first pass.
            #[clap(
                long = "compaction-hot-partition-hours-threshold-2",
                env = "INFLUXDB_IOX_COMPACTION_HOT_PARTITION_HOURS_THRESHOLD_2",
                default_value = "24",
                action
            )]
            pub hot_compaction_hours_threshold_2: u64,

            /// Max number of partitions that can be compacted in parallel at once
            /// We use memory budget to estimate how many partitions can be compacted in parallel at once. 
            /// However, we do not want to have that number too large which will cause the high usage of CPU cores
            /// and may also lead to inaccuracy of memory estimation. This number is to cap that.
            #[clap(
                long = "compaction-max-parallel-partitions",
                env = "INFLUXDB_IOX_COMPACTION_MAX_PARALLEL_PARTITIONS",
                default_value = "20",
                action
            )]
            pub max_parallel_partitions: u64,

            /// When select warm partition candidates, partitions with new created files (any level) after 
            /// this threshold will be considered a candidate. However, only partitions with many contiguous small
            /// L1 files will get warm compacted
            #[clap(
                long = "compaction-warm-partition_candidate-hours-threshold",
                env = "INFLUXDB_IOX_COMPACTION_WARM_PARTITION_CANDIDATES_HOURS_THRESHOLD",
                default_value = "24",
                action
            )]
            pub warm_partition_candidates_hours_threshold: u64,

            /// When querying for partitions suitable for warm compaction, this is the
            /// upper bound on file size to be counted as "small".
            /// Default is half of max_desired_file_size_bytes's default (see above).
            #[clap(
                long = "compaction-warm-small-size-threshold-bytes",
                env = "INFLUXDB_IOX_COMPACTION_WARM_SMALL_SIZE_THRESHOLD_BYTES",
                default_value = "13107200",
                action
            )]
            pub warm_compaction_small_size_threshold_bytes: i64,

            /// When querying for partitions suitable for warm compaction, this is the minimum
            /// number of "small" files a partition must have in order for it to be selected
            /// as a candidate for warm compaction.
            #[clap(
                long = "compaction-warm-min-small-file-count",
                env = "INFLUXDB_IOX_COMPACTION_WARM_MIN_SMALL_FILE_COUNT",
                default_value = "10",
                action
            )]
            pub warm_compaction_min_small_file_count: usize,
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
            max_number_partitions_per_shard: self.max_number_partitions_per_shard,
            min_number_recent_ingested_files_per_partition: self
                .min_number_recent_ingested_files_per_partition,
            hot_multiple: self.hot_multiple,
            warm_multiple: self.warm_multiple,
            memory_budget_bytes: self.memory_budget_bytes,
            min_num_rows_allocated_per_record_batch_to_datafusion_plan: self
                .min_num_rows_allocated_per_record_batch_to_datafusion_plan,
            max_num_compacting_files: self.max_num_compacting_files,
            max_num_compacting_files_first_in_partition: self.max_num_compacting_files_first_in_partition,
            minutes_without_new_writes_to_be_cold: self.minutes_without_new_writes_to_be_cold,
            cold_partition_candidates_hours_threshold: self.cold_partition_candidates_hours_threshold,
            hot_compaction_hours_threshold_1: self.hot_compaction_hours_threshold_1,
            hot_compaction_hours_threshold_2: self.hot_compaction_hours_threshold_2,
            max_parallel_partitions: self.max_parallel_partitions,
            warm_partition_candidates_hours_threshold: self.warm_partition_candidates_hours_threshold,
            warm_compaction_small_size_threshold_bytes: self.warm_compaction_small_size_threshold_bytes,
            warm_compaction_min_small_file_count: self.warm_compaction_min_small_file_count,
        }
    }
}
