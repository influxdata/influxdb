use datafusion::{common::extensions_options, config::ConfigExtension};

/// IOx-specific config extension prefix.
pub const IOX_CONFIG_PREFIX: &str = "iox";

extensions_options! {
    /// Config options for IOx.
    pub struct IoxConfigExt {
        /// When splitting de-duplicate operations based on IOx partitions[^iox_part], this is the maximum number of IOx
        /// partitions that should be considered. If there are more partitions, the split will NOT be performed.
        ///
        /// This protects against certain highly degenerative plans.
        ///
        ///
        /// [^iox_part]: "IOx partition" refers to a partition within the IOx catalog, i.e. a partition within the
        ///              primary key space. This is NOT the same as a DataFusion partition which refers to a stream
        ///              within the physical plan data flow.
        pub max_dedup_partition_split: usize, default = 100

        /// When splitting de-duplicate operations based on time-based overlaps, this is the maximum number of groups
        /// that should be considered. If there are more groups, the split will NOT be performed.
        ///
        /// This protects against certain highly degenerative plans.
        pub max_dedup_time_split: usize, default = 100

        /// When multiple parquet files are required in a sorted way (e.g. for de-duplication), we have two options:
        ///
        /// 1. **In-mem sorting:** Put them into [`target_partitions`] DataFusion partitions. This limits the fan-out,
        ///    but requires that we potentially chain multiple parquet files into a single DataFusion partition. Since
        ///    chaining sorted data does NOT automatically result in sorted data (e.g. AB-AB is not sorted), we need to
        ///    preform an in-memory sort using [`SortExec`] afterwards. This is expensive.
        /// 2. **Fan-out:** Instead of chaining files within DataFusion partitions, we can accept a fan-out beyond
        ///    [`target_partitions`]. This prevents in-memory sorting but may result in OOMs (out-of-memory).
        ///
        /// We try to pick option 2 up to a certain number of files, which is configured by this setting.
        ///
        ///
        /// [`SortExec`]: datafusion::physical_plan::sorts::sort::SortExec
        /// [`target_partitions`]: datafusion::common::config::ExecutionOptions::target_partitions
        pub max_parquet_fanout: usize, default = 40
    }
}

impl ConfigExtension for IoxConfigExt {
    const PREFIX: &'static str = IOX_CONFIG_PREFIX;
}
