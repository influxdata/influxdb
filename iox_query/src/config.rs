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
    }
}

impl ConfigExtension for IoxConfigExt {
    const PREFIX: &'static str = IOX_CONFIG_PREFIX;
}
