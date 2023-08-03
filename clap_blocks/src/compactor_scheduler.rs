//! Compactor-Scheduler-related configs.

/// Compaction Scheduler type.
#[derive(Debug, Default, Clone, Copy, PartialEq, clap::ValueEnum)]
pub enum CompactorSchedulerType {
    /// Perform scheduling decisions locally.
    #[default]
    Local,

    /// Perform scheduling decisions remotely.
    Remote,
}

/// CLI config for compactor scheduler.
#[derive(Debug, Clone, Default, clap::Parser)]
pub struct ShardConfigForLocalScheduler {
    /// Number of shards.
    ///
    /// If this is set then the shard ID MUST also be set. If both are not provided, sharding is disabled.
    /// (shard ID can be provided by the host name)
    #[clap(
        long = "compaction-shard-count",
        env = "INFLUXDB_IOX_COMPACTION_SHARD_COUNT",
        action
    )]
    pub shard_count: Option<usize>,

    /// Shard ID.
    ///
    /// Starts at 0, must be smaller than the number of shard.
    ///
    /// If this is set then the shard count MUST also be set. If both are not provided, sharding is disabled.
    #[clap(
        long = "compaction-shard-id",
        env = "INFLUXDB_IOX_COMPACTION_SHARD_ID",
        requires("shard_count"),
        action
    )]
    pub shard_id: Option<usize>,

    /// Host Name
    ///
    /// comprised of leading text (e.g. 'iox-shared-compactor-'), ending with shard_id (e.g. '0').
    /// When shard_count is specified, but shard_id is not specified, the id is extracted from hostname.
    #[clap(env = "HOSTNAME")]
    pub hostname: Option<String>,
}

/// CLI config for partitions_source used by the scheduler.
#[derive(Debug, Clone, Default, clap::Parser)]
pub struct PartitionSourceConfigForLocalScheduler {
    /// The compactor will only consider compacting partitions that
    /// have new Parquet files created within this many minutes.
    #[clap(
        long = "compaction_partition_minute_threshold",
        env = "INFLUXDB_IOX_COMPACTION_PARTITION_MINUTE_THRESHOLD",
        default_value = "10",
        action
    )]
    pub compaction_partition_minute_threshold: u64,

    /// Filter partitions to the given set of IDs.
    ///
    /// This is mostly useful for debugging.
    #[clap(
        long = "compaction-partition-filter",
        env = "INFLUXDB_IOX_COMPACTION_PARTITION_FILTER",
        action
    )]
    pub partition_filter: Option<Vec<i64>>,

    /// Compact all partitions found in the catalog, no matter if/when
    /// they received writes.
    #[clap(
        long = "compaction-process-all-partitions",
        env = "INFLUXDB_IOX_COMPACTION_PROCESS_ALL_PARTITIONS",
        default_value = "false",
        action
    )]
    pub process_all_partitions: bool,

    /// Ignores "partition marked w/ error and shall be skipped" entries in the catalog.
    ///
    /// This is mostly useful for debugging.
    #[clap(
        long = "compaction-ignore-partition-skip-marker",
        env = "INFLUXDB_IOX_COMPACTION_IGNORE_PARTITION_SKIP_MARKER",
        action
    )]
    pub ignore_partition_skip_marker: bool,
}

/// CLI config for compactor scheduler.
#[derive(Debug, Clone, Default, clap::Parser)]
pub struct CompactorSchedulerConfig {
    /// Scheduler type to use.
    #[clap(
        value_enum,
        long = "compactor-scheduler",
        env = "INFLUXDB_IOX_COMPACTION_SCHEDULER",
        default_value = "local",
        action
    )]
    pub compactor_scheduler_type: CompactorSchedulerType,

    /// Partition source config used by the local scheduler.
    #[clap(flatten)]
    pub partition_source_config: PartitionSourceConfigForLocalScheduler,

    /// Shard config used by the local scheduler.
    #[clap(flatten)]
    pub shard_config: ShardConfigForLocalScheduler,
}

#[cfg(test)]
mod tests {
    use super::*;
    use clap::Parser;
    use test_helpers::assert_contains;

    #[test]
    fn default_compactor_scheduler_type_is_local() {
        let config = CompactorSchedulerConfig::try_parse_from(["my_binary"]).unwrap();
        assert_eq!(
            config.compactor_scheduler_type,
            CompactorSchedulerType::Local
        );
    }

    #[test]
    fn can_specify_local() {
        let config = CompactorSchedulerConfig::try_parse_from([
            "my_binary",
            "--compactor-scheduler",
            "local",
        ])
        .unwrap();
        assert_eq!(
            config.compactor_scheduler_type,
            CompactorSchedulerType::Local
        );
    }

    #[test]
    fn any_other_scheduler_type_string_is_invalid() {
        let error = CompactorSchedulerConfig::try_parse_from([
            "my_binary",
            "--compactor-scheduler",
            "hello",
        ])
        .unwrap_err()
        .to_string();
        assert_contains!(
            &error,
            "invalid value 'hello' for '--compactor-scheduler <COMPACTOR_SCHEDULER_TYPE>'"
        );
        assert_contains!(&error, "[possible values: local, remote]");
    }
}
