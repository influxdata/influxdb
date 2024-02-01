//! Compactor-Scheduler-related configs.

use crate::socket_addr::SocketAddr;
use std::str::FromStr;

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

/// CLI config for scheduler's gossip.
#[derive(Debug, Clone, clap::Parser)]
pub struct CompactorSchedulerGossipConfig {
    /// A comma-delimited set of seed gossip peer addresses.
    ///
    /// Example: "10.0.0.1:4242,10.0.0.2:4242"
    ///
    /// These seeds will be used to discover all other peers that talk to the
    /// same seeds. Typically all nodes in the cluster should use the same set
    /// of seeds.
    #[clap(
        long = "compactor-scheduler-gossip-seed-list",
        env = "INFLUXDB_IOX_COMPACTOR_SCHEDULER_GOSSIP_SEED_LIST",
        required = false,
        num_args=1..,
        value_delimiter = ',',
        requires = "scheduler_gossip_bind_address", // Field name, not flag
    )]
    pub scheduler_seed_list: Vec<String>,

    /// The UDP socket address IOx will use for gossip communication between
    /// peers.
    ///
    /// Example: "0.0.0.0:4242"
    ///
    /// If not provided, the gossip sub-system is disabled.
    #[clap(
        long = "compactor-scheduler-gossip-bind-address",
        env = "INFLUXDB_IOX_COMPACTOR_SCHEDULER_GOSSIP_BIND_ADDR",
        default_value = "0.0.0.0:0",
        required = false,
        action
    )]
    pub scheduler_gossip_bind_address: SocketAddr,
}

impl Default for CompactorSchedulerGossipConfig {
    fn default() -> Self {
        Self {
            scheduler_seed_list: vec![],
            scheduler_gossip_bind_address: SocketAddr::from_str("0.0.0.0:4324").unwrap(),
        }
    }
}

impl CompactorSchedulerGossipConfig {
    /// constructor for GossipConfig
    ///
    pub fn new(bind_address: &str, seed_list: Vec<String>) -> Self {
        Self {
            scheduler_seed_list: seed_list,
            scheduler_gossip_bind_address: SocketAddr::from_str(bind_address).unwrap(),
        }
    }
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

    /// Maximum number of files that the compactor will try and
    /// compact in a single plan.
    ///
    /// The higher this setting is the fewer compactor plans are run
    /// and thus fewer resources over time are consumed by the
    /// compactor. Increasing this setting also increases the peak
    /// memory used for each compaction plan, and thus if it is set
    /// too high, the compactor plans may exceed available memory.
    #[clap(
        long = "compaction-max-num-files-per-plan",
        env = "INFLUXDB_IOX_COMPACTION_MAX_NUM_FILES_PER_PLAN",
        default_value = "20",
        action
    )]
    pub max_num_files_per_plan: usize,

    /// Desired max size of compacted parquet files.
    ///
    /// Note this is a target desired value, rather than a guarantee.
    /// 1024 * 1024 * 100 =  104,857,600
    #[clap(
        long = "compaction-max-desired-size-bytes",
        env = "INFLUXDB_IOX_COMPACTION_MAX_DESIRED_FILE_SIZE_BYTES",
        default_value = "104857600",
        action
    )]
    pub max_desired_file_size_bytes: u64,

    /// Minimum number of L1 files to compact to L2.
    ///
    /// If there are more than this many L1 (by definition non
    /// overlapping) files in a partition, the compactor will compact
    /// them together into one or more larger L2 files.
    ///
    /// Setting this value higher in general results in fewer overall
    /// resources spent on compaction but more files per partition (and
    /// thus less optimal compression and query performance).
    #[clap(
        long = "compaction-min-num-l1-files-to-compact",
        env = "INFLUXDB_IOX_COMPACTION_MIN_NUM_L1_FILES_TO_COMPACT",
        default_value = "10",
        action
    )]
    pub min_num_l1_files_to_compact: usize,

    /// Maximum number of columns in a table of a partition that
    /// will be able to considered to get compacted
    ///
    /// If a table has more than this many columns, the compactor will
    /// not compact it, to avoid large memory use.
    #[clap(
        long = "compaction-max-num-columns-per-table",
        env = "INFLUXDB_IOX_COMPACTION_MAX_NUM_COLUMNS_PER_TABLE",
        default_value = "10000",
        action
    )]
    pub max_num_columns_per_table: usize,

    /// Percentage of desired max file size for "leading edge split"
    /// optimization.
    ///
    /// This setting controls the estimated output file size at which
    /// the compactor will apply the "leading edge" optimization.
    ///
    /// When compacting files together, if the output size is
    /// estimated to be greater than the following quantity, the
    /// "leading edge split" optimization will be applied:
    ///
    /// percentage_max_file_size * target_file_size
    ///
    /// This value must be between (0, 100)
    ///
    /// Default is 20
    #[clap(
        long = "compaction-percentage-max-file_size",
        env = "INFLUXDB_IOX_COMPACTION_PERCENTAGE_MAX_FILE_SIZE",
        default_value = "20",
        action
    )]
    pub percentage_max_file_size: u16,

    /// Enable new priority-based compaction selection.
    ///
    /// Eventually, this will be the only way to select partitions.
    ///
    /// Default is false
    #[clap(
        long = "compaction-priority-based-selection",
        env = "INFLUXDB_IOX_COMPACTION_PRIORITY_BASED_SELECTION",
        default_value = "false",
        action
    )]
    pub priority_based_selection: bool,

    /// Split file percentage for "leading edge split"
    ///
    /// To reduce the likelihood of recompacting the same data too many
    /// times, the compactor uses the "leading edge split"
    /// optimization for the common case where the new data written
    /// into a partition also has the most recent timestamps.
    ///
    /// When compacting multiple files together, if the compactor
    /// estimates the resulting file will be large enough (see
    /// `percentage_max_file_size`) it creates two output files
    /// rather than one, split by time, like this:
    ///
    /// `|-------------- older_data -----------------||---- newer_data ----|`
    ///
    /// In the common case, the file containing `older_data` is less
    /// likely to overlap with new data written in.
    ///
    /// This setting controls what percentage of data is placed into
    /// the `older_data` portion.
    ///
    /// Increasing this value increases the average size of compacted
    /// files after the first round of compaction. However, doing so
    /// also increase the likelihood that late arriving data will
    /// overlap with larger existing files, necessitating additional
    /// compaction rounds.
    ///
    /// This value must be between (0, 100)
    #[clap(
        long = "compaction-split-percentage",
        env = "INFLUXDB_IOX_COMPACTION_SPLIT_PERCENTAGE",
        default_value = "80",
        action
    )]
    pub split_percentage: u16,

    /// Partition source config used by the local scheduler.
    #[clap(flatten)]
    pub partition_source_config: PartitionSourceConfigForLocalScheduler,

    /// Shard config used by the local scheduler.
    #[clap(flatten)]
    pub shard_config: ShardConfigForLocalScheduler,

    /// Gossip config.
    #[clap(flatten)]
    pub gossip_config: CompactorSchedulerGossipConfig,
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
