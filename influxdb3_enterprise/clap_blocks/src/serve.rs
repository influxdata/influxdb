//! Extend the `influxdb3 serve` command for InfluxDB Enterprise

use std::{
    collections::{BTreeSet, btree_set},
    ops::Deref,
    str::FromStr,
    sync::Arc,
};

use anyhow::bail;
use influxdb3_catalog::log::NodeMode;
#[derive(Debug, clap::Parser)]
pub struct EnterpriseServeConfig {
    /// The cluster id
    #[clap(long = "cluster-id", env = "INFLUXDB3_ENTERPRISE_CLUSTER_ID", action)]
    pub cluster_identifier_prefix: Option<String>,

    /// The mode to start the server in
    #[clap(long = "mode", value_enum, default_values = vec!["all"], env = "INFLUXDB3_ENTERPRISE_MODE", value_delimiter=',')]
    pub mode: Vec<BufferMode>,

    /// Comma-separated list of node identifier prefixes, i.e., `node-id`s to read WAL files from
    ///
    /// Each node in the list will have its data queryable by this server by checking for new WAL files produced
    /// by that node on object storage on the interval specified by the `replication-interval` option.
    ///
    /// If `run-compactions` is set to true, this node list, if provided, will also serve as
    /// the list of nodes to compact data from.
    ///
    /// If the reader for any given node fails to initialize, the server will not start.
    #[clap(
        long = "read-from-node-ids",
        // TODO: this alias should be deprecated
        alias = "replicas",
        env = "INFLUXDB3_ENTERPRISE_READ_FROM_NODE_IDS",
        action
    )]
    pub read_from_node_ids: Option<NodeIdList>,

    /// The interval at which each reader specified in the `read-from-node-ids` option will be replicated
    #[clap(
        long = "replication-interval",
        env = "INFLUXDB3_ENTERPRISE_REPLICATION_INTERVAL",
        default_value = "250ms",
        action
    )]
    pub replication_interval: humantime::Duration,

    /// The prefix in object store where all compacted data will be written to. Only provide this
    /// option if this server should be running compaction for its own write buffer and any
    /// replicas it is managing.
    ///
    /// Only a single server should be running at any time that has this option set with this value.
    /// It should have exclusive write access to this prefix in object storage.
    #[clap(
        long = "compactor-id",
        env = "INFLUXDB3_ENTERPRISE_COMPACTOR_ID",
        action
    )]
    pub compactor_id: Option<Arc<str>>,

    /// Comma-separated list of node identifier prefixes to compact data from.
    ///
    /// The compactor will look for new snapshot files from each node in the list of
    /// `compact-from-node-ids`. It will compact gen1 file from those nodes into a single
    /// compacted view under the `compactor-id` prefix.
    #[clap(
        long = "compact-from-node-ids",
        // TODO: deprecate this alias
        alias = "compaction-hosts",
        env = "INFLUXDB3_ENTERPRISE_COMPACT_FROM_NODE_IDS",
        action
    )]
    pub compact_from_node_ids: Option<NodeIdList>,

    /// This tells the server to run compactions. Only a single server should ever be running
    /// compactions for a given compactor_id. All other servers can read from that compactor id
    /// to pick up compacted files. This option is only applicable if a compactor-id is set.
    /// Set this flag if this server should be running compactions.
    #[clap(
        long = "run-compactions",
        env = "INFLUXDB3_ENTERPRISE_RUN_COMPACTIONS",
        default_value = "false",
        action
    )]
    pub run_compactions: bool,

    /// The limit to the number of rows per file that the compactor will write. This is a soft limit
    /// and the compactor may write more rows than this limit.
    #[clap(
        long = "compaction-row-limit",
        env = "INFLUXDB3_ENTERPRISE_COMPACTION_ROW_LIMIT",
        default_value = "1000000",
        action
    )]
    pub compaction_row_limit: usize,

    /// Set the maximum number of files that will be included in any compaction plan.
    ///
    /// This will limit memory usage during higher generation compactions.
    #[clap(
        long = "compaction-max-num-files-per-plan",
        env = "INFLUXDB3_ENTERPRISE_COMPACTION_MAX_NUM_FILES_PER_PLAN",
        default_value = "500",
        action
    )]
    pub compaction_max_num_files_per_plan: usize,

    /// This is the duration of the first level of compaction (gen2). Later levels of compaction
    /// will be multiples of this duration. This number should be equal to or greater than
    /// the gen1 duration. The default is 20m, which is 2x the default gen1 duration of 10m.
    #[clap(
        long = "compaction-gen2-duration",
        env = "INFLUXDB3_ENTERPRISE_COMPACTION_GEN2_DURATION",
        default_value = "20m",
        action
    )]
    pub compaction_gen2_duration: humantime::Duration,

    /// This comma-separated list of multiples specifies the duration of each level of compaction.
    /// The number of elements in this list will also determine the number of levels of compaction.
    /// The first element in the list will be the duration of the first level of compaction (gen3).
    /// Each subsequent level will be a multiple of the previous level.
    ///
    /// The default values of 3,4,6,5 when paired with the default gen2 duration of 20m will result
    /// in the following compaction levels: 20m (gen2), 1h (gen3), 4h (gen4), 24h (gen5), 5d (gen6).
    #[clap(
        long = "compaction-multipliers",
        env = "INFLUXDB3_ENTERPRISE_COMPACTION_MULTIPLIERS",
        default_value = "3,4,6,5",
        action
    )]
    pub compaction_multipliers: CompactionMultipliers,

    /// This is the amount of time that the compactor waits after finishing a compaction run to
    /// delete files marked as needing deletion during that compaction run.
    #[clap(
        long = "compaction-cleanup-wait",
        env = "INFLUXDB3_ENTERPRISE_COMPACTION_CLEANUP_WAIT",
        default_value = "10m",
        action
    )]
    pub compaction_cleanup_wait: humantime::Duration,

    /// The interval to prefetch into parquet cache when compacting.
    ///
    /// Enter as a human-readable time, e.g., "1d", etc.
    #[clap(
        long = "preemptive-cache-age",
        env = "INFLUXDB3_PREEMPTIVE_CACHE_AGE",
        default_value = "3d",
        action
    )]
    pub preemptive_cache_age: humantime::Duration,

    #[clap(
        long = "license-email",
        env = "INFLUXDB3_ENTERPRISE_LICENSE_EMAIL",
        action
    )]
    pub license_email: Option<String>,
}

/// Mode of operation for the InfluxDB Pro write buffer
#[derive(Debug, Clone, Copy, Default, clap::ValueEnum, PartialEq, Eq, Hash, PartialOrd, Ord)]
#[clap(rename_all = "snake_case")]
pub enum BufferMode {
    /// Will act as a read-replica and only accept queries
    Query,
    /// Will only accept writes and serve system table queries.
    Ingest,
    /// Will act as a compactor only, pulling snapshots from the host list and compacting them.
    /// Only one node in a cluster may run in this mode.
    Compact,
    /// Similar to 'query' but called out separately for use serving processing engine requests.
    Process,
    /// Run ingestion, query, compaction, and processing. If there are other ingest nodes, this
    /// node will pick up WAL and persisted snapshots from those for query and compaction.
    #[default]
    All,
}
impl std::fmt::Display for BufferMode {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            BufferMode::Query => write!(f, "query"),
            BufferMode::Ingest => write!(f, "ingest"),
            BufferMode::Compact => write!(f, "compact"),
            BufferMode::Process => write!(f, "process"),
            BufferMode::All => write!(f, "all"),
        }
    }
}

impl From<BufferMode> for NodeMode {
    fn from(mode: BufferMode) -> Self {
        match mode {
            BufferMode::Query => Self::Query,
            BufferMode::Ingest => Self::Ingest,
            BufferMode::Compact => Self::Compact,
            BufferMode::Process => Self::Process,
            BufferMode::All => Self::All,
        }
    }
}

#[derive(Clone, Debug)]
pub struct BufferModes(BTreeSet<BufferMode>);

impl From<Vec<BufferMode>> for BufferModes {
    fn from(bs: Vec<BufferMode>) -> Self {
        let mut s = BTreeSet::new();

        for b in bs {
            s.insert(b);
        }

        BufferModes(s)
    }
}

impl BufferModes {
    pub fn is_compactor(&self) -> bool {
        self.0.contains(&BufferMode::Compact) || self.0.contains(&BufferMode::All)
    }

    pub fn is_ingester(&self) -> bool {
        self.0.contains(&BufferMode::Ingest) || self.0.contains(&BufferMode::All)
    }

    pub fn is_querier(&self) -> bool {
        self.0.contains(&BufferMode::Query) || self.0.contains(&BufferMode::All)
    }

    pub fn contains(&self, mode: &BufferMode) -> bool {
        self.0.contains(mode)
    }

    pub fn contains_only(&self, mode: &BufferMode) -> bool {
        self.0.len() == 1 && self.0.contains(mode)
    }

    pub fn into_iter(&self) -> btree_set::IntoIter<BufferMode> {
        self.0.clone().into_iter()
    }
}

#[derive(Debug, Clone)]
pub struct NodeIdList(Vec<String>);

impl From<NodeIdList> for Vec<String> {
    fn from(list: NodeIdList) -> Self {
        list.0
    }
}

impl Deref for NodeIdList {
    type Target = Vec<String>;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl FromStr for NodeIdList {
    type Err = anyhow::Error;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let replicas: Vec<String> = s.split(',').map(|s| s.to_owned()).collect();
        if replicas.is_empty() {
            bail!("list of replicas cannot be empty")
        }
        Ok(Self(replicas))
    }
}

#[derive(Debug, Clone)]
pub struct CompactionMultipliers(pub Vec<u8>);

impl FromStr for CompactionMultipliers {
    type Err = anyhow::Error;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let multipliers: Result<Vec<u8>, _> = s.split(',').map(|s| s.trim().parse()).collect();
        match multipliers {
            Ok(multipliers) if !multipliers.is_empty() => Ok(Self(multipliers)),
            _ => bail!("Invalid list of compaction multipliers"),
        }
    }
}
