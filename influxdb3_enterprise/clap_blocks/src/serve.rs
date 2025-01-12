//! Extend the `influxdb3 serve` command for InfluxDB Enterprise

use std::{ops::Deref, str::FromStr, sync::Arc};

use anyhow::bail;
#[derive(Debug, clap::Parser)]
pub struct EnterpriseServeConfig {
    /// The mode to start the server in
    #[clap(long = "mode", value_enum, default_value_t = BufferMode::ReadWrite, env = "INFLUXDB3_ENTERPRISE_MODE", action)]
    pub mode: BufferMode,

    /// Comma-separated list of writer identifier prefixes, i.e., `writer-id`s to read WAL files from
    ///
    /// Each writer in the list will have its data queryable by this server by checking for new WAL files produced
    /// by that writer on object storage on the interval specified by the `replication-interval` option.
    ///
    /// If `run-compactions` is set to true, this writer list, if provided, will also serve as
    /// the list of writers to compact data from.
    ///
    /// If the reader for any given writer fails to initialize, the server will not start.
    #[clap(
        long = "read-from-writer-ids",
        // TODO: this alias should be deprecated
        alias = "replicas",
        env = "INFLUXDB3_ENTERPRISE_READ_FROM_WRITER_IDS",
        action
    )]
    pub read_from_writer_ids: Option<WriterIdList>,

    /// The interval at which each reader specified in the `read-from-writer-ids` option will be replicated
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

    /// Comma-separated list of writer identifier prefixes to compact data from.
    ///
    /// The compactor will look for new snapshot files from each writer in the list of
    /// `compact-from-writer-ids`. It will compact gen1 file from those writers into a single
    /// compacted view under the `compactor-id` prefix.
    #[clap(
        long = "compact-from-writer-ids",
        // TODO: deprecate this alias
        alias = "compaction-hosts",
        env = "INFLUXDB3_ENTERPRISE_COMPACT_FROM_WRITER_IDS",
        action
    )]
    pub compact_from_writer_ids: Option<WriterIdList>,

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
}

/// Mode of operation for the InfluxDB Pro write buffer
#[derive(Debug, Clone, Copy, Default, clap::ValueEnum)]
#[clap(rename_all = "snake_case")]
pub enum BufferMode {
    /// Will act as a read-replica and only accept queries
    Read,
    /// Can accept writes and serve queries, also with the capability to replicate other buffers
    #[default]
    ReadWrite,
    /// Will act as a compactor only, pulling snapshots from the host list and compacting them
    Compactor,
}

impl BufferMode {
    pub fn is_compactor(&self) -> bool {
        matches!(self, Self::Compactor)
    }
}

impl std::fmt::Display for BufferMode {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            BufferMode::Read => write!(f, "read"),
            BufferMode::ReadWrite => write!(f, "read_write"),
            BufferMode::Compactor => write!(f, "compactor"),
        }
    }
}

#[derive(Debug, Clone)]
pub struct WriterIdList(Vec<String>);

impl From<WriterIdList> for Vec<String> {
    fn from(list: WriterIdList) -> Self {
        list.0
    }
}

impl Deref for WriterIdList {
    type Target = Vec<String>;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl FromStr for WriterIdList {
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
