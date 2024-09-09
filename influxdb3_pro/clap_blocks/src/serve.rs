//! Extend the `influxdb3 serve` command for InfluxDB Pro

use std::{ops::Deref, str::FromStr};

use anyhow::bail;
#[derive(Debug, clap::Parser)]
pub struct ProServeConfig {
    /// The mode to start the server in
    #[clap(long = "mode", value_enum, default_value_t = BufferMode::ReadWrite, env = "INFLUXDB3_PRO_MODE", action)]
    pub mode: BufferMode,

    /// Comma-separated list of host identifier prefixes to replicate
    ///
    /// Each host in the list will have its buffer replicated by checking for new WAL files produced
    /// by that host on object storage on the interval specified by the `replication-interval` option.
    ///
    /// If the replica for any given host fails to initialize, the server will not start.
    #[clap(long = "replicas", env = "INFLUXDB3_PRO_REPLICAS", action)]
    pub replicas: Option<ReplicaList>,

    /// The interval at which each replica specified in the `replicas` option will be replicated
    #[clap(
        long = "replication-interval",
        env = "INFLUXDB3_PRO_REPLICATION_INTERVAL",
        default_value = "250ms",
        action
    )]
    pub replication_interval: humantime::Duration,
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
}

#[derive(Debug, Clone)]
pub struct ReplicaList(Vec<String>);

impl From<ReplicaList> for Vec<String> {
    fn from(list: ReplicaList) -> Self {
        list.0
    }
}

impl Deref for ReplicaList {
    type Target = Vec<String>;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl FromStr for ReplicaList {
    type Err = anyhow::Error;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let replicas: Vec<String> = s.split(',').map(|s| s.to_owned()).collect();
        if replicas.is_empty() {
            bail!("list of replicas cannot be empty")
        }
        Ok(Self(replicas))
    }
}
