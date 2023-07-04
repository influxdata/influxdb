use std::fmt::{Debug, Display};

use async_trait::async_trait;
use data_types::PartitionId;
use uuid::Uuid;

use crate::LocalSchedulerConfig;

/// Scheduler configuration.
#[derive(Debug, Clone)]
pub enum SchedulerConfig {
    /// Configuration specific to the [`LocalScheduler`](crate::LocalScheduler).
    Local(LocalSchedulerConfig),
}

impl Default for SchedulerConfig {
    fn default() -> Self {
        Self::Local(LocalSchedulerConfig::default())
    }
}

impl std::fmt::Display for SchedulerConfig {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            SchedulerConfig::Local(LocalSchedulerConfig {
                shard_config,
                partitions_source_config: _,
            }) => match &shard_config {
                None => write!(f, "local_compaction_scheduler"),
                Some(shard_config) => write!(f, "local_compaction_scheduler({shard_config})",),
            },
        }
    }
}

/// Job assignment for a given partition.
#[derive(Debug)]
pub struct CompactionJob {
    #[allow(dead_code)]
    /// Unique identifier for this job.
    /// Should not be the same as the partition id.
    uuid: Uuid,
    /// Leased partition.
    pub partition_id: PartitionId,
}

impl CompactionJob {
    /// Create new job.
    pub fn new(partition_id: PartitionId) -> Self {
        Self {
            uuid: Uuid::new_v4(),
            partition_id,
        }
    }
}

/// Core trait used for all schedulers.
#[async_trait]
pub trait Scheduler: Send + Sync + Debug + Display {
    /// Get partitions to be compacted.
    async fn get_jobs(&self) -> Vec<CompactionJob>;
}
