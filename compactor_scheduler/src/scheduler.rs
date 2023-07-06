use std::{
    fmt::{Debug, Display},
    sync::Arc,
};

use async_trait::async_trait;
use data_types::{CompactionLevel, ParquetFile, ParquetFileId, ParquetFileParams, PartitionId};
use uuid::Uuid;

use crate::{CommitWrapper, ErrorKind, LocalSchedulerConfig, PartitionsSourceConfig};

/// Scheduler configuration.
#[derive(Debug, Clone)]
pub enum SchedulerConfig {
    /// Configuration specific to the [`LocalScheduler`](crate::LocalScheduler).
    Local(LocalSchedulerConfig),
}

impl SchedulerConfig {
    /// Create new [`LocalScheduler`](crate::LocalScheduler) config with a [`CommitWrapper`].
    ///
    /// This is useful for testing.
    pub fn new_local_with_wrapper(commit_wrapper: Arc<dyn CommitWrapper>) -> Self {
        Self::Local(LocalSchedulerConfig {
            shard_config: None,
            partitions_source_config: PartitionsSourceConfig::default(),
            commit_wrapper: Some(commit_wrapper),
        })
    }
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
                commit_wrapper,
                shard_config,
                partitions_source_config: _,
            }) => match (&shard_config, commit_wrapper) {
                (None, None) => write!(f, "local_compaction_scheduler_cfg"),
                (Some(shard_config), None) => {
                    write!(f, "local_compaction_scheduler_cfg({shard_config})",)
                }
                (Some(shard_config), Some(_)) => write!(
                    f,
                    "local_compaction_scheduler_cfg({shard_config},commit_wrapper=Some)",
                ),
                (None, Some(_)) => {
                    write!(f, "local_compaction_scheduler_cfg(commit_wrapper=Some)",)
                }
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

/// Commit update for a given partition.
#[derive(Debug)]
pub struct CommitUpdate {
    /// Partition to be updated.
    pub(crate) partition_id: PartitionId,
    /// Files to be deleted.
    pub(crate) delete: Vec<ParquetFile>,
    /// Files to be upgraded.
    pub(crate) upgrade: Vec<ParquetFile>,
    /// Target level for upgraded files.
    pub(crate) target_level: CompactionLevel,
    /// Files to be created.
    pub(crate) create: Vec<ParquetFileParams>,
}

impl CommitUpdate {
    /// Create new commit update.
    pub fn new(
        partition_id: PartitionId,
        delete: Vec<ParquetFile>,
        upgrade: Vec<ParquetFile>,
        create: Vec<ParquetFileParams>,
        target_level: CompactionLevel,
    ) -> Self {
        Self {
            partition_id,
            delete,
            upgrade,
            target_level,
            create,
        }
    }
}

/// Reason for skipping a partition.
#[derive(Debug)]
pub enum SkipReason {
    /// Partition is not compactible, due to an encountered error.
    CompactionError(String),
}

/// Status.
#[derive(Debug)]
pub enum CompactionJobStatusVariant {
    /// Updates associated with ongoing compaction job.
    Update(CommitUpdate),
    /// Request to skip partition.
    RequestToSkip(SkipReason),
    /// Compaction job is complete.
    Complete,
    /// Compaction job has failed.
    Error(ErrorKind),
}

/// Status ([`CompactionJobStatusVariant`]) associated with a [`CompactionJob`].
#[derive(Debug)]
pub struct CompactionJobStatus {
    /// Job.
    pub job: CompactionJob,
    /// Status.
    pub status: CompactionJobStatusVariant,
}

/// Status of a compaction job.
#[derive(Debug)]
pub enum CompactionJobStatusResult {
    /// Ack only.
    Ack,
    /// Updates which were processed.
    UpdatedParquetFiles(Vec<ParquetFileId>),
}

/// Core trait used for all schedulers.
#[async_trait]
pub trait Scheduler: Send + Sync + Debug + Display {
    /// Get partitions to be compacted.
    async fn get_jobs(&self) -> Vec<CompactionJob>;

    /// Update job status.
    async fn job_status(
        &self,
        job_status: CompactionJobStatus,
    ) -> Result<CompactionJobStatusResult, Box<dyn std::error::Error>>;
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use crate::Commit;

    use super::*;

    #[test]
    fn test_cfg_display_new_local_with_wrapper() {
        #[derive(Debug)]
        struct MockCommitWrapper;

        impl CommitWrapper for MockCommitWrapper {
            fn wrap(&self, commit: Arc<dyn Commit>) -> Arc<dyn Commit> {
                commit
            }
        }

        let config = SchedulerConfig::new_local_with_wrapper(Arc::new(MockCommitWrapper));

        assert_eq!(
            config.to_string(),
            "local_compaction_scheduler_cfg(commit_wrapper=Some)"
        );
    }
}
