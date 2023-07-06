use std::sync::Arc;

use async_trait::async_trait;
use compactor_scheduler::{
    Commit, CommitUpdate, CompactionJob, CompactionJobStatus, CompactionJobStatusResult,
    CompactionJobStatusVariant, Scheduler,
};
use data_types::{CompactionLevel, ParquetFile, ParquetFileId, ParquetFileParams, PartitionId};

#[derive(Debug)]
pub(crate) struct CommitToScheduler {
    scheduler: Arc<dyn Scheduler>,
}

impl CommitToScheduler {
    pub(crate) fn new(scheduler: Arc<dyn Scheduler>) -> Self {
        Self { scheduler }
    }
}

#[async_trait]
impl Commit for CommitToScheduler {
    async fn commit(
        &self,
        partition_id: PartitionId,
        delete: &[ParquetFile],
        upgrade: &[ParquetFile],
        create: &[ParquetFileParams],
        target_level: CompactionLevel,
    ) -> Vec<ParquetFileId> {
        match self
            .scheduler
            .job_status(CompactionJobStatus {
                job: CompactionJob::new(partition_id),
                status: CompactionJobStatusVariant::Update(CommitUpdate::new(
                    partition_id,
                    delete.into(),
                    upgrade.into(),
                    create.into(),
                    target_level,
                )),
            })
            .await
        {
            Ok(CompactionJobStatusResult::UpdatedParquetFiles(ids)) => ids,
            _ => panic!("commit failed"),
        }
    }
}

impl std::fmt::Display for CommitToScheduler {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "CommitToScheduler")
    }
}
