use std::{fmt::Display, sync::Arc};

use async_trait::async_trait;
use compactor_scheduler::{
    CompactionJob, CompactionJobStatus, CompactionJobStatusResult, CompactionJobStatusVariant,
    PartitionDoneSink, Scheduler, SkipReason,
};
use data_types::PartitionId;

use crate::DynError;

#[derive(Debug)]
pub struct PartitionDoneSinkToScheduler {
    scheduler: Arc<dyn Scheduler>,
}

impl PartitionDoneSinkToScheduler {
    pub fn new(scheduler: Arc<dyn Scheduler>) -> Self {
        Self { scheduler }
    }
}

impl Display for PartitionDoneSinkToScheduler {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "PartitionDoneSinkToScheduler")
    }
}

#[async_trait]
impl PartitionDoneSink for PartitionDoneSinkToScheduler {
    async fn record(&self, partition: PartitionId, res: Result<(), DynError>) {
        let mut job_status = CompactionJobStatus {
            job: CompactionJob::new(partition),
            status: CompactionJobStatusVariant::Complete,
        };
        if let Err(e) = res {
            job_status = CompactionJobStatus {
                job: CompactionJob::new(partition),
                status: CompactionJobStatusVariant::RequestToSkip(SkipReason::CompactionError(
                    e.to_string(),
                )),
            };
        };
        match self.scheduler.job_status(job_status).await {
            Ok(CompactionJobStatusResult::Ack) => {}
            _ => panic!("unexpected result from scheduler"),
        }
    }
}
