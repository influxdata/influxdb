use std::{fmt::Display, sync::Arc};

use async_trait::async_trait;
use compactor_scheduler::{
    CompactionJob, CompactionJobEnd, CompactionJobEndVariant, Scheduler, SkipReason,
};
use data_types::PartitionId;

use crate::DynError;

use super::PartitionDoneSink;

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
    async fn record(
        &self,
        partition: PartitionId,
        res: Result<(), DynError>,
    ) -> Result<(), DynError> {
        let end_action = CompactionJobEnd {
            job: CompactionJob::new(partition),
            end_action: match res {
                Ok(_) => CompactionJobEndVariant::Complete,
                Err(e) => CompactionJobEndVariant::RequestToSkip(SkipReason(e.to_string())),
            },
        };

        self.scheduler.end_job(end_action).await
    }
}
