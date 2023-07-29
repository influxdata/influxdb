use std::{fmt::Display, sync::Arc};

use async_trait::async_trait;
use compactor_scheduler::{
    CompactionJob, CompactionJobEnd, CompactionJobEndVariant, Scheduler, SkipReason,
};

use crate::DynError;

use super::CompactionJobDoneSink;

#[derive(Debug)]
pub struct CompactionJobDoneSinkToScheduler {
    scheduler: Arc<dyn Scheduler>,
}

impl CompactionJobDoneSinkToScheduler {
    pub fn new(scheduler: Arc<dyn Scheduler>) -> Self {
        Self { scheduler }
    }
}

impl Display for CompactionJobDoneSinkToScheduler {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "CompactionJobDoneSinkToScheduler")
    }
}

#[async_trait]
impl CompactionJobDoneSink for CompactionJobDoneSinkToScheduler {
    async fn record(&self, job: CompactionJob, res: Result<(), DynError>) -> Result<(), DynError> {
        let end_action = CompactionJobEnd {
            job,
            end_action: match res {
                Ok(_) => CompactionJobEndVariant::Complete,
                Err(e) => CompactionJobEndVariant::RequestToSkip(SkipReason(e.to_string())),
            },
        };

        self.scheduler.end_job(end_action).await
    }
}
