use std::{
    fmt::{Debug, Display},
    sync::Arc,
};

use async_trait::async_trait;
use compactor_scheduler::CompactionJob;

use crate::DynError;

pub mod error_kind;
pub mod logging;
pub mod metrics;
pub mod mock;
pub mod outcome;

/// Records "compaction job is done" status for given partition.
#[async_trait]
pub trait CompactionJobDoneSink: Debug + Display + Send + Sync {
    /// Record "compaction job is done" status for given partition.
    ///
    /// This method should retry.
    async fn record(&self, job: CompactionJob, res: Result<(), DynError>) -> Result<(), DynError>;
}

#[async_trait]
impl<T> CompactionJobDoneSink for Arc<T>
where
    T: CompactionJobDoneSink + ?Sized,
{
    async fn record(&self, job: CompactionJob, res: Result<(), DynError>) -> Result<(), DynError> {
        self.as_ref().record(job, res).await
    }
}
