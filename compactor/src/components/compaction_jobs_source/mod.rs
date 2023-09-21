//! Abstractions that provide functionality over a [`CompactionJobsSource`] of compaction jobs.
//!
//! These abstractions are for actions taken in a compactor using the CompactionJobs received from a compactor_scheduler.
pub mod logging;
pub mod metrics;
pub mod mock;
pub mod not_empty;
pub mod randomize_order;
pub mod scheduled;

use std::{
    fmt::{Debug, Display},
    sync::Arc,
};

use async_trait::async_trait;
use compactor_scheduler::CompactionJob;

/// A source of partitions, noted by [`CompactionJob`](compactor_scheduler::CompactionJob), that may potentially need compacting.
#[async_trait]
pub trait CompactionJobsSource: Debug + Display + Send + Sync {
    /// Get compaction jobs. (For now, 1 job equals 1 partition ID).
    ///
    /// This method performs retries.
    ///
    /// This should only perform basic, efficient filtering. It MUST NOT inspect individual parquet files.
    async fn fetch(&self) -> Vec<CompactionJob>;
}

#[async_trait]
impl<T> CompactionJobsSource for Arc<T>
where
    T: CompactionJobsSource + ?Sized,
{
    async fn fetch(&self) -> Vec<CompactionJob> {
        self.as_ref().fetch().await
    }
}
