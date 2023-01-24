use std::fmt::{Debug, Display};

use async_trait::async_trait;
use data_types::{PartitionId, SkippedCompaction};

pub mod catalog;
pub mod mock;

#[async_trait]
pub trait SkippedCompactionsSource: Debug + Display + Send + Sync {
    /// Get skipped partition recrod for a given partition
    ///
    /// This method performs retries.
    async fn fetch(&self, partition: PartitionId) -> Option<SkippedCompaction>;
}
