use std::fmt::{Debug, Display};

use async_trait::async_trait;
use data_types::{Partition, PartitionId};

pub mod catalog;
pub mod logging;
pub mod metrics;
pub mod mock;

/// A source of [partition](Partition) that may potentially need compacting.
#[async_trait]
pub trait PartitionSource: Debug + Display + Send + Sync {
    /// Get partition for a given partition ID.
    ///
    /// This method performs retries.
    async fn fetch_by_id(&self, partition_id: PartitionId) -> Option<Partition>;
}
