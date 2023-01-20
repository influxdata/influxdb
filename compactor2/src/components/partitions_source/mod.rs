use std::fmt::{Debug, Display};

use async_trait::async_trait;
use data_types::{Partition, PartitionId};

pub mod catalog;
pub mod logging;
pub mod metrics;
pub mod mock;
pub mod randomize_order;

/// A source of [partitions](PartitionId) that may potentially need compacting.
#[async_trait]
pub trait PartitionsSource: Debug + Display + Send + Sync {
    /// Get partition IDs.
    ///
    /// This method performs retries.
    ///
    /// This should only perform basic, efficient filtering. It MUST NOT inspect individual parquet files.
    async fn fetch(&self) -> Vec<PartitionId>;

    /// Get partition for a given partition ID.
    ///
    /// This method performs retries.
    async fn fetch_by_id(&self, partition_id: PartitionId) -> Option<Partition>;
}
