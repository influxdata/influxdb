pub(crate) mod mock;
pub(crate) mod skipped;

use std::{
    fmt::{Debug, Display},
    sync::Arc,
};

use async_trait::async_trait;
use data_types::PartitionId;

/// A source of partitions, noted by [`PartitionId`](data_types::PartitionId).
///
/// Specifically finds a subset of the given partitions,
/// usually by performing a catalog lookup using the grouped set of partitions.
#[async_trait]
pub(crate) trait PartitionsSubsetSource: Debug + Display + Send + Sync {
    /// Given a set of partitions, return a subset.
    ///
    /// This method performs retries.
    async fn fetch(&self, partitions: &[PartitionId]) -> Vec<PartitionId>;
}

#[async_trait]
impl<T> PartitionsSubsetSource for Arc<T>
where
    T: PartitionsSubsetSource + ?Sized,
{
    async fn fetch(&self, partitions: &[PartitionId]) -> Vec<PartitionId> {
        self.as_ref().fetch(partitions).await
    }
}
