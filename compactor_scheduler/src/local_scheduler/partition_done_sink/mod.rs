pub(crate) mod catalog;
pub(crate) mod mock;

use std::{
    fmt::{Debug, Display},
    sync::Arc,
};

use async_trait::async_trait;
use data_types::PartitionId;

/// Dynamic error type that is used throughout the stack.
pub(crate) type DynError = Box<dyn std::error::Error + Send + Sync>;

/// Records "partition is done" status for given partition.
#[async_trait]
pub trait PartitionDoneSink: Debug + Display + Send + Sync {
    /// Record "partition is done" status for given partition.
    ///
    /// This method should retry.
    async fn record(&self, partition: PartitionId, res: Result<(), DynError>);
}

#[async_trait]
impl<T> PartitionDoneSink for Arc<T>
where
    T: PartitionDoneSink + ?Sized,
{
    async fn record(&self, partition: PartitionId, res: Result<(), DynError>) {
        self.as_ref().record(partition, res).await
    }
}
