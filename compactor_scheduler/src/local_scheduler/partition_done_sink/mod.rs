pub(crate) mod catalog;
pub(crate) mod mock;

use std::{
    fmt::{Debug, Display},
    sync::Arc,
};

use async_trait::async_trait;
use data_types::PartitionId;

/// Dynamic error type that is provided from the Compactor.
pub(crate) type DynError = Box<dyn std::error::Error + Send + Sync>;

/// Error returned by [`PartitionDoneSink`] implementations.
#[derive(Debug, thiserror::Error)]
pub(crate) enum Error {
    /// PartitionDoneSink failed during connection with catalog
    #[error("Failure during catalog communication: {0}")]
    Catalog(String),

    /// PartitionDoneSink failed because of an error in the unique partitions source wrapper
    #[error("Failure in unique_partitions: {0}")]
    UniquePartitions(#[from] crate::UniquePartitionsError),

    /// PartitionDoneSink failed because of an error in the throttler
    #[error("Failure in throttler: {0}")]
    Throttler(#[from] crate::ThrottleError),
}

/// Records "partition is done" status for given partition.
#[async_trait]
pub(crate) trait PartitionDoneSink: Debug + Display + Send + Sync {
    /// Record "partition is done" status for given partition.
    ///
    /// This method should retry.
    async fn record(&self, partition: PartitionId, res: Result<(), DynError>) -> Result<(), Error>;
}

#[async_trait]
impl<T> PartitionDoneSink for Arc<T>
where
    T: PartitionDoneSink + ?Sized,
{
    async fn record(&self, partition: PartitionId, res: Result<(), DynError>) -> Result<(), Error> {
        self.as_ref().record(partition, res).await
    }
}
