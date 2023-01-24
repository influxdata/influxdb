use std::{
    fmt::{Debug, Display},
    sync::Arc,
};

use async_trait::async_trait;
use data_types::PartitionId;

pub mod catalog;
pub mod kind;
pub mod logging;
pub mod metrics;
pub mod mock;

#[async_trait]
pub trait PartitionErrorSink: Debug + Display + Send + Sync {
    /// Record error for given partition.
    ///
    /// This method should retry.
    async fn record(&self, partition: PartitionId, e: Box<dyn std::error::Error + Send + Sync>);
}

#[async_trait]
impl<T> PartitionErrorSink for Arc<T>
where
    T: PartitionErrorSink,
{
    async fn record(&self, partition: PartitionId, e: Box<dyn std::error::Error + Send + Sync>) {
        self.as_ref().record(partition, e).await
    }
}
