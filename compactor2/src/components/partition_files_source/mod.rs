use std::fmt::{Debug, Display};

use async_trait::async_trait;
use data_types::{ParquetFile, PartitionId};

pub mod catalog;
pub mod mock;

#[async_trait]
pub trait PartitionFilesSource: Debug + Display + Send + Sync {
    /// Get undeleted parquet files for given partition.
    ///
    /// This MUST NOT perform any filtering (expect for the "not marked for deletion" flag).
    ///
    /// This method performs retries.
    async fn fetch(&self, partition: PartitionId) -> Vec<ParquetFile>;
}
