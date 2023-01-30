use std::fmt::{Debug, Display};

use async_trait::async_trait;
use data_types::{ParquetFile, PartitionId};

pub mod and;
pub mod by_id;
pub mod has_files;
pub mod has_matching_file;
pub mod logging;
pub mod max_files;
pub mod max_parquet_bytes;
pub mod metrics;
pub mod never_skipped;

#[async_trait]
pub trait PartitionFilter: Debug + Display + Send + Sync {
    async fn apply(&self, partition_id: PartitionId, files: &[ParquetFile]) -> bool;
}
