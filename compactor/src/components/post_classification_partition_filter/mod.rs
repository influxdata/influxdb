use std::fmt::{Debug, Display};

use async_trait::async_trait;

use crate::{error::DynError, file_classification::FilesForProgress, PartitionInfo};

pub mod logging;
pub mod metrics;
pub mod mock;
pub mod possible_progress;

/// Filters partition based on ID and Parquet files after the files have been classified.
///
/// May return an error. In this case, the partition will be marked as "skipped".
#[async_trait]
pub trait PostClassificationPartitionFilter: Debug + Display + Send + Sync {
    /// Return `true` if the compactor should run a compaction on this partition. Return `false`
    /// if this partition does not need any more compaction.
    async fn apply(
        &self,
        partition_info: &PartitionInfo,
        files_to_make_progress_on: &FilesForProgress,
    ) -> Result<bool, DynError>;
}
