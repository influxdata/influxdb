use std::fmt::{Debug, Display};

use data_types::PartitionId;

pub mod and;
pub mod by_id;
pub mod shard;

/// Filters partition based on ID.
///
/// This will usually be used BEFORE any parquet files for the given partition are fetched and hence is a quite
/// efficient filter stage.
///
/// If you need to inspect the files as well or perform any IO, check
/// [`PartitionFilter`](crate::components::partition_filter::PartitionFilter).
pub trait IdOnlyPartitionFilter: Debug + Display + Send + Sync {
    fn apply(&self, partition_id: PartitionId) -> bool;
}
