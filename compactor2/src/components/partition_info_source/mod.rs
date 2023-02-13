use std::{
    fmt::{Debug, Display},
    sync::Arc,
};

use async_trait::async_trait;
use data_types::PartitionId;

use crate::{error::DynError, partition_info::PartitionInfo};

pub mod sub_sources;

/// Fetches the subset of information about a partition neededed for compaction
#[async_trait]
pub trait PartitionInfoSource: Debug + Display + Send + Sync {
    async fn fetch(&self, partition_id: PartitionId) -> Result<Arc<PartitionInfo>, DynError>;
}
