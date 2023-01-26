use std::{
    fmt::{Debug, Display},
    sync::Arc,
};

use async_trait::async_trait;
use data_types::{CompactionLevel, ParquetFile};
use datafusion::{error::DataFusionError, physical_plan::ExecutionPlan};

pub mod logging;
pub mod panic;
pub mod planner_v1;
mod query_chunk;

use crate::partition_info::PartitionInfo;

#[async_trait]
pub trait DataFusionPlanner: Debug + Display + Send + Sync {
    async fn plan(
        &self,
        files: Vec<ParquetFile>,
        partition: Arc<PartitionInfo>,
        compaction_level: CompactionLevel,
    ) -> Result<Arc<dyn ExecutionPlan>, DataFusionError>;
}
