use std::{
    fmt::{Debug, Display},
    sync::Arc,
};

use async_trait::async_trait;
use datafusion::{error::DataFusionError, physical_plan::ExecutionPlan};

pub mod panic;
pub mod planner_v1;
mod query_chunk;

use crate::{partition_info::PartitionInfo, plan_ir::PlanIR};

#[async_trait]
pub trait DataFusionPlanner: Debug + Display + Send + Sync {
    async fn plan(
        &self,
        ir: &PlanIR,
        partition: Arc<PartitionInfo>,
    ) -> Result<Arc<dyn ExecutionPlan>, DataFusionError>;
}
