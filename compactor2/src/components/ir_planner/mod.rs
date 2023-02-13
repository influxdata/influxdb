use std::{
    fmt::{Debug, Display},
    sync::Arc,
};

use data_types::{CompactionLevel, ParquetFile};

pub mod logging;
pub mod planner_v1;

use crate::{partition_info::PartitionInfo, plan_ir::PlanIR};

/// Creates [`PlanIR`] that describes what files should be compacted and updated
pub trait IRPlanner: Debug + Display + Send + Sync {
    fn plan(
        &self,
        files: Vec<ParquetFile>,
        partition: Arc<PartitionInfo>,
        compaction_level: CompactionLevel,
    ) -> PlanIR;
}
