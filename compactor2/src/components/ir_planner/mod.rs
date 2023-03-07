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
    /// Build a plan to compact give files
    fn compact_plan(
        &self,
        files: Vec<ParquetFile>,
        partition: Arc<PartitionInfo>,
        compaction_level: CompactionLevel,
    ) -> PlanIR;

    /// Build a plan to split a given file into given split times
    fn split_plan(
        &self,
        file: ParquetFile,
        split_times: Vec<i64>,
        partition: Arc<PartitionInfo>,
        compaction_level: CompactionLevel,
    ) -> PlanIR;
}
