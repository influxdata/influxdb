use std::{
    fmt::{Debug, Display},
    sync::Arc,
};

use data_types::{CompactionLevel, ParquetFile};
use uuid::Uuid;

pub mod logging;
pub mod planner_v1;

use crate::{
    file_classification::{FileToSplit, FilesToSplitOrCompact},
    partition_info::PartitionInfo,
    plan_ir::PlanIR,
};

/// Creates [`PlanIR`] that describes what files should be compacted and updated
pub trait IRPlanner: Debug + Display + Send + Sync {
    /// Build compact or split plans as appropriate
    fn create_plans(
        &self,
        partition: Arc<PartitionInfo>,
        target_level: CompactionLevel,
        split_or_compact: FilesToSplitOrCompact,
        object_store_ids: Vec<Uuid>,
    ) -> Vec<PlanIR>;

    /// Build a plan to compact give files
    fn compact_plan(
        &self,
        files: Vec<ParquetFile>,
        object_store_ids: Vec<Uuid>,
        partition: Arc<PartitionInfo>,
        target_level: CompactionLevel,
    ) -> PlanIR;

    /// Build a plan to split a given file into given split times
    fn split_plan(
        &self,
        file_to_split: FileToSplit,
        object_store_id: Uuid,
        partition: Arc<PartitionInfo>,
        target_level: CompactionLevel,
    ) -> PlanIR;
}
