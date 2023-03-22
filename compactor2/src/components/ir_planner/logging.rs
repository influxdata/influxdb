use std::{fmt::Display, sync::Arc};

use data_types::{CompactionLevel, ParquetFile};
use observability_deps::tracing::info;
use uuid::Uuid;

use crate::{file_classification::FileToSplit, partition_info::PartitionInfo, plan_ir::PlanIR};

use super::IRPlanner;

#[derive(Debug)]
pub struct LoggingIRPlannerWrapper<T>
where
    T: IRPlanner,
{
    inner: T,
}

impl<T> LoggingIRPlannerWrapper<T>
where
    T: IRPlanner,
{
    pub fn new(inner: T) -> Self {
        Self { inner }
    }
}

impl<T> Display for LoggingIRPlannerWrapper<T>
where
    T: IRPlanner,
{
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "logging({})", self.inner)
    }
}

impl<T> IRPlanner for LoggingIRPlannerWrapper<T>
where
    T: IRPlanner,
{
    fn compact_plan(
        &self,
        files: Vec<ParquetFile>,
        object_store_ids: Vec<Uuid>,
        partition: Arc<PartitionInfo>,
        compaction_level: CompactionLevel,
    ) -> PlanIR {
        let partition_id = partition.partition_id;
        let n_input_files = files.len();
        let column_count = partition.column_count();
        let input_file_size_bytes = files.iter().map(|f| f.file_size_bytes).sum::<i64>();
        let plan = self
            .inner
            .compact_plan(files, object_store_ids, partition, compaction_level);

        info!(
            partition_id = partition_id.get(),
            n_input_files,
            column_count,
            input_file_size_bytes,
            n_output_files = plan.n_output_files(),
            compaction_level = compaction_level as i16,
            %plan,
            "created IR compact plan",
        );

        plan
    }

    fn split_plan(
        &self,
        file_to_split: FileToSplit,
        object_store_id: Uuid,
        partition: Arc<PartitionInfo>,
        compaction_level: CompactionLevel,
    ) -> PlanIR {
        let partition_id = partition.partition_id;
        let n_input_files = 1;
        let column_count = partition.column_count();
        let input_file_size_bytes = file_to_split.file.file_size_bytes;
        let plan =
            self.inner
                .split_plan(file_to_split, object_store_id, partition, compaction_level);

        info!(
            partition_id = partition_id.get(),
            n_input_files,
            column_count,
            input_file_size_bytes,
            n_output_files = plan.n_output_files(),
            compaction_level = compaction_level as i16,
            %plan,
            "created IR split plan",
        );

        plan
    }
}
