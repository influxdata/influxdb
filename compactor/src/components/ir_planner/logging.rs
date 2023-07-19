use std::{fmt::Display, sync::Arc};

use data_types::{CompactionLevel, ParquetFile};
use observability_deps::tracing::info;
use parquet_file::ParquetFilePath;
use uuid::Uuid;

use crate::{
    file_classification::{CompactReason, FileToSplit, FilesToSplitOrCompact, SplitReason},
    partition_info::PartitionInfo,
    plan_ir::PlanIR,
};

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
    fn create_plans(
        &self,
        partition: Arc<PartitionInfo>,
        target_level: CompactionLevel,
        split_or_compact: FilesToSplitOrCompact,
        object_store_ids: Vec<Uuid>,
        object_store_paths: Vec<ParquetFilePath>,
    ) -> Vec<PlanIR> {
        self.inner.create_plans(
            partition,
            target_level,
            split_or_compact,
            object_store_ids,
            object_store_paths,
        )
    }

    fn compact_plan(
        &self,
        files: Vec<ParquetFile>,
        object_store_paths: Vec<ParquetFilePath>,
        object_store_ids: Vec<Uuid>,
        reason: CompactReason,
        partition: Arc<PartitionInfo>,
        compaction_level: CompactionLevel,
    ) -> PlanIR {
        let partition_id = partition.partition_id;
        let n_input_files = files.len();
        let column_count = partition.column_count();
        let input_file_size_bytes = files.iter().map(|f| f.file_size_bytes).sum::<i64>();
        let plan = self.inner.compact_plan(
            files,
            object_store_paths,
            object_store_ids,
            reason,
            partition,
            compaction_level,
        );

        info!(
            partition_id = partition_id.get(),
            n_input_files,
            column_count,
            input_file_size_bytes,
            n_output_files = plan.n_output_files(),
            compaction_level = compaction_level as i16,
            ?reason,
            %plan,
            "created IR compact plan",
        );

        plan
    }

    fn split_plan(
        &self,
        file_to_split: FileToSplit,
        object_store_path: ParquetFilePath,
        object_store_id: Uuid,
        reason: SplitReason,
        partition: Arc<PartitionInfo>,
        compaction_level: CompactionLevel,
    ) -> PlanIR {
        let partition_id = partition.partition_id;
        let n_input_files = 1;
        let column_count = partition.column_count();
        let input_file_size_bytes = file_to_split.file.file_size_bytes;
        let plan = self.inner.split_plan(
            file_to_split,
            object_store_path,
            object_store_id,
            reason,
            partition,
            compaction_level,
        );

        info!(
            partition_id = partition_id.get(),
            n_input_files,
            column_count,
            input_file_size_bytes,
            n_output_files = plan.n_output_files(),
            compaction_level = compaction_level as i16,
            ?reason,
            %plan,
            "created IR split plan",
        );

        plan
    }
}
