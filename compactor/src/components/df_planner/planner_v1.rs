use std::{fmt::Display, sync::Arc};

use async_trait::async_trait;
use datafusion::{error::DataFusionError, physical_plan::ExecutionPlan};
use iox_query::{
    exec::{Executor, ExecutorType},
    frontend::reorg::ReorgPlanner,
};
use parquet_file::storage::ParquetStorage;

use crate::{
    components::df_planner::query_chunk::{to_query_chunks, QueryableParquetChunk},
    partition_info::PartitionInfo,
    plan_ir::PlanIR,
};

use super::DataFusionPlanner;

/// Builder for compaction plans.
///
/// This uses the first draft / version of how the compactor splits files / time ranges. There will probably future
/// implementations (maybe called V2, but maybe it also gets a proper name).
#[derive(Debug)]
pub struct V1DataFusionPlanner {
    store: ParquetStorage,
    exec: Arc<Executor>,
}

impl V1DataFusionPlanner {
    /// Create a new compact plan builder.
    pub fn new(store: ParquetStorage, exec: Arc<Executor>) -> Self {
        Self { store, exec }
    }
}

impl Display for V1DataFusionPlanner {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "v1")
    }
}

#[async_trait]
impl DataFusionPlanner for V1DataFusionPlanner {
    async fn plan(
        &self,
        ir: &PlanIR,
        partition: Arc<PartitionInfo>,
    ) -> Result<Arc<dyn ExecutionPlan>, DataFusionError> {
        let ctx = self.exec.new_context(ExecutorType::Reorg);

        let plan = match ir {
            PlanIR::None { .. } => unreachable!("filter out None plans before calling plan"),
            PlanIR::Compact { files, .. } => {
                let query_chunks = to_query_chunks(files, &partition, self.store.clone());
                let merged_schema = QueryableParquetChunk::merge_schemas(&query_chunks);
                let sort_key = partition
                    .sort_key
                    .as_ref()
                    .expect("no partition sort key in catalog")
                    .filter_to(&merged_schema.primary_key(), partition.partition_id.get());

                ReorgPlanner::new()
                    .compact_plan(
                        Arc::from(partition.table.name.clone()),
                        &merged_schema,
                        query_chunks,
                        sort_key,
                    )
                    .map_err(|e| {
                        DataFusionError::Context(
                            String::from("planner"),
                            Box::new(DataFusionError::External(Box::new(e))),
                        )
                    })?
            }
            PlanIR::Split {
                files, split_times, ..
            } => {
                let query_chunks = to_query_chunks(files, &partition, self.store.clone());
                let merged_schema = QueryableParquetChunk::merge_schemas(&query_chunks);
                let sort_key = partition
                    .sort_key
                    .as_ref()
                    .expect("no partition sort key in catalog")
                    .filter_to(&merged_schema.primary_key(), partition.partition_id.get());

                ReorgPlanner::new()
                    .split_plan(
                        Arc::from(partition.table.name.clone()),
                        &merged_schema,
                        query_chunks,
                        sort_key,
                        split_times.clone(),
                    )
                    .map_err(|e| {
                        DataFusionError::Context(
                            String::from("planner"),
                            Box::new(DataFusionError::External(Box::new(e))),
                        )
                    })?
            }
        };

        // Build physical compact plan
        ctx.create_physical_plan(&plan).await.map_err(|e| {
            DataFusionError::Context(
                String::from("planner"),
                Box::new(DataFusionError::External(Box::new(e))),
            )
        })
    }
}
