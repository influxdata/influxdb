use std::{fmt::Display, sync::Arc};

use async_trait::async_trait;
use data_types::{CompactionLevel, ParquetFile};
use datafusion::{error::DataFusionError, physical_plan::ExecutionPlan};
use observability_deps::tracing::{info, warn};

use crate::partition_info::PartitionInfo;

use super::DataFusionPlanner;

#[derive(Debug)]
pub struct LoggingDataFusionPlannerWrapper<T>
where
    T: DataFusionPlanner,
{
    inner: T,
}

impl<T> LoggingDataFusionPlannerWrapper<T>
where
    T: DataFusionPlanner,
{
    pub fn new(inner: T) -> Self {
        Self { inner }
    }
}

impl<T> Display for LoggingDataFusionPlannerWrapper<T>
where
    T: DataFusionPlanner,
{
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "logging({})", self.inner)
    }
}

#[async_trait]
impl<T> DataFusionPlanner for LoggingDataFusionPlannerWrapper<T>
where
    T: DataFusionPlanner,
{
    async fn plan(
        &self,
        files: Vec<ParquetFile>,
        partition: Arc<PartitionInfo>,
        compaction_level: CompactionLevel,
    ) -> Result<Arc<dyn ExecutionPlan>, DataFusionError> {
        let partition_id = partition.partition_id;
        let n_input_files = files.len();
        let input_file_size_bytes = files.iter().map(|f| f.file_size_bytes).sum::<i64>();
        let res = self.inner.plan(files, partition, compaction_level).await;

        match &res {
            Ok(plan) => {
                info!(
                    partition_id = partition_id.get(),
                    n_input_files,
                    input_file_size_bytes,
                    n_output_files = plan.output_partitioning().partition_count(),
                    compaction_level = compaction_level as i16,
                    "created DataFusion plan",
                );
            }
            Err(e) => {
                warn!(
                    partition_id=partition_id.get(),
                    n_input_files,
                    input_file_size_bytes,
                    compaction_level=compaction_level as i16,
                    %e,
                    "failed to create DataFusion plan",
                )
            }
        }

        res
    }
}
