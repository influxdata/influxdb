use std::{any::Any, fmt::Display, sync::Arc};

use async_trait::async_trait;
use data_types::{CompactionLevel, ParquetFile};
use datafusion::{
    arrow::datatypes::SchemaRef,
    error::DataFusionError,
    execution::context::TaskContext,
    physical_expr::PhysicalSortExpr,
    physical_plan::{
        stream::RecordBatchStreamAdapter, ExecutionPlan, Partitioning, SendableRecordBatchStream,
        Statistics,
    },
};
use schema::SchemaBuilder;

use crate::partition_info::PartitionInfo;

use super::DataFusionPlanner;

#[derive(Debug, Default)]
pub struct PanicDataFusionPlanner;

impl PanicDataFusionPlanner {
    #[allow(dead_code)] // not used anywhere
    pub fn new() -> Self {
        Self::default()
    }
}

impl Display for PanicDataFusionPlanner {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "panic")
    }
}

#[async_trait]
impl DataFusionPlanner for PanicDataFusionPlanner {
    async fn plan(
        &self,
        _files: Vec<ParquetFile>,
        _partition: Arc<PartitionInfo>,
        _compaction_level: CompactionLevel,
    ) -> Result<Arc<dyn ExecutionPlan>, DataFusionError> {
        Ok(Arc::new(PanicPlan))
    }
}

#[derive(Debug)]
pub struct PanicPlan;

impl ExecutionPlan for PanicPlan {
    fn as_any(&self) -> &dyn Any {
        self as _
    }

    fn schema(&self) -> SchemaRef {
        SchemaBuilder::new().build().unwrap().as_arrow()
    }

    fn output_partitioning(&self) -> Partitioning {
        Partitioning::UnknownPartitioning(1)
    }

    fn output_ordering(&self) -> Option<&[PhysicalSortExpr]> {
        None
    }

    fn children(&self) -> Vec<Arc<dyn ExecutionPlan>> {
        vec![]
    }

    fn with_new_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> datafusion::error::Result<Arc<dyn ExecutionPlan>> {
        assert!(children.is_empty());
        Ok(self)
    }

    fn execute(
        &self,
        partition: usize,
        _context: Arc<TaskContext>,
    ) -> datafusion::error::Result<SendableRecordBatchStream> {
        assert_eq!(partition, 0);
        let stream = futures::stream::once(async move { panic!("foo") });
        let stream = RecordBatchStreamAdapter::new(self.schema(), stream);
        Ok(Box::pin(stream))
    }

    fn statistics(&self) -> Statistics {
        unimplemented!()
    }
}

#[cfg(test)]
mod tests {
    use datafusion::{physical_plan::collect, prelude::SessionContext};

    use crate::test_util::partition_info;

    use super::*;

    #[test]
    fn test_display() {
        assert_eq!(PanicDataFusionPlanner::new().to_string(), "panic");
    }

    #[tokio::test]
    #[should_panic(expected = "foo")]
    async fn test_panic() {
        let planner = PanicDataFusionPlanner::new();
        let partition = partition_info();
        let plan = planner
            .plan(vec![], partition, CompactionLevel::FileNonOverlapped)
            .await
            .unwrap();

        let session_ctx = SessionContext::new();
        let task_ctx = Arc::new(TaskContext::from(&session_ctx));
        collect(plan, task_ctx).await.ok();
    }
}
