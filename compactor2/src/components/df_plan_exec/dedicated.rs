use std::{fmt::Display, sync::Arc};

use datafusion::physical_plan::{
    stream::RecordBatchStreamAdapter, ExecutionPlan, SendableRecordBatchStream,
};
use futures::TryStreamExt;
use iox_query::exec::{Executor, ExecutorType};

use super::DataFusionPlanExec;

#[derive(Debug)]
pub struct DedicatedDataFusionPlanExec {
    exec: Arc<Executor>,
}

impl DedicatedDataFusionPlanExec {
    pub fn new(exec: Arc<Executor>) -> Self {
        Self { exec }
    }
}

impl Display for DedicatedDataFusionPlanExec {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "dedicated")
    }
}

impl DataFusionPlanExec for DedicatedDataFusionPlanExec {
    fn exec(&self, plan: Arc<dyn ExecutionPlan>) -> Vec<SendableRecordBatchStream> {
        let stream_count = plan.output_partitioning().partition_count();
        let schema = plan.schema();
        let ctx = self.exec.new_context(ExecutorType::Reorg);

        (0..stream_count)
            .map(|i| {
                let plan = Arc::clone(&plan);
                let ctx = ctx.child_ctx("partition");

                let stream =
                    futures::stream::once(
                        async move { ctx.execute_stream_partitioned(plan, i).await },
                    )
                    .try_flatten();
                let stream = RecordBatchStreamAdapter::new(Arc::clone(&schema), stream);
                Box::pin(stream) as SendableRecordBatchStream
            })
            .collect()
    }
}

#[cfg(test)]
mod tests {
    use std::any::Any;

    use datafusion::{
        arrow::datatypes::SchemaRef,
        execution::context::TaskContext,
        physical_expr::PhysicalSortExpr,
        physical_plan::{Partitioning, Statistics},
    };
    use schema::SchemaBuilder;

    use super::*;

    #[test]
    fn test_display() {
        let exec = DedicatedDataFusionPlanExec::new(Arc::new(Executor::new_testing()));
        assert_eq!(exec.to_string(), "dedicated");
    }

    #[tokio::test]
    async fn test_panic() {
        let exec = DedicatedDataFusionPlanExec::new(Arc::new(Executor::new_testing()));
        let mut streams = exec.exec(Arc::new(PanicPlan));
        assert_eq!(streams.len(), 1);
        let stream = streams.pop().unwrap();
        let err = stream.try_collect::<Vec<_>>().await.unwrap_err();
        assert_eq!(err.to_string(), "External error: foo");
    }

    #[derive(Debug)]
    struct PanicPlan;

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
}
