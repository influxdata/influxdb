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
    use crate::components::df_planner::panic::PanicPlan;

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
        assert_eq!(
            err.to_string(),
            "Join Error (panic)\ncaused by\nExternal error: foo"
        );
    }
}
