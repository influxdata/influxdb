use std::{fmt::Display, sync::Arc};

use datafusion::physical_plan::{
    stream::RecordBatchStreamAdapter, ExecutionPlan, SendableRecordBatchStream,
};

use super::DataFusionPlanExec;

/// Creates a DataFusion plan that does nothing (for use in testing)
#[derive(Debug, Default)]
pub struct NoopDataFusionPlanExec;

impl NoopDataFusionPlanExec {
    pub fn new() -> Self {
        Self
    }
}

impl Display for NoopDataFusionPlanExec {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "noop")
    }
}

impl DataFusionPlanExec for NoopDataFusionPlanExec {
    fn exec(&self, plan: Arc<dyn ExecutionPlan>) -> Vec<SendableRecordBatchStream> {
        let stream_count = plan.output_partitioning().partition_count();
        let schema = plan.schema();

        (0..stream_count)
            .map(|_| {
                let stream = futures::stream::empty();
                let stream = RecordBatchStreamAdapter::new(Arc::clone(&schema), stream);
                Box::pin(stream) as SendableRecordBatchStream
            })
            .collect()
    }
}
