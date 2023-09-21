use std::{
    fmt::{Debug, Display},
    sync::Arc,
};

use datafusion::physical_plan::{ExecutionPlan, SendableRecordBatchStream};

pub mod dedicated;
pub mod noop;

pub trait DataFusionPlanExec: Debug + Display + Send + Sync {
    /// Convert DataFusion [`ExecutionPlan`] to multiple output streams.
    ///
    /// # Stream Polling
    /// These streams *must* to run in parallel otherwise a deadlock
    /// can occur. Since there is a merge in the plan, in order to make
    /// progress on one stream there must be (potential space) on the
    /// other streams.
    ///
    /// See:
    /// - <https://github.com/influxdata/influxdb_iox/issues/4306>
    /// - <https://github.com/influxdata/influxdb_iox/issues/4324>
    fn exec(&self, plan: Arc<dyn ExecutionPlan>) -> Vec<SendableRecordBatchStream>;
}
