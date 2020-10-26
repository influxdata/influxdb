//! This module contains plumbing to connect the Delorean extensions to DataFusion

use std::sync::Arc;

use delorean_arrow::{
    arrow::record_batch::RecordBatch,
    arrow::record_batch::RecordBatchReader,
    datafusion::physical_plan::common::RecordBatchIterator,
    datafusion::physical_plan::merge::MergeExec,
    datafusion::{
        execution::context::ExecutionContextState,
        execution::context::QueryPlanner,
        logical_plan::LogicalPlan,
        logical_plan::UserDefinedLogicalNode,
        physical_plan::{
            planner::{DefaultPhysicalPlanner, ExtensionPlanner},
            ExecutionPlan, PhysicalPlanner,
        },
        prelude::{ExecutionConfig, ExecutionContext},
    },
};

use crate::exec::schema_pivot::{SchemaPivotExec, SchemaPivotNode};
use crate::util::dump_plan;

use tracing::debug;

// Reuse DataFusion error and Result types for this module
pub use delorean_arrow::datafusion::error::{ExecutionError as Error, Result};

use super::counters::ExecutionCounters;

struct DeloreanQueryPlanner {}

impl QueryPlanner for DeloreanQueryPlanner {
    fn rewrite_logical_plan(&self, plan: LogicalPlan) -> Result<LogicalPlan> {
        // TODO: implement any Delorean specific query rewrites needed
        Ok(plan)
    }

    /// Given a `LogicalPlan` created from above, create an
    /// `ExecutionPlan` suitable for execution
    fn create_physical_plan(
        &self,
        logical_plan: &LogicalPlan,
        ctx_state: &ExecutionContextState,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        // Teach the default physical planner how to plan SchemaPivot nodes.
        let physical_planner =
            DefaultPhysicalPlanner::with_extension_planner(Arc::new(DeloreanExtensionPlanner {}));
        // Delegate most work of physical planning to the default physical planner
        physical_planner.create_physical_plan(logical_plan, ctx_state)
    }
}

/// Physical planner for Delorean extension plans
struct DeloreanExtensionPlanner {}

impl ExtensionPlanner for DeloreanExtensionPlanner {
    /// Create a physical plan for an extension node
    fn plan_extension(
        &self,
        node: &dyn UserDefinedLogicalNode,
        inputs: Vec<Arc<dyn ExecutionPlan>>,
        _ctx_state: &ExecutionContextState,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        match node.as_any().downcast_ref::<SchemaPivotNode>() {
            Some(schema_pivot) => {
                assert_eq!(inputs.len(), 1, "Inconsistent number of inputs");
                Ok(Arc::new(SchemaPivotExec::new(
                    inputs[0].clone(),
                    schema_pivot.schema().clone(),
                )))
            }
            None => Err(Error::General(format!(
                "Unknown extension node type {:?}",
                node
            ))),
        }
    }
}

pub struct DeloreanExecutionContext {
    counters: Arc<ExecutionCounters>,
    inner: ExecutionContext,
}

impl DeloreanExecutionContext {
    /// Create an ExecutionContext suitable for executing DataFusion plans
    pub fn new(counters: Arc<ExecutionCounters>) -> Self {
        const BATCH_SIZE: usize = 1000;

        // TBD: Should we be reusing an execution context across all executions?
        let config = ExecutionConfig::new().with_batch_size(BATCH_SIZE);

        let config = config.with_query_planner(Arc::new(DeloreanQueryPlanner {}));
        let inner = ExecutionContext::with_config(config);

        Self { counters, inner }
    }

    pub async fn make_plan(&self, plan: &LogicalPlan) -> Result<Arc<dyn ExecutionPlan>> {
        debug!("Running plan, input\n----\n{}\n----", dump_plan(plan));

        // TODO the datafusion optimizer was removing filters..
        //let logical_plan = ctx.optimize(&plan).context(DataFusionOptimization)?;
        let logical_plan = plan;
        debug!(
            "Running optimized plan\n----\n{}\n----",
            dump_plan(logical_plan)
        );

        self.inner.create_physical_plan(&logical_plan)
    }

    /// Executes the logical plan using DataFusion and produces RecordBatches
    pub async fn collect(&self, physical_plan: Arc<dyn ExecutionPlan>) -> Result<Vec<RecordBatch>> {
        self.counters.inc_plans_run();

        debug!("Running plan, physical:\n{:?}", physical_plan);

        self.inner.collect(physical_plan).await
    }

    /// Executes the physical plan and produces a RecordBatchReader
    /// that iterates over the results.
    pub async fn execute(
        &self,
        physical_plan: Arc<dyn ExecutionPlan>,
    ) -> Result<Box<dyn RecordBatchReader + Send>> {
        match physical_plan.output_partitioning().partition_count() {
            0 => {
                let empty_iterator = RecordBatchIterator::new(physical_plan.schema(), vec![]);
                Ok(Box::new(empty_iterator))
            }
            1 => physical_plan.execute(0).await,
            _ => {
                // merge into a single partition
                let plan = MergeExec::new(physical_plan);
                // MergeExec must produce a single partition
                assert_eq!(1, plan.output_partitioning().partition_count());
                plan.execute(0).await
            }
        }
    }
}
