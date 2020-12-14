//! This module contains plumbing to connect InfluxDB IOx extensions to
//! DataFusion

use std::sync::Arc;

use arrow_deps::{
    arrow::{
        array::{ArrayRef, Int64Array, Int64Builder},
        datatypes::DataType,
        record_batch::RecordBatch,
    },
    datafusion::{
        execution::context::{ExecutionContextState, QueryPlanner},
        logical_plan::{Expr, LogicalPlan, UserDefinedLogicalNode},
        physical_plan::{
            functions::ScalarFunctionImplementation,
            merge::MergeExec,
            planner::{DefaultPhysicalPlanner, ExtensionPlanner},
            ExecutionPlan, PhysicalPlanner, SendableRecordBatchStream,
        },
        prelude::*,
    },
};

use crate::exec::schema_pivot::{SchemaPivotExec, SchemaPivotNode};
use crate::group_by::WindowDuration;
use crate::window;

use tracing::debug;

// Reuse DataFusion error and Result types for this module
pub use arrow_deps::datafusion::error::{DataFusionError as Error, Result};

use super::counters::ExecutionCounters;

struct IOxQueryPlanner {}

impl QueryPlanner for IOxQueryPlanner {
    fn rewrite_logical_plan(&self, plan: LogicalPlan) -> Result<LogicalPlan> {
        // TODO: implement any IOx specific query rewrites needed
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
            DefaultPhysicalPlanner::with_extension_planner(Arc::new(IOxExtensionPlanner {}));
        // Delegate most work of physical planning to the default physical planner
        physical_planner.create_physical_plan(logical_plan, ctx_state)
    }
}

/// Physical planner for InfluxDB IOx extension plans
struct IOxExtensionPlanner {}

impl ExtensionPlanner for IOxExtensionPlanner {
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
            None => Err(Error::Internal(format!(
                "Unknown extension node type {:?}",
                node
            ))),
        }
    }
}

pub struct IOxExecutionContext {
    counters: Arc<ExecutionCounters>,
    inner: ExecutionContext,
}

impl IOxExecutionContext {
    /// Create an ExecutionContext suitable for executing DataFusion plans
    pub fn new(counters: Arc<ExecutionCounters>) -> Self {
        const BATCH_SIZE: usize = 1000;

        // TBD: Should we be reusing an execution context across all executions?
        let config = ExecutionConfig::new().with_batch_size(BATCH_SIZE);

        let config = config.with_query_planner(Arc::new(IOxQueryPlanner {}));
        let inner = ExecutionContext::with_config(config);

        Self { counters, inner }
    }

    pub async fn make_plan(&self, plan: &LogicalPlan) -> Result<Arc<dyn ExecutionPlan>> {
        debug!(
            "Creating plan: Initial plan\n----\n{}\n{}\n----",
            plan.display_indent_schema(),
            plan.display_graphviz(),
        );

        let plan = self.inner.optimize(&plan)?;

        debug!(
            "Creating plan: Optimized plan\n----\n{}\n{}\n----",
            plan.display_indent_schema(),
            plan.display_graphviz(),
        );

        self.inner.create_physical_plan(&plan)
    }

    /// Executes the logical plan using DataFusion and produces RecordBatches
    pub async fn collect(&self, physical_plan: Arc<dyn ExecutionPlan>) -> Result<Vec<RecordBatch>> {
        self.counters.inc_plans_run();

        debug!("Running plan, physical:\n{:?}", physical_plan);

        self.inner.collect(physical_plan).await
    }

    /// Executes the physical plan and produces a RecordBatchStream to stream
    /// over the result that iterates over the results.
    pub async fn execute(
        &self,
        physical_plan: Arc<dyn ExecutionPlan>,
    ) -> Result<SendableRecordBatchStream> {
        if physical_plan.output_partitioning().partition_count() <= 1 {
            physical_plan.execute(0).await
        } else {
            // merge into a single partition
            let plan = MergeExec::new(physical_plan);
            // MergeExec must produce a single partition
            assert_eq!(1, plan.output_partitioning().partition_count());
            plan.execute(0).await
        }
    }
}

/// This is the implementation of the `window_bounds` user defined
/// function used in IOx to compute window boundaries when doing
/// grouping by windows.
fn window_bounds(
    args: &[ArrayRef],
    every: &WindowDuration,
    offset: &WindowDuration,
) -> Result<ArrayRef> {
    // Note:  At the time of writing, DataFusion creates arrays of constants for
    // constant arguments (which 4 of 5 arguments to window bounds are). We
    // should eventually contribute some way back upstream to make DataFusion
    // pass 4 constants rather than 4 arrays of constants.

    // There are any number of ways this function could also be further
    // optimized, which we leave as an exercise to our future selves

    // `args` and output are dynamically-typed Arrow arrays, which means that we
    // need to:
    //
    // 1. cast the values to the type we want
    // 2. perform the window_bounds calculation for every element in the
    //     timestamp array
    // 3. construct the resulting array

    // this is guaranteed by DataFusion based on the function's signature.
    assert_eq!(args.len(), 1);

    let time = &args[0]
        .as_any()
        .downcast_ref::<Int64Array>()
        .expect("cast of time failed");

    // Note: the Go code uses the `Stop` field of the `GetEarliestBounds` call as
    // the window boundary https://github.com/influxdata/influxdb/blob/master/storage/reads/array_cursor.gen.go#L546

    // Note window doesn't use the period argument
    let period = window::Duration::from_nsecs(0);
    let window = window::Window::new(every.into(), period, offset.into());

    // calculate the output times, one at a time, one element at a time
    let mut builder = Int64Builder::new(time.len());
    time.iter().try_for_each(|ts| match ts {
        Some(ts) => {
            let bounds = window.get_earliest_bounds(ts);
            builder.append_value(bounds.stop)
        }
        None => builder.append_null(),
    })?;

    Ok(Arc::new(builder.finish()))
}

/// Create a DataFusion `Expr` that invokes `window_bounds` with the
/// appropriate every and offset arguments at runtime
pub fn make_window_bound_expr(
    time_arg: Expr,
    every: &WindowDuration,
    offset: &WindowDuration,
) -> Expr {
    // Bind a copy of the arguments in a closure
    let every = every.clone();
    let offset = offset.clone();
    let func_ptr: ScalarFunctionImplementation =
        Arc::new(move |args| window_bounds(args, &every, &offset));

    let udf = create_udf(
        "window_bounds",
        vec![DataType::Int64],     // argument types
        Arc::new(DataType::Int64), // return type
        func_ptr,
    );

    udf.call(vec![time_arg])
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_window_bounds() {
        let input: ArrayRef = Arc::new(Int64Array::from(vec![
            Some(100),
            None,
            Some(200),
            Some(300),
            Some(400),
        ]));

        let every = WindowDuration::from_nanoseconds(200);
        let offset = WindowDuration::from_nanoseconds(50);

        let bounds_array =
            window_bounds(&[input], &every, &offset).expect("window_bounds executed correctly");

        let expected_array: ArrayRef = Arc::new(Int64Array::from(vec![
            Some(250),
            None,
            Some(250),
            Some(450),
            Some(450),
        ]));

        assert_eq!(
            &expected_array, &bounds_array,
            "Expected:\n{:?}\nActual:\n{:?}",
            expected_array, bounds_array,
        );
    }
}
