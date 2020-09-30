//! This module contains plumbing to connect the Delorean extensions to DataFusion

use std::sync::Arc;

use delorean_arrow::datafusion::{
    execution::context::ExecutionContextState,
    execution::context::QueryPlanner,
    logical_plan::LogicalPlan,
    logical_plan::UserDefinedLogicalNode,
    physical_plan::{
        planner::{DefaultPhysicalPlanner, ExtensionPlanner},
        ExecutionPlan, PhysicalPlanner,
    },
    prelude::{ExecutionConfig, ExecutionContext},
};

use crate::exec::schema_pivot::{SchemaPivotExec, SchemaPivotNode};

// Reuse DataFusion error and Result types for this module
pub use delorean_arrow::datafusion::error::{ExecutionError as Error, Result};

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

/// Create an ExecutionContext suitable for executing DataFusion plans
pub fn make_exec_context(config: ExecutionConfig) -> ExecutionContext {
    let config = config.with_query_planner(Arc::new(DeloreanQueryPlanner {}));

    ExecutionContext::with_config(config)
}
