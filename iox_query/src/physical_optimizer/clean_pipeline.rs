use std::sync::Arc;

use datafusion::{
    common::tree_node::{Transformed, TreeNode},
    config::ConfigOptions,
    error::Result,
    physical_optimizer::PhysicalOptimizerRule,
    physical_plan::{
        coalesce_batches::CoalesceBatchesExec, coalesce_partitions::CoalescePartitionsExec,
        repartition::RepartitionExec, ExecutionPlan,
    },
};

/// Clean query plan so we can run all DataFusion builtin passes a 2nd time.
///
/// This is required because some passes are NOT idempotent.
#[derive(Debug, Default)]
pub struct CleanPipeline;

impl PhysicalOptimizerRule for CleanPipeline {
    fn optimize(
        &self,
        plan: Arc<dyn ExecutionPlan>,
        _config: &ConfigOptions,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        plan.transform_up(&|plan| {
            let plan_any = plan.as_any();

            if let Some(sub) = plan_any.downcast_ref::<CoalesceBatchesExec>() {
                return Ok(Transformed::Yes(Arc::clone(sub.input())));
            } else if let Some(sub) = plan_any.downcast_ref::<CoalescePartitionsExec>() {
                return Ok(Transformed::Yes(Arc::clone(sub.input())));
            } else if let Some(sub) = plan_any.downcast_ref::<RepartitionExec>() {
                return Ok(Transformed::Yes(Arc::clone(sub.input())));
            }

            Ok(Transformed::No(plan))
        })
    }

    fn name(&self) -> &str {
        "clean_pipeline"
    }

    fn schema_check(&self) -> bool {
        true
    }
}
