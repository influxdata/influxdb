use std::sync::Arc;

use datafusion::{
    common::plan_datafusion_err,
    error::Result,
    physical_expr::{LexOrdering, PhysicalSortExpr},
    physical_plan::{ExecutionPlan, sorts::sort_preserving_merge::SortPreservingMergeExec},
};

/// Add SortPreservingMerge to the plan with many partitions to ensure the order is preserved
pub(crate) fn add_sort_preserving_merge(
    input: Arc<dyn ExecutionPlan>,
    sort_exprs: &[PhysicalSortExpr],
    fetch_number: Option<usize>,
) -> Result<Arc<dyn ExecutionPlan>> {
    if input.properties().output_partitioning().partition_count() > 1 {
        // Add SortPreservingMergeExec on top of this input
        let expr = LexOrdering::new(sort_exprs.iter().cloned())
            .ok_or_else(|| plan_datafusion_err!("sort key empoty"))?;
        let sort_preserving_merge_exec =
            Arc::new(SortPreservingMergeExec::new(expr, input).with_fetch(fetch_number));
        Ok(sort_preserving_merge_exec as _)
    } else {
        Ok(input)
    }
}
