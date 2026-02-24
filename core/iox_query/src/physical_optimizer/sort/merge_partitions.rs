use std::sync::Arc;

use datafusion::{
    common::tree_node::{Transformed, TreeNode, TreeNodeRecursion},
    error::Result,
    physical_expr::LexOrdering,
    physical_plan::{
        ExecutionPlan, repartition::RepartitionExec, sorts::sort::SortExec, tree_node::PlanContext,
        union::UnionExec,
    },
};
use itertools::Itertools;

use super::{extract_ranges::extract_ranges_from_plan, util::add_sort_preserving_merge};

/// This function inserts an SPM above SortExecs, in order to enable to parallelize the SPM loser trees,
/// while leaving the final ProgressiveEvalExec which concats the incoming partitions. This maximizes our
/// performance in circumstances
///
/// An example is the InfluxQL `SHOW TAG VALUES WITH KEY != "tag0"` plan which can look like:
/// ```text
/// SortPreservingMergeExec to convert to ProgressiveEvalExec
///    UnionExec
///     ...output is 4 sorted partitions with all have the same lexical range of (m0,tag1)->(m0,tag1)...
///        SortExec: expr=[value@2 ASC NULLS LAST], preserve_partitioning=[true]
///          ProjectionExec: expr=[m0 as iox::measurement, tag1 as key, tag1@0 as value]
///            AggregateExec: mode=FinalPartitioned, gby=[tag1@0 as tag1], aggr=[]
///              CoalesceBatchesExec: target_batch_size=8192
///                RepartitionExec: partitioning=Hash([tag1@0], 4), input_partitions=2
///                  AggregateExec: mode=Partial, gby=[tag1@0 as tag1], aggr=[]
///                    DataSourceExec: file_groups={2 groups: [[a.parquet], [b.parquet]]}, projection=[tag1]
///     ...output is 4 sorted partitions with all have the same lexical range of (m0,tag2)->(m0,tag2)...
///        SortExec: expr=[value@2 ASC NULLS LAST], preserve_partitioning=[true]
///          ProjectionExec: expr=[m0 as iox::measurement, tag2 as key, tag2@0 as value]
///            AggregateExec: mode=FinalPartitioned, gby=[tag2@0 as tag2], aggr=[]
///              CoalesceBatchesExec: target_batch_size=8192
///                RepartitionExec: partitioning=Hash([tag2@0], 4), input_partitions=2
///                  AggregateExec: mode=Partial, gby=[tag2@0 as tag2], aggr=[]
///                    DataSourceExec: file_groups={2 groups: [[a.parquet], [b.parquet]]}, projection=[tag2]
///     ...continue for all other tags...
/// ```
///
/// The above plan cannot use the ProgressiveEvalExec at the root (after the union), since the lexical ranges are
/// overlapping: there are 4 partitions of (m0,tag1)->(m0,tag1) and 4 partitions of (m0,tag2)->(m0,tag2).
///
/// In order to enable the most efficient merging, we should perform the merging above the sort exec,
/// such that each merge occurs in parallel. And the final ProgressiveEvalExec can then be applied:
/// ```text
/// ProgressiveEvalExec: input_ranges=[(m0,tag1)->(m0,tag1), (m0,tag2)->(m0,tag2)]
///    UnionExec
///      SortPreservingMergeExec: [iox::measurement@0 ASC NULLS LAST, key@1 ASC NULLS LAST, value@2 ASC NULLS LAST]
///        SortExec: expr=[value@2 ASC NULLS LAST], preserve_partitioning=[true]
///          ProjectionExec: expr=[m0 as iox::measurement, tag1 as key, tag1@0 as value]
///            AggregateExec: mode=FinalPartitioned, gby=[tag1@0 as tag1], aggr=[]
///              CoalesceBatchesExec: target_batch_size=8192
///                RepartitionExec: partitioning=Hash([tag1@0], 4), input_partitions=2
///                  AggregateExec: mode=Partial, gby=[tag1@0 as tag1], aggr=[]
///                    DataSourceExec: file_groups={2 groups: [[a.parquet], [b.parquet]]}, projection=[tag1]
///      SortPreservingMergeExec: [iox::measurement@0 ASC NULLS LAST, key@1 ASC NULLS LAST, value@2 ASC NULLS LAST]
///        SortExec: expr=[value@2 ASC NULLS LAST], preserve_partitioning=[true]
///          ProjectionExec: expr=[m0 as iox::measurement, tag2 as key, tag2@0 as value]
///            AggregateExec: mode=FinalPartitioned, gby=[tag2@0 as tag2], aggr=[]
///              CoalesceBatchesExec: target_batch_size=8192
///                RepartitionExec: partitioning=Hash([tag2@0], 4), input_partitions=2
///                  AggregateExec: mode=Partial, gby=[tag2@0 as tag2], aggr=[]
///                    DataSourceExec: file_groups={2 groups: [[a.parquet], [b.parquet]]}, projection=[tag2]
/// ```
///
/// The conditions for insertion of the SPM are as follows:
///     * have a union
///     * we have a SortExec with preserve_partitioning=[true]
///     * input into sort exec has been repartitioned (for parallelized sorting)
///     * all partitions on the sort exec have the same lexical range
///
pub fn merge_partitions_after_parallelized_sorting(
    original_plan: Arc<dyn ExecutionPlan>,
    ordering_req: &LexOrdering,
) -> Result<Arc<dyn ExecutionPlan>> {
    let ctx = MergePartitionsContext::new_default(Arc::clone(&original_plan));

    let transformed = ctx
        .transform_up(|ctx| {
            // update current plan, taking in any changes from the children
            let mut ctx = update_data_from_children(ctx).update_plan_from_children()?;

            if let Some(sort_exec) = ctx.plan.as_any().downcast_ref::<SortExec>() {
                let Context {
                    was_repartitioned, ..
                } = ctx.data;

                if !sort_exec
                    .properties()
                    .equivalence_properties()
                    .ordering_satisfy(ordering_req.iter().cloned())?
                    || !sort_exec.preserve_partitioning()
                    || !was_repartitioned
                {
                    // halt on DAG branch
                    Ok(Transformed::new(ctx, false, TreeNodeRecursion::Jump))
                } else {
                    // If all lexical ranges are the same, then the partitions are a result of repartitioning. Insert an SPM above the sort.
                    if let Some(lexical_ranges) = extract_ranges_from_plan(ordering_req, &ctx.plan)?
                        && lexical_ranges.iter().dedup().collect_vec().len() == 1
                    {
                        let plan = add_sort_preserving_merge(
                            Arc::clone(&ctx.plan),
                            sort_exec.expr(),
                            sort_exec.fetch(),
                        )?;
                        let mut new_ctx = MergePartitionsContext::new_default(plan);
                        new_ctx.data.has_merged_parallelized_sort = true;
                        return Ok(Transformed::yes(new_ctx));
                    };

                    Ok(Transformed::no(ctx))
                }
            } else if ctx.plan.as_any().is::<UnionExec>() {
                ctx.data.was_unioned = true;
                Ok(Transformed::new(ctx, true, TreeNodeRecursion::Jump))
            } else if ctx.plan.as_any().is::<RepartitionExec>() {
                ctx.data.was_repartitioned = true;
                ctx.data.was_unioned = false;
                Ok(Transformed::yes(ctx))
            } else {
                Ok(Transformed::no(ctx))
            }
        })
        .map(|t| t.data)?;

    if transformed.data.was_unioned && transformed.data.has_merged_parallelized_sort {
        Ok(transformed.plan)
    } else {
        Ok(original_plan)
    }
}

/// Context where the [`PlanContext::data`] is a bool representing if
/// the sort's input was repartitioned.
#[derive(Default)]
struct Context {
    /// have encountered a repartitioning before the parallelized sort
    was_repartitioned: bool,

    /// we have inserted a merge for the parallelized sort streams
    has_merged_parallelized_sort: bool,

    /// merged streams are then union'ed
    was_unioned: bool,
}
type MergePartitionsContext = PlanContext<Context>;

/// Update the current [`PlanContext::data`] based upon the children.
fn update_data_from_children(mut ctx: MergePartitionsContext) -> MergePartitionsContext {
    ctx.data = Context {
        was_repartitioned: ctx
            .children
            .iter()
            .any(|child| child.data.was_repartitioned),
        has_merged_parallelized_sort: ctx
            .children
            .iter()
            .any(|child| child.data.has_merged_parallelized_sort),
        was_unioned: ctx.children.iter().any(|child| child.data.was_unioned),
    };
    ctx
}
