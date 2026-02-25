use std::sync::Arc;

use datafusion::{
    common::tree_node::{Transformed, TreeNode, TreeNodeRecursion},
    config::ConfigOptions,
    error::Result,
    physical_optimizer::PhysicalOptimizerRule,
    physical_plan::{
        ExecutionPlan, Partitioning, repartition::RepartitionExec,
        sorts::sort_preserving_merge::SortPreservingMergeExec,
    },
};
use itertools::Itertools;

use crate::provider::reorder_partitions::ReorderPartitionsExec;
use crate::{
    physical_optimizer::sort::merge_partitions::merge_partitions_after_parallelized_sorting,
    provider::progressive_eval::ProgressiveEvalExec,
};

use super::{
    extract_ranges::extract_disjoint_ranges_from_plan,
    regroup_files::split_and_regroup_parquet_files,
};

/// IOx specific optimization that eliminates a `SortPreservingMerge` by reordering inputs in terms
/// of their value ranges. If all inputs are non overlapping and ordered by value range, they can
/// be concatenated by `ProgressiveEval`  while maintaining the desired output order without
/// actually merging.
///
/// Find this structure:
///     SortPreservingMergeExec - on one column (DESC or ASC)
///         UnionExec
/// and if:
///
/// - all inputs of UnionExec are already sorted (or has SortExec) with sortExpr also on time DESC
///   or ASC accordingly and
/// - the streams do not overlap in values of the sorted column
///
/// do:
///
/// - order them by the sorted column DESC or ASC accordingly and
/// - replace SortPreservingMergeExec with ProgressiveEvalExec
///
/// Notes: The difference between SortPreservingMergeExec & ProgressiveEvalExec:
///
/// - SortPreservingMergeExec do the merge of sorted input streams. It needs each stream sorted but
///   the streams themselves can be in any random order and they can also overlap in values of
///   sorted columns.
/// - ProgressiveEvalExec only outputs data in their input order of the streams and not do any
///   merges. Thus in order to output data in the right sort order, these three conditions must be
///   true:
///     1. Each input streams (a.k.a. partitions) be must sorted on the same sort key
///     2. For partitions 0..N, they must be sorted across partitions using the same sort key
///     3. The partitioned streams must not overlap in the values of the sort key
///
///
/// Refer to [`swap_spm_for_progeval`] for supported plan transformations.
#[derive(Debug)]
pub(crate) struct OrderUnionSortedInputs;

impl PhysicalOptimizerRule for OrderUnionSortedInputs {
    fn optimize(
        &self,
        plan: Arc<dyn ExecutionPlan>,
        _config: &ConfigOptions,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        plan.transform_up(|plan| {
            // Find SortPreservingMergeExec
            let Some(sort_preserving_merge_exec) =
                plan.as_any().downcast_ref::<SortPreservingMergeExec>()
            else {
                return Ok(Transformed::no(plan));
            };

            // Assess a possible SPM->ProgressiveEval swap.
            swap_spm_for_progeval(sort_preserving_merge_exec, Arc::clone(&plan))
        })
        .map(|t| t.data)
    }

    fn name(&self) -> &str {
        "order_union_sorted_inputs"
    }

    fn schema_check(&self) -> bool {
        true
    }
}

/// Handles 2 scenarios:
///     * (1) when the regrouping & reordering of the `DataSourceExec` is sufficient to use ProgressiveEval
///     * (2) when the reordering of the partitions, (with or without `DataSourceExec` changes), is sufficient to use ProgressiveEval
///
///
/// Using sort key ranges are non-overlapping and ordered:
///   |---r0--|--- r1---|-- r2 ---|-- r3 ---|-- r4 --|
///
/// Where r3 and r4 are from different [`PartitionedFile`](datafusion::datasource::listing::PartitionedFile)
/// within the same parquet.
///
///
/// Starting with this structure:
///   ```text
///   SortPreservingMergeExec: expr=[a@0 DESC, b@2 ASC]
///     ...
///       DataSourceExec: file_groups={4 groups: [[2.parquet],[3.parquet:1..100],[3.parquet:100..200],[1.parquet,0.parquet]]} output_ordering=[a@0 DESC, b@2 ASC]
///   ```
///
/// This function creates a progressive eval by regrouping & reordering of the `DataSourceExec`:
///   ```text
///   ProgressiveEvalExec:
///     ...
///       DataSourceExec: file_groups={4 groups: [[0.parquet],[1.parquet],[2.parquet],[3.parquet:1..200]]} output_ordering=[a@0 DESC, b@2 ASC]
///   ```
///
///
/// Starting with this structure:
///   ```text
///   SortPreservingMergeExec: expr=[a@0 DESC, b@2 ASC]
///     ...
///       UnionExec
///         DataSourceExec: file_groups={2 groups: [[1.parquet,3.parquet:1..100],[3.parquet:100..200]]} output_ordering=[a@0 DESC, b@2 ASC]  <--- 2 partitions with ranges r1 then r3, & r4
///         DataSourceExec: file_groups={1 group: [[2.parquet,0.parquet]]} output_ordering=[a@0 DESC, b@2 ASC]  <--- 1 partition with ranges r2 then r0
///   ```
///
/// This function creates a progressive eval by re-ordering the partitioned via the ReorderPartitionsExec (** NOT transforming the union inputs **):
///   ```text
///   ProgressiveEvalExec:
///     ReorderPartitionsExec: mapped_partition_indices=[2, 0, 3, 1]
///       ...
///         UnionExec
///           DataSourceExec: file_groups={2 groups: [[1.parquet],[3.parquet:1..200]]} output_ordering=[a@0 DESC, b@2 ASC]  <--- 2 partitions with ranges r1, & r3/r4
///           DataSourceExec: file_groups={2 groups: [[0.parquet],[2.parquet]]} output_ordering=[a@0 DESC, b@2 ASC]  <--- 2 partitions with ranges r0 & r2
///   ```
/// Where ReorderPartitionsExec::execute(partition=0) will pull from its input partition 2 (a.k.a. `0.parquet`).
///
///
/// ProgressiveEval will then pull from each partition in order, covering ranges r0..=r4
///
///
/// Proper lexical ordering must be obtainable from the following transformation:
///     * regrouping and reordering within each `DataSourceExec`. (Not across `DataSourceExec`.)
///     * mapping from input to output partition using the [`ReorderPartitionsExec`]
///
fn swap_spm_for_progeval(
    original_spm: &SortPreservingMergeExec,
    return_unaltered_plan: Arc<dyn ExecutionPlan>,
) -> Result<Transformed<Arc<dyn ExecutionPlan>>> {
    let ordering_req = original_spm.expr();

    // Step 1: Remove any RoundRobin repartition nodes that may interfere with optimization
    let input = Arc::clone(original_spm.input())
        .transform_down(remove_rr_repartition_if_exists)
        .map(|t| t.data)?;

    // Step 2: Split and regroup partitioned file scans. Also re-orders the scan partitions.
    // This step maximizes our chances of getting a disjoint, nonoverlapping lexical ranges.
    let input = input
        .transform_down(|plan| split_and_regroup_parquet_files(plan, ordering_req))
        .map(|t| t.data)?;

    // Step 3: compensate for previous redistribution (for parallelized sorting) passes.
    let input = merge_partitions_after_parallelized_sorting(input, ordering_req)?;

    // Step 4: try to extract the lexical ranges for the input partitions
    let Some(lexical_ranges) = extract_disjoint_ranges_from_plan(ordering_req, &input)? else {
        return Ok(Transformed::no(return_unaltered_plan));
    };

    // Step 5: if needed, re-order the partitions
    let ordered_input = if lexical_ranges.indices().is_sorted() {
        input
    } else {
        Arc::new(ReorderPartitionsExec::new(
            input,
            lexical_ranges.indices().to_owned(),
            ordering_req.clone(),
        )?) as Arc<dyn ExecutionPlan>
    };

    // Step 6: Replace SortPreservingMergeExec with ProgressiveEvalExec
    let progresive_eval_exec = Arc::new(ProgressiveEvalExec::new(
        ordered_input,
        Some(lexical_ranges.ordered_ranges().cloned().collect_vec()),
        original_spm.fetch(),
    ));

    Ok(Transformed::yes(progresive_eval_exec))
}

/// Remove any RoundRobin repartition nodes that may interfere with optimization.
///
/// If the current node is a RepartitionExec with Partitioning::RoundRobinBatch,
/// then remove that node and return its child.
fn remove_rr_repartition_if_exists(
    plan: Arc<dyn ExecutionPlan>,
) -> Result<Transformed<Arc<dyn ExecutionPlan>>> {
    if let Some(repartition_exec) = plan.as_any().downcast_ref::<RepartitionExec>()
        && matches!(
            repartition_exec.partitioning(),
            Partitioning::RoundRobinBatch(_)
        )
    {
        // Remove the RoundRobin repartition node and return its child
        Ok(Transformed::new(
            Arc::clone(repartition_exec.input()),
            true,
            TreeNodeRecursion::Continue,
        ))
    } else if plan.as_any().is::<SortPreservingMergeExec>() {
        // halt at the next SPM.
        // that will be considered separately at the root PhysicalOptimizer::optimize(), as it checks per SPM found
        Ok(Transformed::new(plan, false, TreeNodeRecursion::Jump))
    } else {
        Ok(Transformed::no(plan))
    }
}

#[cfg(test)]
mod test {
    use std::sync::Arc;

    use arrow::{compute::SortOptions, datatypes::SchemaRef};
    use datafusion::{
        datasource::provider_as_source,
        logical_expr::{LogicalPlanBuilder, Operator},
        physical_expr::{LexOrdering, PhysicalSortExpr},
        physical_plan::{
            ExecutionPlan, Partitioning, PhysicalExpr,
            expressions::{BinaryExpr, Column},
            limit::GlobalLimitExec,
            projection::ProjectionExec,
            repartition::RepartitionExec,
            sorts::{sort::SortExec, sort_preserving_merge::SortPreservingMergeExec},
            union::UnionExec,
        },
        prelude::{col, lit},
        scalar::ScalarValue,
    };
    use executor::DedicatedExecutor;
    use schema::{InfluxFieldType, SchemaBuilder as IOxSchemaBuilder, sort::SortKey};

    use crate::{
        CHUNK_ORDER_COLUMN_NAME, QueryChunk,
        exec::{Executor, ExecutorConfig},
        physical_optimizer::{
            sort::order_union_sorted_inputs::OrderUnionSortedInputs, test_util::OptimizationTest,
        },
        provider::{DeduplicateExec, ProviderBuilder, RecordBatchesExec, chunks_to_physical_nodes},
        statistics::{column_statistics_min_max, compute_stats_column_min_max},
        test::{TestChunk, format_execution_plan},
    };

    // ------------------------------------------------------------------
    // Positive tests: the right structure found -> plan optimized
    // ------------------------------------------------------------------

    #[test]
    fn test_limit_mix_record_batch_parquet_2_desc() {
        test_helpers::maybe_start_logging();

        let schema = schema();
        let sort_exprs = [
            ("col2", SortOp::Asc),
            ("col1", SortOp::Asc),
            ("time", SortOp::Asc),
        ];

        let plan_parquet = PlanBuilder::data_source_exec_parquet(&schema, 1000, 2000);
        let plan_parquet2 = PlanBuilder::data_source_exec_parquet(&schema, 2001, 3000);
        let plan_batches = PlanBuilder::record_batches_exec(2, 2500, 3500);

        let plan_sort1 = plan_batches.sort(sort_exprs);
        let plan_union_1 = plan_sort1.union(plan_parquet2);
        let plan_spm_for_dedupe = plan_union_1.sort_preserving_merge(sort_exprs);
        let plan_dedupe = plan_spm_for_dedupe.deduplicate(sort_exprs, false);

        let sort_exprs = [("time", SortOp::Desc)];
        let plan_sort2 = plan_parquet.sort(sort_exprs);
        let plan_sort3 = plan_dedupe.sort(sort_exprs);

        let plan_union_2 = plan_sort2.union(plan_sort3);

        let plan_spm = plan_union_2.sort_preserving_merge(sort_exprs);

        let plan_limit = plan_spm.limit(0, Some(1));

        let opt = OrderUnionSortedInputs;
        insta::assert_yaml_snapshot!(
            OptimizationTest::new(plan_limit.build(), opt),
            @r#"
        input:
          - " GlobalLimitExec: skip=0, fetch=1"
          - "   SortPreservingMergeExec: [time@3 DESC NULLS LAST]"
          - "     UnionExec"
          - "       SortExec: expr=[time@3 DESC NULLS LAST], preserve_partitioning=[false]"
          - "         DataSourceExec: file_groups={1 group: [[0.parquet]]}, projection=[col1, col2, field1, time, __chunk_order], output_ordering=[__chunk_order@4 ASC], file_type=parquet"
          - "       SortExec: expr=[time@3 DESC NULLS LAST], preserve_partitioning=[false]"
          - "         DeduplicateExec: [col2@1 ASC NULLS LAST,col1@0 ASC NULLS LAST,time@3 ASC NULLS LAST]"
          - "           SortPreservingMergeExec: [col2@1 ASC NULLS LAST, col1@0 ASC NULLS LAST, time@3 ASC NULLS LAST]"
          - "             UnionExec"
          - "               SortExec: expr=[col2@1 ASC NULLS LAST, col1@0 ASC NULLS LAST, time@3 ASC NULLS LAST], preserve_partitioning=[false]"
          - "                 RecordBatchesExec: chunks=2, projection=[col1, col2, field1, time, __chunk_order]"
          - "               DataSourceExec: file_groups={1 group: [[0.parquet]]}, projection=[col1, col2, field1, time, __chunk_order], output_ordering=[__chunk_order@4 ASC], file_type=parquet"
        output:
          Ok:
            - " GlobalLimitExec: skip=0, fetch=1"
            - "   ProgressiveEvalExec: input_ranges=[(2001)->(3500), (1000)->(2000)]"
            - "     ReorderPartitionsExec: mapped_partition_indices=[1, 0]"
            - "       UnionExec"
            - "         SortExec: expr=[time@3 DESC NULLS LAST], preserve_partitioning=[false]"
            - "           DataSourceExec: file_groups={1 group: [[0.parquet]]}, projection=[col1, col2, field1, time, __chunk_order], output_ordering=[__chunk_order@4 ASC], file_type=parquet"
            - "         SortExec: expr=[time@3 DESC NULLS LAST], preserve_partitioning=[false]"
            - "           DeduplicateExec: [col2@1 ASC NULLS LAST,col1@0 ASC NULLS LAST,time@3 ASC NULLS LAST]"
            - "             SortPreservingMergeExec: [col2@1 ASC NULLS LAST, col1@0 ASC NULLS LAST, time@3 ASC NULLS LAST]"
            - "               UnionExec"
            - "                 SortExec: expr=[col2@1 ASC NULLS LAST, col1@0 ASC NULLS LAST, time@3 ASC NULLS LAST], preserve_partitioning=[false]"
            - "                   RecordBatchesExec: chunks=2, projection=[col1, col2, field1, time, __chunk_order]"
            - "                 DataSourceExec: file_groups={1 group: [[0.parquet]]}, projection=[col1, col2, field1, time, __chunk_order], output_ordering=[__chunk_order@4 ASC], file_type=parquet"
        "#
        );
    }

    // test on non-time column & order desc
    #[test]
    fn test_limit_mix_record_batch_parquet_non_time_sort_desc() {
        test_helpers::maybe_start_logging();

        let schema = schema();
        let sort_exprs = [
            ("col2", SortOp::Asc),
            ("col1", SortOp::Asc),
            ("time", SortOp::Asc),
        ];

        let plan_parquet = PlanBuilder::data_source_exec_parquet(&schema, 1000, 2000);
        let plan_parquet2 = PlanBuilder::data_source_exec_parquet(&schema, 2001, 3000);
        let plan_batches = PlanBuilder::record_batches_exec(2, 2500, 3500);

        let plan_sort1 = plan_batches.sort(sort_exprs);
        let plan_union_1 = plan_sort1.union(plan_parquet2);

        let plan_spm_for_dedupe = plan_union_1.sort_preserving_merge(sort_exprs);
        let plan_dedupe = plan_spm_for_dedupe.deduplicate(sort_exprs, false);

        let sort_exprs = [("field1", SortOp::Desc)];

        let plan_sort2 = plan_parquet.sort(sort_exprs);
        let plan_sort3 = plan_dedupe.sort(sort_exprs);

        let plan_union_2 = plan_sort2.union(plan_sort3);

        let plan_spm = plan_union_2.sort_preserving_merge(sort_exprs);

        let plan_limit = plan_spm.limit(0, Some(1));

        // Output plan: the 2 SortExecs will be swapped the order
        let opt = OrderUnionSortedInputs;
        insta::assert_yaml_snapshot!(
            OptimizationTest::new(plan_limit.build(), opt),
            @r#"
        input:
          - " GlobalLimitExec: skip=0, fetch=1"
          - "   SortPreservingMergeExec: [field1@2 DESC NULLS LAST]"
          - "     UnionExec"
          - "       SortExec: expr=[field1@2 DESC NULLS LAST], preserve_partitioning=[false]"
          - "         DataSourceExec: file_groups={1 group: [[0.parquet]]}, projection=[col1, col2, field1, time, __chunk_order], output_ordering=[__chunk_order@4 ASC], file_type=parquet"
          - "       SortExec: expr=[field1@2 DESC NULLS LAST], preserve_partitioning=[false]"
          - "         DeduplicateExec: [col2@1 ASC NULLS LAST,col1@0 ASC NULLS LAST,time@3 ASC NULLS LAST]"
          - "           SortPreservingMergeExec: [col2@1 ASC NULLS LAST, col1@0 ASC NULLS LAST, time@3 ASC NULLS LAST]"
          - "             UnionExec"
          - "               SortExec: expr=[col2@1 ASC NULLS LAST, col1@0 ASC NULLS LAST, time@3 ASC NULLS LAST], preserve_partitioning=[false]"
          - "                 RecordBatchesExec: chunks=2, projection=[col1, col2, field1, time, __chunk_order]"
          - "               DataSourceExec: file_groups={1 group: [[0.parquet]]}, projection=[col1, col2, field1, time, __chunk_order], output_ordering=[__chunk_order@4 ASC], file_type=parquet"
        output:
          Ok:
            - " GlobalLimitExec: skip=0, fetch=1"
            - "   ProgressiveEvalExec: input_ranges=[(2001)->(3500), (1000)->(2000)]"
            - "     ReorderPartitionsExec: mapped_partition_indices=[1, 0]"
            - "       UnionExec"
            - "         SortExec: expr=[field1@2 DESC NULLS LAST], preserve_partitioning=[false]"
            - "           DataSourceExec: file_groups={1 group: [[0.parquet]]}, projection=[col1, col2, field1, time, __chunk_order], output_ordering=[__chunk_order@4 ASC], file_type=parquet"
            - "         SortExec: expr=[field1@2 DESC NULLS LAST], preserve_partitioning=[false]"
            - "           DeduplicateExec: [col2@1 ASC NULLS LAST,col1@0 ASC NULLS LAST,time@3 ASC NULLS LAST]"
            - "             SortPreservingMergeExec: [col2@1 ASC NULLS LAST, col1@0 ASC NULLS LAST, time@3 ASC NULLS LAST]"
            - "               UnionExec"
            - "                 SortExec: expr=[col2@1 ASC NULLS LAST, col1@0 ASC NULLS LAST, time@3 ASC NULLS LAST], preserve_partitioning=[false]"
            - "                   RecordBatchesExec: chunks=2, projection=[col1, col2, field1, time, __chunk_order]"
            - "                 DataSourceExec: file_groups={1 group: [[0.parquet]]}, projection=[col1, col2, field1, time, __chunk_order], output_ordering=[__chunk_order@4 ASC], file_type=parquet"
        "#
        );
    }

    // test on non-time column & order asc
    #[test]
    fn test_limit_mix_record_batch_parquet_non_time_sort_asc() {
        test_helpers::maybe_start_logging();

        let schema = schema();
        let sort_exprs = [
            ("col2", SortOp::Asc),
            ("col1", SortOp::Asc),
            ("time", SortOp::Asc),
        ];

        let plan_parquet = PlanBuilder::data_source_exec_parquet(&schema, 1000, 2000);
        let plan_parquet2 = PlanBuilder::data_source_exec_parquet(&schema, 2001, 3000);
        let plan_batches = PlanBuilder::record_batches_exec(2, 2500, 3500);

        let plan_sort1 = plan_batches.sort(sort_exprs);
        let plan_union_1 = plan_sort1.union(plan_parquet2);
        let plan_spm_for_dedupe = plan_union_1.sort_preserving_merge(sort_exprs);
        let plan_dedupe = plan_spm_for_dedupe.deduplicate(sort_exprs, false);

        let sort_exprs = [("field1", SortOp::Asc)];

        let plan_sort2 = plan_parquet.sort(sort_exprs);
        let plan_sort3 = plan_dedupe.sort(sort_exprs);

        let plan_union_2 = plan_sort2.union(plan_sort3);

        let plan_spm = plan_union_2.sort_preserving_merge(sort_exprs);

        let plan_limit = plan_spm.limit(0, Some(1));

        // input and output are the same
        let opt = OrderUnionSortedInputs;
        insta::assert_yaml_snapshot!(
            OptimizationTest::new(plan_limit.build(), opt),
            @r#"
        input:
          - " GlobalLimitExec: skip=0, fetch=1"
          - "   SortPreservingMergeExec: [field1@2 ASC NULLS LAST]"
          - "     UnionExec"
          - "       SortExec: expr=[field1@2 ASC NULLS LAST], preserve_partitioning=[false]"
          - "         DataSourceExec: file_groups={1 group: [[0.parquet]]}, projection=[col1, col2, field1, time, __chunk_order], output_ordering=[__chunk_order@4 ASC], file_type=parquet"
          - "       SortExec: expr=[field1@2 ASC NULLS LAST], preserve_partitioning=[false]"
          - "         DeduplicateExec: [col2@1 ASC NULLS LAST,col1@0 ASC NULLS LAST,time@3 ASC NULLS LAST]"
          - "           SortPreservingMergeExec: [col2@1 ASC NULLS LAST, col1@0 ASC NULLS LAST, time@3 ASC NULLS LAST]"
          - "             UnionExec"
          - "               SortExec: expr=[col2@1 ASC NULLS LAST, col1@0 ASC NULLS LAST, time@3 ASC NULLS LAST], preserve_partitioning=[false]"
          - "                 RecordBatchesExec: chunks=2, projection=[col1, col2, field1, time, __chunk_order]"
          - "               DataSourceExec: file_groups={1 group: [[0.parquet]]}, projection=[col1, col2, field1, time, __chunk_order], output_ordering=[__chunk_order@4 ASC], file_type=parquet"
        output:
          Ok:
            - " GlobalLimitExec: skip=0, fetch=1"
            - "   ProgressiveEvalExec: input_ranges=[(1000)->(2000), (2001)->(3500)]"
            - "     UnionExec"
            - "       SortExec: expr=[field1@2 ASC NULLS LAST], preserve_partitioning=[false]"
            - "         DataSourceExec: file_groups={1 group: [[0.parquet]]}, projection=[col1, col2, field1, time, __chunk_order], output_ordering=[__chunk_order@4 ASC], file_type=parquet"
            - "       SortExec: expr=[field1@2 ASC NULLS LAST], preserve_partitioning=[false]"
            - "         DeduplicateExec: [col2@1 ASC NULLS LAST,col1@0 ASC NULLS LAST,time@3 ASC NULLS LAST]"
            - "           SortPreservingMergeExec: [col2@1 ASC NULLS LAST, col1@0 ASC NULLS LAST, time@3 ASC NULLS LAST]"
            - "             UnionExec"
            - "               SortExec: expr=[col2@1 ASC NULLS LAST, col1@0 ASC NULLS LAST, time@3 ASC NULLS LAST], preserve_partitioning=[false]"
            - "                 RecordBatchesExec: chunks=2, projection=[col1, col2, field1, time, __chunk_order]"
            - "               DataSourceExec: file_groups={1 group: [[0.parquet]]}, projection=[col1, col2, field1, time, __chunk_order], output_ordering=[__chunk_order@4 ASC], file_type=parquet"
        "#
        );
    }

    // No limit & the input is in the right sort preserving merge struct --> optimize
    #[test]
    fn test_spm_time_desc() {
        test_helpers::maybe_start_logging();

        let schema = schema();
        let sort_exprs = [
            ("col2", SortOp::Asc),
            ("col1", SortOp::Asc),
            ("time", SortOp::Asc),
        ];

        let plan_parquet = PlanBuilder::data_source_exec_parquet(&schema, 1000, 2000);
        let plan_parquet2 = PlanBuilder::data_source_exec_parquet(&schema, 2001, 3000);
        let plan_batches = PlanBuilder::record_batches_exec(2, 2500, 3500);

        let plan_sort1 = plan_batches.sort(sort_exprs);
        let plan_union_1 = plan_sort1.union(plan_parquet2);
        let plan_spm_for_dedupe = plan_union_1.sort_preserving_merge(sort_exprs);
        let plan_dedupe = plan_spm_for_dedupe.deduplicate(sort_exprs, false);

        let sort_exprs = [("time", SortOp::Desc)];
        let plan_sort1 = plan_parquet.sort(sort_exprs);
        let plan_sort2 = plan_dedupe.sort(sort_exprs);

        let plan_union_2 = plan_sort1.union(plan_sort2);

        let plan_spm = plan_union_2.sort_preserving_merge(sort_exprs);

        // Output plan: the 2 SortExecs will be swapped the order
        let opt = OrderUnionSortedInputs;
        insta::assert_yaml_snapshot!(
            OptimizationTest::new(plan_spm.build(), opt),
            @r#"
        input:
          - " SortPreservingMergeExec: [time@3 DESC NULLS LAST]"
          - "   UnionExec"
          - "     SortExec: expr=[time@3 DESC NULLS LAST], preserve_partitioning=[false]"
          - "       DataSourceExec: file_groups={1 group: [[0.parquet]]}, projection=[col1, col2, field1, time, __chunk_order], output_ordering=[__chunk_order@4 ASC], file_type=parquet"
          - "     SortExec: expr=[time@3 DESC NULLS LAST], preserve_partitioning=[false]"
          - "       DeduplicateExec: [col2@1 ASC NULLS LAST,col1@0 ASC NULLS LAST,time@3 ASC NULLS LAST]"
          - "         SortPreservingMergeExec: [col2@1 ASC NULLS LAST, col1@0 ASC NULLS LAST, time@3 ASC NULLS LAST]"
          - "           UnionExec"
          - "             SortExec: expr=[col2@1 ASC NULLS LAST, col1@0 ASC NULLS LAST, time@3 ASC NULLS LAST], preserve_partitioning=[false]"
          - "               RecordBatchesExec: chunks=2, projection=[col1, col2, field1, time, __chunk_order]"
          - "             DataSourceExec: file_groups={1 group: [[0.parquet]]}, projection=[col1, col2, field1, time, __chunk_order], output_ordering=[__chunk_order@4 ASC], file_type=parquet"
        output:
          Ok:
            - " ProgressiveEvalExec: input_ranges=[(2001)->(3500), (1000)->(2000)]"
            - "   ReorderPartitionsExec: mapped_partition_indices=[1, 0]"
            - "     UnionExec"
            - "       SortExec: expr=[time@3 DESC NULLS LAST], preserve_partitioning=[false]"
            - "         DataSourceExec: file_groups={1 group: [[0.parquet]]}, projection=[col1, col2, field1, time, __chunk_order], output_ordering=[__chunk_order@4 ASC], file_type=parquet"
            - "       SortExec: expr=[time@3 DESC NULLS LAST], preserve_partitioning=[false]"
            - "         DeduplicateExec: [col2@1 ASC NULLS LAST,col1@0 ASC NULLS LAST,time@3 ASC NULLS LAST]"
            - "           SortPreservingMergeExec: [col2@1 ASC NULLS LAST, col1@0 ASC NULLS LAST, time@3 ASC NULLS LAST]"
            - "             UnionExec"
            - "               SortExec: expr=[col2@1 ASC NULLS LAST, col1@0 ASC NULLS LAST, time@3 ASC NULLS LAST], preserve_partitioning=[false]"
            - "                 RecordBatchesExec: chunks=2, projection=[col1, col2, field1, time, __chunk_order]"
            - "               DataSourceExec: file_groups={1 group: [[0.parquet]]}, projection=[col1, col2, field1, time, __chunk_order], output_ordering=[__chunk_order@4 ASC], file_type=parquet"
        "#
        );
    }

    // No limit & the input is in the right sort preserving merge struct
    // has a rr repartitoning --> should remove
    // then --> optimize
    #[test]
    fn test_spm_time_desc_rr_repartition() {
        test_helpers::maybe_start_logging();

        let schema = schema();
        let sort_exprs = [
            ("col2", SortOp::Asc),
            ("col1", SortOp::Asc),
            ("time", SortOp::Asc),
        ];

        let plan_parquet = PlanBuilder::data_source_exec_parquet(&schema, 1000, 2000);
        let plan_parquet2 = PlanBuilder::data_source_exec_parquet(&schema, 2001, 3000);
        let plan_batches = PlanBuilder::record_batches_exec(2, 2500, 3500);

        let plan_sort1 = plan_batches.sort(sort_exprs);
        let plan_union_1 = plan_sort1.union(plan_parquet2);
        let plan_spm_for_dedupe = plan_union_1.sort_preserving_merge(sort_exprs);
        let plan_dedupe = plan_spm_for_dedupe.deduplicate(sort_exprs, false);

        let sort_exprs = [("time", SortOp::Desc)];
        let plan_sort1 = plan_parquet.sort(sort_exprs);
        let plan_sort2 = plan_dedupe.sort(sort_exprs);

        let plan_union_2 = plan_sort1.union(plan_sort2);
        let repartioned = plan_union_2.round_robin_repartition(4);

        let plan_spm = repartioned.sort_preserving_merge(sort_exprs);

        // Output plan: rr Repartition will be removed
        let opt = OrderUnionSortedInputs;
        insta::assert_yaml_snapshot!(
            OptimizationTest::new(plan_spm.build(), opt),
            @r#"
        input:
          - " SortPreservingMergeExec: [time@3 DESC NULLS LAST]"
          - "   RepartitionExec: partitioning=RoundRobinBatch(4), input_partitions=2"
          - "     UnionExec"
          - "       SortExec: expr=[time@3 DESC NULLS LAST], preserve_partitioning=[false]"
          - "         DataSourceExec: file_groups={1 group: [[0.parquet]]}, projection=[col1, col2, field1, time, __chunk_order], output_ordering=[__chunk_order@4 ASC], file_type=parquet"
          - "       SortExec: expr=[time@3 DESC NULLS LAST], preserve_partitioning=[false]"
          - "         DeduplicateExec: [col2@1 ASC NULLS LAST,col1@0 ASC NULLS LAST,time@3 ASC NULLS LAST]"
          - "           SortPreservingMergeExec: [col2@1 ASC NULLS LAST, col1@0 ASC NULLS LAST, time@3 ASC NULLS LAST]"
          - "             UnionExec"
          - "               SortExec: expr=[col2@1 ASC NULLS LAST, col1@0 ASC NULLS LAST, time@3 ASC NULLS LAST], preserve_partitioning=[false]"
          - "                 RecordBatchesExec: chunks=2, projection=[col1, col2, field1, time, __chunk_order]"
          - "               DataSourceExec: file_groups={1 group: [[0.parquet]]}, projection=[col1, col2, field1, time, __chunk_order], output_ordering=[__chunk_order@4 ASC], file_type=parquet"
        output:
          Ok:
            - " ProgressiveEvalExec: input_ranges=[(2001)->(3500), (1000)->(2000)]"
            - "   ReorderPartitionsExec: mapped_partition_indices=[1, 0]"
            - "     UnionExec"
            - "       SortExec: expr=[time@3 DESC NULLS LAST], preserve_partitioning=[false]"
            - "         DataSourceExec: file_groups={1 group: [[0.parquet]]}, projection=[col1, col2, field1, time, __chunk_order], output_ordering=[__chunk_order@4 ASC], file_type=parquet"
            - "       SortExec: expr=[time@3 DESC NULLS LAST], preserve_partitioning=[false]"
            - "         DeduplicateExec: [col2@1 ASC NULLS LAST,col1@0 ASC NULLS LAST,time@3 ASC NULLS LAST]"
            - "           SortPreservingMergeExec: [col2@1 ASC NULLS LAST, col1@0 ASC NULLS LAST, time@3 ASC NULLS LAST]"
            - "             UnionExec"
            - "               SortExec: expr=[col2@1 ASC NULLS LAST, col1@0 ASC NULLS LAST, time@3 ASC NULLS LAST], preserve_partitioning=[false]"
            - "                 RecordBatchesExec: chunks=2, projection=[col1, col2, field1, time, __chunk_order]"
            - "               DataSourceExec: file_groups={1 group: [[0.parquet]]}, projection=[col1, col2, field1, time, __chunk_order], output_ordering=[__chunk_order@4 ASC], file_type=parquet"
        "#
        );
    }

    // No limit & but the input is in the right sort preserving merge struct --> optimize
    #[test]
    fn test_spm_non_time_desc() {
        test_helpers::maybe_start_logging();

        // plan:
        //  SortPreservingMerge: [field1@2 DESC]
        //    UnionExec
        //      SortExec: expr=[field1@2 DESC]
        //        DataSourceExec
        //      SortExec: expr=[field1@2 DESC]
        //        UnionExec
        //          RecordBatchesExec
        //          DataSourceExec
        //
        // Output: 2 SortExec are swapped

        let schema = schema();
        let sort_exprs = [
            ("col2", SortOp::Asc),
            ("col1", SortOp::Asc),
            ("time", SortOp::Asc),
        ];

        let plan_parquet = PlanBuilder::data_source_exec_parquet(&schema, 1000, 2000);
        let plan_parquet2 = PlanBuilder::data_source_exec_parquet(&schema, 2001, 3000);
        let plan_batches = PlanBuilder::record_batches_exec(2, 2500, 3500);

        let plan_sort1 = plan_batches.sort(sort_exprs);
        let plan_union_1 = plan_sort1.union(plan_parquet2);
        let plan_spm_for_dedupe = plan_union_1.sort_preserving_merge(sort_exprs);
        let plan_dedupe = plan_spm_for_dedupe.deduplicate(sort_exprs, false);

        let sort_exprs = [("field1", SortOp::Desc)];
        let plan_sort1 = plan_parquet.sort(sort_exprs);
        let plan_sort2 = plan_dedupe.sort(sort_exprs);

        let plan_union_2 = plan_sort1.union(plan_sort2);

        let plan_spm = plan_union_2.sort_preserving_merge(sort_exprs);

        // Output plan: the 2 SortExecs will be swapped the order
        let opt = OrderUnionSortedInputs;
        insta::assert_yaml_snapshot!(
            OptimizationTest::new(plan_spm.build(), opt),
            @r#"
        input:
          - " SortPreservingMergeExec: [field1@2 DESC NULLS LAST]"
          - "   UnionExec"
          - "     SortExec: expr=[field1@2 DESC NULLS LAST], preserve_partitioning=[false]"
          - "       DataSourceExec: file_groups={1 group: [[0.parquet]]}, projection=[col1, col2, field1, time, __chunk_order], output_ordering=[__chunk_order@4 ASC], file_type=parquet"
          - "     SortExec: expr=[field1@2 DESC NULLS LAST], preserve_partitioning=[false]"
          - "       DeduplicateExec: [col2@1 ASC NULLS LAST,col1@0 ASC NULLS LAST,time@3 ASC NULLS LAST]"
          - "         SortPreservingMergeExec: [col2@1 ASC NULLS LAST, col1@0 ASC NULLS LAST, time@3 ASC NULLS LAST]"
          - "           UnionExec"
          - "             SortExec: expr=[col2@1 ASC NULLS LAST, col1@0 ASC NULLS LAST, time@3 ASC NULLS LAST], preserve_partitioning=[false]"
          - "               RecordBatchesExec: chunks=2, projection=[col1, col2, field1, time, __chunk_order]"
          - "             DataSourceExec: file_groups={1 group: [[0.parquet]]}, projection=[col1, col2, field1, time, __chunk_order], output_ordering=[__chunk_order@4 ASC], file_type=parquet"
        output:
          Ok:
            - " ProgressiveEvalExec: input_ranges=[(2001)->(3500), (1000)->(2000)]"
            - "   ReorderPartitionsExec: mapped_partition_indices=[1, 0]"
            - "     UnionExec"
            - "       SortExec: expr=[field1@2 DESC NULLS LAST], preserve_partitioning=[false]"
            - "         DataSourceExec: file_groups={1 group: [[0.parquet]]}, projection=[col1, col2, field1, time, __chunk_order], output_ordering=[__chunk_order@4 ASC], file_type=parquet"
            - "       SortExec: expr=[field1@2 DESC NULLS LAST], preserve_partitioning=[false]"
            - "         DeduplicateExec: [col2@1 ASC NULLS LAST,col1@0 ASC NULLS LAST,time@3 ASC NULLS LAST]"
            - "           SortPreservingMergeExec: [col2@1 ASC NULLS LAST, col1@0 ASC NULLS LAST, time@3 ASC NULLS LAST]"
            - "             UnionExec"
            - "               SortExec: expr=[col2@1 ASC NULLS LAST, col1@0 ASC NULLS LAST, time@3 ASC NULLS LAST], preserve_partitioning=[false]"
            - "                 RecordBatchesExec: chunks=2, projection=[col1, col2, field1, time, __chunk_order]"
            - "               DataSourceExec: file_groups={1 group: [[0.parquet]]}, projection=[col1, col2, field1, time, __chunk_order], output_ordering=[__chunk_order@4 ASC], file_type=parquet"
        "#
        );
    }

    // No limit & but the input is in the right sort preserving merge struct --> optimize
    #[test]
    fn test_spm_non_time_asc() {
        test_helpers::maybe_start_logging();

        let schema = schema();
        let sort_exprs = [
            ("col2", SortOp::Asc),
            ("col1", SortOp::Asc),
            ("time", SortOp::Asc),
        ];

        let plan_parquet = PlanBuilder::data_source_exec_parquet(&schema, 1000, 2000);
        let plan_parquet2 = PlanBuilder::data_source_exec_parquet(&schema, 2001, 3000);
        let plan_batches = PlanBuilder::record_batches_exec(2, 2500, 3500);

        let plan_sort1 = plan_batches.sort(sort_exprs);
        let plan_union_1 = plan_sort1.union(plan_parquet2);
        let plan_spm_for_dedupe = plan_union_1.sort_preserving_merge(sort_exprs);
        let plan_dedupe = plan_spm_for_dedupe.deduplicate(sort_exprs, false);

        let sort_exprs = [("field1", SortOp::Asc)];
        let plan_sort1 = plan_parquet.sort(sort_exprs);
        let plan_sort2 = plan_dedupe.sort(sort_exprs);

        let plan_union_2 = plan_sort1.union(plan_sort2);

        let plan_spm = plan_union_2.sort_preserving_merge(sort_exprs);

        // output stays the same as input
        let opt = OrderUnionSortedInputs;
        insta::assert_yaml_snapshot!(
            OptimizationTest::new(plan_spm.build(), opt),
            @r#"
        input:
          - " SortPreservingMergeExec: [field1@2 ASC NULLS LAST]"
          - "   UnionExec"
          - "     SortExec: expr=[field1@2 ASC NULLS LAST], preserve_partitioning=[false]"
          - "       DataSourceExec: file_groups={1 group: [[0.parquet]]}, projection=[col1, col2, field1, time, __chunk_order], output_ordering=[__chunk_order@4 ASC], file_type=parquet"
          - "     SortExec: expr=[field1@2 ASC NULLS LAST], preserve_partitioning=[false]"
          - "       DeduplicateExec: [col2@1 ASC NULLS LAST,col1@0 ASC NULLS LAST,time@3 ASC NULLS LAST]"
          - "         SortPreservingMergeExec: [col2@1 ASC NULLS LAST, col1@0 ASC NULLS LAST, time@3 ASC NULLS LAST]"
          - "           UnionExec"
          - "             SortExec: expr=[col2@1 ASC NULLS LAST, col1@0 ASC NULLS LAST, time@3 ASC NULLS LAST], preserve_partitioning=[false]"
          - "               RecordBatchesExec: chunks=2, projection=[col1, col2, field1, time, __chunk_order]"
          - "             DataSourceExec: file_groups={1 group: [[0.parquet]]}, projection=[col1, col2, field1, time, __chunk_order], output_ordering=[__chunk_order@4 ASC], file_type=parquet"
        output:
          Ok:
            - " ProgressiveEvalExec: input_ranges=[(1000)->(2000), (2001)->(3500)]"
            - "   UnionExec"
            - "     SortExec: expr=[field1@2 ASC NULLS LAST], preserve_partitioning=[false]"
            - "       DataSourceExec: file_groups={1 group: [[0.parquet]]}, projection=[col1, col2, field1, time, __chunk_order], output_ordering=[__chunk_order@4 ASC], file_type=parquet"
            - "     SortExec: expr=[field1@2 ASC NULLS LAST], preserve_partitioning=[false]"
            - "       DeduplicateExec: [col2@1 ASC NULLS LAST,col1@0 ASC NULLS LAST,time@3 ASC NULLS LAST]"
            - "         SortPreservingMergeExec: [col2@1 ASC NULLS LAST, col1@0 ASC NULLS LAST, time@3 ASC NULLS LAST]"
            - "           UnionExec"
            - "             SortExec: expr=[col2@1 ASC NULLS LAST, col1@0 ASC NULLS LAST, time@3 ASC NULLS LAST], preserve_partitioning=[false]"
            - "               RecordBatchesExec: chunks=2, projection=[col1, col2, field1, time, __chunk_order]"
            - "             DataSourceExec: file_groups={1 group: [[0.parquet]]}, projection=[col1, col2, field1, time, __chunk_order], output_ordering=[__chunk_order@4 ASC], file_type=parquet"
        "#
        );
    }

    #[test]
    fn test_spm_time_desc_with_dedupe_and_proj() {
        test_helpers::maybe_start_logging();

        // plan:
        //  SortPreservingMerge: [time@2 DESC]
        //    UnionExec
        //      SortExec: expr=[time@2 DESC]   -- time range [1000, 2000]
        //        ProjectionExec: expr=[time]
        //          DataSourceExec                           -- [1000, 2000]
        //      SortExec: expr=[time@2 DESC]   -- time range [2001, 3500] from combine time range of record batches & parquet
        //        ProjectionExec: expr=[time]
        //          DeduplicateExec: [col1, col2, time]
        //              SortPreservingMergeExec: [col1 ASC, col2 ASC, time ASC]
        //                  UnionExec
        //                      SortExec: expr=[col1 ASC, col2 ASC, time ASC]
        //                          RecordBatchesExec           -- 2 chunks [2500, 3500]
        //                      SortExec: expr=[col1 ASC, col2 ASC, time ASC]
        //                          DataSourceExec                     -- [2001, 3000]
        //
        // Output: 2 SortExec are swapped

        let schema = schema();

        let final_sort_exprs = [("time", SortOp::Desc)];

        // Sort plan of the first parquet:
        //      SortExec: expr=[time@2 DESC]   -- time range [1000, 2000]
        //        ProjectionExec: expr=[time]
        //          DataSourceExec
        let plan_parquet_1 = PlanBuilder::data_source_exec_parquet(&schema, 1000, 2000);
        let plan_projection_1 = plan_parquet_1.project(["time"]);
        let plan_sort1 = plan_projection_1.sort(final_sort_exprs);

        // Sort plan of the second parquet and the record batch
        //      SortExec: expr=[time@2 DESC]   -- time range [2001, 3500] from combine time range of record batches & parquet
        //        ProjectionExec: expr=[time]
        //          DeduplicateExec: [col1, col2, time]
        //              SortPreservingMergeExec: [col1 ASC, col2 ASC, time ASC]
        //                  UnionExec
        //                      SortExec: expr=[col1 ASC, col2 ASC, time ASC]
        //                          RecordBatchesExec           -- 2 chunks [2500, 3500]
        //                      SortExec: expr=[col1 ASC, col2 ASC, time ASC]
        //                          DataSourceExec                     -- [2001, 3000]
        let plan_parquet_2 = PlanBuilder::data_source_exec_parquet(&schema, 2001, 3000);
        let plan_batches = PlanBuilder::record_batches_exec(2, 2500, 3500);
        let dedupe_sort_exprs = [
            ("col1", SortOp::Asc),
            ("col2", SortOp::Asc),
            ("time", SortOp::Asc),
        ];
        let plan_sort_rb = plan_batches.sort(dedupe_sort_exprs);
        let plan_sort_pq = plan_parquet_2.sort(dedupe_sort_exprs);
        let plan_union_1 = plan_sort_rb.union(plan_sort_pq);
        let plan_spm_1 = plan_union_1.sort_preserving_merge(dedupe_sort_exprs);

        let plan_dedupe = plan_spm_1.deduplicate(dedupe_sort_exprs, false);
        let plan_projection_2 = plan_dedupe.project(["time"]);
        let plan_sort2 = plan_projection_2.sort(final_sort_exprs);

        // Union them together
        let plan_union_2 = plan_sort1.union(plan_sort2);

        // SortPreservingMerge them
        let plan_spm = plan_union_2.sort_preserving_merge(final_sort_exprs);

        // compute statistics
        let min_max_spm = compute_stats_column_min_max(plan_spm.inner(), "time").unwrap();
        let min_max = column_statistics_min_max(min_max_spm).unwrap();
        assert_eq!(
            min_max,
            (
                ScalarValue::TimestampNanosecond(Some(1000), None),
                ScalarValue::TimestampNanosecond(Some(3500), None)
            )
        );

        // Output plan: the 2 SortExecs will be swapped the order
        let opt = OrderUnionSortedInputs;
        insta::assert_yaml_snapshot!(
            OptimizationTest::new(plan_spm.build(), opt),
            @r#"
        input:
          - " SortPreservingMergeExec: [time@0 DESC NULLS LAST]"
          - "   UnionExec"
          - "     SortExec: expr=[time@0 DESC NULLS LAST], preserve_partitioning=[false]"
          - "       ProjectionExec: expr=[time@3 as time]"
          - "         DataSourceExec: file_groups={1 group: [[0.parquet]]}, projection=[col1, col2, field1, time, __chunk_order], output_ordering=[__chunk_order@4 ASC], file_type=parquet"
          - "     SortExec: expr=[time@0 DESC NULLS LAST], preserve_partitioning=[false]"
          - "       ProjectionExec: expr=[time@3 as time]"
          - "         DeduplicateExec: [col1@0 ASC NULLS LAST,col2@1 ASC NULLS LAST,time@3 ASC NULLS LAST]"
          - "           SortPreservingMergeExec: [col1@0 ASC NULLS LAST, col2@1 ASC NULLS LAST, time@3 ASC NULLS LAST]"
          - "             UnionExec"
          - "               SortExec: expr=[col1@0 ASC NULLS LAST, col2@1 ASC NULLS LAST, time@3 ASC NULLS LAST], preserve_partitioning=[false]"
          - "                 RecordBatchesExec: chunks=2, projection=[col1, col2, field1, time, __chunk_order]"
          - "               SortExec: expr=[col1@0 ASC NULLS LAST, col2@1 ASC NULLS LAST, time@3 ASC NULLS LAST], preserve_partitioning=[false]"
          - "                 DataSourceExec: file_groups={1 group: [[0.parquet]]}, projection=[col1, col2, field1, time, __chunk_order], output_ordering=[__chunk_order@4 ASC], file_type=parquet"
        output:
          Ok:
            - " ProgressiveEvalExec: input_ranges=[(2001)->(3500), (1000)->(2000)]"
            - "   ReorderPartitionsExec: mapped_partition_indices=[1, 0]"
            - "     UnionExec"
            - "       SortExec: expr=[time@0 DESC NULLS LAST], preserve_partitioning=[false]"
            - "         ProjectionExec: expr=[time@3 as time]"
            - "           DataSourceExec: file_groups={1 group: [[0.parquet]]}, projection=[col1, col2, field1, time, __chunk_order], output_ordering=[__chunk_order@4 ASC], file_type=parquet"
            - "       SortExec: expr=[time@0 DESC NULLS LAST], preserve_partitioning=[false]"
            - "         ProjectionExec: expr=[time@3 as time]"
            - "           DeduplicateExec: [col1@0 ASC NULLS LAST,col2@1 ASC NULLS LAST,time@3 ASC NULLS LAST]"
            - "             SortPreservingMergeExec: [col1@0 ASC NULLS LAST, col2@1 ASC NULLS LAST, time@3 ASC NULLS LAST]"
            - "               UnionExec"
            - "                 SortExec: expr=[col1@0 ASC NULLS LAST, col2@1 ASC NULLS LAST, time@3 ASC NULLS LAST], preserve_partitioning=[false]"
            - "                   RecordBatchesExec: chunks=2, projection=[col1, col2, field1, time, __chunk_order]"
            - "                 SortExec: expr=[col1@0 ASC NULLS LAST, col2@1 ASC NULLS LAST, time@3 ASC NULLS LAST], preserve_partitioning=[false]"
            - "                   DataSourceExec: file_groups={1 group: [[0.parquet]]}, projection=[col1, col2, field1, time, __chunk_order], output_ordering=[__chunk_order@4 ASC], file_type=parquet"
        "#
        );
    }

    // Test split non-overlapped parquet files in the same `DataSourceExec`
    // 5 non-overlapped files split into 3 DF partitions
    #[test]
    fn test_split_partitioned_files_in_multiple_groups() {
        test_helpers::maybe_start_logging();

        let schema = schema();

        let final_sort_exprs = [("time", SortOp::Desc)];

        // ------------------------------------------------------------------
        // Sort plan of the first parquet of 5 non-overlapped files split into 3 DF partitions:
        //      SortExec: expr=[time@2 DESC]   -- time range [1000, 1999]
        //          DataSourceExec: file_groups={3 groups: [[0.parquet, 3.parquet], [1.parquet, 4.parquet], [2.parquet]]}, ..."

        // 5 non-overlapped files split into 3 DF partitions that cover the time range [1000, 1999]
        // Larger the file name more recent their time range
        //   . 0.parquet: [1000, 1199]
        //   . 1.parquet: [1200, 1399]
        //   . 2.parquet: [1400, 1599]
        //   . 3.parquet: [1600, 1799]
        //   . 4.parquet: [1800, 1999]
        let target_partition = 3;
        let plan_parquet_1 = PlanBuilder::data_source_exec_parquet_non_overlapped_chunks(
            &schema,
            5,
            1000,
            200,
            target_partition,
        );
        insta::assert_yaml_snapshot!(
            plan_parquet_1.formatted(),
            @r#"- " DataSourceExec: file_groups={3 groups: [[0.parquet, 3.parquet], [1.parquet, 4.parquet], [2.parquet]]}, projection=[col1, col2, field1, time, __chunk_order], file_type=parquet""#
        );

        let plan_sort1 = plan_parquet_1.sort_with_preserve_partitioning(final_sort_exprs);

        let min_max_sort1 = compute_stats_column_min_max(plan_sort1.inner(), "time").unwrap();
        let min_max = column_statistics_min_max(min_max_sort1).unwrap();
        assert_eq!(
            min_max,
            (
                ScalarValue::TimestampNanosecond(Some(1000), None),
                ScalarValue::TimestampNanosecond(Some(1999), None)
            )
        );

        // ------------------------------------------------------------------
        // Sort plan of a parquet overlapped with the record batch:
        let plan_parquet_2 = PlanBuilder::data_source_exec_parquet(&schema, 2001, 3000);
        let plan_batches = PlanBuilder::record_batches_exec(2, 2500, 3500);
        let dedupe_sort_exprs = [
            ("col1", SortOp::Asc),
            ("col2", SortOp::Asc),
            ("time", SortOp::Asc),
        ];
        let plan_sort_rb = plan_batches.sort(dedupe_sort_exprs);
        let plan_union_1 = plan_sort_rb.union(plan_parquet_2);
        let plan_spm_1 = plan_union_1.sort_preserving_merge(dedupe_sort_exprs);

        let plan_dedupe = plan_spm_1.deduplicate(dedupe_sort_exprs, false);
        let plan_sort2 = plan_dedupe.sort(final_sort_exprs);

        let plan_union_2 = plan_sort1.union(plan_sort2);

        let plan_spm = plan_union_2.sort_preserving_merge(final_sort_exprs);

        // Differences between the input and output plan:
        //   1. The top SortPreservingMergeExec is replaced with ProgressiveEvalExec to get the
        //      benefits of reading its inputs sequentailly and stop when the limit hits
        //   2. The two SortExecs are swap order to have latest time range as first input to
        //      garuantee correct results
        //   3. Five non-overlapped parquet files are now in 5 different groups one each and
        //      sorted by final_sort_exprs which is time DESC. This is needed to ensure files are
        //      executed sequentially, one by one, and the latest time range is read first by the ProgressiveEvalExec above
        //      - File are sorted in time descending order: 4.parquet, 3.parquet, 2.parquet, 1.parquet, 0.parquet
        //      - Larger the file name represents more recent their time range
        let opt = OrderUnionSortedInputs;
        insta::assert_yaml_snapshot!(
            OptimizationTest::new(plan_spm.build(), opt),
            @r#"
        input:
          - " SortPreservingMergeExec: [time@3 DESC NULLS LAST]"
          - "   UnionExec"
          - "     SortExec: expr=[time@3 DESC NULLS LAST], preserve_partitioning=[true]"
          - "       DataSourceExec: file_groups={3 groups: [[0.parquet, 3.parquet], [1.parquet, 4.parquet], [2.parquet]]}, projection=[col1, col2, field1, time, __chunk_order], file_type=parquet"
          - "     SortExec: expr=[time@3 DESC NULLS LAST], preserve_partitioning=[false]"
          - "       DeduplicateExec: [col1@0 ASC NULLS LAST,col2@1 ASC NULLS LAST,time@3 ASC NULLS LAST]"
          - "         SortPreservingMergeExec: [col1@0 ASC NULLS LAST, col2@1 ASC NULLS LAST, time@3 ASC NULLS LAST]"
          - "           UnionExec"
          - "             SortExec: expr=[col1@0 ASC NULLS LAST, col2@1 ASC NULLS LAST, time@3 ASC NULLS LAST], preserve_partitioning=[false]"
          - "               RecordBatchesExec: chunks=2, projection=[col1, col2, field1, time, __chunk_order]"
          - "             DataSourceExec: file_groups={1 group: [[0.parquet]]}, projection=[col1, col2, field1, time, __chunk_order], output_ordering=[__chunk_order@4 ASC], file_type=parquet"
        output:
          Ok:
            - " ProgressiveEvalExec: input_ranges=[(2001)->(3500), (1800)->(1999), (1600)->(1799), (1400)->(1599), (1200)->(1399), (1000)->(1199)]"
            - "   ReorderPartitionsExec: mapped_partition_indices=[5, 0, 1, 2, 3, 4]"
            - "     UnionExec"
            - "       SortExec: expr=[time@3 DESC NULLS LAST], preserve_partitioning=[true]"
            - "         DataSourceExec: file_groups={5 groups: [[4.parquet], [3.parquet], [2.parquet], [1.parquet], [0.parquet]]}, projection=[col1, col2, field1, time, __chunk_order], output_ordering=[__chunk_order@4 ASC], file_type=parquet"
            - "       SortExec: expr=[time@3 DESC NULLS LAST], preserve_partitioning=[false]"
            - "         DeduplicateExec: [col1@0 ASC NULLS LAST,col2@1 ASC NULLS LAST,time@3 ASC NULLS LAST]"
            - "           SortPreservingMergeExec: [col1@0 ASC NULLS LAST, col2@1 ASC NULLS LAST, time@3 ASC NULLS LAST]"
            - "             UnionExec"
            - "               SortExec: expr=[col1@0 ASC NULLS LAST, col2@1 ASC NULLS LAST, time@3 ASC NULLS LAST], preserve_partitioning=[false]"
            - "                 RecordBatchesExec: chunks=2, projection=[col1, col2, field1, time, __chunk_order]"
            - "               DataSourceExec: file_groups={1 group: [[0.parquet]]}, projection=[col1, col2, field1, time, __chunk_order], output_ordering=[__chunk_order@4 ASC], file_type=parquet"
        "#
        );
    }

    // Test split non-overlapped parquet files in the same `DataSourceExec`
    // 5 non-overlapped files all in the same DF partition/group and preserve_partitioning is set to true
    #[test]
    fn test_split_partitioned_files_in_one_group() {
        test_helpers::maybe_start_logging();

        let schema = schema();

        let final_sort_exprs = [("time", SortOp::Desc)];

        // ------------------------------------------------------------------
        // Sort plan of the first parquet of 5 non-overlapped files all in one group:
        //      SortExec: expr=[time@2 DESC]   -- time range [1000, 1999]
        //          DataSourceExec: file_groups={1 group: [[0.parquet, 1.parquet, 2.parquet, 3.parquet, 4.parquet]]}, ..."

        // 5 non-overlapped files all in one DF partitions that cover the time range [1000, 1999]
        // Larger the file name more recent their time range
        //   . 0.parquet: [1000, 1199]
        //   . 1.parquet: [1200, 1399]
        //   . 2.parquet: [1400, 1599]
        //   . 3.parquet: [1600, 1799]
        //   . 4.parquet: [1800, 1999]
        let target_partition = 1;
        let plan_parquet_1 = PlanBuilder::data_source_exec_parquet_non_overlapped_chunks(
            &schema,
            5,
            1000,
            200,
            target_partition,
        );
        insta::assert_yaml_snapshot!(
            plan_parquet_1.formatted(),
            @r#"- " DataSourceExec: file_groups={1 group: [[0.parquet, 1.parquet, 2.parquet, 3.parquet, 4.parquet]]}, projection=[col1, col2, field1, time, __chunk_order], file_type=parquet""#
        );

        let plan_sort1 = plan_parquet_1.sort_with_preserve_partitioning(final_sort_exprs);
        // verify preserve_partitioning is set to true from the function above
        insta::assert_yaml_snapshot!(
            plan_sort1.formatted(),
            @r#"
        - " SortExec: expr=[time@3 DESC NULLS LAST], preserve_partitioning=[true]"
        - "   DataSourceExec: file_groups={1 group: [[0.parquet, 1.parquet, 2.parquet, 3.parquet, 4.parquet]]}, projection=[col1, col2, field1, time, __chunk_order], file_type=parquet"
        "#
        );

        let min_max_sort1 = compute_stats_column_min_max(plan_sort1.inner(), "time").unwrap();
        let min_max = column_statistics_min_max(min_max_sort1).unwrap();
        assert_eq!(
            min_max,
            (
                ScalarValue::TimestampNanosecond(Some(1000), None),
                ScalarValue::TimestampNanosecond(Some(1999), None)
            )
        );

        // ------------------------------------------------------------------
        // Sort plan of a parquet overlapped with the record batch:
        let plan_parquet_2 = PlanBuilder::data_source_exec_parquet(&schema, 2001, 3000);
        let plan_batches = PlanBuilder::record_batches_exec(2, 2500, 3500);
        let dedupe_sort_exprs = [
            ("col1", SortOp::Asc),
            ("col2", SortOp::Asc),
            ("time", SortOp::Asc),
        ];
        let plan_sort_rb = plan_batches.sort(dedupe_sort_exprs);
        let plan_sort_pq = plan_parquet_2.sort(dedupe_sort_exprs);
        let plan_union_1 = plan_sort_rb.union(plan_sort_pq);
        let plan_spm_1 = plan_union_1.sort_preserving_merge(dedupe_sort_exprs);

        let plan_dedupe = plan_spm_1.deduplicate(dedupe_sort_exprs, false);
        let plan_sort2 = plan_dedupe.sort(final_sort_exprs);

        let plan_union_2 = plan_sort1.union(plan_sort2);

        let plan_spm = plan_union_2.sort_preserving_merge(final_sort_exprs);

        // Differences between the input and output plan:
        //   1. The top SortPreservingMergeExec is replaced with ProgressiveEvalExec
        //   2. The two SortExecs are swap order to have latest time range first
        //   3. Five non-overlapped parquet files are now in 5 different groups one eachand sorted by final_sort_exprs which is time DESC
        //      File are sorted in time descending order: 4.parquet, 3.parquet, 2.parquet, 1.parquet, 0.parquet
        //      Larger the file name more recent their time range
        let opt = OrderUnionSortedInputs;
        insta::assert_yaml_snapshot!(
            OptimizationTest::new(plan_spm.build(), opt),
            @r#"
        input:
          - " SortPreservingMergeExec: [time@3 DESC NULLS LAST]"
          - "   UnionExec"
          - "     SortExec: expr=[time@3 DESC NULLS LAST], preserve_partitioning=[true]"
          - "       DataSourceExec: file_groups={1 group: [[0.parquet, 1.parquet, 2.parquet, 3.parquet, 4.parquet]]}, projection=[col1, col2, field1, time, __chunk_order], file_type=parquet"
          - "     SortExec: expr=[time@3 DESC NULLS LAST], preserve_partitioning=[false]"
          - "       DeduplicateExec: [col1@0 ASC NULLS LAST,col2@1 ASC NULLS LAST,time@3 ASC NULLS LAST]"
          - "         SortPreservingMergeExec: [col1@0 ASC NULLS LAST, col2@1 ASC NULLS LAST, time@3 ASC NULLS LAST]"
          - "           UnionExec"
          - "             SortExec: expr=[col1@0 ASC NULLS LAST, col2@1 ASC NULLS LAST, time@3 ASC NULLS LAST], preserve_partitioning=[false]"
          - "               RecordBatchesExec: chunks=2, projection=[col1, col2, field1, time, __chunk_order]"
          - "             SortExec: expr=[col1@0 ASC NULLS LAST, col2@1 ASC NULLS LAST, time@3 ASC NULLS LAST], preserve_partitioning=[false]"
          - "               DataSourceExec: file_groups={1 group: [[0.parquet]]}, projection=[col1, col2, field1, time, __chunk_order], output_ordering=[__chunk_order@4 ASC], file_type=parquet"
        output:
          Ok:
            - " ProgressiveEvalExec: input_ranges=[(2001)->(3500), (1800)->(1999), (1600)->(1799), (1400)->(1599), (1200)->(1399), (1000)->(1199)]"
            - "   ReorderPartitionsExec: mapped_partition_indices=[5, 0, 1, 2, 3, 4]"
            - "     UnionExec"
            - "       SortExec: expr=[time@3 DESC NULLS LAST], preserve_partitioning=[true]"
            - "         DataSourceExec: file_groups={5 groups: [[4.parquet], [3.parquet], [2.parquet], [1.parquet], [0.parquet]]}, projection=[col1, col2, field1, time, __chunk_order], output_ordering=[__chunk_order@4 ASC], file_type=parquet"
            - "       SortExec: expr=[time@3 DESC NULLS LAST], preserve_partitioning=[false]"
            - "         DeduplicateExec: [col1@0 ASC NULLS LAST,col2@1 ASC NULLS LAST,time@3 ASC NULLS LAST]"
            - "           SortPreservingMergeExec: [col1@0 ASC NULLS LAST, col2@1 ASC NULLS LAST, time@3 ASC NULLS LAST]"
            - "             UnionExec"
            - "               SortExec: expr=[col1@0 ASC NULLS LAST, col2@1 ASC NULLS LAST, time@3 ASC NULLS LAST], preserve_partitioning=[false]"
            - "                 RecordBatchesExec: chunks=2, projection=[col1, col2, field1, time, __chunk_order]"
            - "               SortExec: expr=[col1@0 ASC NULLS LAST, col2@1 ASC NULLS LAST, time@3 ASC NULLS LAST], preserve_partitioning=[false]"
            - "                 DataSourceExec: file_groups={1 group: [[0.parquet]]}, projection=[col1, col2, field1, time, __chunk_order], output_ordering=[__chunk_order@4 ASC], file_type=parquet"
        "#
        );
    }

    // Reproducer of https://github.com/influxdata/influxdb_iox/issues/12584
    // Test split non-overlapped parquet files in the same `DataSourceExec`
    // 5 non-overlapped files all in the same DF partition/group and preserve_partitioning is set to false
    #[test]
    fn test_split_partitioned_files_in_one_group_and_preserve_partitioning_is_false() {
        test_helpers::maybe_start_logging();

        let schema = schema();

        let final_sort_exprs = [("time", SortOp::Desc)];

        // ------------------------------------------------------------------
        // Sort plan of the first parquet of 5 non-overlapped files all in one group:
        //      SortExec: expr=[time@2 DESC]   -- time range [1000, 1999]
        //          DataSourceExec: file_groups={1 group: [[0.parquet, 1.parquet, 2.parquet, 3.parquet, 4.parquet]]}, ..."

        // 5 non-overlapped files all in one DF partitions that cover the time range [1000, 1999]
        // Larger the file name more recent their time range
        //   . 0.parquet: [1000, 1199]
        //   . 1.parquet: [1200, 1399]
        //   . 2.parquet: [1400, 1599]
        //   . 3.parquet: [1600, 1799]
        //   . 4.parquet: [1800, 1999]
        let target_partition = 1;
        let plan_parquet_1 = PlanBuilder::data_source_exec_parquet_non_overlapped_chunks(
            &schema,
            5,
            1000,
            200,
            target_partition,
        );
        insta::assert_yaml_snapshot!(
            plan_parquet_1.formatted(),
            @r#"- " DataSourceExec: file_groups={1 group: [[0.parquet, 1.parquet, 2.parquet, 3.parquet, 4.parquet]]}, projection=[col1, col2, field1, time, __chunk_order], file_type=parquet""#
        );

        let plan_sort1 =
            plan_parquet_1.sort_with_preserve_partitioning_setting(final_sort_exprs, false);
        // verify preserve_partitioning is false
        insta::assert_yaml_snapshot!(
            plan_sort1.formatted(),
            @r#"
        - " SortExec: expr=[time@3 DESC NULLS LAST], preserve_partitioning=[false]"
        - "   DataSourceExec: file_groups={1 group: [[0.parquet, 1.parquet, 2.parquet, 3.parquet, 4.parquet]]}, projection=[col1, col2, field1, time, __chunk_order], file_type=parquet"
        "#
        );

        let min_max_sort1 = compute_stats_column_min_max(plan_sort1.inner(), "time").unwrap();
        let min_max = column_statistics_min_max(min_max_sort1).unwrap();
        assert_eq!(
            min_max,
            (
                ScalarValue::TimestampNanosecond(Some(1000), None),
                ScalarValue::TimestampNanosecond(Some(1999), None)
            )
        );

        // ------------------------------------------------------------------
        // Sort plan of a parquet overlapped with the record batch
        // There must be SortPreservingMergeExec and DeduplicateExec in this subplan to deduplicate data of
        //   the record batch and the parquet file
        let plan_parquet_2 = PlanBuilder::data_source_exec_parquet(&schema, 2001, 3000);
        let plan_batches = PlanBuilder::record_batches_exec(2, 2500, 3500);
        let dedupe_sort_exprs = [
            ("col1", SortOp::Asc),
            ("col2", SortOp::Asc),
            ("time", SortOp::Asc),
        ];
        let plan_sort_rb = plan_batches.sort(dedupe_sort_exprs);
        let plan_sort_pq = plan_parquet_2.sort(dedupe_sort_exprs);
        let plan_union_1 = plan_sort_rb.union(plan_sort_pq);
        let plan_spm_1 = plan_union_1.sort_preserving_merge(dedupe_sort_exprs);

        let plan_dedupe = plan_spm_1.deduplicate(dedupe_sort_exprs, false);
        let plan_sort2 = plan_dedupe.sort(final_sort_exprs);

        // Union them together
        let plan_union_2 = plan_sort1.union(plan_sort2);

        // SortPreservingMerge them
        let plan_spm = plan_union_2.sort_preserving_merge(final_sort_exprs);

        // Differences between the input and output plan:
        //   1. The top SortPreservingMergeExec is replaced with ProgressiveEvalExec
        //   2. The two SortExecs are swap order to have latest time range first
        //   3. Five non-overlapped parquet files are now in 5 different groups one eachand sorted by final_sort_exprs which is time DESC
        //      File are sorted in time descending order: 4.parquet, 3.parquet, 2.parquet, 1.parquet, 0.parquet
        //      Larger the file name more recent their time range
        //   4. The  SortExec of the non-overlapped parquet files is now have preserve_partitioning set to true
        let opt = OrderUnionSortedInputs;
        insta::assert_yaml_snapshot!(
            OptimizationTest::new(plan_spm.build(), opt),
            @r#"
        input:
          - " SortPreservingMergeExec: [time@3 DESC NULLS LAST]"
          - "   UnionExec"
          - "     SortExec: expr=[time@3 DESC NULLS LAST], preserve_partitioning=[false]"
          - "       DataSourceExec: file_groups={1 group: [[0.parquet, 1.parquet, 2.parquet, 3.parquet, 4.parquet]]}, projection=[col1, col2, field1, time, __chunk_order], file_type=parquet"
          - "     SortExec: expr=[time@3 DESC NULLS LAST], preserve_partitioning=[false]"
          - "       DeduplicateExec: [col1@0 ASC NULLS LAST,col2@1 ASC NULLS LAST,time@3 ASC NULLS LAST]"
          - "         SortPreservingMergeExec: [col1@0 ASC NULLS LAST, col2@1 ASC NULLS LAST, time@3 ASC NULLS LAST]"
          - "           UnionExec"
          - "             SortExec: expr=[col1@0 ASC NULLS LAST, col2@1 ASC NULLS LAST, time@3 ASC NULLS LAST], preserve_partitioning=[false]"
          - "               RecordBatchesExec: chunks=2, projection=[col1, col2, field1, time, __chunk_order]"
          - "             SortExec: expr=[col1@0 ASC NULLS LAST, col2@1 ASC NULLS LAST, time@3 ASC NULLS LAST], preserve_partitioning=[false]"
          - "               DataSourceExec: file_groups={1 group: [[0.parquet]]}, projection=[col1, col2, field1, time, __chunk_order], output_ordering=[__chunk_order@4 ASC], file_type=parquet"
        output:
          Ok:
            - " ProgressiveEvalExec: input_ranges=[(2001)->(3500), (1000)->(1999)]"
            - "   ReorderPartitionsExec: mapped_partition_indices=[1, 0]"
            - "     UnionExec"
            - "       SortExec: expr=[time@3 DESC NULLS LAST], preserve_partitioning=[false]"
            - "         DataSourceExec: file_groups={1 group: [[0.parquet, 1.parquet, 2.parquet, 3.parquet, 4.parquet]]}, projection=[col1, col2, field1, time, __chunk_order], file_type=parquet"
            - "       SortExec: expr=[time@3 DESC NULLS LAST], preserve_partitioning=[false]"
            - "         DeduplicateExec: [col1@0 ASC NULLS LAST,col2@1 ASC NULLS LAST,time@3 ASC NULLS LAST]"
            - "           SortPreservingMergeExec: [col1@0 ASC NULLS LAST, col2@1 ASC NULLS LAST, time@3 ASC NULLS LAST]"
            - "             UnionExec"
            - "               SortExec: expr=[col1@0 ASC NULLS LAST, col2@1 ASC NULLS LAST, time@3 ASC NULLS LAST], preserve_partitioning=[false]"
            - "                 RecordBatchesExec: chunks=2, projection=[col1, col2, field1, time, __chunk_order]"
            - "               SortExec: expr=[col1@0 ASC NULLS LAST, col2@1 ASC NULLS LAST, time@3 ASC NULLS LAST], preserve_partitioning=[false]"
            - "                 DataSourceExec: file_groups={1 group: [[0.parquet]]}, projection=[col1, col2, field1, time, __chunk_order], output_ordering=[__chunk_order@4 ASC], file_type=parquet"
        "#
        );
    }

    // Test mix of split non-overlapped parquet files in the same `DataSourceExec` & non split on overllaped parquet files
    //  . One `DataSourceExec` with 5 non-overlapped files split into 3 DF partitions
    //  . One `DataSourceExec` with 2 overlapped files
    #[test]
    fn test_split_mix() {
        test_helpers::maybe_start_logging();

        let schema = schema();

        let final_sort_exprs = [("time", SortOp::Desc)];

        // ------------------------------------------------------------------
        // Sort plan of the first parquet of 5 non-overlapped files split into 3 DF partitions:
        //      SortExec: expr=[time@2 DESC]   -- time range [1000, 1999]
        //          DataSourceExec: file_groups={3 groups: [[0.parquet, 3.parquet], [1.parquet, 4.parquet], [2.parquet]]}, ..."

        // 5 non-overlapped files split into 3 DF partitions that cover the time range [1000, 1999]
        // Larger the file name more recent their time range
        //   . 0.parquet: [1000, 1199]
        //   . 1.parquet: [1200, 1399]
        //   . 2.parquet: [1400, 1599]
        //   . 3.parquet: [1600, 1799]
        //   . 4.parquet: [1800, 1999]
        let target_partition = 3;
        let plan_parquet_1 = PlanBuilder::data_source_exec_parquet_non_overlapped_chunks(
            &schema,
            5,
            1000,
            200,
            target_partition,
        );
        insta::assert_yaml_snapshot!(
            plan_parquet_1.formatted(),
            @r#"- " DataSourceExec: file_groups={3 groups: [[0.parquet, 3.parquet], [1.parquet, 4.parquet], [2.parquet]]}, projection=[col1, col2, field1, time, __chunk_order], file_type=parquet""#
        );

        let plan_sort_1 = plan_parquet_1.sort_with_preserve_partitioning(final_sort_exprs);
        let min_max_sort1 = compute_stats_column_min_max(plan_sort_1.inner(), "time").unwrap();
        let min_max = column_statistics_min_max(min_max_sort1).unwrap();
        assert_eq!(
            min_max,
            (
                ScalarValue::TimestampNanosecond(Some(1000), None),
                ScalarValue::TimestampNanosecond(Some(1999), None)
            )
        );

        // ------------------------------------------------------------------
        // Sort plan of a parquet overlapped files
        // Two overlapped files [2001, 2202] and [2201, 2402]
        let plan_parquet_2 =
            PlanBuilder::data_source_exec_parquet_overlapped_chunks(&schema, 3, 2001, 200, 2);
        insta::assert_yaml_snapshot!(
            plan_parquet_2.formatted(),
            @r#"- " DataSourceExec: file_groups={2 groups: [[0.parquet, 2.parquet], [1.parquet]]}, projection=[col1, col2, field1, time, __chunk_order], file_type=parquet""#
        );

        // sort expression for deduplication
        let sort_exprs = [
            ("col2", SortOp::Asc),
            ("col1", SortOp::Asc),
            ("time", SortOp::Asc),
        ];
        let plan_spm_2 = plan_parquet_2.sort_preserving_merge(sort_exprs);
        let plan_dedup_2 = plan_spm_2.deduplicate(sort_exprs, false);
        let plan_sort_2 = plan_dedup_2.sort(final_sort_exprs);

        // union 2 plan
        let plan_union = plan_sort_1.union(plan_sort_2);

        let plan_spm = plan_union.sort_preserving_merge(final_sort_exprs);

        // Differences between the input and output plan:
        //   1. The top SortPreservingMergeExec is replaced with ProgressiveEvalExec
        //   2. The two SortExecs are swap order to have latest time range first
        //   3. Five non-overlapped parquet files are now in 5 different groups one eachand sorted by final_sort_exprs which is time DESC
        //      File are sorted in time descending order: 4.parquet, 3.parquet, 2.parquet, 1.parquet, 0.parquet
        //      Larger the file name more recent their time range
        //  Note that the other `DataSourceExec` with 2 non-overlapped files are not split
        let opt = OrderUnionSortedInputs;
        insta::assert_yaml_snapshot!(
            OptimizationTest::new(plan_spm.build(), opt),
            @r#"
        input:
          - " SortPreservingMergeExec: [time@3 DESC NULLS LAST]"
          - "   UnionExec"
          - "     SortExec: expr=[time@3 DESC NULLS LAST], preserve_partitioning=[true]"
          - "       DataSourceExec: file_groups={3 groups: [[0.parquet, 3.parquet], [1.parquet, 4.parquet], [2.parquet]]}, projection=[col1, col2, field1, time, __chunk_order], file_type=parquet"
          - "     SortExec: expr=[time@3 DESC NULLS LAST], preserve_partitioning=[false]"
          - "       DeduplicateExec: [col2@1 ASC NULLS LAST,col1@0 ASC NULLS LAST,time@3 ASC NULLS LAST]"
          - "         SortPreservingMergeExec: [col2@1 ASC NULLS LAST, col1@0 ASC NULLS LAST, time@3 ASC NULLS LAST]"
          - "           DataSourceExec: file_groups={2 groups: [[0.parquet, 2.parquet], [1.parquet]]}, projection=[col1, col2, field1, time, __chunk_order], file_type=parquet"
        output:
          Ok:
            - " ProgressiveEvalExec: input_ranges=[(2001)->(2602), (1800)->(1999), (1600)->(1799), (1400)->(1599), (1200)->(1399), (1000)->(1199)]"
            - "   ReorderPartitionsExec: mapped_partition_indices=[5, 0, 1, 2, 3, 4]"
            - "     UnionExec"
            - "       SortExec: expr=[time@3 DESC NULLS LAST], preserve_partitioning=[true]"
            - "         DataSourceExec: file_groups={5 groups: [[4.parquet], [3.parquet], [2.parquet], [1.parquet], [0.parquet]]}, projection=[col1, col2, field1, time, __chunk_order], output_ordering=[__chunk_order@4 ASC], file_type=parquet"
            - "       SortExec: expr=[time@3 DESC NULLS LAST], preserve_partitioning=[false]"
            - "         DeduplicateExec: [col2@1 ASC NULLS LAST,col1@0 ASC NULLS LAST,time@3 ASC NULLS LAST]"
            - "           SortPreservingMergeExec: [col2@1 ASC NULLS LAST, col1@0 ASC NULLS LAST, time@3 ASC NULLS LAST]"
            - "             DataSourceExec: file_groups={2 groups: [[0.parquet, 2.parquet], [1.parquet]]}, projection=[col1, col2, field1, time, __chunk_order], file_type=parquet"
        "#
        );
    }

    // Right stucture and sort on 2 columns --> optimize
    #[test]
    fn test_spm_2_column_sort_desc() {
        test_helpers::maybe_start_logging();

        // plan:
        //  SortPreservingMerge: [time@3 DESC, field1@2 DESC]
        //    UnionExec
        //      SortExec: expr=[time@3 DESC, field1@2 DESC]
        //        DataSourceExec
        //      SortExec: expr=[time@3 DESC, field1@2 DESC]
        //        UnionExec
        //          RecordBatchesExec
        //          DataSourceExec
        //
        // Output: same as input

        let schema = schema();

        let plan_parquet = PlanBuilder::data_source_exec_parquet(&schema, 1000, 2000);
        let plan_parquet2 = PlanBuilder::data_source_exec_parquet(&schema, 2001, 3000);
        let plan_batches = PlanBuilder::record_batches_exec(2, 2500, 3500);

        let plan_union_1 = plan_batches.union(plan_parquet2);

        let sort_exprs = [("time", SortOp::Desc), ("field1", SortOp::Desc)];
        let plan_sort1 = plan_parquet.sort(sort_exprs);
        let plan_sort2 = plan_union_1.sort(sort_exprs);

        let plan_union_2 = plan_sort1.union(plan_sort2);

        let plan_spm = plan_union_2.sort_preserving_merge(sort_exprs);

        // input and output are the same
        let opt = OrderUnionSortedInputs;
        insta::assert_yaml_snapshot!(
            OptimizationTest::new(plan_spm.build(), opt),
            @r#"
        input:
          - " SortPreservingMergeExec: [time@3 DESC NULLS LAST, field1@2 DESC NULLS LAST]"
          - "   UnionExec"
          - "     SortExec: expr=[time@3 DESC NULLS LAST, field1@2 DESC NULLS LAST], preserve_partitioning=[false]"
          - "       DataSourceExec: file_groups={1 group: [[0.parquet]]}, projection=[col1, col2, field1, time, __chunk_order], output_ordering=[__chunk_order@4 ASC], file_type=parquet"
          - "     SortExec: expr=[time@3 DESC NULLS LAST, field1@2 DESC NULLS LAST], preserve_partitioning=[false]"
          - "       UnionExec"
          - "         RecordBatchesExec: chunks=2, projection=[col1, col2, field1, time, __chunk_order]"
          - "         DataSourceExec: file_groups={1 group: [[0.parquet]]}, projection=[col1, col2, field1, time, __chunk_order], output_ordering=[__chunk_order@4 ASC], file_type=parquet"
        output:
          Ok:
            - " ProgressiveEvalExec: input_ranges=[(2001,2001)->(3500,3500), (1000,1000)->(2000,2000)]"
            - "   ReorderPartitionsExec: mapped_partition_indices=[1, 0]"
            - "     UnionExec"
            - "       SortExec: expr=[time@3 DESC NULLS LAST, field1@2 DESC NULLS LAST], preserve_partitioning=[false]"
            - "         DataSourceExec: file_groups={1 group: [[0.parquet]]}, projection=[col1, col2, field1, time, __chunk_order], output_ordering=[__chunk_order@4 ASC], file_type=parquet"
            - "       SortExec: expr=[time@3 DESC NULLS LAST, field1@2 DESC NULLS LAST], preserve_partitioning=[false]"
            - "         UnionExec"
            - "           RecordBatchesExec: chunks=2, projection=[col1, col2, field1, time, __chunk_order]"
            - "           DataSourceExec: file_groups={1 group: [[0.parquet]]}, projection=[col1, col2, field1, time, __chunk_order], output_ordering=[__chunk_order@4 ASC], file_type=parquet"
        "#
        );
    }

    // right structure and same sort order
    // inputs of union touch, but do not overlap --> optimize
    #[test]
    fn test_touching_ranges() {
        test_helpers::maybe_start_logging();

        // Input plan:
        //
        // GlobalLimitExec: skip=0, fetch=2
        //  SortPreservingMerge: [time@2 DESC]
        //    UnionExec
        //      SortExec: expr=[time@2 DESC]   -- time range [1000, 2000]  that overlaps with the other SorExec
        //        DataSourceExec                         -- [1000, 2000]
        //      SortExec: expr=[time@2 DESC]   -- time range [2000, 3500] from combine time range of two record batches
        //        UnionExec
        //           SortExec: expr=[time@2 DESC]
        //              RecordBatchesExec             -- 2 chunks [2500, 3500]
        //           DataSourceExec                      -- [2000, 3000]

        let schema = schema();
        let sort_exprs = [
            ("col2", SortOp::Asc),
            ("col1", SortOp::Asc),
            ("time", SortOp::Asc),
        ];

        let plan_parquet = PlanBuilder::data_source_exec_parquet(&schema, 1000, 2000);
        let plan_parquet2 = PlanBuilder::data_source_exec_parquet(&schema, 2000, 3000);
        let plan_batches = PlanBuilder::record_batches_exec(2, 2500, 3500);

        let plan_sort1 = plan_batches.sort(sort_exprs);
        let plan_union_1 = plan_sort1.union(plan_parquet2);

        let sort_exprs = [("time", SortOp::Desc)];
        let plan_sort2 = plan_parquet.sort(sort_exprs);
        let plan_sort3 = plan_union_1.sort(sort_exprs);

        let plan_union_2 = plan_sort2.union(plan_sort3);

        let plan_spm = plan_union_2.sort_preserving_merge(sort_exprs);

        let plan_limit = plan_spm.limit(0, Some(1));

        // input and output are the same
        let opt = OrderUnionSortedInputs;
        insta::assert_yaml_snapshot!(
            OptimizationTest::new(plan_limit.build(), opt),
            @r#"
        input:
          - " GlobalLimitExec: skip=0, fetch=1"
          - "   SortPreservingMergeExec: [time@3 DESC NULLS LAST]"
          - "     UnionExec"
          - "       SortExec: expr=[time@3 DESC NULLS LAST], preserve_partitioning=[false]"
          - "         DataSourceExec: file_groups={1 group: [[0.parquet]]}, projection=[col1, col2, field1, time, __chunk_order], output_ordering=[__chunk_order@4 ASC], file_type=parquet"
          - "       SortExec: expr=[time@3 DESC NULLS LAST], preserve_partitioning=[false]"
          - "         UnionExec"
          - "           SortExec: expr=[col2@1 ASC NULLS LAST, col1@0 ASC NULLS LAST, time@3 ASC NULLS LAST], preserve_partitioning=[false]"
          - "             RecordBatchesExec: chunks=2, projection=[col1, col2, field1, time, __chunk_order]"
          - "           DataSourceExec: file_groups={1 group: [[0.parquet]]}, projection=[col1, col2, field1, time, __chunk_order], output_ordering=[__chunk_order@4 ASC], file_type=parquet"
        output:
          Ok:
            - " GlobalLimitExec: skip=0, fetch=1"
            - "   ProgressiveEvalExec: input_ranges=[(2000)->(3500), (1000)->(2000)]"
            - "     ReorderPartitionsExec: mapped_partition_indices=[1, 0]"
            - "       UnionExec"
            - "         SortExec: expr=[time@3 DESC NULLS LAST], preserve_partitioning=[false]"
            - "           DataSourceExec: file_groups={1 group: [[0.parquet]]}, projection=[col1, col2, field1, time, __chunk_order], output_ordering=[__chunk_order@4 ASC], file_type=parquet"
            - "         SortExec: expr=[time@3 DESC NULLS LAST], preserve_partitioning=[false]"
            - "           UnionExec"
            - "             SortExec: expr=[col2@1 ASC NULLS LAST, col1@0 ASC NULLS LAST, time@3 ASC NULLS LAST], preserve_partitioning=[false]"
            - "               RecordBatchesExec: chunks=2, projection=[col1, col2, field1, time, __chunk_order]"
            - "             DataSourceExec: file_groups={1 group: [[0.parquet]]}, projection=[col1, col2, field1, time, __chunk_order], output_ordering=[__chunk_order@4 ASC], file_type=parquet"
        "#
        );
    }

    // Projection expression (field + field)
    // but the sort order is not on field, only time ==> optimize
    #[test]
    fn test_spm_time_desc_with_dedupe_and_proj_on_expr() {
        test_helpers::maybe_start_logging();

        // plan:
        //  SortPreservingMerge: [time@2 DESC]
        //    UnionExec
        //      SortExec: expr=[time@2 DESC]   -- time range [1000, 2000]
        //        ProjectionExec: expr=[field1 + field1, time]                                <-- NOTE: has expresssion col1+col2
        //          DataSourceExec                           -- [1000, 2000]
        //      SortExec: expr=[time@2 DESC]   -- time range [2001, 3500] from combine time range of record batches & parquet
        //        ProjectionExec: expr=[field1 + field1, time]                                <-- NOTE: has expresssion col1+col2
        //          DeduplicateExec: [col1, col2, time]
        //              SortPreservingMergeExec: [col1 ASC, col2 ASC, time ASC]
        //                  UnionExec
        //                      SortExec: expr=[col1 ASC, col2 ASC, time ASC]
        //                          RecordBatchesExec           -- 2 chunks [2500, 3500]
        //                      SortExec: expr=[col1 ASC, col2 ASC, time ASC]
        //                          DataSourceExec                     -- [2001, 3000]

        let schema = schema();

        let final_sort_exprs = [("time", SortOp::Desc)];

        // Sort plan of the first parquet:
        //      SortExec: expr=[time@2 DESC]   -- time range [1000, 2000]
        //        ProjectionExec: expr=[field1 + field1, time]
        //          DataSourceExec
        let plan_parquet_1 = PlanBuilder::data_source_exec_parquet(&schema, 1000, 2000);

        let field_expr = Arc::new(BinaryExpr::new(
            Arc::new(Column::new_with_schema("field1", &schema).unwrap()),
            Operator::Plus,
            Arc::new(Column::new_with_schema("field1", &schema).unwrap()),
        )) as Arc<dyn PhysicalExpr>;
        let project_exprs = vec![
            (Arc::clone(&field_expr), String::from("field")),
            (
                expr_col("time", &plan_parquet_1.schema()),
                String::from("time"),
            ),
        ];
        let plan_projection_1 = plan_parquet_1.project_with_exprs(project_exprs);

        let plan_sort1 = plan_projection_1.sort(final_sort_exprs);

        // Sort plan of the second parquet and the record batch
        //      SortExec: expr=[time@2 DESC]   -- time range [2001, 3500] from combine time range of record batches & parquet
        //        ProjectionExec: expr=[field1 + field1, time]
        //          DeduplicateExec: [col1, col2, time]
        //              SortPreservingMergeExec: [col1 ASC, col2 ASC, time ASC]
        //                  UnionExec
        //                      SortExec: expr=[col1 ASC, col2 ASC, time ASC]
        //                          RecordBatchesExec           -- 2 chunks [2500, 3500]
        //                      SortExec: expr=[col1 ASC, col2 ASC, time ASC]
        //                          DataSourceExec                     -- [2001, 3000]
        let plan_parquet_2 = PlanBuilder::data_source_exec_parquet(&schema, 2001, 3000);
        let plan_batches = PlanBuilder::record_batches_exec(2, 2500, 3500);
        let dedupe_sort_exprs = [
            ("col1", SortOp::Asc),
            ("col2", SortOp::Asc),
            ("time", SortOp::Asc),
        ];
        let plan_sort_rb = plan_batches.sort(dedupe_sort_exprs);
        let plan_sort_pq = plan_parquet_2.sort(dedupe_sort_exprs);
        let plan_union_1 = plan_sort_rb.union(plan_sort_pq);
        let plan_spm_1 = plan_union_1.sort_preserving_merge(dedupe_sort_exprs);

        let plan_dedupe = plan_spm_1.deduplicate(dedupe_sort_exprs, false);
        let project_exprs = vec![
            (field_expr, String::from("field")),
            (
                expr_col("time", &plan_dedupe.schema()),
                String::from("time"),
            ),
        ];
        let plan_projection_2 = plan_dedupe.project_with_exprs(project_exprs);
        let plan_sort2 = plan_projection_2.sort(final_sort_exprs);

        // Union them together
        let plan_union_2 = plan_sort1.union(plan_sort2);

        // SortPreservingMerge them
        let plan_spm = plan_union_2.sort_preserving_merge(final_sort_exprs);

        // compute statistics: no stats becasue the ProjectionExec includes expression
        let min_max_spm = compute_stats_column_min_max(plan_spm.inner(), "time").unwrap();
        let min_max = column_statistics_min_max(min_max_spm);
        assert!(min_max.is_none());

        // output plan stays the same
        let opt = OrderUnionSortedInputs;
        insta::assert_yaml_snapshot!(
            OptimizationTest::new(plan_spm.build(), opt),
            @r#"
        input:
          - " SortPreservingMergeExec: [time@1 DESC NULLS LAST]"
          - "   UnionExec"
          - "     SortExec: expr=[time@1 DESC NULLS LAST], preserve_partitioning=[false]"
          - "       ProjectionExec: expr=[field1@2 + field1@2 as field, time@3 as time]"
          - "         DataSourceExec: file_groups={1 group: [[0.parquet]]}, projection=[col1, col2, field1, time, __chunk_order], output_ordering=[__chunk_order@4 ASC], file_type=parquet"
          - "     SortExec: expr=[time@1 DESC NULLS LAST], preserve_partitioning=[false]"
          - "       ProjectionExec: expr=[field1@2 + field1@2 as field, time@3 as time]"
          - "         DeduplicateExec: [col1@0 ASC NULLS LAST,col2@1 ASC NULLS LAST,time@3 ASC NULLS LAST]"
          - "           SortPreservingMergeExec: [col1@0 ASC NULLS LAST, col2@1 ASC NULLS LAST, time@3 ASC NULLS LAST]"
          - "             UnionExec"
          - "               SortExec: expr=[col1@0 ASC NULLS LAST, col2@1 ASC NULLS LAST, time@3 ASC NULLS LAST], preserve_partitioning=[false]"
          - "                 RecordBatchesExec: chunks=2, projection=[col1, col2, field1, time, __chunk_order]"
          - "               SortExec: expr=[col1@0 ASC NULLS LAST, col2@1 ASC NULLS LAST, time@3 ASC NULLS LAST], preserve_partitioning=[false]"
          - "                 DataSourceExec: file_groups={1 group: [[0.parquet]]}, projection=[col1, col2, field1, time, __chunk_order], output_ordering=[__chunk_order@4 ASC], file_type=parquet"
        output:
          Ok:
            - " ProgressiveEvalExec: input_ranges=[(2001)->(3500), (1000)->(2000)]"
            - "   ReorderPartitionsExec: mapped_partition_indices=[1, 0]"
            - "     UnionExec"
            - "       SortExec: expr=[time@1 DESC NULLS LAST], preserve_partitioning=[false]"
            - "         ProjectionExec: expr=[field1@2 + field1@2 as field, time@3 as time]"
            - "           DataSourceExec: file_groups={1 group: [[0.parquet]]}, projection=[col1, col2, field1, time, __chunk_order], output_ordering=[__chunk_order@4 ASC], file_type=parquet"
            - "       SortExec: expr=[time@1 DESC NULLS LAST], preserve_partitioning=[false]"
            - "         ProjectionExec: expr=[field1@2 + field1@2 as field, time@3 as time]"
            - "           DeduplicateExec: [col1@0 ASC NULLS LAST,col2@1 ASC NULLS LAST,time@3 ASC NULLS LAST]"
            - "             SortPreservingMergeExec: [col1@0 ASC NULLS LAST, col2@1 ASC NULLS LAST, time@3 ASC NULLS LAST]"
            - "               UnionExec"
            - "                 SortExec: expr=[col1@0 ASC NULLS LAST, col2@1 ASC NULLS LAST, time@3 ASC NULLS LAST], preserve_partitioning=[false]"
            - "                   RecordBatchesExec: chunks=2, projection=[col1, col2, field1, time, __chunk_order]"
            - "                 SortExec: expr=[col1@0 ASC NULLS LAST, col2@1 ASC NULLS LAST, time@3 ASC NULLS LAST], preserve_partitioning=[false]"
            - "                   DataSourceExec: file_groups={1 group: [[0.parquet]]}, projection=[col1, col2, field1, time, __chunk_order], output_ordering=[__chunk_order@4 ASC], file_type=parquet"
        "#
        );
    }

    // ------------------------------------------------------------------
    // Negative tests: the right structure not found -> nothing optimized
    // ------------------------------------------------------------------

    // No limit  & random plan --> plan stay the same
    #[test]
    fn test_negative_no_limit() {
        test_helpers::maybe_start_logging();

        let schema = schema();
        let sort_exprs = [
            ("col2", SortOp::Asc),
            ("col1", SortOp::Asc),
            ("time", SortOp::Asc),
        ];

        let plan_parquet = PlanBuilder::data_source_exec_parquet(&schema, 1000, 2000);
        let plan_batches = PlanBuilder::record_batches_exec(2, 1500, 2500);

        let plan = plan_batches
            .union(plan_parquet)
            .round_robin_repartition(8)
            .hash_repartition(vec!["col2", "col1", "time"], 8)
            .sort(sort_exprs)
            .deduplicate(sort_exprs, true);

        // input and output are the same
        let opt = OrderUnionSortedInputs;
        insta::assert_yaml_snapshot!(
            OptimizationTest::new(plan.build(), opt),
            @r#"
        input:
          - " DeduplicateExec: [col2@1 ASC NULLS LAST,col1@0 ASC NULLS LAST,time@3 ASC NULLS LAST]"
          - "   SortExec: expr=[col2@1 ASC NULLS LAST, col1@0 ASC NULLS LAST, time@3 ASC NULLS LAST], preserve_partitioning=[false]"
          - "     RepartitionExec: partitioning=Hash([col2@1, col1@0, time@3], 8), input_partitions=8"
          - "       RepartitionExec: partitioning=RoundRobinBatch(8), input_partitions=3"
          - "         UnionExec"
          - "           RecordBatchesExec: chunks=2, projection=[col1, col2, field1, time, __chunk_order]"
          - "           DataSourceExec: file_groups={1 group: [[0.parquet]]}, projection=[col1, col2, field1, time, __chunk_order], output_ordering=[__chunk_order@4 ASC], file_type=parquet"
        output:
          Ok:
            - " DeduplicateExec: [col2@1 ASC NULLS LAST,col1@0 ASC NULLS LAST,time@3 ASC NULLS LAST]"
            - "   SortExec: expr=[col2@1 ASC NULLS LAST, col1@0 ASC NULLS LAST, time@3 ASC NULLS LAST], preserve_partitioning=[false]"
            - "     RepartitionExec: partitioning=Hash([col2@1, col1@0, time@3], 8), input_partitions=8"
            - "       RepartitionExec: partitioning=RoundRobinBatch(8), input_partitions=3"
            - "         UnionExec"
            - "           RecordBatchesExec: chunks=2, projection=[col1, col2, field1, time, __chunk_order]"
            - "           DataSourceExec: file_groups={1 group: [[0.parquet]]}, projection=[col1, col2, field1, time, __chunk_order], output_ordering=[__chunk_order@4 ASC], file_type=parquet"
        "#
        );
    }

    // has limit but no sort preserving merge --> plan stay the same
    #[test]
    fn test_negative_limit_no_preserving_merge() {
        test_helpers::maybe_start_logging();

        let plan_batches1 = PlanBuilder::record_batches_exec(1, 1000, 2000);
        let plan_batches2 = PlanBuilder::record_batches_exec(3, 2001, 3000);
        let plan_batches3 = PlanBuilder::record_batches_exec(2, 2500, 3500);

        let plan_union_1 = plan_batches2.union(plan_batches3);

        let sort_exprs = [("time", SortOp::Desc)];
        let plan_sort1 = plan_batches1.sort(sort_exprs);
        let plan_sort2 = plan_union_1.sort(sort_exprs);

        let plan_union_2 = plan_sort1.union(plan_sort2);

        let plan_limit = plan_union_2.limit(0, Some(1));

        // input and output are the same
        let opt = OrderUnionSortedInputs;
        insta::assert_yaml_snapshot!(
            OptimizationTest::new(plan_limit.build(), opt),
            @r#"
        input:
          - " GlobalLimitExec: skip=0, fetch=1"
          - "   UnionExec"
          - "     SortExec: expr=[time@3 DESC NULLS LAST], preserve_partitioning=[false]"
          - "       RecordBatchesExec: chunks=1, projection=[col1, col2, field1, time, __chunk_order]"
          - "     SortExec: expr=[time@3 DESC NULLS LAST], preserve_partitioning=[false]"
          - "       UnionExec"
          - "         RecordBatchesExec: chunks=3, projection=[col1, col2, field1, time, __chunk_order]"
          - "         RecordBatchesExec: chunks=2, projection=[col1, col2, field1, time, __chunk_order]"
        output:
          Ok:
            - " GlobalLimitExec: skip=0, fetch=1"
            - "   UnionExec"
            - "     SortExec: expr=[time@3 DESC NULLS LAST], preserve_partitioning=[false]"
            - "       RecordBatchesExec: chunks=1, projection=[col1, col2, field1, time, __chunk_order]"
            - "     SortExec: expr=[time@3 DESC NULLS LAST], preserve_partitioning=[false]"
            - "       UnionExec"
            - "         RecordBatchesExec: chunks=3, projection=[col1, col2, field1, time, __chunk_order]"
            - "         RecordBatchesExec: chunks=2, projection=[col1, col2, field1, time, __chunk_order]"
        "#
        );
    }

    // No limit & but the input is in the right union struct --> plan stay the same
    #[test]
    fn test_negative_no_sortpreservingmerge_input_union() {
        test_helpers::maybe_start_logging();

        // plan:
        //    UnionExec
        //      SortExec: expr=[time@2 DESC]
        //        DataSourceExec
        //      SortExec: expr=[time@2 DESC]
        //        UnionExec
        //          RecordBatchesExec
        //          DataSourceExec

        let schema = schema();

        let plan_parquet = PlanBuilder::data_source_exec_parquet(&schema, 1000, 2000);
        let plan_parquet2 = PlanBuilder::data_source_exec_parquet(&schema, 2001, 3000);
        let plan_batches = PlanBuilder::record_batches_exec(2, 2500, 3500);

        let plan_union_1 = plan_batches.union(plan_parquet2);

        let sort_exprs = [("time", SortOp::Desc)];

        let plan_sort1 = plan_parquet.sort(sort_exprs);
        let plan_sort2 = plan_union_1.sort(sort_exprs);

        let plan_union_2 = plan_sort1.union(plan_sort2);

        // input and output are the same
        let opt = OrderUnionSortedInputs;
        insta::assert_yaml_snapshot!(
            OptimizationTest::new(plan_union_2.build(), opt),
            @r#"
        input:
          - " UnionExec"
          - "   SortExec: expr=[time@3 DESC NULLS LAST], preserve_partitioning=[false]"
          - "     DataSourceExec: file_groups={1 group: [[0.parquet]]}, projection=[col1, col2, field1, time, __chunk_order], output_ordering=[__chunk_order@4 ASC], file_type=parquet"
          - "   SortExec: expr=[time@3 DESC NULLS LAST], preserve_partitioning=[false]"
          - "     UnionExec"
          - "       RecordBatchesExec: chunks=2, projection=[col1, col2, field1, time, __chunk_order]"
          - "       DataSourceExec: file_groups={1 group: [[0.parquet]]}, projection=[col1, col2, field1, time, __chunk_order], output_ordering=[__chunk_order@4 ASC], file_type=parquet"
        output:
          Ok:
            - " UnionExec"
            - "   SortExec: expr=[time@3 DESC NULLS LAST], preserve_partitioning=[false]"
            - "     DataSourceExec: file_groups={1 group: [[0.parquet]]}, projection=[col1, col2, field1, time, __chunk_order], output_ordering=[__chunk_order@4 ASC], file_type=parquet"
            - "   SortExec: expr=[time@3 DESC NULLS LAST], preserve_partitioning=[false]"
            - "     UnionExec"
            - "       RecordBatchesExec: chunks=2, projection=[col1, col2, field1, time, __chunk_order]"
            - "       DataSourceExec: file_groups={1 group: [[0.parquet]]}, projection=[col1, col2, field1, time, __chunk_order], output_ordering=[__chunk_order@4 ASC], file_type=parquet"
        "#
        );
    }

    // ------------------------------------------------------------------
    // Many partitioned files tests
    // ------------------------------------------------------------------

    // Reproduce of https://github.com/influxdata/influxdb_iox/issues/12461#issuecomment-2430196754
    // The reproducer needs big non-overlapped files so its first physical plan will have DataSourceExec with multiple
    // file groups, each file group has multiple partitioned files.
    // The  OrderUnionSortedInputs optimizer step will merge those partitioned files of the same file into one partitioned file
    // and each will be in its own file group
    #[tokio::test]
    async fn test_many_partition_files() {
        // DF session setup
        let config = ExecutorConfig {
            target_query_partitions: 4.try_into().unwrap(),
            ..ExecutorConfig::testing()
        };
        let exec = Executor::new_with_config_and_executor(config, DedicatedExecutor::new_testing());
        let ctx = exec.new_context();
        let state = ctx.inner().state();

        // chunks
        let c = TestChunk::new("t").with_tag_column("tag");

        // Ingester data time[90, 100]
        let c_mem = c
            .clone()
            .with_row_count(100_000)
            .with_may_contain_pk_duplicates(true)
            .with_time_column_with_full_stats(Some(90), Some(100), None);

        // Two files overlapping with each other and with c_mem
        //
        // File 1: time[90, 100] and overlaps with c_mem
        let c_file_1 = c
            .clone()
            .with_row_count(100_000)
            .with_time_column_with_full_stats(Some(90), Some(100), None)
            .with_dummy_parquet_file_and_size(1000)
            .with_may_contain_pk_duplicates(false)
            .with_sort_key(SortKey::from_columns([Arc::from("tag"), Arc::from("time")]));
        // File 2: overlaps with c_file_1 and c_mem
        let c_file_2 = c_file_1.clone();

        // Five files that are not overlapped with any
        let overlapped_c = c
            .clone()
            .with_sort_key(SortKey::from_columns([Arc::from("tag"), Arc::from("time")]))
            .with_dummy_parquet_file_and_size(100000000)
            .with_may_contain_pk_duplicates(false);
        //
        // File 3: time[65, 69] that is not overlapped any
        let c_file_3 = overlapped_c
            .clone()
            .with_row_count(1_000_000)
            .with_time_column_with_full_stats(Some(65), Some(69), None);
        let c_file_4 = overlapped_c
            .clone()
            .with_row_count(1_000_000)
            .with_time_column_with_full_stats(Some(60), Some(64), None);
        let c_file_5 = overlapped_c
            .clone()
            .with_row_count(1_000_000)
            .with_time_column_with_full_stats(Some(55), Some(58), None);
        let c_file_6 = overlapped_c
            .clone()
            .with_row_count(1_000_000)
            .with_time_column_with_full_stats(Some(50), Some(54), None);
        let c_file_7 = overlapped_c
            .clone()
            .with_row_count(1_000_000)
            .with_time_column_with_full_stats(Some(45), Some(49), None);

        // Schema & provider
        let schema = c_mem.schema().clone();
        let provider = ProviderBuilder::new("t".into(), schema)
            .add_chunk(Arc::new(c_mem.clone().with_id(1).with_order(i64::MAX)))
            .add_chunk(Arc::new(c_file_1.with_id(2).with_order(2)))
            .add_chunk(Arc::new(c_file_2.with_id(3).with_order(3)))
            // add non-overlapped chunks in random order
            .add_chunk(Arc::new(c_file_7.with_id(8).with_order(8)))
            .add_chunk(Arc::new(c_file_3.with_id(4).with_order(4)))
            .add_chunk(Arc::new(c_file_5.with_id(6).with_order(6)))
            .add_chunk(Arc::new(c_file_6.with_id(7).with_order(7)))
            .add_chunk(Arc::new(c_file_4.with_id(5).with_order(5)))
            .build()
            .unwrap();

        // expression: time > 0
        let expr = col("time").gt(lit(ScalarValue::TimestampNanosecond(Some(0), None)));

        // logical plan: select * from t where time > 0 order by time desc limit 1
        let plan = LogicalPlanBuilder::scan(
            "t".to_owned(),
            provider_as_source(Arc::new(provider.clone())),
            None,
        )
        .unwrap()
        .filter(expr.clone())
        .unwrap()
        // order by time DESC
        .sort(vec![col("time").sort(false, true)])
        .unwrap()
        // limit
        .limit(0, Some(1))
        .unwrap()
        .build()
        .unwrap();

        // create physical plan
        let plan = state.create_physical_plan(&plan).await.unwrap();

        // Since this is time DESC, the ProgressiveEvalExec must reflect the correct input ranges from largest to smallest
        // The LAST DataSourceExec must include non-overlapped files sorted from smallest file name: 4.parquet, 5.parquet, 6.parquet, 7.parquet, 8.parquet
        //   Note: larger file name includes older time range
        insta::assert_yaml_snapshot!(
            format_execution_plan(&plan),
            @r#"
        - " ProgressiveEvalExec: fetch=1, input_ranges=[(90)->(100), (65)->(69), (60)->(64), (55)->(58), (50)->(54), (45)->(49)]"
        - "   ReorderPartitionsExec: mapped_partition_indices=[5, 0, 1, 2, 3, 4]"
        - "     SortExec: TopK(fetch=1), expr=[time@1 DESC], preserve_partitioning=[true]"
        - "       UnionExec"
        - "         DataSourceExec: file_groups={5 groups: [[4.parquet:0..100000000], [5.parquet:0..100000000], [6.parquet:0..100000000], [7.parquet:0..100000000], [8.parquet:0..100000000]]}, projection=[tag, time], output_ordering=[tag@0 ASC, time@1 ASC], file_type=parquet, predicate=time@1 > 0, pruning_predicate=time_null_count@1 != row_count@2 AND time_max@0 > 0, required_guarantees=[]"
        - "         ProjectionExec: expr=[tag@0 as tag, time@1 as time]"
        - "           DeduplicateExec: [tag@0 ASC,time@1 ASC]"
        - "             SortPreservingMergeExec: [tag@0 ASC, time@1 ASC, __chunk_order@2 ASC]"
        - "               UnionExec"
        - "                 SortExec: expr=[tag@0 ASC, time@1 ASC, __chunk_order@2 ASC], preserve_partitioning=[true]"
        - "                   CoalesceBatchesExec: target_batch_size=8192"
        - "                     FilterExec: time@1 > 0"
        - "                       RepartitionExec: partitioning=RoundRobinBatch(4), input_partitions=1"
        - "                         RecordBatchesExec: chunks=1, projection=[tag, time, __chunk_order]"
        - "                 DataSourceExec: file_groups={4 groups: [[2.parquet:0..500], [3.parquet:0..500], [2.parquet:500..1000], [3.parquet:500..1000]]}, projection=[tag, time, __chunk_order], output_ordering=[tag@0 ASC, time@1 ASC, __chunk_order@2 ASC], file_type=parquet, predicate=time@1 > 0, pruning_predicate=time_null_count@1 != row_count@2 AND time_max@0 > 0, required_guarantees=[]"
        "#
        );

        // logical plan: select * from t where time > 0 order by time ASC limit 1
        let plan =
            LogicalPlanBuilder::scan("t".to_owned(), provider_as_source(Arc::new(provider)), None)
                .unwrap()
                .filter(expr)
                .unwrap()
                .sort(vec![col("time").sort(true, true)])
                .unwrap()
                // limit
                .limit(0, Some(1))
                .unwrap()
                .build()
                .unwrap();

        // create physical plan
        let plan = state.create_physical_plan(&plan).await.unwrap();

        // Since this is time ASC, the ProgressiveEvalExec must reflect the correct input ranges from smallest to largest
        // The FRIST DataSourceExec must include non-overlapped files sorted from largest file name: 8.parquet, 7.parquet, 6.parquet, 5.parquet, 4.parquet
        //   Note: larger file name includes older time range
        insta::assert_yaml_snapshot!(
            format_execution_plan(&plan),
            @r#"
        - " ProgressiveEvalExec: fetch=1, input_ranges=[(45)->(49), (50)->(54), (55)->(58), (60)->(64), (65)->(69), (90)->(100)]"
        - "   SortExec: TopK(fetch=1), expr=[time@1 ASC], preserve_partitioning=[true]"
        - "     UnionExec"
        - "       DataSourceExec: file_groups={5 groups: [[8.parquet:0..100000000], [7.parquet:0..100000000], [6.parquet:0..100000000], [5.parquet:0..100000000], [4.parquet:0..100000000]]}, projection=[tag, time], output_ordering=[tag@0 ASC, time@1 ASC], file_type=parquet, predicate=time@1 > 0, pruning_predicate=time_null_count@1 != row_count@2 AND time_max@0 > 0, required_guarantees=[]"
        - "       ProjectionExec: expr=[tag@0 as tag, time@1 as time]"
        - "         DeduplicateExec: [tag@0 ASC,time@1 ASC]"
        - "           SortPreservingMergeExec: [tag@0 ASC, time@1 ASC, __chunk_order@2 ASC]"
        - "             UnionExec"
        - "               SortExec: expr=[tag@0 ASC, time@1 ASC, __chunk_order@2 ASC], preserve_partitioning=[true]"
        - "                 CoalesceBatchesExec: target_batch_size=8192"
        - "                   FilterExec: time@1 > 0"
        - "                     RepartitionExec: partitioning=RoundRobinBatch(4), input_partitions=1"
        - "                       RecordBatchesExec: chunks=1, projection=[tag, time, __chunk_order]"
        - "               DataSourceExec: file_groups={4 groups: [[2.parquet:0..500], [3.parquet:0..500], [2.parquet:500..1000], [3.parquet:500..1000]]}, projection=[tag, time, __chunk_order], output_ordering=[tag@0 ASC, time@1 ASC, __chunk_order@2 ASC], file_type=parquet, predicate=time@1 > 0, pruning_predicate=time_null_count@1 != row_count@2 AND time_max@0 > 0, required_guarantees=[]"
        "#
        );
    }

    #[tokio::test]
    async fn test_many_partition_files_all_non_overlapped_files_no_ingester_data() {
        // DF session setup
        let config = ExecutorConfig {
            target_query_partitions: 4.try_into().unwrap(),
            ..ExecutorConfig::testing()
        };
        let exec = Executor::new_with_config_and_executor(config, DedicatedExecutor::new_testing());
        let ctx = exec.new_context();
        let state = ctx.inner().state();

        // chunks
        let c = TestChunk::new("t").with_tag_column("tag");

        // Five files that are not overlapped with any
        let overlapped_c = c
            .clone()
            .with_row_count(1_000_000)
            .with_sort_key(SortKey::from_columns([Arc::from("tag"), Arc::from("time")]))
            .with_dummy_parquet_file_and_size(100000000)
            .with_may_contain_pk_duplicates(false);
        //
        let c_file_1 =
            overlapped_c
                .clone()
                .with_time_column_with_full_stats(Some(65), Some(69), None);
        let c_file_2 =
            overlapped_c
                .clone()
                .with_time_column_with_full_stats(Some(60), Some(64), None);
        let c_file_3 =
            overlapped_c
                .clone()
                .with_time_column_with_full_stats(Some(55), Some(58), None);
        let c_file_4 =
            overlapped_c
                .clone()
                .with_time_column_with_full_stats(Some(50), Some(54), None);
        let c_file_5 =
            overlapped_c
                .clone()
                .with_time_column_with_full_stats(Some(45), Some(49), None);

        // Schema & provider
        let schema = c_file_1.schema().clone();
        let provider = ProviderBuilder::new("t".into(), schema)
            // add non-overlapped chunks in random order
            .add_chunk(Arc::new(c_file_4.with_id(4).with_order(4)))
            .add_chunk(Arc::new(c_file_3.with_id(3).with_order(3)))
            .add_chunk(Arc::new(c_file_1.with_id(1).with_order(1)))
            .add_chunk(Arc::new(c_file_2.with_id(2).with_order(2)))
            .add_chunk(Arc::new(c_file_5.with_id(5).with_order(5)))
            .build()
            .unwrap();

        // expression: time > 0
        let expr = col("time").gt(lit(ScalarValue::TimestampNanosecond(Some(0), None)));

        // logical plan: select * from t where time > 0 order by time desc limit 1
        let plan = LogicalPlanBuilder::scan(
            "t".to_owned(),
            provider_as_source(Arc::new(provider.clone())),
            None,
        )
        .unwrap()
        .filter(expr.clone())
        .unwrap()
        // order by time DESC
        .sort(vec![col("time").sort(false, true)])
        .unwrap()
        // limit
        .limit(0, Some(1))
        .unwrap()
        .build()
        .unwrap();

        // create physical plan
        let plan = state.create_physical_plan(&plan).await.unwrap();

        // Since this is time DESC, the ProgressiveEvalExec must reflect the correct input ranges from largest to smallest
        // The LAST DataSourceExec must include non-overlapped files sorted from smallest file name: 1.parquet, 2.parquet, 3.parquet, 4.parquet, 5.parquet
        //   Note: larger file name includes older time range
        insta::assert_yaml_snapshot!(
            format_execution_plan(&plan),
            @r#"
        - " ProgressiveEvalExec: fetch=1, input_ranges=[(65)->(69), (60)->(64), (55)->(58), (50)->(54), (45)->(49)]"
        - "   SortExec: TopK(fetch=1), expr=[time@1 DESC], preserve_partitioning=[true]"
        - "     DataSourceExec: file_groups={5 groups: [[1.parquet:0..100000000], [2.parquet:0..100000000], [3.parquet:0..100000000], [4.parquet:0..100000000], [5.parquet:0..100000000]]}, projection=[tag, time], output_ordering=[tag@0 ASC, time@1 ASC], file_type=parquet, predicate=time@1 > 0 AND DynamicFilterPhysicalExpr [ true ], pruning_predicate=time_null_count@1 != row_count@2 AND time_max@0 > 0, required_guarantees=[]"
        "#
        );

        // logical plan: select * from t where time > 0 order by time ASC limit 1
        let plan =
            LogicalPlanBuilder::scan("t".to_owned(), provider_as_source(Arc::new(provider)), None)
                .unwrap()
                .filter(expr)
                .unwrap()
                // order by time ASC
                .sort(vec![col("time").sort(true, true)])
                .unwrap()
                // limit
                .limit(0, Some(1))
                .unwrap()
                .build()
                .unwrap();

        // create physical plan
        let plan = state.create_physical_plan(&plan).await.unwrap();

        // Since this is time ASC, the ProgressiveEvalExec must reflect the correct input ranges from smallest to largest
        // The FRIST DataSourceExec must include non-overlapped files sorted from largest file name: 5.parquet, 4.parquet, 3.parquet, 2.parquet, 1.parquet
        //   Note: larger file name includes older time range
        insta::assert_yaml_snapshot!(
            format_execution_plan(&plan),
            @r#"
        - " ProgressiveEvalExec: fetch=1, input_ranges=[(45)->(49), (50)->(54), (55)->(58), (60)->(64), (65)->(69)]"
        - "   SortExec: TopK(fetch=1), expr=[time@1 ASC], preserve_partitioning=[true]"
        - "     DataSourceExec: file_groups={5 groups: [[5.parquet:0..100000000], [4.parquet:0..100000000], [3.parquet:0..100000000], [2.parquet:0..100000000], [1.parquet:0..100000000]]}, projection=[tag, time], output_ordering=[tag@0 ASC, time@1 ASC], file_type=parquet, predicate=time@1 > 0 AND DynamicFilterPhysicalExpr [ true ], pruning_predicate=time_null_count@1 != row_count@2 AND time_max@0 > 0, required_guarantees=[]"
        "#
        );
    }

    // ------------------------------------------------------------------
    // Helper functions
    // ------------------------------------------------------------------

    /// Builder for plans that uses the correct schemas are used when
    /// constructing embedded expressions.
    #[derive(Debug, Clone)]
    struct PlanBuilder {
        /// The current plan being constructed
        inner: Arc<dyn ExecutionPlan>,
    }

    impl PlanBuilder {
        /// Create a new builder to scan the parquet file with the specified range
        fn data_source_exec_parquet(schema: &SchemaRef, min: i64, max: i64) -> Self {
            let chunk = test_chunk(min, max, true);
            let plan = chunks_to_physical_nodes(schema, None, vec![chunk], 1);

            Self::remove_union(plan)
        }

        // Create a parquet-based `DataSourceExec` with a given number of chunks
        fn data_source_exec_parquet_non_overlapped_chunks(
            schema: &SchemaRef,
            n_chunks: usize,
            min: i64,
            duration: usize,
            target_partition: usize,
        ) -> Self {
            let mut chunks = Vec::with_capacity(n_chunks);
            for i in 0..n_chunks {
                let min = min + (duration * i) as i64;
                let max = min + duration as i64 - 1;
                let chunk = test_chunk_with_id(min, max, true, i as u128);
                chunks.push(chunk);
            }

            let inner = chunks_to_physical_nodes(schema, None, chunks, target_partition);

            Self::remove_union(inner)
        }

        // Create a parquet-based `DataSourceExec` with a given number of chunks
        fn data_source_exec_parquet_overlapped_chunks(
            schema: &SchemaRef,
            n_chunks: usize,
            min: i64,
            duration: usize,
            target_partition: usize,
        ) -> Self {
            let mut chunks = Vec::with_capacity(n_chunks);
            for i in 0..n_chunks {
                let min = min + (duration * i) as i64;
                let max = min + duration as i64 + 1; // overlap by 2
                let chunk = test_chunk_with_id(min, max, true, i as u128);
                chunks.push(chunk);
            }

            let inner = chunks_to_physical_nodes(schema, None, chunks, target_partition);

            Self::remove_union(inner)
        }

        fn remove_union(plan: Arc<dyn ExecutionPlan>) -> Self {
            let inner = if let Some(union_exec) = plan.as_any().downcast_ref::<UnionExec>()
                && union_exec.inputs().len() == 1
            {
                Arc::clone(&union_exec.inputs()[0])
            } else {
                plan
            };
            Self { inner }
        }

        /// Create a builder for scanning record batches with the specified value range
        fn record_batches_exec(n_chunks: usize, min: i64, max: i64) -> Self {
            let chunks =
                std::iter::repeat_n(test_chunk(min, max, false), n_chunks).collect::<Vec<_>>();

            Self {
                inner: Arc::new(RecordBatchesExec::new(chunks, schema(), None)),
            }
        }

        /// Create a union of this plan with another plan
        fn union(self, other: Self) -> Self {
            let inner = Arc::new(UnionExec::new(vec![self.inner, other.inner]));
            Self { inner }
        }

        /// Sort the output of this plan with the specified expressions
        fn sort<'a>(self, cols: impl IntoIterator<Item = (&'a str, SortOp)>) -> Self {
            Self {
                inner: Arc::new(SortExec::new(self.sort_exprs(cols), self.inner)),
            }
        }

        /// Sort the output of this plan with the specified expressions
        fn sort_with_preserve_partitioning<'a>(
            self,
            cols: impl IntoIterator<Item = (&'a str, SortOp)>,
        ) -> Self {
            Self::sort_with_preserve_partitioning_setting(self, cols, true)
        }

        /// Sort the output of this plan with the specified expressions &
        fn sort_with_preserve_partitioning_setting<'a>(
            self,
            cols: impl IntoIterator<Item = (&'a str, SortOp)>,
            preserve_partitioning: bool,
        ) -> Self {
            Self {
                inner: Arc::new(
                    SortExec::new(self.sort_exprs(cols), self.inner)
                        .with_preserve_partitioning(preserve_partitioning),
                ),
            }
        }

        /// Deduplicate the output of this plan with the specified sort expressions
        fn deduplicate<'a>(
            self,
            cols: impl IntoIterator<Item = (&'a str, SortOp)>,
            use_chunk_order_col: bool,
        ) -> Self {
            let sort_exprs = self.sort_exprs(cols);
            Self {
                inner: Arc::new(DeduplicateExec::new(
                    self.inner,
                    sort_exprs,
                    use_chunk_order_col,
                )),
            }
        }

        /// adds a ProjectionExec node to the plan with the specified columns
        fn project<'a>(self, cols: impl IntoIterator<Item = &'a str>) -> Self {
            let schema = self.inner.schema();
            let project_exprs = cols
                .into_iter()
                .map(|col| {
                    let expr: Arc<dyn PhysicalExpr> =
                        Arc::new(Column::new_with_schema(col, &schema).unwrap());
                    (expr, col.to_string())
                })
                .collect::<Vec<_>>();

            self.project_with_exprs(project_exprs)
        }

        /// adds a ProjectionExec node to the plan with the specified exprs
        fn project_with_exprs(self, project_exprs: Vec<(Arc<dyn PhysicalExpr>, String)>) -> Self {
            Self {
                inner: Arc::new(ProjectionExec::try_new(project_exprs, self.inner).unwrap()),
            }
        }

        /// Create a sort_preserving_merge with the specified sort order
        fn sort_preserving_merge<'a>(
            self,
            cols: impl IntoIterator<Item = (&'a str, SortOp)>,
        ) -> Self {
            Self {
                inner: Arc::new(SortPreservingMergeExec::new(
                    self.sort_exprs(cols),
                    self.inner,
                )),
            }
        }

        /// round robin repartition into the specified number of partitions
        fn round_robin_repartition(self, n_partitions: usize) -> Self {
            Self {
                inner: Arc::new(
                    RepartitionExec::try_new(
                        self.inner,
                        Partitioning::RoundRobinBatch(n_partitions),
                    )
                    .unwrap(),
                ),
            }
        }

        /// hash repartition into the specified number of partitions
        fn hash_repartition<'a>(
            self,
            cols: impl IntoIterator<Item = &'a str>,
            n_partitions: usize,
        ) -> Self {
            let schema = self.inner.schema();
            let hash_exprs = cols
                .into_iter()
                .map(|col| {
                    Arc::new(Column::new_with_schema(col, &schema).unwrap())
                        as Arc<dyn PhysicalExpr>
                })
                .collect();
            Self {
                inner: Arc::new(
                    RepartitionExec::try_new(
                        self.inner,
                        Partitioning::Hash(hash_exprs, n_partitions),
                    )
                    .unwrap(),
                ),
            }
        }

        /// create a `Vec<PhysicalSortExpr>` from the specified columns for the current node
        fn sort_exprs<'a>(&self, cols: impl IntoIterator<Item = (&'a str, SortOp)>) -> LexOrdering {
            // sort expressions are based on the schema of the input
            let schema = self.inner.schema();
            LexOrdering::new(cols.into_iter().map(|col| PhysicalSortExpr {
                expr: Arc::new(Column::new_with_schema(col.0, schema.as_ref()).unwrap()),
                options: SortOptions {
                    descending: col.1 == SortOp::Desc,
                    nulls_first: false,
                },
            }))
            .unwrap()
        }

        fn limit(self, skip: usize, fetch: Option<usize>) -> Self {
            Self {
                inner: Arc::new(GlobalLimitExec::new(self.inner, skip, fetch)),
            }
        }

        /// Return the current plan as formatted strings
        fn formatted(&self) -> Vec<String> {
            format_execution_plan(&self.inner)
        }

        /// return the inner plan
        fn build(self) -> Arc<dyn ExecutionPlan> {
            self.inner
        }

        /// return the schema of the inner plan
        fn schema(&self) -> SchemaRef {
            self.inner.schema()
        }

        /// return a reference to the inner plan
        fn inner(&self) -> &dyn ExecutionPlan {
            self.inner.as_ref()
        }
    }

    fn schema() -> SchemaRef {
        IOxSchemaBuilder::new()
            .tag("col1")
            .tag("col2")
            .influx_field("field1", InfluxFieldType::Float)
            .timestamp()
            .influx_field(CHUNK_ORDER_COLUMN_NAME, InfluxFieldType::Integer)
            .build()
            .unwrap()
            .into()
    }

    fn expr_col(name: &str, schema: &SchemaRef) -> Arc<dyn PhysicalExpr> {
        Arc::new(Column::new_with_schema(name, schema).unwrap())
    }

    // test chunk with time range and field1's value range
    fn test_chunk(min: i64, max: i64, parquet_data: bool) -> Arc<dyn QueryChunk> {
        test_chunk_with_id(min, max, parquet_data, 0)
    }

    // test chunk with time range and field1's value range and with a given chunk id
    fn test_chunk_with_id(
        min: i64,
        max: i64,
        parquet_data: bool,
        chunk_id: u128,
    ) -> Arc<dyn QueryChunk> {
        let chunk = TestChunk::new("t")
            .with_id(chunk_id)
            .with_time_column_with_stats(Some(min), Some(max))
            .with_tag_column_with_stats("col1", Some("AL"), Some("MT"))
            .with_tag_column_with_stats("col2", Some("MA"), Some("VY"))
            .with_i64_field_column_with_stats("field1", Some(min), Some(max));

        let chunk = if parquet_data {
            chunk.with_dummy_parquet_file()
        } else {
            chunk
        };

        Arc::new(chunk) as Arc<dyn QueryChunk>
    }

    #[derive(Debug, PartialEq, Clone, Copy)]
    enum SortOp {
        Asc,
        Desc,
    }
}
