use std::sync::Arc;

use datafusion::{
    common::tree_node::{Transformed, TreeNode},
    config::ConfigOptions,
    error::Result,
    physical_optimizer::PhysicalOptimizerRule,
    physical_plan::{
        displayable, expressions::Column, sorts::sort_preserving_merge::SortPreservingMergeExec,
        union::UnionExec, ExecutionPlan,
    },
};
use observability_deps::tracing::{trace, warn};

use crate::{
    physical_optimizer::sort::util::{collect_statistics_min_max, sort_by_value_ranges},
    provider::progressive_eval::ProgressiveEvalExec,
};

/// IOx specific optimization that eliminates a `SortPreservingMerge`
/// by reordering inputs in terms  of their value ranges. If all inputs are non overlapping and ordered
/// by value range, they can be concatenated by `ProgressiveEval`  while
/// maintaining the desired output order without actually merging.
///
/// Find this structure:
///     SortPreservingMergeExec  - on one column (DESC or ASC)
///         UnionExec
/// and if
///    - all inputs of UnionExec are already sorted (or has SortExec) with sortExpr also on time DESC or ASC accarsdingly and
///    - the streams do not overlap in values of the sorted column
/// do:
///   - order them by the sorted column DESC or ASC accordingly and
///   - replace SortPreservingMergeExec with ProgressiveEvalExec
///
/// Notes: The difference between SortPreservingMergeExec & ProgressiveEvalExec
///    - SortPreservingMergeExec do the merge of sorted input streams. It needs each stream sorted but the streams themselves
///      can be in any random order and they can also overlap in values of sorted columns.
///    - ProgressiveEvalExec only outputs data in their input order of the streams and not do any merges. Thus in order to
///      output data in the right sort order, these three conditions must be true:
///        1. Each input stream must sorted on the same column DESC or ASC accordingly
///        2. The streams must be sorted on the column DESC or ASC accordingly
///        3. The streams must not overlap in the values of that column.
///
/// Example: for col_name ranges:
///   |--- r1---|-- r2 ---|-- r3 ---|-- r4 --|
///
/// Here is what the input look like:
///
///   SortPreservingMergeExec: time@2 DESC, fetch=1
///     UnionExec
///       SortExec: expr=col_name@2 DESC  <--- input stream with col_name range r3
///         ...
///       SortExec: expr=col_name@2 DESC  <--- input stream with col_name range r1
///         ...
///       SortExec: expr=col_name@2 DESC  <--- input stream with col_name range r4
///         ...
///       SortExec: expr=col_name@2 DESC  <--- input stream with col_name range r2  -- assuming this SortExec has 2 output sorted streams
///          ...
///
/// The streams do not overlap in time, and they are already sorted by time DESC.
///
/// The output will be the same except that all the input streams will be sorted by time DESC too and looks like
///
///   SortPreservingMergeExec: time@2 DESC, fetch=1
///     UnionExec
///       SortExec: expr=col_name@2 DESC  <--- input stream with col_name range r1
///         ...
///       SortPreservingMergeExec:                                                  -- need this extra to merge the 2 streams into one
///          SortExec: expr=col_name@2 DESC  <--- input stream with col_name range r2
///             ...
///       SortExec: expr=col_name@2 DESC  <--- input stream with col_name range r3
///         ...
///       SortExec: expr=col_name@2 DESC  <--- input stream with col_name range r4
///          ...
///

pub(crate) struct OrderUnionSortedInputs;

impl PhysicalOptimizerRule for OrderUnionSortedInputs {
    fn optimize(
        &self,
        plan: Arc<dyn ExecutionPlan>,
        _config: &ConfigOptions,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        plan.transform_up(&|plan| {
            // Find SortPreservingMergeExec
            let Some(sort_preserving_merge_exec) =
                plan.as_any().downcast_ref::<SortPreservingMergeExec>()
            else {
                return Ok(Transformed::No(plan));
            };

            // Check if the sortExpr is only on one column
            let sort_expr = sort_preserving_merge_exec.expr();
            if sort_expr.len() != 1 {
                trace!(
                    ?sort_expr,
                    "-------- sortExpr is not on one column. No optimization"
                );
                return Ok(Transformed::No(plan));
            };
            let Some(sorted_col) = sort_expr[0].expr.as_any().downcast_ref::<Column>() else {
                trace!(
                    ?sort_expr,
                    "-------- sortExpr is not on pure column but expression. No optimization"
                );
                return Ok(Transformed::No(plan));
            };
            let sort_options = sort_expr[0].options;

            // Find UnionExec
            let Some(union_exec) = sort_preserving_merge_exec
                .input()
                .as_any()
                .downcast_ref::<UnionExec>()
            else {
                trace!("-------- SortPreservingMergeExec input is not UnionExec. No optimization");
                return Ok(Transformed::No(plan));
            };

            // Check all inputs of UnionExec must be already sorted and on the same sort_expr of SortPreservingMergeExec
            let Some(union_output_ordering) = union_exec.output_ordering() else {
                warn!(plan=%displayable(plan.as_ref()).indent(false), "Union input to SortPreservingMerge is not sorted");
                return Ok(Transformed::No(plan));
            };

            // Check if the first PhysicalSortExpr is the same as the sortExpr[0] in SortPreservingMergeExec
            if sort_expr[0] != union_output_ordering[0] {
                warn!(?sort_expr, ?union_output_ordering, plan=%displayable(plan.as_ref()).indent(false), "-------- Sort order of SortPreservingMerge and its children are different");
                return Ok(Transformed::No(plan));
            }

            let Some(value_ranges) = collect_statistics_min_max(union_exec.inputs(), sorted_col.name())?
            else {
                return Ok(Transformed::No(plan));
            };

            // Sort the inputs by their value ranges
            trace!("-------- value_ranges: {:?}", value_ranges);
            let Some(plans_value_ranges) =
                sort_by_value_ranges(union_exec.inputs().to_vec(), value_ranges, sort_options)?
            else {
                trace!("-------- inputs are not sorted by value ranges. No optimization");
                return Ok(Transformed::No(plan));
            };

            // If each input of UnionExec outputs many sorted streams, data of different streams may overlap and
            // even if they do not overlapped, their streams can be in any order. We need to (sort) merge them first
            // to have a single output stream out to guarantee the output is sorted.
            let new_inputs = plans_value_ranges.plans
                .iter()
                .map(|input| {
                    if input.output_partitioning().partition_count() > 1 {
                        // Add SortPreservingMergeExec on top of this input
                        let sort_preserving_merge_exec = Arc::new(
                            SortPreservingMergeExec::new(sort_expr.to_vec(), Arc::clone(input))
                                .with_fetch(sort_preserving_merge_exec.fetch()),
                        );
                        Ok(sort_preserving_merge_exec as _)
                    } else {
                        Ok(Arc::clone(input))
                    }
                })
                .collect::<Result<Vec<_>>>()?;

            let new_union_exec = Arc::new(UnionExec::new(new_inputs));

            // Replace SortPreservingMergeExec with ProgressiveEvalExec
            let progresive_eval_exec = Arc::new(ProgressiveEvalExec::new(
                new_union_exec,
                Some(plans_value_ranges.value_ranges),
                sort_preserving_merge_exec.fetch(),
            ));

            Ok(Transformed::Yes(progresive_eval_exec))
        })
    }

    fn name(&self) -> &str {
        "order_union_sorted_inputs"
    }

    fn schema_check(&self) -> bool {
        true
    }
}

#[cfg(test)]
mod test {
    use std::sync::Arc;

    use arrow::{compute::SortOptions, datatypes::SchemaRef};
    use datafusion::{
        logical_expr::Operator,
        physical_expr::PhysicalSortExpr,
        physical_plan::{
            expressions::{BinaryExpr, Column},
            limit::GlobalLimitExec,
            projection::ProjectionExec,
            repartition::RepartitionExec,
            sorts::{sort::SortExec, sort_preserving_merge::SortPreservingMergeExec},
            union::UnionExec,
            ExecutionPlan, Partitioning, PhysicalExpr,
        },
        scalar::ScalarValue,
    };
    use schema::{InfluxFieldType, SchemaBuilder as IOxSchemaBuilder};

    use crate::{
        physical_optimizer::{
            sort::order_union_sorted_inputs::OrderUnionSortedInputs, test_util::OptimizationTest,
        },
        provider::{chunks_to_physical_nodes, DeduplicateExec, RecordBatchesExec},
        statistics::{column_statistics_min_max, compute_stats_column_min_max},
        test::{format_execution_plan, TestChunk},
        QueryChunk, CHUNK_ORDER_COLUMN_NAME,
    };

    // ------------------------------------------------------------------
    // Positive tests: the right structure found -> plan optimized
    // ------------------------------------------------------------------

    #[test]
    fn test_limit_mix_record_batch_parquet_1_desc() {
        test_helpers::maybe_start_logging();

        // Input plan:
        //
        // GlobalLimitExec: skip=0, fetch=2
        //  SortPreservingMerge: [time@2 DESC]
        //    UnionExec
        //      SortExec: expr=[time@2 DESC]   -- time range [1000, 2000]
        //        ParquetExec                         -- [1000, 2000]
        //      SortExec: expr=[time@2 DESC]   -- time range [2001, 3500] from combine time range of two record batches
        //        UnionExec
        //          RecordBatchesExec                 -- 3 chunks [2001, 3000]
        //          RecordBatchesExec                 -- 2 chunks [2500, 3500]
        //
        // Output plan: the 2 SortExecs will be swapped the order to have time range [2001, 3500] first

        let schema = schema();

        let plan_parquet = parquet_exec_with_value_range(&schema, 1000, 2000);
        let plan_batches1 = record_batches_exec_with_value_range(3, 2001, 3000);
        let plan_batches2 = record_batches_exec_with_value_range(2, 2500, 3500);

        let plan_union_1 = Arc::new(UnionExec::new(vec![plan_batches1, plan_batches2]));

        let sort_order = ordering_with_options([("time", SortOp::Desc)], &schema);
        let plan_sort1 = Arc::new(SortExec::new(sort_order.clone(), plan_parquet));
        let plan_sort2 = Arc::new(SortExec::new(sort_order.clone(), plan_union_1));

        // min max of plan_sorted1 is [1000, 2000]
        // structure of plan_sorted1
        let p_sort1 = Arc::clone(&plan_sort1) as Arc<dyn ExecutionPlan>;
        insta::assert_yaml_snapshot!(
            format_execution_plan(&p_sort1),
            @r###"
        ---
        - " SortExec: expr=[time@3 DESC NULLS LAST]"
        - "   ParquetExec: file_groups={1 group: [[0.parquet]]}, projection=[col1, col2, field1, time, __chunk_order], output_ordering=[__chunk_order@4 ASC]"
        "###
        );
        let min_max_sort1 = compute_stats_column_min_max(&*plan_sort1, "time").unwrap();
        let min_max = column_statistics_min_max(&min_max_sort1).unwrap();
        assert_eq!(
            min_max,
            (
                ScalarValue::TimestampNanosecond(Some(1000), None),
                ScalarValue::TimestampNanosecond(Some(2000), None)
            )
        );
        //
        // min max of plan_sorted2 is [2001, 3500]
        let p_sort2 = Arc::clone(&plan_sort2) as Arc<dyn ExecutionPlan>;
        insta::assert_yaml_snapshot!(
            format_execution_plan(&p_sort2),
            @r###"
        ---
        - " SortExec: expr=[time@3 DESC NULLS LAST]"
        - "   UnionExec"
        - "     RecordBatchesExec: chunks=3, projection=[col1, col2, field1, time, __chunk_order]"
        - "     RecordBatchesExec: chunks=2, projection=[col1, col2, field1, time, __chunk_order]"
        "###
        );
        let min_max_sort2 = compute_stats_column_min_max(&*plan_sort2, "time").unwrap();
        let min_max = column_statistics_min_max(&min_max_sort2).unwrap();
        assert_eq!(
            min_max,
            (
                ScalarValue::TimestampNanosecond(Some(2001), None),
                ScalarValue::TimestampNanosecond(Some(3500), None)
            )
        );

        let plan_union_2 = Arc::new(UnionExec::new(vec![plan_sort1, plan_sort2]));

        let plan_spm = Arc::new(SortPreservingMergeExec::new(
            sort_order.clone(),
            plan_union_2,
        ));

        // min max of plan_spm is [1000, 3500]
        let p_spm = Arc::clone(&plan_spm) as Arc<dyn ExecutionPlan>;
        insta::assert_yaml_snapshot!(
            format_execution_plan(&p_spm),
            @r###"
        ---
        - " SortPreservingMergeExec: [time@3 DESC NULLS LAST]"
        - "   UnionExec"
        - "     SortExec: expr=[time@3 DESC NULLS LAST]"
        - "       ParquetExec: file_groups={1 group: [[0.parquet]]}, projection=[col1, col2, field1, time, __chunk_order], output_ordering=[__chunk_order@4 ASC]"
        - "     SortExec: expr=[time@3 DESC NULLS LAST]"
        - "       UnionExec"
        - "         RecordBatchesExec: chunks=3, projection=[col1, col2, field1, time, __chunk_order]"
        - "         RecordBatchesExec: chunks=2, projection=[col1, col2, field1, time, __chunk_order]"
        "###
        );
        let min_max_spm = compute_stats_column_min_max(&*plan_spm, "time").unwrap();
        let min_max = column_statistics_min_max(&min_max_spm).unwrap();
        assert_eq!(
            min_max,
            (
                ScalarValue::TimestampNanosecond(Some(1000), None),
                ScalarValue::TimestampNanosecond(Some(3500), None)
            )
        );

        let plan_limit = Arc::new(GlobalLimitExec::new(plan_spm, 0, Some(1)));

        // Output plan: the 2 SortExecs will be swapped the order
        let opt = OrderUnionSortedInputs;
        insta::assert_yaml_snapshot!(
            OptimizationTest::new(plan_limit, opt),
            @r###"
        ---
        input:
          - " GlobalLimitExec: skip=0, fetch=1"
          - "   SortPreservingMergeExec: [time@3 DESC NULLS LAST]"
          - "     UnionExec"
          - "       SortExec: expr=[time@3 DESC NULLS LAST]"
          - "         ParquetExec: file_groups={1 group: [[0.parquet]]}, projection=[col1, col2, field1, time, __chunk_order], output_ordering=[__chunk_order@4 ASC]"
          - "       SortExec: expr=[time@3 DESC NULLS LAST]"
          - "         UnionExec"
          - "           RecordBatchesExec: chunks=3, projection=[col1, col2, field1, time, __chunk_order]"
          - "           RecordBatchesExec: chunks=2, projection=[col1, col2, field1, time, __chunk_order]"
        output:
          Ok:
            - " GlobalLimitExec: skip=0, fetch=1"
            - "   ProgressiveEvalExec: input_ranges=[(TimestampNanosecond(2001, None), TimestampNanosecond(3500, None)), (TimestampNanosecond(1000, None), TimestampNanosecond(2000, None))]"
            - "     UnionExec"
            - "       SortExec: expr=[time@3 DESC NULLS LAST]"
            - "         UnionExec"
            - "           RecordBatchesExec: chunks=3, projection=[col1, col2, field1, time, __chunk_order]"
            - "           RecordBatchesExec: chunks=2, projection=[col1, col2, field1, time, __chunk_order]"
            - "       SortExec: expr=[time@3 DESC NULLS LAST]"
            - "         ParquetExec: file_groups={1 group: [[0.parquet]]}, projection=[col1, col2, field1, time, __chunk_order], output_ordering=[__chunk_order@4 ASC]"
        "###
        );
    }

    #[test]
    fn test_limit_mix_record_batch_parquet_2_desc() {
        test_helpers::maybe_start_logging();

        // Input plan:
        //
        // GlobalLimitExec: skip=0, fetch=2
        //  SortPreservingMerge: [time@2 DESC]
        //    UnionExec
        //      SortExec: expr=[time@2 DESC]   -- time range [1000, 2000]
        //        ParquetExec                         -- [1000, 2000]
        //      SortExec: expr=[time@2 DESC]   -- time range [2001, 3500] from combine time range of two record batches
        //        UnionExec
        //           SortExec: expr=[time@2 DESC]
        //              RecordBatchesExec             -- 2 chunks [2500, 3500]
        //           ParquetExec                      -- [2001, 3000]
        //
        // Output plan: the 2 SortExecs will be swapped the order to have time range [2001, 3500] first

        let schema = schema();
        let order = ordering_with_options(
            [
                ("col2", SortOp::Asc),
                ("col1", SortOp::Asc),
                ("time", SortOp::Asc),
            ],
            &schema,
        );

        let plan_parquet = parquet_exec_with_value_range(&schema, 1000, 2000);
        let plan_parquet2 = parquet_exec_with_value_range(&schema, 2001, 3000);
        let plan_batches = record_batches_exec_with_value_range(2, 2500, 3500);

        let plan_sort1 = Arc::new(SortExec::new(order.clone(), plan_batches));
        let plan_union_1 = Arc::new(UnionExec::new(vec![plan_sort1, plan_parquet2]));

        let sort_order = ordering_with_options([("time", SortOp::Desc)], &schema);
        let plan_sort2 = Arc::new(SortExec::new(sort_order.clone(), plan_parquet));
        let plan_sort3 = Arc::new(SortExec::new(sort_order.clone(), plan_union_1));

        let plan_union_2 = Arc::new(UnionExec::new(vec![plan_sort2, plan_sort3]));

        let plan_spm = Arc::new(SortPreservingMergeExec::new(
            sort_order.clone(),
            plan_union_2,
        ));

        let plan_limit = Arc::new(GlobalLimitExec::new(plan_spm, 0, Some(1)));

        let opt = OrderUnionSortedInputs;
        insta::assert_yaml_snapshot!(
            OptimizationTest::new(plan_limit, opt),
            @r###"
        ---
        input:
          - " GlobalLimitExec: skip=0, fetch=1"
          - "   SortPreservingMergeExec: [time@3 DESC NULLS LAST]"
          - "     UnionExec"
          - "       SortExec: expr=[time@3 DESC NULLS LAST]"
          - "         ParquetExec: file_groups={1 group: [[0.parquet]]}, projection=[col1, col2, field1, time, __chunk_order], output_ordering=[__chunk_order@4 ASC]"
          - "       SortExec: expr=[time@3 DESC NULLS LAST]"
          - "         UnionExec"
          - "           SortExec: expr=[col2@1 ASC NULLS LAST,col1@0 ASC NULLS LAST,time@3 ASC NULLS LAST]"
          - "             RecordBatchesExec: chunks=2, projection=[col1, col2, field1, time, __chunk_order]"
          - "           ParquetExec: file_groups={1 group: [[0.parquet]]}, projection=[col1, col2, field1, time, __chunk_order], output_ordering=[__chunk_order@4 ASC]"
        output:
          Ok:
            - " GlobalLimitExec: skip=0, fetch=1"
            - "   ProgressiveEvalExec: input_ranges=[(TimestampNanosecond(2001, None), TimestampNanosecond(3500, None)), (TimestampNanosecond(1000, None), TimestampNanosecond(2000, None))]"
            - "     UnionExec"
            - "       SortExec: expr=[time@3 DESC NULLS LAST]"
            - "         UnionExec"
            - "           SortExec: expr=[col2@1 ASC NULLS LAST,col1@0 ASC NULLS LAST,time@3 ASC NULLS LAST]"
            - "             RecordBatchesExec: chunks=2, projection=[col1, col2, field1, time, __chunk_order]"
            - "           ParquetExec: file_groups={1 group: [[0.parquet]]}, projection=[col1, col2, field1, time, __chunk_order], output_ordering=[__chunk_order@4 ASC]"
            - "       SortExec: expr=[time@3 DESC NULLS LAST]"
            - "         ParquetExec: file_groups={1 group: [[0.parquet]]}, projection=[col1, col2, field1, time, __chunk_order], output_ordering=[__chunk_order@4 ASC]"
        "###
        );
    }

    // test on non-time column & order desc
    #[test]
    fn test_limit_mix_record_batch_parquet_non_time_sort_desc() {
        test_helpers::maybe_start_logging();

        // Input plan:
        //
        // GlobalLimitExec: skip=0, fetch=2
        //  SortPreservingMerge: [field1@2 DESC]
        //    UnionExec
        //      SortExec: expr=[field1@2 DESC]   -- time range [1000, 2000]
        //        ParquetExec                         -- [1000, 2000]
        //      SortExec: expr=[field1@2 DESC]   -- time range [2001, 3500] from combine time range of two record batches
        //        UnionExec
        //           SortExec: expr=[field1@2 DESC]
        //              RecordBatchesExec             -- 2 chunks [2500, 3500]
        //           ParquetExec                      -- [2001, 3000]
        //
        // Output plan: the 2 SortExecs will be swapped the order to have time range [2001, 3500] first

        let schema = schema();
        let order = ordering_with_options(
            [
                ("col2", SortOp::Asc),
                ("col1", SortOp::Asc),
                ("field1", SortOp::Asc),
                ("time", SortOp::Asc),
            ],
            &schema,
        );

        let plan_parquet = parquet_exec_with_value_range(&schema, 1000, 2000);
        let plan_parquet2 = parquet_exec_with_value_range(&schema, 2001, 3000);
        let plan_batches = record_batches_exec_with_value_range(2, 2500, 3500);

        let plan_sort1 = Arc::new(SortExec::new(order.clone(), plan_batches));
        let plan_union_1 = Arc::new(UnionExec::new(vec![plan_sort1, plan_parquet2]));

        let sort_order = ordering_with_options([("field1", SortOp::Desc)], &schema);
        let plan_sort2 = Arc::new(SortExec::new(sort_order.clone(), plan_parquet));
        let plan_sort3 = Arc::new(SortExec::new(sort_order.clone(), plan_union_1));

        let plan_union_2 = Arc::new(UnionExec::new(vec![plan_sort2, plan_sort3]));

        let plan_spm = Arc::new(SortPreservingMergeExec::new(
            sort_order.clone(),
            plan_union_2,
        ));

        let plan_limit = Arc::new(GlobalLimitExec::new(plan_spm, 0, Some(1)));

        // Output plan: the 2 SortExecs will be swapped the order
        let opt = OrderUnionSortedInputs;
        insta::assert_yaml_snapshot!(
            OptimizationTest::new(plan_limit, opt),
            @r###"
        ---
        input:
          - " GlobalLimitExec: skip=0, fetch=1"
          - "   SortPreservingMergeExec: [field1@2 DESC NULLS LAST]"
          - "     UnionExec"
          - "       SortExec: expr=[field1@2 DESC NULLS LAST]"
          - "         ParquetExec: file_groups={1 group: [[0.parquet]]}, projection=[col1, col2, field1, time, __chunk_order], output_ordering=[__chunk_order@4 ASC]"
          - "       SortExec: expr=[field1@2 DESC NULLS LAST]"
          - "         UnionExec"
          - "           SortExec: expr=[col2@1 ASC NULLS LAST,col1@0 ASC NULLS LAST,field1@2 ASC NULLS LAST,time@3 ASC NULLS LAST]"
          - "             RecordBatchesExec: chunks=2, projection=[col1, col2, field1, time, __chunk_order]"
          - "           ParquetExec: file_groups={1 group: [[0.parquet]]}, projection=[col1, col2, field1, time, __chunk_order], output_ordering=[__chunk_order@4 ASC]"
        output:
          Ok:
            - " GlobalLimitExec: skip=0, fetch=1"
            - "   ProgressiveEvalExec: input_ranges=[(Int64(2001), Int64(3500)), (Int64(1000), Int64(2000))]"
            - "     UnionExec"
            - "       SortExec: expr=[field1@2 DESC NULLS LAST]"
            - "         UnionExec"
            - "           SortExec: expr=[col2@1 ASC NULLS LAST,col1@0 ASC NULLS LAST,field1@2 ASC NULLS LAST,time@3 ASC NULLS LAST]"
            - "             RecordBatchesExec: chunks=2, projection=[col1, col2, field1, time, __chunk_order]"
            - "           ParquetExec: file_groups={1 group: [[0.parquet]]}, projection=[col1, col2, field1, time, __chunk_order], output_ordering=[__chunk_order@4 ASC]"
            - "       SortExec: expr=[field1@2 DESC NULLS LAST]"
            - "         ParquetExec: file_groups={1 group: [[0.parquet]]}, projection=[col1, col2, field1, time, __chunk_order], output_ordering=[__chunk_order@4 ASC]"
        "###
        );
    }

    // test on non-time column & order asc
    #[test]
    fn test_limit_mix_record_batch_parquet_non_time_sort_asc() {
        test_helpers::maybe_start_logging();

        // Input plan:
        //
        // GlobalLimitExec: skip=0, fetch=2
        //  SortPreservingMerge: [field1@2 ASC]
        //    UnionExec
        //      SortExec: expr=[field1@2 ASC]   -- time range [1000, 2000]
        //        ParquetExec                         -- [1000, 2000]
        //      SortExec: expr=[field1@2 ASC]   -- time range [2001, 3500] from combine time range of two record batches
        //        UnionExec
        //           SortExec: expr=[field1@2 ASC]
        //              RecordBatchesExec             -- 2 chunks [2500, 3500]
        //           ParquetExec                      -- [2001, 3000]
        //
        // Output plan: same as input plan

        let schema = schema();
        let order = ordering_with_options(
            [
                ("col2", SortOp::Asc),
                ("col1", SortOp::Asc),
                ("field1", SortOp::Asc),
                ("time", SortOp::Asc),
            ],
            &schema,
        );

        let plan_parquet = parquet_exec_with_value_range(&schema, 1000, 2000);
        let plan_parquet2 = parquet_exec_with_value_range(&schema, 2001, 3000);
        let plan_batches = record_batches_exec_with_value_range(2, 2500, 3500);

        let plan_sort1 = Arc::new(SortExec::new(order.clone(), plan_batches));
        let plan_union_1 = Arc::new(UnionExec::new(vec![plan_sort1, plan_parquet2]));

        let sort_order = ordering_with_options([("field1", SortOp::Asc)], &schema);
        let plan_sort2 = Arc::new(SortExec::new(sort_order.clone(), plan_parquet));
        let plan_sort3 = Arc::new(SortExec::new(sort_order.clone(), plan_union_1));

        let plan_union_2 = Arc::new(UnionExec::new(vec![plan_sort2, plan_sort3]));

        let plan_spm = Arc::new(SortPreservingMergeExec::new(
            sort_order.clone(),
            plan_union_2,
        ));

        let plan_limit = Arc::new(GlobalLimitExec::new(plan_spm, 0, Some(1)));

        // input and output are the same
        let opt = OrderUnionSortedInputs;
        insta::assert_yaml_snapshot!(
            OptimizationTest::new(plan_limit, opt),
            @r###"
        ---
        input:
          - " GlobalLimitExec: skip=0, fetch=1"
          - "   SortPreservingMergeExec: [field1@2 ASC NULLS LAST]"
          - "     UnionExec"
          - "       SortExec: expr=[field1@2 ASC NULLS LAST]"
          - "         ParquetExec: file_groups={1 group: [[0.parquet]]}, projection=[col1, col2, field1, time, __chunk_order], output_ordering=[__chunk_order@4 ASC]"
          - "       SortExec: expr=[field1@2 ASC NULLS LAST]"
          - "         UnionExec"
          - "           SortExec: expr=[col2@1 ASC NULLS LAST,col1@0 ASC NULLS LAST,field1@2 ASC NULLS LAST,time@3 ASC NULLS LAST]"
          - "             RecordBatchesExec: chunks=2, projection=[col1, col2, field1, time, __chunk_order]"
          - "           ParquetExec: file_groups={1 group: [[0.parquet]]}, projection=[col1, col2, field1, time, __chunk_order], output_ordering=[__chunk_order@4 ASC]"
        output:
          Ok:
            - " GlobalLimitExec: skip=0, fetch=1"
            - "   ProgressiveEvalExec: input_ranges=[(Int64(1000), Int64(2000)), (Int64(2001), Int64(3500))]"
            - "     UnionExec"
            - "       SortExec: expr=[field1@2 ASC NULLS LAST]"
            - "         ParquetExec: file_groups={1 group: [[0.parquet]]}, projection=[col1, col2, field1, time, __chunk_order], output_ordering=[__chunk_order@4 ASC]"
            - "       SortExec: expr=[field1@2 ASC NULLS LAST]"
            - "         UnionExec"
            - "           SortExec: expr=[col2@1 ASC NULLS LAST,col1@0 ASC NULLS LAST,field1@2 ASC NULLS LAST,time@3 ASC NULLS LAST]"
            - "             RecordBatchesExec: chunks=2, projection=[col1, col2, field1, time, __chunk_order]"
            - "           ParquetExec: file_groups={1 group: [[0.parquet]]}, projection=[col1, col2, field1, time, __chunk_order], output_ordering=[__chunk_order@4 ASC]"
        "###
        );
    }

    // No limit & but the input is in the right sort preserving merge struct --> optimize
    #[test]
    fn test_spm_time_desc() {
        test_helpers::maybe_start_logging();

        // plan:
        //  SortPreservingMerge: [time@2 DESC]
        //    UnionExec
        //      SortExec: expr=[time@2 DESC]
        //        ParquetExec
        //      SortExec: expr=[time@2 DESC]
        //        UnionExec
        //          RecordBatchesExec
        //          ParquetExec
        //
        // Output: 2 SortExec are swapped

        let schema = schema();

        let plan_parquet = parquet_exec_with_value_range(&schema, 1000, 2000);
        let plan_parquet2 = parquet_exec_with_value_range(&schema, 2001, 3000);
        let plan_batches = record_batches_exec_with_value_range(2, 2500, 3500);

        let plan_union_1 = Arc::new(UnionExec::new(vec![plan_batches, plan_parquet2]));

        let sort_order = ordering_with_options([("time", SortOp::Desc)], &schema);
        let plan_sort1 = Arc::new(SortExec::new(sort_order.clone(), plan_parquet));
        let plan_sort2 = Arc::new(SortExec::new(sort_order.clone(), plan_union_1));

        let plan_union_2 = Arc::new(UnionExec::new(vec![plan_sort1, plan_sort2]));

        let plan_spm = Arc::new(SortPreservingMergeExec::new(
            sort_order.clone(),
            plan_union_2,
        ));

        // Output plan: the 2 SortExecs will be swapped the order
        let opt = OrderUnionSortedInputs;
        insta::assert_yaml_snapshot!(
            OptimizationTest::new(plan_spm, opt),
            @r###"
        ---
        input:
          - " SortPreservingMergeExec: [time@3 DESC NULLS LAST]"
          - "   UnionExec"
          - "     SortExec: expr=[time@3 DESC NULLS LAST]"
          - "       ParquetExec: file_groups={1 group: [[0.parquet]]}, projection=[col1, col2, field1, time, __chunk_order], output_ordering=[__chunk_order@4 ASC]"
          - "     SortExec: expr=[time@3 DESC NULLS LAST]"
          - "       UnionExec"
          - "         RecordBatchesExec: chunks=2, projection=[col1, col2, field1, time, __chunk_order]"
          - "         ParquetExec: file_groups={1 group: [[0.parquet]]}, projection=[col1, col2, field1, time, __chunk_order], output_ordering=[__chunk_order@4 ASC]"
        output:
          Ok:
            - " ProgressiveEvalExec: input_ranges=[(TimestampNanosecond(2001, None), TimestampNanosecond(3500, None)), (TimestampNanosecond(1000, None), TimestampNanosecond(2000, None))]"
            - "   UnionExec"
            - "     SortExec: expr=[time@3 DESC NULLS LAST]"
            - "       UnionExec"
            - "         RecordBatchesExec: chunks=2, projection=[col1, col2, field1, time, __chunk_order]"
            - "         ParquetExec: file_groups={1 group: [[0.parquet]]}, projection=[col1, col2, field1, time, __chunk_order], output_ordering=[__chunk_order@4 ASC]"
            - "     SortExec: expr=[time@3 DESC NULLS LAST]"
            - "       ParquetExec: file_groups={1 group: [[0.parquet]]}, projection=[col1, col2, field1, time, __chunk_order], output_ordering=[__chunk_order@4 ASC]"
        "###
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
        //        ParquetExec
        //      SortExec: expr=[field1@2 DESC]
        //        UnionExec
        //          RecordBatchesExec
        //          ParquetExec
        //
        // Output: 2 SortExec are swapped

        let schema = schema();

        let plan_parquet = parquet_exec_with_value_range(&schema, 1000, 2000);
        let plan_parquet2 = parquet_exec_with_value_range(&schema, 2001, 3000);
        let plan_batches = record_batches_exec_with_value_range(2, 2500, 3500);

        let plan_union_1 = Arc::new(UnionExec::new(vec![plan_batches, plan_parquet2]));

        let sort_order = ordering_with_options([("field1", SortOp::Desc)], &schema);
        let plan_sort1 = Arc::new(SortExec::new(sort_order.clone(), plan_parquet));
        let plan_sort2 = Arc::new(SortExec::new(sort_order.clone(), plan_union_1));

        let plan_union_2 = Arc::new(UnionExec::new(vec![plan_sort1, plan_sort2]));

        let plan_spm = Arc::new(SortPreservingMergeExec::new(
            sort_order.clone(),
            plan_union_2,
        ));

        // Output plan: the 2 SortExecs will be swapped the order
        let opt = OrderUnionSortedInputs;
        insta::assert_yaml_snapshot!(
            OptimizationTest::new(plan_spm, opt),
            @r###"
        ---
        input:
          - " SortPreservingMergeExec: [field1@2 DESC NULLS LAST]"
          - "   UnionExec"
          - "     SortExec: expr=[field1@2 DESC NULLS LAST]"
          - "       ParquetExec: file_groups={1 group: [[0.parquet]]}, projection=[col1, col2, field1, time, __chunk_order], output_ordering=[__chunk_order@4 ASC]"
          - "     SortExec: expr=[field1@2 DESC NULLS LAST]"
          - "       UnionExec"
          - "         RecordBatchesExec: chunks=2, projection=[col1, col2, field1, time, __chunk_order]"
          - "         ParquetExec: file_groups={1 group: [[0.parquet]]}, projection=[col1, col2, field1, time, __chunk_order], output_ordering=[__chunk_order@4 ASC]"
        output:
          Ok:
            - " ProgressiveEvalExec: input_ranges=[(Int64(2001), Int64(3500)), (Int64(1000), Int64(2000))]"
            - "   UnionExec"
            - "     SortExec: expr=[field1@2 DESC NULLS LAST]"
            - "       UnionExec"
            - "         RecordBatchesExec: chunks=2, projection=[col1, col2, field1, time, __chunk_order]"
            - "         ParquetExec: file_groups={1 group: [[0.parquet]]}, projection=[col1, col2, field1, time, __chunk_order], output_ordering=[__chunk_order@4 ASC]"
            - "     SortExec: expr=[field1@2 DESC NULLS LAST]"
            - "       ParquetExec: file_groups={1 group: [[0.parquet]]}, projection=[col1, col2, field1, time, __chunk_order], output_ordering=[__chunk_order@4 ASC]"
        "###
        );
    }

    // No limit & but the input is in the right sort preserving merge struct --> optimize
    #[test]
    fn test_spm_non_time_asc() {
        test_helpers::maybe_start_logging();

        // plan:
        //  SortPreservingMerge: [field1@2 ASC]
        //    UnionExec
        //      SortExec: expr=[field1@2 ASC]
        //        ParquetExec
        //      SortExec: expr=[field1@2 ASC]
        //        UnionExec
        //          RecordBatchesExec
        //          ParquetExec
        //
        // Output: 2 SortExec ordered as above

        let schema = schema();

        let plan_parquet = parquet_exec_with_value_range(&schema, 1000, 2000);
        let plan_parquet2 = parquet_exec_with_value_range(&schema, 2001, 3000);
        let plan_batches = record_batches_exec_with_value_range(2, 2500, 3500);

        let plan_union_1 = Arc::new(UnionExec::new(vec![plan_batches, plan_parquet2]));

        let sort_order = ordering_with_options([("field1", SortOp::Asc)], &schema);
        let plan_sort1 = Arc::new(SortExec::new(sort_order.clone(), plan_parquet));
        let plan_sort2 = Arc::new(SortExec::new(sort_order.clone(), plan_union_1));

        let plan_union_2 = Arc::new(UnionExec::new(vec![plan_sort1, plan_sort2]));

        let plan_spm = Arc::new(SortPreservingMergeExec::new(
            sort_order.clone(),
            plan_union_2,
        ));

        // output stays the same as input
        let opt = OrderUnionSortedInputs;
        insta::assert_yaml_snapshot!(
            OptimizationTest::new(plan_spm, opt),
            @r###"
        ---
        input:
          - " SortPreservingMergeExec: [field1@2 ASC NULLS LAST]"
          - "   UnionExec"
          - "     SortExec: expr=[field1@2 ASC NULLS LAST]"
          - "       ParquetExec: file_groups={1 group: [[0.parquet]]}, projection=[col1, col2, field1, time, __chunk_order], output_ordering=[__chunk_order@4 ASC]"
          - "     SortExec: expr=[field1@2 ASC NULLS LAST]"
          - "       UnionExec"
          - "         RecordBatchesExec: chunks=2, projection=[col1, col2, field1, time, __chunk_order]"
          - "         ParquetExec: file_groups={1 group: [[0.parquet]]}, projection=[col1, col2, field1, time, __chunk_order], output_ordering=[__chunk_order@4 ASC]"
        output:
          Ok:
            - " ProgressiveEvalExec: input_ranges=[(Int64(1000), Int64(2000)), (Int64(2001), Int64(3500))]"
            - "   UnionExec"
            - "     SortExec: expr=[field1@2 ASC NULLS LAST]"
            - "       ParquetExec: file_groups={1 group: [[0.parquet]]}, projection=[col1, col2, field1, time, __chunk_order], output_ordering=[__chunk_order@4 ASC]"
            - "     SortExec: expr=[field1@2 ASC NULLS LAST]"
            - "       UnionExec"
            - "         RecordBatchesExec: chunks=2, projection=[col1, col2, field1, time, __chunk_order]"
            - "         ParquetExec: file_groups={1 group: [[0.parquet]]}, projection=[col1, col2, field1, time, __chunk_order], output_ordering=[__chunk_order@4 ASC]"
        "###
        );
    }

    // Plan starts with SortPreservingMerge and includes deduplication & projections.
    // All conditions meet --> optimize
    #[test]
    fn test_spm_time_desc_with_dedupe_and_proj() {
        test_helpers::maybe_start_logging();

        // plan:
        //  SortPreservingMerge: [time@2 DESC]
        //    UnionExec
        //      SortExec: expr=[time@2 DESC]   -- time range [1000, 2000]
        //        ProjectionExec: expr=[time]
        //          ParquetExec                           -- [1000, 2000]
        //      SortExec: expr=[time@2 DESC]   -- time range [2001, 3500] from combine time range of record batches & parquet
        //        ProjectionExec: expr=[time]
        //          DeduplicateExec: [col1, col2, time]
        //              SortPreservingMergeExec: [col1 ASC, col2 ASC, time ASC]
        //                  UnionExec
        //                      SortExec: expr=[col1 ASC, col2 ASC, time ASC]
        //                          RecordBatchesExec           -- 2 chunks [2500, 3500]
        //                      SortExec: expr=[col1 ASC, col2 ASC, time ASC]
        //                          ParquetExec                     -- [2001, 3000]
        //
        // Output: 2 SortExec are swapped

        let schema = schema();

        let final_sort_order = ordering_with_options([("time", SortOp::Desc)], &schema);

        // Sort plan of the first parquet:
        //      SortExec: expr=[time@2 DESC]   -- time range [1000, 2000]
        //        ProjectionExec: expr=[time]
        //          ParquetExec
        let plan_parquet_1 = parquet_exec_with_value_range(&schema, 1000, 2000);
        let plan_projection_1 = Arc::new(
            ProjectionExec::try_new(
                vec![(expr_col("time", &schema), String::from("time"))],
                plan_parquet_1,
            )
            .unwrap(),
        );
        let plan_sort1 = Arc::new(SortExec::new(final_sort_order.clone(), plan_projection_1));

        // Sort plan of the second parquet and the record batch
        //      SortExec: expr=[time@2 DESC]   -- time range [2001, 3500] from combine time range of record batches & parquet
        //        ProjectionExec: expr=[time]
        //          DeduplicateExec: [col1, col2, time]
        //              SortPreservingMergeExec: [col1 ASC, col2 ASC, time ASC]
        //                  UnionExec
        //                      SortExec: expr=[col1 ASC, col2 ASC, time ASC]
        //                          RecordBatchesExec           -- 2 chunks [2500, 3500]
        //                      SortExec: expr=[col1 ASC, col2 ASC, time ASC]
        //                          ParquetExec                     -- [2001, 3000]
        let plan_parquet_2 = parquet_exec_with_value_range(&schema, 2001, 3000);
        let plan_batches = record_batches_exec_with_value_range(2, 2500, 3500);
        let dedupe_sort_order = ordering_with_options(
            [
                ("col1", SortOp::Asc),
                ("col2", SortOp::Asc),
                ("time", SortOp::Asc),
            ],
            &schema,
        );
        let plan_sort_rb = Arc::new(SortExec::new(dedupe_sort_order.clone(), plan_batches));
        let plan_sort_pq = Arc::new(SortExec::new(dedupe_sort_order.clone(), plan_parquet_2));
        let plan_union_1 = Arc::new(UnionExec::new(vec![plan_sort_rb, plan_sort_pq]));
        let plan_spm_1 = Arc::new(SortPreservingMergeExec::new(
            dedupe_sort_order.clone(),
            plan_union_1,
        ));
        let plan_dedupe = Arc::new(DeduplicateExec::new(plan_spm_1, dedupe_sort_order, false));
        let plan_projection_2 = Arc::new(
            ProjectionExec::try_new(
                vec![(expr_col("time", &schema), String::from("time"))],
                plan_dedupe,
            )
            .unwrap(),
        );
        let plan_sort2 = Arc::new(SortExec::new(final_sort_order.clone(), plan_projection_2));

        // Union them together
        let plan_union_2 = Arc::new(UnionExec::new(vec![plan_sort1, plan_sort2]));

        // SortPreservingMerge them
        let plan_spm = Arc::new(SortPreservingMergeExec::new(
            final_sort_order.clone(),
            plan_union_2,
        ));

        // compute statistics
        let min_max_spm = compute_stats_column_min_max(&*plan_spm, "time").unwrap();
        let min_max = column_statistics_min_max(&min_max_spm).unwrap();
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
            OptimizationTest::new(plan_spm, opt),
            @r###"
        ---
        input:
          - " SortPreservingMergeExec: [time@3 DESC NULLS LAST]"
          - "   UnionExec"
          - "     SortExec: expr=[time@3 DESC NULLS LAST]"
          - "       ProjectionExec: expr=[time@3 as time]"
          - "         ParquetExec: file_groups={1 group: [[0.parquet]]}, projection=[col1, col2, field1, time, __chunk_order], output_ordering=[__chunk_order@4 ASC]"
          - "     SortExec: expr=[time@3 DESC NULLS LAST]"
          - "       ProjectionExec: expr=[time@3 as time]"
          - "         DeduplicateExec: [col1@0 ASC NULLS LAST,col2@1 ASC NULLS LAST,time@3 ASC NULLS LAST]"
          - "           SortPreservingMergeExec: [col1@0 ASC NULLS LAST,col2@1 ASC NULLS LAST,time@3 ASC NULLS LAST]"
          - "             UnionExec"
          - "               SortExec: expr=[col1@0 ASC NULLS LAST,col2@1 ASC NULLS LAST,time@3 ASC NULLS LAST]"
          - "                 RecordBatchesExec: chunks=2, projection=[col1, col2, field1, time, __chunk_order]"
          - "               SortExec: expr=[col1@0 ASC NULLS LAST,col2@1 ASC NULLS LAST,time@3 ASC NULLS LAST]"
          - "                 ParquetExec: file_groups={1 group: [[0.parquet]]}, projection=[col1, col2, field1, time, __chunk_order], output_ordering=[__chunk_order@4 ASC]"
        output:
          Ok:
            - " ProgressiveEvalExec: input_ranges=[(TimestampNanosecond(2001, None), TimestampNanosecond(3500, None)), (TimestampNanosecond(1000, None), TimestampNanosecond(2000, None))]"
            - "   UnionExec"
            - "     SortExec: expr=[time@3 DESC NULLS LAST]"
            - "       ProjectionExec: expr=[time@3 as time]"
            - "         DeduplicateExec: [col1@0 ASC NULLS LAST,col2@1 ASC NULLS LAST,time@3 ASC NULLS LAST]"
            - "           SortPreservingMergeExec: [col1@0 ASC NULLS LAST,col2@1 ASC NULLS LAST,time@3 ASC NULLS LAST]"
            - "             UnionExec"
            - "               SortExec: expr=[col1@0 ASC NULLS LAST,col2@1 ASC NULLS LAST,time@3 ASC NULLS LAST]"
            - "                 RecordBatchesExec: chunks=2, projection=[col1, col2, field1, time, __chunk_order]"
            - "               SortExec: expr=[col1@0 ASC NULLS LAST,col2@1 ASC NULLS LAST,time@3 ASC NULLS LAST]"
            - "                 ParquetExec: file_groups={1 group: [[0.parquet]]}, projection=[col1, col2, field1, time, __chunk_order], output_ordering=[__chunk_order@4 ASC]"
            - "     SortExec: expr=[time@3 DESC NULLS LAST]"
            - "       ProjectionExec: expr=[time@3 as time]"
            - "         ParquetExec: file_groups={1 group: [[0.parquet]]}, projection=[col1, col2, field1, time, __chunk_order], output_ordering=[__chunk_order@4 ASC]"
        "###
        );
    }

    // ------------------------------------------------------------------
    // Negative tests: the right structure not found -> nothing optimized
    // ------------------------------------------------------------------

    // Right stucture but sort on 2 columns --> plan stays the same
    #[test]
    fn test_negative_spm_2_column_sort_desc() {
        test_helpers::maybe_start_logging();

        // plan:
        //  SortPreservingMerge: [time@3 DESC, field1@2 DESC]
        //    UnionExec
        //      SortExec: expr=[time@3 DESC, field1@2 DESC]
        //        ParquetExec
        //      SortExec: expr=[time@3 DESC, field1@2 DESC]
        //        UnionExec
        //          RecordBatchesExec
        //          ParquetExec
        //
        // Output: same as input

        let schema = schema();

        let plan_parquet = parquet_exec_with_value_range(&schema, 1000, 2000);
        let plan_parquet2 = parquet_exec_with_value_range(&schema, 2001, 3000);
        let plan_batches = record_batches_exec_with_value_range(2, 2500, 3500);

        let plan_union_1 = Arc::new(UnionExec::new(vec![plan_batches, plan_parquet2]));

        let sort_order =
            ordering_with_options([("time", SortOp::Desc), ("field1", SortOp::Desc)], &schema);
        let plan_sort1 = Arc::new(SortExec::new(sort_order.clone(), plan_parquet));
        let plan_sort2 = Arc::new(SortExec::new(sort_order.clone(), plan_union_1));

        let plan_union_2 = Arc::new(UnionExec::new(vec![plan_sort1, plan_sort2]));

        let plan_spm = Arc::new(SortPreservingMergeExec::new(
            sort_order.clone(),
            plan_union_2,
        ));

        // input and output are the same
        let opt = OrderUnionSortedInputs;
        insta::assert_yaml_snapshot!(
            OptimizationTest::new(plan_spm, opt),
            @r###"
        ---
        input:
          - " SortPreservingMergeExec: [time@3 DESC NULLS LAST,field1@2 DESC NULLS LAST]"
          - "   UnionExec"
          - "     SortExec: expr=[time@3 DESC NULLS LAST,field1@2 DESC NULLS LAST]"
          - "       ParquetExec: file_groups={1 group: [[0.parquet]]}, projection=[col1, col2, field1, time, __chunk_order], output_ordering=[__chunk_order@4 ASC]"
          - "     SortExec: expr=[time@3 DESC NULLS LAST,field1@2 DESC NULLS LAST]"
          - "       UnionExec"
          - "         RecordBatchesExec: chunks=2, projection=[col1, col2, field1, time, __chunk_order]"
          - "         ParquetExec: file_groups={1 group: [[0.parquet]]}, projection=[col1, col2, field1, time, __chunk_order], output_ordering=[__chunk_order@4 ASC]"
        output:
          Ok:
            - " SortPreservingMergeExec: [time@3 DESC NULLS LAST,field1@2 DESC NULLS LAST]"
            - "   UnionExec"
            - "     SortExec: expr=[time@3 DESC NULLS LAST,field1@2 DESC NULLS LAST]"
            - "       ParquetExec: file_groups={1 group: [[0.parquet]]}, projection=[col1, col2, field1, time, __chunk_order], output_ordering=[__chunk_order@4 ASC]"
            - "     SortExec: expr=[time@3 DESC NULLS LAST,field1@2 DESC NULLS LAST]"
            - "       UnionExec"
            - "         RecordBatchesExec: chunks=2, projection=[col1, col2, field1, time, __chunk_order]"
            - "         ParquetExec: file_groups={1 group: [[0.parquet]]}, projection=[col1, col2, field1, time, __chunk_order], output_ordering=[__chunk_order@4 ASC]"
        "###
        );
    }

    // No limit  & random plan --> plan stay the same
    #[test]
    fn test_negative_no_limit() {
        test_helpers::maybe_start_logging();

        let schema = schema();
        let order = ordering_with_options(
            [
                ("col2", SortOp::Asc),
                ("col1", SortOp::Asc),
                ("time", SortOp::Asc),
            ],
            &schema,
        );

        let plan_parquet = parquet_exec_with_value_range(&schema, 1000, 2000);
        let plan_batches = record_batches_exec_with_value_range(2, 1500, 2500);

        let plan = Arc::new(UnionExec::new(vec![plan_batches, plan_parquet]));
        let plan =
            Arc::new(RepartitionExec::try_new(plan, Partitioning::RoundRobinBatch(8)).unwrap());
        let hash_exprs = order.iter().cloned().map(|e| e.expr).collect();
        let plan =
            Arc::new(RepartitionExec::try_new(plan, Partitioning::Hash(hash_exprs, 8)).unwrap());
        let plan = Arc::new(SortExec::new(order.clone(), plan));
        let plan = Arc::new(DeduplicateExec::new(plan, order, true));

        // input and output are the same
        let opt = OrderUnionSortedInputs;
        insta::assert_yaml_snapshot!(
            OptimizationTest::new(plan, opt),
            @r###"
        ---
        input:
          - " DeduplicateExec: [col2@1 ASC NULLS LAST,col1@0 ASC NULLS LAST,time@3 ASC NULLS LAST]"
          - "   SortExec: expr=[col2@1 ASC NULLS LAST,col1@0 ASC NULLS LAST,time@3 ASC NULLS LAST]"
          - "     RepartitionExec: partitioning=Hash([col2@1, col1@0, time@3], 8), input_partitions=8"
          - "       RepartitionExec: partitioning=RoundRobinBatch(8), input_partitions=3"
          - "         UnionExec"
          - "           RecordBatchesExec: chunks=2, projection=[col1, col2, field1, time, __chunk_order]"
          - "           ParquetExec: file_groups={1 group: [[0.parquet]]}, projection=[col1, col2, field1, time, __chunk_order], output_ordering=[__chunk_order@4 ASC]"
        output:
          Ok:
            - " DeduplicateExec: [col2@1 ASC NULLS LAST,col1@0 ASC NULLS LAST,time@3 ASC NULLS LAST]"
            - "   SortExec: expr=[col2@1 ASC NULLS LAST,col1@0 ASC NULLS LAST,time@3 ASC NULLS LAST]"
            - "     RepartitionExec: partitioning=Hash([col2@1, col1@0, time@3], 8), input_partitions=8"
            - "       RepartitionExec: partitioning=RoundRobinBatch(8), input_partitions=3"
            - "         UnionExec"
            - "           RecordBatchesExec: chunks=2, projection=[col1, col2, field1, time, __chunk_order]"
            - "           ParquetExec: file_groups={1 group: [[0.parquet]]}, projection=[col1, col2, field1, time, __chunk_order], output_ordering=[__chunk_order@4 ASC]"
        "###
        );
    }

    // has limit but no sort preserving merge --> plan stay the same
    #[test]
    fn test_negative_limit_no_preserving_merge() {
        test_helpers::maybe_start_logging();

        let schema = schema();

        let plan_batches1 = record_batches_exec_with_value_range(1, 1000, 2000);
        let plan_batches2 = record_batches_exec_with_value_range(3, 2001, 3000);
        let plan_batches3 = record_batches_exec_with_value_range(2, 2500, 3500);

        let plan_union_1 = Arc::new(UnionExec::new(vec![plan_batches2, plan_batches3]));

        let sort_order = ordering_with_options([("time", SortOp::Desc)], &schema);
        let plan_sort1 = Arc::new(SortExec::new(sort_order.clone(), plan_batches1));
        let plan_sort2 = Arc::new(SortExec::new(sort_order.clone(), plan_union_1));

        let plan_union_2 = Arc::new(UnionExec::new(vec![plan_sort1, plan_sort2]));

        let plan_limit = Arc::new(GlobalLimitExec::new(plan_union_2, 0, Some(1)));

        // input and output are the same
        let opt = OrderUnionSortedInputs;
        insta::assert_yaml_snapshot!(
            OptimizationTest::new(plan_limit, opt),
            @r###"
        ---
        input:
          - " GlobalLimitExec: skip=0, fetch=1"
          - "   UnionExec"
          - "     SortExec: expr=[time@3 DESC NULLS LAST]"
          - "       RecordBatchesExec: chunks=1, projection=[col1, col2, field1, time, __chunk_order]"
          - "     SortExec: expr=[time@3 DESC NULLS LAST]"
          - "       UnionExec"
          - "         RecordBatchesExec: chunks=3, projection=[col1, col2, field1, time, __chunk_order]"
          - "         RecordBatchesExec: chunks=2, projection=[col1, col2, field1, time, __chunk_order]"
        output:
          Ok:
            - " GlobalLimitExec: skip=0, fetch=1"
            - "   UnionExec"
            - "     SortExec: expr=[time@3 DESC NULLS LAST]"
            - "       RecordBatchesExec: chunks=1, projection=[col1, col2, field1, time, __chunk_order]"
            - "     SortExec: expr=[time@3 DESC NULLS LAST]"
            - "       UnionExec"
            - "         RecordBatchesExec: chunks=3, projection=[col1, col2, field1, time, __chunk_order]"
            - "         RecordBatchesExec: chunks=2, projection=[col1, col2, field1, time, __chunk_order]"
        "###
        );
    }

    // right structure and same sort order but inputs of uion overlap --> plan stay the same
    #[test]
    fn test_negative_overlap() {
        test_helpers::maybe_start_logging();

        // Input plan:
        //
        // GlobalLimitExec: skip=0, fetch=2
        //  SortPreservingMerge: [time@2 DESC]
        //    UnionExec
        //      SortExec: expr=[time@2 DESC]   -- time range [1000, 2000]  that overlaps with the other SorExec
        //        ParquetExec                         -- [1000, 2000]
        //      SortExec: expr=[time@2 DESC]   -- time range [2000, 3500] from combine time range of two record batches
        //        UnionExec
        //           SortExec: expr=[time@2 DESC]
        //              RecordBatchesExec             -- 2 chunks [2500, 3500]
        //           ParquetExec                      -- [2000, 3000]

        let schema = schema();
        let order = ordering_with_options(
            [
                ("col2", SortOp::Asc),
                ("col1", SortOp::Asc),
                ("time", SortOp::Asc),
            ],
            &schema,
        );

        let plan_parquet = parquet_exec_with_value_range(&schema, 1000, 2000);
        let plan_parquet2 = parquet_exec_with_value_range(&schema, 2000, 3000);
        let plan_batches = record_batches_exec_with_value_range(2, 2500, 3500);

        let plan_sort1 = Arc::new(SortExec::new(order.clone(), plan_batches));
        let plan_union_1 = Arc::new(UnionExec::new(vec![plan_sort1, plan_parquet2]));

        let sort_order = ordering_with_options([("time", SortOp::Desc)], &schema);
        let plan_sort2 = Arc::new(SortExec::new(sort_order.clone(), plan_parquet));
        let plan_sort3 = Arc::new(SortExec::new(sort_order.clone(), plan_union_1));

        let plan_union_2 = Arc::new(UnionExec::new(vec![plan_sort2, plan_sort3]));

        let plan_spm = Arc::new(SortPreservingMergeExec::new(
            sort_order.clone(),
            plan_union_2,
        ));

        let plan_limit = Arc::new(GlobalLimitExec::new(plan_spm, 0, Some(1)));

        // input and output are the same
        let opt = OrderUnionSortedInputs;
        insta::assert_yaml_snapshot!(
            OptimizationTest::new(plan_limit, opt),
            @r###"
        ---
        input:
          - " GlobalLimitExec: skip=0, fetch=1"
          - "   SortPreservingMergeExec: [time@3 DESC NULLS LAST]"
          - "     UnionExec"
          - "       SortExec: expr=[time@3 DESC NULLS LAST]"
          - "         ParquetExec: file_groups={1 group: [[0.parquet]]}, projection=[col1, col2, field1, time, __chunk_order], output_ordering=[__chunk_order@4 ASC]"
          - "       SortExec: expr=[time@3 DESC NULLS LAST]"
          - "         UnionExec"
          - "           SortExec: expr=[col2@1 ASC NULLS LAST,col1@0 ASC NULLS LAST,time@3 ASC NULLS LAST]"
          - "             RecordBatchesExec: chunks=2, projection=[col1, col2, field1, time, __chunk_order]"
          - "           ParquetExec: file_groups={1 group: [[0.parquet]]}, projection=[col1, col2, field1, time, __chunk_order], output_ordering=[__chunk_order@4 ASC]"
        output:
          Ok:
            - " GlobalLimitExec: skip=0, fetch=1"
            - "   SortPreservingMergeExec: [time@3 DESC NULLS LAST]"
            - "     UnionExec"
            - "       SortExec: expr=[time@3 DESC NULLS LAST]"
            - "         ParquetExec: file_groups={1 group: [[0.parquet]]}, projection=[col1, col2, field1, time, __chunk_order], output_ordering=[__chunk_order@4 ASC]"
            - "       SortExec: expr=[time@3 DESC NULLS LAST]"
            - "         UnionExec"
            - "           SortExec: expr=[col2@1 ASC NULLS LAST,col1@0 ASC NULLS LAST,time@3 ASC NULLS LAST]"
            - "             RecordBatchesExec: chunks=2, projection=[col1, col2, field1, time, __chunk_order]"
            - "           ParquetExec: file_groups={1 group: [[0.parquet]]}, projection=[col1, col2, field1, time, __chunk_order], output_ordering=[__chunk_order@4 ASC]"
        "###
        );
    }

    // No limit & but the input is in the right union struct --> plan stay the same
    #[test]
    fn test_negative_no_sortpreservingmerge_input_union() {
        test_helpers::maybe_start_logging();

        // plan:
        //    UnionExec
        //      SortExec: expr=[time@2 DESC]
        //        ParquetExec
        //      SortExec: expr=[time@2 DESC]
        //        UnionExec
        //          RecordBatchesExec
        //          ParquetExec

        let schema = schema();

        let plan_parquet = parquet_exec_with_value_range(&schema, 1000, 2000);
        let plan_parquet2 = parquet_exec_with_value_range(&schema, 2001, 3000);
        let plan_batches = record_batches_exec_with_value_range(2, 2500, 3500);

        let plan_union_1 = Arc::new(UnionExec::new(vec![plan_batches, plan_parquet2]));

        let sort_order = ordering_with_options([("time", SortOp::Desc)], &schema);
        let plan_sort1 = Arc::new(SortExec::new(sort_order.clone(), plan_parquet));
        let plan_sort2 = Arc::new(SortExec::new(sort_order.clone(), plan_union_1));

        let plan_union_2 = Arc::new(UnionExec::new(vec![plan_sort1, plan_sort2]));

        // input and output are the same
        let opt = OrderUnionSortedInputs;
        insta::assert_yaml_snapshot!(
            OptimizationTest::new(plan_union_2, opt),
            @r###"
        ---
        input:
          - " UnionExec"
          - "   SortExec: expr=[time@3 DESC NULLS LAST]"
          - "     ParquetExec: file_groups={1 group: [[0.parquet]]}, projection=[col1, col2, field1, time, __chunk_order], output_ordering=[__chunk_order@4 ASC]"
          - "   SortExec: expr=[time@3 DESC NULLS LAST]"
          - "     UnionExec"
          - "       RecordBatchesExec: chunks=2, projection=[col1, col2, field1, time, __chunk_order]"
          - "       ParquetExec: file_groups={1 group: [[0.parquet]]}, projection=[col1, col2, field1, time, __chunk_order], output_ordering=[__chunk_order@4 ASC]"
        output:
          Ok:
            - " UnionExec"
            - "   SortExec: expr=[time@3 DESC NULLS LAST]"
            - "     ParquetExec: file_groups={1 group: [[0.parquet]]}, projection=[col1, col2, field1, time, __chunk_order], output_ordering=[__chunk_order@4 ASC]"
            - "   SortExec: expr=[time@3 DESC NULLS LAST]"
            - "     UnionExec"
            - "       RecordBatchesExec: chunks=2, projection=[col1, col2, field1, time, __chunk_order]"
            - "       ParquetExec: file_groups={1 group: [[0.parquet]]}, projection=[col1, col2, field1, time, __chunk_order], output_ordering=[__chunk_order@4 ASC]"
        "###
        );
    }

    // Projection expression (field + field) ==> not optimze. Plan stays the same
    #[test]
    fn test_negative_spm_time_desc_with_dedupe_and_proj_on_expr() {
        test_helpers::maybe_start_logging();

        // plan:
        //  SortPreservingMerge: [time@2 DESC]
        //    UnionExec
        //      SortExec: expr=[time@2 DESC]   -- time range [1000, 2000]
        //        ProjectionExec: expr=[field1 + field1, time]                                <-- NOTE: has expresssion col1+col2
        //          ParquetExec                           -- [1000, 2000]
        //      SortExec: expr=[time@2 DESC]   -- time range [2001, 3500] from combine time range of record batches & parquet
        //        ProjectionExec: expr=[field1 + field1, time]                                <-- NOTE: has expresssion col1+col2
        //          DeduplicateExec: [col1, col2, time]
        //              SortPreservingMergeExec: [col1 ASC, col2 ASC, time ASC]
        //                  UnionExec
        //                      SortExec: expr=[col1 ASC, col2 ASC, time ASC]
        //                          RecordBatchesExec           -- 2 chunks [2500, 3500]
        //                      SortExec: expr=[col1 ASC, col2 ASC, time ASC]
        //                          ParquetExec                     -- [2001, 3000]

        let schema = schema();

        let final_sort_order = ordering_with_options([("time", SortOp::Desc)], &schema);

        // Sort plan of the first parquet:
        //      SortExec: expr=[time@2 DESC]   -- time range [1000, 2000]
        //        ProjectionExec: expr=[field1 + field1, time]
        //          ParquetExec
        let plan_parquet_1 = parquet_exec_with_value_range(&schema, 1000, 2000);

        let field_expr = Arc::new(BinaryExpr::new(
            Arc::new(Column::new_with_schema("field1", &schema).unwrap()),
            Operator::Plus,
            Arc::new(Column::new_with_schema("field1", &schema).unwrap()),
        ));
        let plan_projection_1 = Arc::new(
            ProjectionExec::try_new(
                vec![
                    (Arc::<BinaryExpr>::clone(&field_expr), String::from("field")),
                    (expr_col("time", &schema), String::from("time")),
                ],
                plan_parquet_1,
            )
            .unwrap(),
        );
        let plan_sort1 = Arc::new(SortExec::new(final_sort_order.clone(), plan_projection_1));

        // Sort plan of the second parquet and the record batch
        //      SortExec: expr=[time@2 DESC]   -- time range [2001, 3500] from combine time range of record batches & parquet
        //        ProjectionExec: expr=[field1 + field1, time]
        //          DeduplicateExec: [col1, col2, time]
        //              SortPreservingMergeExec: [col1 ASC, col2 ASC, time ASC]
        //                  UnionExec
        //                      SortExec: expr=[col1 ASC, col2 ASC, time ASC]
        //                          RecordBatchesExec           -- 2 chunks [2500, 3500]
        //                      SortExec: expr=[col1 ASC, col2 ASC, time ASC]
        //                          ParquetExec                     -- [2001, 3000]
        let plan_parquet_2 = parquet_exec_with_value_range(&schema, 2001, 3000);
        let plan_batches = record_batches_exec_with_value_range(2, 2500, 3500);
        let dedupe_sort_order = ordering_with_options(
            [
                ("col1", SortOp::Asc),
                ("col2", SortOp::Asc),
                ("time", SortOp::Asc),
            ],
            &schema,
        );
        let plan_sort_rb = Arc::new(SortExec::new(dedupe_sort_order.clone(), plan_batches));
        let plan_sort_pq = Arc::new(SortExec::new(dedupe_sort_order.clone(), plan_parquet_2));
        let plan_union_1 = Arc::new(UnionExec::new(vec![plan_sort_rb, plan_sort_pq]));
        let plan_spm_1 = Arc::new(SortPreservingMergeExec::new(
            dedupe_sort_order.clone(),
            plan_union_1,
        ));
        let plan_dedupe = Arc::new(DeduplicateExec::new(plan_spm_1, dedupe_sort_order, false));
        let plan_projection_2 = Arc::new(
            ProjectionExec::try_new(
                vec![
                    (field_expr, String::from("field")),
                    (expr_col("time", &schema), String::from("time")),
                ],
                plan_dedupe,
            )
            .unwrap(),
        );
        let plan_sort2 = Arc::new(SortExec::new(final_sort_order.clone(), plan_projection_2));

        // Union them together
        let plan_union_2 = Arc::new(UnionExec::new(vec![plan_sort1, plan_sort2]));

        // SortPreservingMerge them
        let plan_spm = Arc::new(SortPreservingMergeExec::new(
            final_sort_order.clone(),
            plan_union_2,
        ));

        // compute statistics: no stats becasue the ProjectionExec includes expression
        let min_max_spm = compute_stats_column_min_max(&*plan_spm, "time").unwrap();
        let min_max = column_statistics_min_max(&min_max_spm);
        assert!(min_max.is_none());

        // output plan stays the same
        let opt = OrderUnionSortedInputs;
        insta::assert_yaml_snapshot!(
            OptimizationTest::new(plan_spm, opt),
            @r###"
        ---
        input:
          - " SortPreservingMergeExec: [time@3 DESC NULLS LAST]"
          - "   UnionExec"
          - "     SortExec: expr=[time@3 DESC NULLS LAST]"
          - "       ProjectionExec: expr=[field1@2 + field1@2 as field, time@3 as time]"
          - "         ParquetExec: file_groups={1 group: [[0.parquet]]}, projection=[col1, col2, field1, time, __chunk_order], output_ordering=[__chunk_order@4 ASC]"
          - "     SortExec: expr=[time@3 DESC NULLS LAST]"
          - "       ProjectionExec: expr=[field1@2 + field1@2 as field, time@3 as time]"
          - "         DeduplicateExec: [col1@0 ASC NULLS LAST,col2@1 ASC NULLS LAST,time@3 ASC NULLS LAST]"
          - "           SortPreservingMergeExec: [col1@0 ASC NULLS LAST,col2@1 ASC NULLS LAST,time@3 ASC NULLS LAST]"
          - "             UnionExec"
          - "               SortExec: expr=[col1@0 ASC NULLS LAST,col2@1 ASC NULLS LAST,time@3 ASC NULLS LAST]"
          - "                 RecordBatchesExec: chunks=2, projection=[col1, col2, field1, time, __chunk_order]"
          - "               SortExec: expr=[col1@0 ASC NULLS LAST,col2@1 ASC NULLS LAST,time@3 ASC NULLS LAST]"
          - "                 ParquetExec: file_groups={1 group: [[0.parquet]]}, projection=[col1, col2, field1, time, __chunk_order], output_ordering=[__chunk_order@4 ASC]"
        output:
          Ok:
            - " SortPreservingMergeExec: [time@3 DESC NULLS LAST]"
            - "   UnionExec"
            - "     SortExec: expr=[time@3 DESC NULLS LAST]"
            - "       ProjectionExec: expr=[field1@2 + field1@2 as field, time@3 as time]"
            - "         ParquetExec: file_groups={1 group: [[0.parquet]]}, projection=[col1, col2, field1, time, __chunk_order], output_ordering=[__chunk_order@4 ASC]"
            - "     SortExec: expr=[time@3 DESC NULLS LAST]"
            - "       ProjectionExec: expr=[field1@2 + field1@2 as field, time@3 as time]"
            - "         DeduplicateExec: [col1@0 ASC NULLS LAST,col2@1 ASC NULLS LAST,time@3 ASC NULLS LAST]"
            - "           SortPreservingMergeExec: [col1@0 ASC NULLS LAST,col2@1 ASC NULLS LAST,time@3 ASC NULLS LAST]"
            - "             UnionExec"
            - "               SortExec: expr=[col1@0 ASC NULLS LAST,col2@1 ASC NULLS LAST,time@3 ASC NULLS LAST]"
            - "                 RecordBatchesExec: chunks=2, projection=[col1, col2, field1, time, __chunk_order]"
            - "               SortExec: expr=[col1@0 ASC NULLS LAST,col2@1 ASC NULLS LAST,time@3 ASC NULLS LAST]"
            - "                 ParquetExec: file_groups={1 group: [[0.parquet]]}, projection=[col1, col2, field1, time, __chunk_order], output_ordering=[__chunk_order@4 ASC]"
        "###
        );
    }

    // ------------------------------------------------------------------
    // Helper functions
    // ------------------------------------------------------------------

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
        let chunk = TestChunk::new("t")
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

    fn record_batches_exec_with_value_range(
        n_chunks: usize,
        min: i64,
        max: i64,
    ) -> Arc<dyn ExecutionPlan> {
        let chunks = std::iter::repeat(test_chunk(min, max, false))
            .take(n_chunks)
            .collect::<Vec<_>>();

        Arc::new(RecordBatchesExec::new(chunks, schema(), None))
    }

    fn parquet_exec_with_value_range(
        schema: &SchemaRef,
        min: i64,
        max: i64,
    ) -> Arc<dyn ExecutionPlan> {
        let chunk = test_chunk(min, max, true);
        let plan = chunks_to_physical_nodes(schema, None, vec![chunk], 1);

        if let Some(union_exec) = plan.as_any().downcast_ref::<UnionExec>() {
            if union_exec.inputs().len() == 1 {
                Arc::clone(&union_exec.inputs()[0])
            } else {
                plan
            }
        } else {
            plan
        }
    }

    fn ordering_with_options<const N: usize>(
        cols: [(&str, SortOp); N],
        schema: &SchemaRef,
    ) -> Vec<PhysicalSortExpr> {
        cols.into_iter()
            .map(|col| PhysicalSortExpr {
                expr: Arc::new(Column::new_with_schema(col.0, schema.as_ref()).unwrap()),
                options: SortOptions {
                    descending: col.1 == SortOp::Desc,
                    nulls_first: false,
                },
            })
            .collect()
    }

    #[derive(Debug, PartialEq)]
    enum SortOp {
        Asc,
        Desc,
    }
}
