/// Unit tests for constants.
#[cfg(test)]
mod test {
    use super::super::order_union_sorted_inputs::OrderUnionSortedInputs;

    use std::sync::Arc;

    use arrow::{
        compute::SortOptions,
        datatypes::{DataType, SchemaRef},
    };
    use datafusion::{
        physical_expr::{LexOrdering, PhysicalSortExpr},
        physical_plan::{
            ExecutionPlan, PhysicalExpr,
            expressions::{Column, Literal},
            projection::ProjectionExec,
            sorts::{sort::SortExec, sort_preserving_merge::SortPreservingMergeExec},
            union::UnionExec,
        },
        scalar::ScalarValue,
    };
    use schema::{InfluxFieldType, SchemaBuilder as IOxSchemaBuilder};

    use crate::{
        CHUNK_ORDER_COLUMN_NAME, QueryChunk,
        physical_optimizer::test_util::OptimizationTest,
        provider::{RecordBatchesExec, chunks_to_physical_nodes},
        test::TestChunk,
    };

    // ------------------------------------------------------------------
    // Positive tests: the right structure found -> plan optimized
    // ------------------------------------------------------------------

    #[test]
    fn test_replace_spm_with_pe() {
        test_helpers::maybe_start_logging();

        let schema = schema();
        let sort_order = sort_order_for_sort();

        // First sort on parquet file
        let plan_parquet = data_source_exec_parquet_with_value_range(&schema, 1000, 2000);
        let plan_projection_1 = Arc::new(
            ProjectionExec::try_new(
                projection_expr_with_2_constants("m1", "tag0", &schema),
                plan_parquet,
            )
            .unwrap(),
        );
        let plan_sort1 = Arc::new(SortExec::new(sort_order.clone(), plan_projection_1));

        // Second sort on record batch
        let plan_batches = record_batches_exec_with_value_range(1, 2001, 3000);
        let plan_projection_2 = Arc::new(
            ProjectionExec::try_new(
                projection_expr_with_2_constants("m0", "tag0", &schema),
                plan_batches,
            )
            .unwrap(),
        );
        let plan_sort2 = Arc::new(SortExec::new(sort_order.clone(), plan_projection_2));

        // union the 2 sorts
        let plan_union = Arc::new(UnionExec::new(vec![plan_sort1, plan_sort2]));

        // add sort preserving merge on top
        let plan_spm = Arc::new(SortPreservingMergeExec::new(
            sort_order_for_sort_preserving_merge(),
            plan_union,
        ));

        // Output plan:
        //   . SortPreservingMergeExec will be replaced with ProgressiveEvalExec
        //   . the 2 SortExecs will be swapped the order to have "m0" first
        let opt = OrderUnionSortedInputs;
        insta::assert_yaml_snapshot!(
            OptimizationTest::new(plan_spm, opt),
            @r#"
        input:
          - " SortPreservingMergeExec: [iox::measurement@0 ASC NULLS LAST, key@1 ASC NULLS LAST, value@2 ASC NULLS LAST]"
          - "   UnionExec"
          - "     SortExec: expr=[value@2 ASC NULLS LAST], preserve_partitioning=[false]"
          - "       ProjectionExec: expr=[m1 as iox::measurement, tag0 as key, tag0@1 as value]"
          - "         DataSourceExec: file_groups={1 group: [[0.parquet]]}, projection=[tag2, tag0, tag1, field1, time, __chunk_order], output_ordering=[__chunk_order@5 ASC], file_type=parquet"
          - "     SortExec: expr=[value@2 ASC NULLS LAST], preserve_partitioning=[false]"
          - "       ProjectionExec: expr=[m0 as iox::measurement, tag0 as key, tag0@1 as value]"
          - "         RecordBatchesExec: chunks=1, projection=[tag2, tag0, tag1, field1, time, __chunk_order]"
        output:
          Ok:
            - " ProgressiveEvalExec: input_ranges=[(m0,tag0)->(m0,tag0), (m1,tag0)->(m1,tag0)]"
            - "   ReorderPartitionsExec: mapped_partition_indices=[1, 0]"
            - "     UnionExec"
            - "       SortExec: expr=[value@2 ASC NULLS LAST], preserve_partitioning=[false]"
            - "         ProjectionExec: expr=[m1 as iox::measurement, tag0 as key, tag0@1 as value]"
            - "           DataSourceExec: file_groups={1 group: [[0.parquet]]}, projection=[tag2, tag0, tag1, field1, time, __chunk_order], output_ordering=[__chunk_order@5 ASC], file_type=parquet"
            - "       SortExec: expr=[value@2 ASC NULLS LAST], preserve_partitioning=[false]"
            - "         ProjectionExec: expr=[m0 as iox::measurement, tag0 as key, tag0@1 as value]"
            - "           RecordBatchesExec: chunks=1, projection=[tag2, tag0, tag1, field1, time, __chunk_order]"
        "#
        );
    }

    #[test]
    fn test_replace_many_spm_with_pe() {
        test_helpers::maybe_start_logging();

        let schema = schema();
        let sort_order = sort_order_for_sort();

        // First sort: first col: constant m1, second column: constant tag0, third column: value of seconnd column (tag0)
        let plan_parquet = data_source_exec_parquet_with_value_range(&schema, 1000, 2000);
        let plan_projection_1 = Arc::new(
            ProjectionExec::try_new(
                projection_expr_with_2_constants("m1", "tag0", &schema),
                plan_parquet,
            )
            .unwrap(),
        );
        let plan_sort1 = Arc::new(SortExec::new(sort_order.clone(), plan_projection_1));

        // Second sort: first col: constant m0, second column: constant tag1, third column: value of seconnd column (tag1)
        let plan_batches = record_batches_exec_with_value_range(1, 2001, 3000);
        let plan_projection_2 = Arc::new(
            ProjectionExec::try_new(
                projection_expr_with_2_constants("m0", "tag1", &schema),
                plan_batches,
            )
            .unwrap(),
        );
        let plan_sort2 = Arc::new(SortExec::new(sort_order.clone(), plan_projection_2));

        // Third sort: first col: constant m0, second column: constant tag0, third column: value of seconnd column (tag0)
        let plan_batches2 = record_batches_exec_with_value_range(1, 2001, 3000);
        let plan_projection_3 = Arc::new(
            ProjectionExec::try_new(
                projection_expr_with_2_constants("m0", "tag0", &schema),
                plan_batches2,
            )
            .unwrap(),
        );
        let plan_sort3 = Arc::new(SortExec::new(sort_order.clone(), plan_projection_3));

        // union the 3 sorts
        let plan_union = Arc::new(UnionExec::new(vec![plan_sort1, plan_sort2, plan_sort3]));

        // add sort preserving merge on top
        let plan_spm = Arc::new(SortPreservingMergeExec::new(
            sort_order_for_sort_preserving_merge(),
            plan_union,
        ));

        // Output plan: There are 2 ProgressiveEvalExecs
        //  . One on top repalcing the top SortPreservingMergeExec
        //  . One on top of a new UnionExec that unions the tow SortExecs on m0
        // All the streams are sorted accordingly to have output data sorted : m0tag0, m0tag1, m1tag0
        let opt = OrderUnionSortedInputs;
        insta::assert_yaml_snapshot!(
            OptimizationTest::new(plan_spm, opt),
            @r#"
        input:
          - " SortPreservingMergeExec: [iox::measurement@0 ASC NULLS LAST, key@1 ASC NULLS LAST, value@2 ASC NULLS LAST]"
          - "   UnionExec"
          - "     SortExec: expr=[value@2 ASC NULLS LAST], preserve_partitioning=[false]"
          - "       ProjectionExec: expr=[m1 as iox::measurement, tag0 as key, tag0@1 as value]"
          - "         DataSourceExec: file_groups={1 group: [[0.parquet]]}, projection=[tag2, tag0, tag1, field1, time, __chunk_order], output_ordering=[__chunk_order@5 ASC], file_type=parquet"
          - "     SortExec: expr=[value@2 ASC NULLS LAST], preserve_partitioning=[false]"
          - "       ProjectionExec: expr=[m0 as iox::measurement, tag1 as key, tag1@2 as value]"
          - "         RecordBatchesExec: chunks=1, projection=[tag2, tag0, tag1, field1, time, __chunk_order]"
          - "     SortExec: expr=[value@2 ASC NULLS LAST], preserve_partitioning=[false]"
          - "       ProjectionExec: expr=[m0 as iox::measurement, tag0 as key, tag0@1 as value]"
          - "         RecordBatchesExec: chunks=1, projection=[tag2, tag0, tag1, field1, time, __chunk_order]"
        output:
          Ok:
            - " ProgressiveEvalExec: input_ranges=[(m0,tag0)->(m0,tag0), (m0,tag1)->(m0,tag1), (m1,tag0)->(m1,tag0)]"
            - "   ReorderPartitionsExec: mapped_partition_indices=[2, 1, 0]"
            - "     UnionExec"
            - "       SortExec: expr=[value@2 ASC NULLS LAST], preserve_partitioning=[false]"
            - "         ProjectionExec: expr=[m1 as iox::measurement, tag0 as key, tag0@1 as value]"
            - "           DataSourceExec: file_groups={1 group: [[0.parquet]]}, projection=[tag2, tag0, tag1, field1, time, __chunk_order], output_ordering=[__chunk_order@5 ASC], file_type=parquet"
            - "       SortExec: expr=[value@2 ASC NULLS LAST], preserve_partitioning=[false]"
            - "         ProjectionExec: expr=[m0 as iox::measurement, tag1 as key, tag1@2 as value]"
            - "           RecordBatchesExec: chunks=1, projection=[tag2, tag0, tag1, field1, time, __chunk_order]"
            - "       SortExec: expr=[value@2 ASC NULLS LAST], preserve_partitioning=[false]"
            - "         ProjectionExec: expr=[m0 as iox::measurement, tag0 as key, tag0@1 as value]"
            - "           RecordBatchesExec: chunks=1, projection=[tag2, tag0, tag1, field1, time, __chunk_order]"
        "#
        );
    }

    // The contants are integers
    #[test]
    fn test_replace_spm_with_pe_integer_constants() {
        test_helpers::maybe_start_logging();

        let schema = schema();
        let sort_order = sort_order_for_sort();

        // First sort on parquet file
        let plan_parquet = data_source_exec_parquet_with_value_range(&schema, 1000, 2000);
        let plan_projection_1 = Arc::new(
            ProjectionExec::try_new(
                projection_expr_with_2_constant_integers(1, 10, &schema),
                plan_parquet,
            )
            .unwrap(),
        );
        let plan_sort1 = Arc::new(SortExec::new(sort_order.clone(), plan_projection_1));

        // Second sort on record batch
        let plan_batches = record_batches_exec_with_value_range(1, 2001, 3000);
        let plan_projection_2 = Arc::new(
            ProjectionExec::try_new(
                projection_expr_with_2_constant_integers(2, 20, &schema),
                plan_batches,
            )
            .unwrap(),
        );
        let plan_sort2 = Arc::new(SortExec::new(sort_order.clone(), plan_projection_2));

        // union the 2 sorts
        let plan_union = Arc::new(UnionExec::new(vec![plan_sort1, plan_sort2]));

        // add sort preserving merge on top
        let plan_spm = Arc::new(SortPreservingMergeExec::new(
            sort_order_for_sort_preserving_merge(),
            plan_union,
        ));

        // Output plan:
        //   . SortPreservingMergeExec will be replaced with ProgressiveEvalExec
        //   . the 2 SortExecs will be swapped the order to have "m0" first
        let opt = OrderUnionSortedInputs;
        insta::assert_yaml_snapshot!(
            OptimizationTest::new(plan_spm, opt),
            @r#"
        input:
          - " SortPreservingMergeExec: [iox::measurement@0 ASC NULLS LAST, key@1 ASC NULLS LAST, value@2 ASC NULLS LAST]"
          - "   UnionExec"
          - "     SortExec: expr=[value@2 ASC NULLS LAST], preserve_partitioning=[false]"
          - "       ProjectionExec: expr=[1 as iox::measurement, 10 as key, tag0@1 as value]"
          - "         DataSourceExec: file_groups={1 group: [[0.parquet]]}, projection=[tag2, tag0, tag1, field1, time, __chunk_order], output_ordering=[__chunk_order@5 ASC], file_type=parquet"
          - "     SortExec: expr=[value@2 ASC NULLS LAST], preserve_partitioning=[false]"
          - "       ProjectionExec: expr=[2 as iox::measurement, 20 as key, tag0@1 as value]"
          - "         RecordBatchesExec: chunks=1, projection=[tag2, tag0, tag1, field1, time, __chunk_order]"
        output:
          Ok:
            - " ProgressiveEvalExec: input_ranges=[(1,10)->(1,10), (2,20)->(2,20)]"
            - "   UnionExec"
            - "     SortExec: expr=[value@2 ASC NULLS LAST], preserve_partitioning=[false]"
            - "       ProjectionExec: expr=[1 as iox::measurement, 10 as key, tag0@1 as value]"
            - "         DataSourceExec: file_groups={1 group: [[0.parquet]]}, projection=[tag2, tag0, tag1, field1, time, __chunk_order], output_ordering=[__chunk_order@5 ASC], file_type=parquet"
            - "     SortExec: expr=[value@2 ASC NULLS LAST], preserve_partitioning=[false]"
            - "       ProjectionExec: expr=[2 as iox::measurement, 20 as key, tag0@1 as value]"
            - "         RecordBatchesExec: chunks=1, projection=[tag2, tag0, tag1, field1, time, __chunk_order]"
        "#
        );
    }

    // Three contants
    #[test]
    fn test_replace_spm_with_pe_three_constants() {
        test_helpers::maybe_start_logging();

        let schema = schema();
        let sort_order = sort_order_for_sort();

        // First sort on parquet file
        let plan_parquet = data_source_exec_parquet_with_value_range(&schema, 1000, 2000);
        let plan_projection_1 = Arc::new(
            ProjectionExec::try_new(
                projection_expr_with_3_constants("m0", "tag0", "tag1", &schema),
                plan_parquet,
            )
            .unwrap(),
        );
        let plan_sort1 = Arc::new(SortExec::new(sort_order.clone(), plan_projection_1));

        // Second sort on record batch
        let plan_batches = record_batches_exec_with_value_range(1, 2001, 3000);
        let plan_projection_2 = Arc::new(
            ProjectionExec::try_new(
                projection_expr_with_3_constants("m1", "tag1", "tag2", &schema),
                plan_batches,
            )
            .unwrap(),
        );
        let plan_sort2 = Arc::new(SortExec::new(sort_order.clone(), plan_projection_2));

        // union the 2 sorts
        let plan_union = Arc::new(UnionExec::new(vec![plan_sort1, plan_sort2]));

        // add sort preserving merge on top
        let plan_spm = Arc::new(SortPreservingMergeExec::new(
            sort_order_for_sort_preserving_merge(),
            plan_union,
        ));

        // Output plan:
        //   . SortPreservingMergeExec will be replaced with ProgressiveEvalExec
        //   . the 2 SortExecs will be swapped the order to have "m0" first
        let opt = OrderUnionSortedInputs;
        insta::assert_yaml_snapshot!(
            OptimizationTest::new(plan_spm, opt),
            @r#"
        input:
          - " SortPreservingMergeExec: [iox::measurement@0 ASC NULLS LAST, key@1 ASC NULLS LAST, value@2 ASC NULLS LAST]"
          - "   UnionExec"
          - "     SortExec: expr=[value@2 ASC NULLS LAST], preserve_partitioning=[false]"
          - "       ProjectionExec: expr=[m0 as iox::measurement, tag0 as key, tag1 as another_key, tag0@1 as value]"
          - "         DataSourceExec: file_groups={1 group: [[0.parquet]]}, projection=[tag2, tag0, tag1, field1, time, __chunk_order], output_ordering=[__chunk_order@5 ASC], file_type=parquet"
          - "     SortExec: expr=[value@2 ASC NULLS LAST], preserve_partitioning=[false]"
          - "       ProjectionExec: expr=[m1 as iox::measurement, tag1 as key, tag2 as another_key, tag0@1 as value]"
          - "         RecordBatchesExec: chunks=1, projection=[tag2, tag0, tag1, field1, time, __chunk_order]"
        output:
          Ok:
            - " ProgressiveEvalExec: input_ranges=[(m0,tag0)->(m0,tag0), (m1,tag1)->(m1,tag1)]"
            - "   UnionExec"
            - "     SortExec: expr=[value@2 ASC NULLS LAST], preserve_partitioning=[false]"
            - "       ProjectionExec: expr=[m0 as iox::measurement, tag0 as key, tag1 as another_key, tag0@1 as value]"
            - "         DataSourceExec: file_groups={1 group: [[0.parquet]]}, projection=[tag2, tag0, tag1, field1, time, __chunk_order], output_ordering=[__chunk_order@5 ASC], file_type=parquet"
            - "     SortExec: expr=[value@2 ASC NULLS LAST], preserve_partitioning=[false]"
            - "       ProjectionExec: expr=[m1 as iox::measurement, tag1 as key, tag2 as another_key, tag0@1 as value]"
            - "         RecordBatchesExec: chunks=1, projection=[tag2, tag0, tag1, field1, time, __chunk_order]"
        "#
        );
    }

    // Under sort preserving merge is not UnionExec,
    // although the new optimizer can handle it.
    #[test]
    fn test_replace_spm_with_no_union_under_spm() {
        test_helpers::maybe_start_logging();

        let schema = schema();
        let sort_order = sort_order_for_sort();

        // First sort on parquet file
        let plan_parquet = data_source_exec_parquet_with_value_range(&schema, 1000, 2000);
        let plan_projection_1 = Arc::new(
            ProjectionExec::try_new(
                projection_expr_with_2_constants("m1", "tag0", &schema),
                plan_parquet,
            )
            .unwrap(),
        );
        let plan_sort1 = Arc::new(SortExec::new(sort_order.clone(), plan_projection_1));

        // add sort preserving merge on top
        let plan_spm = Arc::new(SortPreservingMergeExec::new(
            sort_order_for_sort_preserving_merge(),
            plan_sort1,
        ));

        // input and output are the same
        let opt = OrderUnionSortedInputs;
        insta::assert_yaml_snapshot!(
            OptimizationTest::new(plan_spm, opt),
            @r#"
        input:
          - " SortPreservingMergeExec: [iox::measurement@0 ASC NULLS LAST, key@1 ASC NULLS LAST, value@2 ASC NULLS LAST]"
          - "   SortExec: expr=[value@2 ASC NULLS LAST], preserve_partitioning=[false]"
          - "     ProjectionExec: expr=[m1 as iox::measurement, tag0 as key, tag0@1 as value]"
          - "       DataSourceExec: file_groups={1 group: [[0.parquet]]}, projection=[tag2, tag0, tag1, field1, time, __chunk_order], output_ordering=[__chunk_order@5 ASC], file_type=parquet"
        output:
          Ok:
            - " ProgressiveEvalExec: input_ranges=[(m1,tag0)->(m1,tag0)]"
            - "   SortExec: expr=[value@2 ASC NULLS LAST], preserve_partitioning=[false]"
            - "     ProjectionExec: expr=[m1 as iox::measurement, tag0 as key, tag0@1 as value]"
            - "       DataSourceExec: file_groups={1 group: [[0.parquet]]}, projection=[tag2, tag0, tag1, field1, time, __chunk_order], output_ordering=[__chunk_order@5 ASC], file_type=parquet"
        "#
        );
    }

    // ------------------------------------------------------------------
    // Negative tests: wrong structure -> not optimized
    // ------------------------------------------------------------------

    // Sort expressions each is not on a column
    #[test]
    fn test_negative_sort_not_on_column() {
        test_helpers::maybe_start_logging();

        let schema = schema();
        let sort_order = sort_order_not_on_column();

        // First sort on parquet file
        let plan_parquet = data_source_exec_parquet_with_value_range(&schema, 1000, 2000);
        let plan_projection_1 = Arc::new(
            ProjectionExec::try_new(
                projection_expr_with_2_constants("m1", "tag0", &schema),
                plan_parquet,
            )
            .unwrap(),
        );
        let plan_sort1 = Arc::new(SortExec::new(sort_order.clone(), plan_projection_1));

        // Second sort on record batch
        let plan_batches = record_batches_exec_with_value_range(1, 2001, 3000);
        let plan_projection_2 = Arc::new(
            ProjectionExec::try_new(
                projection_expr_with_2_constants("m0", "tag0", &schema),
                plan_batches,
            )
            .unwrap(),
        );
        let plan_sort2 = Arc::new(SortExec::new(sort_order.clone(), plan_projection_2));

        // union the 2 sorts
        let plan_union = Arc::new(UnionExec::new(vec![plan_sort1, plan_sort2]));

        // add sort preserving merge on top
        let plan_spm = Arc::new(SortPreservingMergeExec::new(sort_order, plan_union));

        // input and output are the same
        let opt = OrderUnionSortedInputs;
        insta::assert_yaml_snapshot!(
            OptimizationTest::new(plan_spm, opt),
            @r#"
        input:
          - " SortPreservingMergeExec: [iox::measurement ASC NULLS LAST]"
          - "   UnionExec"
          - "     SortExec: expr=[iox::measurement ASC NULLS LAST], preserve_partitioning=[false]"
          - "       ProjectionExec: expr=[m1 as iox::measurement, tag0 as key, tag0@1 as value]"
          - "         DataSourceExec: file_groups={1 group: [[0.parquet]]}, projection=[tag2, tag0, tag1, field1, time, __chunk_order], output_ordering=[__chunk_order@5 ASC], file_type=parquet"
          - "     SortExec: expr=[iox::measurement ASC NULLS LAST], preserve_partitioning=[false]"
          - "       ProjectionExec: expr=[m0 as iox::measurement, tag0 as key, tag0@1 as value]"
          - "         RecordBatchesExec: chunks=1, projection=[tag2, tag0, tag1, field1, time, __chunk_order]"
        output:
          Ok:
            - " SortPreservingMergeExec: [iox::measurement ASC NULLS LAST]"
            - "   UnionExec"
            - "     SortExec: expr=[iox::measurement ASC NULLS LAST], preserve_partitioning=[false]"
            - "       ProjectionExec: expr=[m1 as iox::measurement, tag0 as key, tag0@1 as value]"
            - "         DataSourceExec: file_groups={1 group: [[0.parquet]]}, projection=[tag2, tag0, tag1, field1, time, __chunk_order], output_ordering=[__chunk_order@5 ASC], file_type=parquet"
            - "     SortExec: expr=[iox::measurement ASC NULLS LAST], preserve_partitioning=[false]"
            - "       ProjectionExec: expr=[m0 as iox::measurement, tag0 as key, tag0@1 as value]"
            - "         RecordBatchesExec: chunks=1, projection=[tag2, tag0, tag1, field1, time, __chunk_order]"
        "#
        );
    }

    // Under Union is not all SortExec
    #[test]
    fn test_negative_not_all_sorts_under_union() {
        let schema = schema();
        let sort_order = sort_order_for_sort();

        // First branch: sort on parquet file
        let plan_parquet = data_source_exec_parquet_with_value_range(&schema, 1000, 2000);
        let plan_projection_1 = Arc::new(
            ProjectionExec::try_new(
                projection_expr_with_2_constants("m1", "tag0", &schema),
                plan_parquet,
            )
            .unwrap(),
        );
        let plan_sort1 = Arc::new(SortExec::new(sort_order.clone(), plan_projection_1));

        // Second branch: projection on record batch
        let plan_batches = record_batches_exec_with_value_range(1, 2001, 3000);
        let plan_projection_2 = Arc::new(
            ProjectionExec::try_new(
                projection_expr_with_2_constants("m0", "tag0", &schema),
                plan_batches,
            )
            .unwrap(),
        );

        // union the 2 sorts
        let plan_union = Arc::new(UnionExec::new(vec![plan_sort1, plan_projection_2]));

        // add sort preserving merge on top
        let plan_spm = Arc::new(SortPreservingMergeExec::new(
            sort_order_for_sort_preserving_merge(),
            plan_union,
        ));

        // input and output are the same
        let opt = OrderUnionSortedInputs;
        insta::assert_yaml_snapshot!(
            OptimizationTest::new(plan_spm, opt),
            @r#"
        input:
          - " SortPreservingMergeExec: [iox::measurement@0 ASC NULLS LAST, key@1 ASC NULLS LAST, value@2 ASC NULLS LAST]"
          - "   UnionExec"
          - "     SortExec: expr=[value@2 ASC NULLS LAST], preserve_partitioning=[false]"
          - "       ProjectionExec: expr=[m1 as iox::measurement, tag0 as key, tag0@1 as value]"
          - "         DataSourceExec: file_groups={1 group: [[0.parquet]]}, projection=[tag2, tag0, tag1, field1, time, __chunk_order], output_ordering=[__chunk_order@5 ASC], file_type=parquet"
          - "     ProjectionExec: expr=[m0 as iox::measurement, tag0 as key, tag0@1 as value]"
          - "       RecordBatchesExec: chunks=1, projection=[tag2, tag0, tag1, field1, time, __chunk_order]"
        output:
          Ok:
            - " SortPreservingMergeExec: [iox::measurement@0 ASC NULLS LAST, key@1 ASC NULLS LAST, value@2 ASC NULLS LAST]"
            - "   UnionExec"
            - "     SortExec: expr=[value@2 ASC NULLS LAST], preserve_partitioning=[false]"
            - "       ProjectionExec: expr=[m1 as iox::measurement, tag0 as key, tag0@1 as value]"
            - "         DataSourceExec: file_groups={1 group: [[0.parquet]]}, projection=[tag2, tag0, tag1, field1, time, __chunk_order], output_ordering=[__chunk_order@5 ASC], file_type=parquet"
            - "     ProjectionExec: expr=[m0 as iox::measurement, tag0 as key, tag0@1 as value]"
            - "       RecordBatchesExec: chunks=1, projection=[tag2, tag0, tag1, field1, time, __chunk_order]"
        "#
        );
    }

    // Sort expressions of SortExecs are not the same and/or not the same with the parent's sort_expr
    #[test]
    fn test_negative_not_same_sort_exprs() {
        test_helpers::maybe_start_logging();

        let schema = schema();
        let sort_order_3_cols = sort_order_for_sort_preserving_merge();
        let sort_order_2_cols = sort_order_two_cols();
        // First sort on parquet file
        let plan_parquet = data_source_exec_parquet_with_value_range(&schema, 1000, 2000);
        let plan_projection_1 = Arc::new(
            ProjectionExec::try_new(
                projection_expr_with_2_constants("m1", "tag0", &schema),
                plan_parquet,
            )
            .unwrap(),
        );
        let plan_sort1 = Arc::new(SortExec::new(sort_order_2_cols, plan_projection_1));

        // Second sort on record batch
        let plan_batches = record_batches_exec_with_value_range(1, 2001, 3000);
        let plan_projection_2 = Arc::new(
            ProjectionExec::try_new(
                projection_expr_with_2_constants("m0", "tag0", &schema),
                plan_batches,
            )
            .unwrap(),
        );
        let plan_sort2 = Arc::new(SortExec::new(sort_order_3_cols.clone(), plan_projection_2));

        // union the 2 sorts
        let plan_union = Arc::new(UnionExec::new(vec![plan_sort1, plan_sort2]));

        // add sort preserving merge on top
        let plan_spm = Arc::new(SortPreservingMergeExec::new(sort_order_3_cols, plan_union));

        // input and output are the same
        let opt = OrderUnionSortedInputs;
        insta::assert_yaml_snapshot!(
            OptimizationTest::new(plan_spm, opt),
            @r#"
        input:
          - " SortPreservingMergeExec: [iox::measurement@0 ASC NULLS LAST, key@1 ASC NULLS LAST, value@2 ASC NULLS LAST]"
          - "   UnionExec"
          - "     SortExec: expr=[iox::measurement@0 ASC NULLS LAST, key@1 ASC NULLS LAST], preserve_partitioning=[false]"
          - "       ProjectionExec: expr=[m1 as iox::measurement, tag0 as key, tag0@1 as value]"
          - "         DataSourceExec: file_groups={1 group: [[0.parquet]]}, projection=[tag2, tag0, tag1, field1, time, __chunk_order], output_ordering=[__chunk_order@5 ASC], file_type=parquet"
          - "     SortExec: expr=[iox::measurement@0 ASC NULLS LAST, key@1 ASC NULLS LAST, value@2 ASC NULLS LAST], preserve_partitioning=[false]"
          - "       ProjectionExec: expr=[m0 as iox::measurement, tag0 as key, tag0@1 as value]"
          - "         RecordBatchesExec: chunks=1, projection=[tag2, tag0, tag1, field1, time, __chunk_order]"
        output:
          Ok:
            - " SortPreservingMergeExec: [iox::measurement@0 ASC NULLS LAST, key@1 ASC NULLS LAST, value@2 ASC NULLS LAST]"
            - "   UnionExec"
            - "     SortExec: expr=[iox::measurement@0 ASC NULLS LAST, key@1 ASC NULLS LAST], preserve_partitioning=[false]"
            - "       ProjectionExec: expr=[m1 as iox::measurement, tag0 as key, tag0@1 as value]"
            - "         DataSourceExec: file_groups={1 group: [[0.parquet]]}, projection=[tag2, tag0, tag1, field1, time, __chunk_order], output_ordering=[__chunk_order@5 ASC], file_type=parquet"
            - "     SortExec: expr=[iox::measurement@0 ASC NULLS LAST, key@1 ASC NULLS LAST, value@2 ASC NULLS LAST], preserve_partitioning=[false]"
            - "       ProjectionExec: expr=[m0 as iox::measurement, tag0 as key, tag0@1 as value]"
            - "         RecordBatchesExec: chunks=1, projection=[tag2, tag0, tag1, field1, time, __chunk_order]"
        "#
        );
    }

    // bad projection under sort
    #[test]
    fn test_negative_bad_projection_under_sort() {
        test_helpers::maybe_start_logging();

        let schema = schema();
        let sort_order = sort_order_for_sort();

        // First sort on parquet file
        let plan_parquet = data_source_exec_parquet_with_value_range(&schema, 1000, 2000);
        let plan_projection_1 = Arc::new(
            ProjectionExec::try_new(
                projection_expr_with_2_constants("m1", "tag0", &schema),
                plan_parquet,
            )
            .unwrap(),
        );
        let plan_sort1 = Arc::new(SortExec::new(sort_order.clone(), plan_projection_1));

        // Second sort on record batch
        let plan_batches = record_batches_exec_with_value_range(1, 2001, 3000);
        // swap order of projection expressions
        let plan_projection_1 = Arc::new(
            ProjectionExec::try_new(
                projection_expr_with_2_constants("tag0", "tag1", &schema),
                plan_batches,
            )
            .unwrap(),
        );
        let plan_sort2 = Arc::new(SortExec::new(sort_order_two_cols(), plan_projection_1));

        // union the 2 sorts
        let plan_union = Arc::new(UnionExec::new(vec![plan_sort1, plan_sort2]));

        // add sort preserving merge on top
        let plan_spm = Arc::new(SortPreservingMergeExec::new(
            sort_order_for_sort_preserving_merge(),
            plan_union,
        ));

        // input and output are the same
        let opt = OrderUnionSortedInputs;
        insta::assert_yaml_snapshot!(
            OptimizationTest::new(plan_spm, opt),
            @r#"
        input:
          - " SortPreservingMergeExec: [iox::measurement@0 ASC NULLS LAST, key@1 ASC NULLS LAST, value@2 ASC NULLS LAST]"
          - "   UnionExec"
          - "     SortExec: expr=[value@2 ASC NULLS LAST], preserve_partitioning=[false]"
          - "       ProjectionExec: expr=[m1 as iox::measurement, tag0 as key, tag0@1 as value]"
          - "         DataSourceExec: file_groups={1 group: [[0.parquet]]}, projection=[tag2, tag0, tag1, field1, time, __chunk_order], output_ordering=[__chunk_order@5 ASC], file_type=parquet"
          - "     SortExec: expr=[iox::measurement@0 ASC NULLS LAST, key@1 ASC NULLS LAST], preserve_partitioning=[false]"
          - "       ProjectionExec: expr=[tag0 as iox::measurement, tag1 as key, tag1@2 as value]"
          - "         RecordBatchesExec: chunks=1, projection=[tag2, tag0, tag1, field1, time, __chunk_order]"
        output:
          Ok:
            - " SortPreservingMergeExec: [iox::measurement@0 ASC NULLS LAST, key@1 ASC NULLS LAST, value@2 ASC NULLS LAST]"
            - "   UnionExec"
            - "     SortExec: expr=[value@2 ASC NULLS LAST], preserve_partitioning=[false]"
            - "       ProjectionExec: expr=[m1 as iox::measurement, tag0 as key, tag0@1 as value]"
            - "         DataSourceExec: file_groups={1 group: [[0.parquet]]}, projection=[tag2, tag0, tag1, field1, time, __chunk_order], output_ordering=[__chunk_order@5 ASC], file_type=parquet"
            - "     SortExec: expr=[iox::measurement@0 ASC NULLS LAST, key@1 ASC NULLS LAST], preserve_partitioning=[false]"
            - "       ProjectionExec: expr=[tag0 as iox::measurement, tag1 as key, tag1@2 as value]"
            - "         RecordBatchesExec: chunks=1, projection=[tag2, tag0, tag1, field1, time, __chunk_order]"
        "#
        );
    }

    // Projection does not have the first column as a constant value
    #[test]
    fn test_negative_proj_without_first_col_as_const() {
        test_helpers::maybe_start_logging();

        let schema = schema();
        let sort_order = sort_order_for_sort();

        // First sort on parquet file
        let plan_parquet = data_source_exec_parquet_with_value_range(&schema, 1000, 2000);
        let plan_projection_1 = Arc::new(
            ProjectionExec::try_new(
                vec![
                    (expr_col("tag0", &schema), String::from("value")), // tag value -- not constant
                    (
                        expr_dict_const("m1".to_string()),
                        String::from("iox::measurement"),
                    ), // constant table name
                    (expr_dict_const("tag0".to_string()), String::from("key")), // constant tag name
                ],
                plan_parquet,
            )
            .unwrap(),
        );
        let plan_sort1 = Arc::new(SortExec::new(sort_order.clone(), plan_projection_1));

        // Second sort on record batch
        let plan_batches = record_batches_exec_with_value_range(1, 2001, 3000);
        let plan_projection_2 = Arc::new(
            ProjectionExec::try_new(
                projection_expr_with_2_constants("m0", "tag0", &schema),
                plan_batches,
            )
            .unwrap(),
        );
        let plan_sort2 = Arc::new(SortExec::new(sort_order.clone(), plan_projection_2));

        // union the 2 sorts
        let plan_union = Arc::new(UnionExec::new(vec![plan_sort1, plan_sort2]));

        // add sort preserving merge on top
        let plan_spm = Arc::new(SortPreservingMergeExec::new(
            sort_order_for_sort_preserving_merge(),
            plan_union,
        ));

        // input and output are the same
        let opt = OrderUnionSortedInputs;
        insta::assert_yaml_snapshot!(
            OptimizationTest::new(plan_spm, opt),
            @r#"
        input:
          - " SortPreservingMergeExec: [iox::measurement@0 ASC NULLS LAST, key@1 ASC NULLS LAST, value@2 ASC NULLS LAST]"
          - "   UnionExec"
          - "     SortExec: expr=[value@2 ASC NULLS LAST], preserve_partitioning=[false]"
          - "       ProjectionExec: expr=[tag0@1 as value, m1 as iox::measurement, tag0 as key]"
          - "         DataSourceExec: file_groups={1 group: [[0.parquet]]}, projection=[tag2, tag0, tag1, field1, time, __chunk_order], output_ordering=[__chunk_order@5 ASC], file_type=parquet"
          - "     SortExec: expr=[value@2 ASC NULLS LAST], preserve_partitioning=[false]"
          - "       ProjectionExec: expr=[m0 as iox::measurement, tag0 as key, tag0@1 as value]"
          - "         RecordBatchesExec: chunks=1, projection=[tag2, tag0, tag1, field1, time, __chunk_order]"
        output:
          Ok:
            - " SortPreservingMergeExec: [iox::measurement@0 ASC NULLS LAST, key@1 ASC NULLS LAST, value@2 ASC NULLS LAST]"
            - "   UnionExec"
            - "     SortExec: expr=[value@2 ASC NULLS LAST], preserve_partitioning=[false]"
            - "       ProjectionExec: expr=[tag0@1 as value, m1 as iox::measurement, tag0 as key]"
            - "         DataSourceExec: file_groups={1 group: [[0.parquet]]}, projection=[tag2, tag0, tag1, field1, time, __chunk_order], output_ordering=[__chunk_order@5 ASC], file_type=parquet"
            - "     SortExec: expr=[value@2 ASC NULLS LAST], preserve_partitioning=[false]"
            - "       ProjectionExec: expr=[m0 as iox::measurement, tag0 as key, tag0@1 as value]"
            - "         RecordBatchesExec: chunks=1, projection=[tag2, tag0, tag1, field1, time, __chunk_order]"
        "#
        );
    }

    // Projection only has the first column as constant
    #[test]
    fn test_negative_proj_with_only_first_col_as_const() {
        test_helpers::maybe_start_logging();

        let schema = schema();
        let sort_order = sort_order_for_sort();

        // First sort on parquet file
        let plan_parquet = data_source_exec_parquet_with_value_range(&schema, 1000, 2000);
        let plan_projection_1 = Arc::new(
            ProjectionExec::try_new(
                vec![
                    (
                        expr_dict_const("m1".to_string()),
                        String::from("iox::measurement"),
                    ), // constant table name
                    (expr_col("tag0", &schema), String::from("key")), // tag value -- not constant
                    (expr_col("tag1", &schema), String::from("value")), // tag value -- not constant
                ],
                // ProjectionExec::try_new(projection_expr_with_one_constant("m1", &schema),
                plan_parquet,
            )
            .unwrap(),
        );
        let plan_sort1 = Arc::new(SortExec::new(sort_order.clone(), plan_projection_1));

        // Second sort on record batch
        let plan_batches = record_batches_exec_with_value_range(1, 2001, 3000);
        let plan_projection_2 = Arc::new(
            ProjectionExec::try_new(
                projection_expr_with_2_constants("m0", "tag0", &schema),
                plan_batches,
            )
            .unwrap(),
        );
        let plan_sort2 = Arc::new(SortExec::new(sort_order.clone(), plan_projection_2));

        // union the 2 sorts
        let plan_union = Arc::new(UnionExec::new(vec![plan_sort1, plan_sort2]));

        // add sort preserving merge on top
        let plan_spm = Arc::new(SortPreservingMergeExec::new(
            sort_order_for_sort_preserving_merge(),
            plan_union,
        ));

        // input and output are the same
        let opt = OrderUnionSortedInputs;
        insta::assert_yaml_snapshot!(
            OptimizationTest::new(plan_spm, opt),
            @r#"
        input:
          - " SortPreservingMergeExec: [iox::measurement@0 ASC NULLS LAST, key@1 ASC NULLS LAST, value@2 ASC NULLS LAST]"
          - "   UnionExec"
          - "     SortExec: expr=[value@2 ASC NULLS LAST], preserve_partitioning=[false]"
          - "       ProjectionExec: expr=[m1 as iox::measurement, tag0@1 as key, tag1@2 as value]"
          - "         DataSourceExec: file_groups={1 group: [[0.parquet]]}, projection=[tag2, tag0, tag1, field1, time, __chunk_order], output_ordering=[__chunk_order@5 ASC], file_type=parquet"
          - "     SortExec: expr=[value@2 ASC NULLS LAST], preserve_partitioning=[false]"
          - "       ProjectionExec: expr=[m0 as iox::measurement, tag0 as key, tag0@1 as value]"
          - "         RecordBatchesExec: chunks=1, projection=[tag2, tag0, tag1, field1, time, __chunk_order]"
        output:
          Ok:
            - " SortPreservingMergeExec: [iox::measurement@0 ASC NULLS LAST, key@1 ASC NULLS LAST, value@2 ASC NULLS LAST]"
            - "   UnionExec"
            - "     SortExec: expr=[value@2 ASC NULLS LAST], preserve_partitioning=[false]"
            - "       ProjectionExec: expr=[m1 as iox::measurement, tag0@1 as key, tag1@2 as value]"
            - "         DataSourceExec: file_groups={1 group: [[0.parquet]]}, projection=[tag2, tag0, tag1, field1, time, __chunk_order], output_ordering=[__chunk_order@5 ASC], file_type=parquet"
            - "     SortExec: expr=[value@2 ASC NULLS LAST], preserve_partitioning=[false]"
            - "       ProjectionExec: expr=[m0 as iox::measurement, tag0 as key, tag0@1 as value]"
            - "         RecordBatchesExec: chunks=1, projection=[tag2, tag0, tag1, field1, time, __chunk_order]"
        "#
        );
    }

    // ------------------------------------------------------------------
    // Helper functions
    // ------------------------------------------------------------------

    fn schema() -> SchemaRef {
        IOxSchemaBuilder::new()
            .tag("tag2")
            .tag("tag0")
            .tag("tag1")
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

    fn expr_dict_const(val: String) -> Arc<dyn PhysicalExpr> {
        Arc::new(Literal::new(ScalarValue::Dictionary(
            Box::new(DataType::Int32),
            Box::new(ScalarValue::new_utf8(val)),
        )))
    }

    // test chunk with time range and field1's value range
    fn test_chunk(min: i64, max: i64, parquet_data: bool) -> Arc<dyn QueryChunk> {
        let chunk = TestChunk::new("t")
            .with_time_column_with_stats(Some(min), Some(max))
            .with_tag_column_with_stats("tag1", Some("AL"), Some("MT"))
            .with_tag_column_with_stats("tag2", Some("MA"), Some("MT"))
            .with_tag_column_with_stats("tag3", Some("NM"), Some("VY"))
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
        let chunks = std::iter::repeat_n(test_chunk(min, max, false), n_chunks).collect::<Vec<_>>();

        Arc::new(RecordBatchesExec::new(chunks, schema(), None))
    }

    fn data_source_exec_parquet_with_value_range(
        schema: &SchemaRef,
        min: i64,
        max: i64,
    ) -> Arc<dyn ExecutionPlan> {
        let chunk = test_chunk(min, max, true);
        let plan = chunks_to_physical_nodes(schema, None, vec![chunk], 1);

        if let Some(union_exec) = plan.as_any().downcast_ref::<UnionExec>()
            && union_exec.inputs().len() == 1
        {
            Arc::clone(&union_exec.inputs()[0])
        } else {
            plan
        }
    }

    // projection with 2 dictionary constants and an normal column
    fn projection_expr_with_2_constants(
        first_constant: &str,
        second_constant: &str,
        schema: &SchemaRef,
    ) -> Vec<(Arc<dyn PhysicalExpr>, String)> {
        vec![
            (
                // constant table name
                expr_dict_const(first_constant.to_string()),
                String::from("iox::measurement"),
            ),
            (
                // constant tag name
                expr_dict_const(second_constant.to_string()),
                String::from("key"),
            ),
            // tag value
            (expr_col(second_constant, schema), String::from("value")),
        ]
    }

    // projection with 2 constant integers,  one as an literal integer and the other is converted to string
    // and used as a dictionary value. These are done on purpose to ensure they work even if different data types
    fn projection_expr_with_2_constant_integers(
        first_constant: i32,
        second_constant: i32,
        schema: &SchemaRef,
    ) -> Vec<(Arc<dyn PhysicalExpr>, String)> {
        vec![
            (
                // Make a literal of a integer for the first constant
                Arc::new(Literal::new(ScalarValue::Int32(Some(first_constant)))),
                String::from("iox::measurement"),
            ),
            (
                // Convert the second integer to string and use it as a dictionary value
                expr_dict_const(second_constant.to_string()),
                String::from("key"),
            ),
            // tag value
            (expr_col("tag0", schema), String::from("value")),
        ]
    }

    // projection with 3 dictionnary constants and an normal column
    fn projection_expr_with_3_constants(
        first_constant: &str,
        second_constant: &str,
        third_constant: &str,
        schema: &SchemaRef,
    ) -> Vec<(Arc<dyn PhysicalExpr>, String)> {
        vec![
            (
                // constant table name
                expr_dict_const(first_constant.to_string()),
                String::from("iox::measurement"),
            ),
            (
                // constant tag name
                expr_dict_const(second_constant.to_string()),
                String::from("key"),
            ),
            (
                // constant tag name
                expr_dict_const(third_constant.to_string()),
                String::from("another_key"),
            ),
            // tag value
            (expr_col("tag0", schema), String::from("value")),
        ]
    }

    fn sort_order_for_sort_preserving_merge() -> LexOrdering {
        let measurement = Arc::new(Column::new("iox::measurement", 0)) as Arc<dyn PhysicalExpr>;
        let key = Arc::new(Column::new("key", 1)) as Arc<dyn PhysicalExpr>;
        let value = Arc::new(Column::new("value", 2)) as Arc<dyn PhysicalExpr>;

        LexOrdering::new(vec![
            PhysicalSortExpr {
                expr: measurement,
                options: SortOptions {
                    descending: false,
                    nulls_first: false,
                },
            },
            PhysicalSortExpr {
                expr: key,
                options: SortOptions {
                    descending: false,
                    nulls_first: false,
                },
            },
            PhysicalSortExpr {
                expr: value,
                options: SortOptions {
                    descending: false,
                    nulls_first: false,
                },
            },
        ])
        .unwrap()
    }

    fn sort_order_for_sort() -> LexOrdering {
        let value = Arc::new(Column::new("value", 2)) as Arc<dyn PhysicalExpr>;

        LexOrdering::new(vec![PhysicalSortExpr {
            expr: value,
            options: SortOptions {
                descending: false,
                nulls_first: false,
            },
        }])
        .unwrap()
    }

    fn sort_order_two_cols() -> LexOrdering {
        let measurement = Arc::new(Column::new("iox::measurement", 0)) as Arc<dyn PhysicalExpr>;
        let key = Arc::new(Column::new("key", 1)) as Arc<dyn PhysicalExpr>;

        LexOrdering::new(vec![
            PhysicalSortExpr {
                expr: measurement,
                options: SortOptions {
                    descending: false,
                    nulls_first: false,
                },
            },
            PhysicalSortExpr {
                expr: key,
                options: SortOptions {
                    descending: false,
                    nulls_first: false,
                },
            },
        ])
        .unwrap()
    }

    fn sort_order_not_on_column() -> LexOrdering {
        let measurement =
            Arc::new(Literal::new(ScalarValue::from("iox::measurement"))) as Arc<dyn PhysicalExpr>;

        LexOrdering::new(vec![PhysicalSortExpr {
            expr: measurement,
            options: SortOptions {
                descending: false,
                nulls_first: false,
            },
        }])
        .unwrap()
    }
}
