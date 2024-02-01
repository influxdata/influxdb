use std::sync::Arc;

use arrow::compute::SortOptions;
use datafusion::{
    common::{
        plan_err,
        tree_node::{Transformed, TreeNode},
    },
    config::ConfigOptions,
    error::{DataFusionError, Result},
    physical_optimizer::PhysicalOptimizerRule,
    physical_plan::{union::UnionExec, ExecutionPlan},
};
use observability_deps::tracing::trace;
use schema::TIME_COLUMN_NAME;

use crate::{
    physical_optimizer::{
        chunk_extraction::extract_chunks,
        sort::util::{collect_statistics_min_max, sort_by_value_ranges},
    },
    provider::chunks_to_physical_nodes,
};

/// Collects [`QueryChunk`]s and re-creates a appropriate physical nodes.
///
/// Invariants of inputs of the union:
///   1. They do not overlap on time ranges (done in previous step: TimeSplit)
///   2. Each input of the union is either with_chunks or other_plans.
///      - An input with_chunks is a plan that contains only (union of) ParquetExecs or RecordBatchesExec
///      - An input of other_plans is a plan that contains at least one node that is not a ParquetExec or
///        RecordBatchesExec or Union of them. Examples of those other nodes are FilterExec, DeduplicateExec,
///        ProjectionExec, etc.
//
/// Goals of this optimzation step:
///   i. Combine **possible** plans with_chunks into a single union
///   ii. - Keep the the combined plan non-overlapped on time ranges. This will likely help later optimization steps.
///       - If time ranges cannot be computed, combine all plans with_chunks into a single union.
///
/// Example: w = with_chunks, o = other_plans
///   Input:  |--P1 w --| |--P2 w --| |-- P3 o --| |-- P4 w --| |-- P5 w --| |-- P6 o --| |--P7 w --|
///   Output when time ranges can be computed: Only two sets of plans that are combined: [P1, P2], [P4, P5]
///           |------ P1 & P2 w ----| |-- P3 o --| |------ P4 & P5 w ------| |-- P6 o --| |--P7 w --|
///   Output when time ranges cannot be computed: all plans with_chunks are combined into a single union
///           |-------------------------- P1, P2, P4, P5, P7 w -------------------------------------|
///                                   |-- P3 o --|                           |-- P6 o --|
///
///
/// This is mostly useful after multiple re-arrangements (e.g. [`PartitionSplit`]-[`TimeSplit`]-[`RemoveDedup`]) created
/// a bunch of freestanding chunks that can be re-arranged into more packed, more efficient physical nodes.
///
///
/// [`PartitionSplit`]: super::dedup::partition_split::PartitionSplit
/// [`QueryChunk`]: crate::QueryChunk
/// [`RemoveDedup`]: super::dedup::remove_dedup::RemoveDedup
/// [`TimeSplit`]: super::dedup::time_split::TimeSplit
#[derive(Debug, Default)]
pub struct CombineChunks;

impl PhysicalOptimizerRule for CombineChunks {
    fn optimize(
        &self,
        plan: Arc<dyn ExecutionPlan>,
        config: &ConfigOptions,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        plan.transform_up(&|plan| {
            if let Some(union_exec) = plan.as_any().downcast_ref::<UnionExec>() {
                // sort and group the inputs by time range
                let inputs = union_exec.inputs();
                // We only need to ensure the input are sorted by time range,
                // any order is fine and hence we choose to go with ASC here
                let groups = sort_and_group_plans(
                    inputs.clone(),
                    TIME_COLUMN_NAME,
                    SortOptions {
                        descending: false,
                        nulls_first: false,
                    },
                )?;

                // combine plans from each group
                let plans = groups
                    .into_iter()
                    .map(|group| combine_plans(group, config))
                    .collect::<Result<Vec<_>>>()?
                    .into_iter()
                    .flatten()
                    .collect::<Vec<_>>();

                let final_union = UnionExec::new(plans);
                trace!(?final_union, "-------- final union");
                return Ok(Transformed::Yes(Arc::new(final_union)));
            }

            Ok(Transformed::No(plan))
        })
    }

    fn name(&self) -> &str {
        "combine_chunks"
    }

    fn schema_check(&self) -> bool {
        true
    }
}

/// Sort the given plans on the given column name and a given sort order.
///
/// Then group them into non-overlapped groups based on the ranges of the given column, and return the groups.
///
/// # Input Invariants
/// - Plans do not overlap on the given column
///
/// # Output Invariants
/// - Plans in the same group do not overlap on the given column
/// -The groups do not overlap on the given column
///
/// # Example
/// Input:
///
/// ```text
/// 7 plans with value ranges : |--P1 w --| |--P2 w --| |-- P3 o --| |-- P4 w --| |-- P5 w --| |-- P6 o --| |--P7 w --|
/// ```
///
/// Output:
///
/// ```text
/// 5 groups: [P1, P2], [P3], [P4, P5], [P6], [P7]
/// ```
fn sort_and_group_plans(
    plans: Vec<Arc<dyn ExecutionPlan>>,
    col_name: &str,
    sort_options: SortOptions,
) -> Result<Vec<Vec<Arc<dyn ExecutionPlan>>>> {
    if plans.len() <= 1 {
        return Ok(vec![plans]);
    }

    let Some(value_ranges) = collect_statistics_min_max(&plans, col_name)? else {
        // No statistics to sort and group the plans.
        // Return all plans in the same group
        trace!("-------- combine chunks - cannot collect statistics min max for column {col_name}");
        return Ok(vec![plans]);
    };

    // Sort the plans by their value ranges
    trace!("-------- value_ranges: {:?}", value_ranges);
    let Some(plans_value_ranges) = sort_by_value_ranges(plans.clone(), value_ranges, sort_options)?
    else {
        // The inputs are not being sorted by value ranges, cannot group them
        // Return all plans in the same group
        trace!("-------- inputs are not sorted by value ranges. No optimization");
        return Ok(vec![plans]);
    };

    // Group plans that can be combined
    let plans = plans_value_ranges.plans;
    let mut final_groups = Vec::with_capacity(plans.len());
    let mut combinable_plans = Vec::new();
    for plan in plans {
        if extract_chunks(plan.as_ref()).is_some() {
            combinable_plans.push(plan);
        } else {
            if !combinable_plans.is_empty() {
                final_groups.push(combinable_plans);
                combinable_plans = Vec::new();
            }
            final_groups.push(vec![plan]);
        }
    }

    if !combinable_plans.is_empty() {
        final_groups.push(combinable_plans);
    }

    Ok(final_groups)
}

/// Combine the given plans with chunks  into a single union. The other plans stay as is.
fn combine_plans(
    plans: Vec<Arc<dyn ExecutionPlan>>,
    config: &ConfigOptions,
) -> Result<Vec<Arc<dyn ExecutionPlan>>> {
    let (inputs_with_chunks, inputs_other): (Vec<_>, Vec<_>) = plans
        .iter()
        .cloned()
        .partition(|plan| extract_chunks(plan.as_ref()).is_some());

    if inputs_with_chunks.is_empty() {
        return Ok(plans);
    }
    let union_of_chunks = UnionExec::new(inputs_with_chunks);

    if let Some((schema, chunks, output_sort_key)) = extract_chunks(&union_of_chunks) {
        let union_of_chunks = chunks_to_physical_nodes(
            &schema,
            output_sort_key.as_ref(),
            chunks,
            config.execution.target_partitions,
        );
        let Some(union_of_chunks) = union_of_chunks.as_any().downcast_ref::<UnionExec>() else {
            return plan_err!("Expected chunks_to_physical_nodes to produce UnionExec but got {union_of_chunks:?}");
        };

        // return other_plans and the union_of_chunks
        let plans = union_of_chunks
            .inputs()
            .iter()
            .cloned()
            .chain(inputs_other)
            .collect();
        return Ok(plans);
    }

    Ok(plans)
}

#[cfg(test)]
mod tests {
    use datafusion::{
        physical_plan::{expressions::Literal, filter::FilterExec, union::UnionExec},
        scalar::ScalarValue,
    };

    use crate::{physical_optimizer::test_util::OptimizationTest, test::TestChunk, QueryChunk};

    use super::*;

    #[test]
    fn test_combine_single_union_tree() {
        let chunk1 = TestChunk::new("table")
            .with_id(1)
            .with_time_column_with_stats(Some(1), Some(2));
        let chunk2 = TestChunk::new("table")
            .with_id(2)
            .with_dummy_parquet_file()
            .with_time_column_with_stats(Some(3), Some(4));
        let chunk3 = TestChunk::new("table")
            .with_id(3)
            .with_time_column_with_stats(Some(5), Some(6));
        let chunk4 = TestChunk::new("table")
            .with_id(4)
            .with_dummy_parquet_file()
            .with_time_column_with_stats(Some(7), Some(8));
        let chunk5 = TestChunk::new("table")
            .with_id(5)
            .with_dummy_parquet_file()
            .with_time_column_with_stats(Some(9), Some(10));
        let schema = chunk1.schema().as_arrow();
        let plan = Arc::new(UnionExec::new(vec![
            chunks_to_physical_nodes(&schema, None, vec![Arc::new(chunk1), Arc::new(chunk2)], 2),
            chunks_to_physical_nodes(
                &schema,
                None,
                vec![Arc::new(chunk3), Arc::new(chunk4), Arc::new(chunk5)],
                2,
            ),
        ]));
        let opt = CombineChunks;
        let mut config = ConfigOptions::default();
        config.execution.target_partitions = 2;
        insta::assert_yaml_snapshot!(
            OptimizationTest::new_with_config(plan, opt, &config),
            @r###"
        ---
        input:
          - " UnionExec"
          - "   UnionExec"
          - "     RecordBatchesExec: chunks=1, projection=[time]"
          - "     ParquetExec: file_groups={1 group: [[2.parquet]]}, projection=[time]"
          - "   UnionExec"
          - "     RecordBatchesExec: chunks=1, projection=[time]"
          - "     ParquetExec: file_groups={2 groups: [[4.parquet], [5.parquet]]}, projection=[time]"
        output:
          Ok:
            - " UnionExec"
            - "   RecordBatchesExec: chunks=2, projection=[time]"
            - "   ParquetExec: file_groups={2 groups: [[2.parquet, 5.parquet], [4.parquet]]}, projection=[time]"
        "###
        );
    }

    #[test]
    fn test_only_combine_contiguous_arms() {
        let chunk1 = TestChunk::new("table")
            .with_id(1)
            .with_dummy_parquet_file()
            .with_time_column_with_stats(Some(1), Some(2));
        let chunk2 = TestChunk::new("table")
            .with_id(2)
            .with_dummy_parquet_file()
            .with_time_column_with_stats(Some(3), Some(4));
        let chunk3 = TestChunk::new("table")
            .with_id(3)
            .with_dummy_parquet_file()
            .with_time_column_with_stats(Some(5), Some(6));
        let chunk4 = TestChunk::new("table")
            .with_id(4)
            .with_dummy_parquet_file()
            .with_time_column_with_stats(Some(7), Some(8));
        let schema = chunk1.schema().as_arrow();
        let plan = Arc::new(UnionExec::new(vec![
            chunks_to_physical_nodes(&schema, None, vec![Arc::new(chunk1)], 2),
            chunks_to_physical_nodes(&schema, None, vec![Arc::new(chunk2)], 2),
            Arc::new(
                FilterExec::try_new(
                    Arc::new(Literal::new(ScalarValue::from(false))),
                    chunks_to_physical_nodes(&schema, None, vec![Arc::new(chunk3)], 2),
                )
                .unwrap(),
            ),
            chunks_to_physical_nodes(&schema, None, vec![Arc::new(chunk4)], 2),
        ]));
        let opt = CombineChunks;
        insta::assert_yaml_snapshot!(
            OptimizationTest::new(plan, opt),
            @r###"
        ---
        input:
          - " UnionExec"
          - "   UnionExec"
          - "     ParquetExec: file_groups={1 group: [[1.parquet]]}, projection=[time]"
          - "   UnionExec"
          - "     ParquetExec: file_groups={1 group: [[2.parquet]]}, projection=[time]"
          - "   FilterExec: false"
          - "     UnionExec"
          - "       ParquetExec: file_groups={1 group: [[3.parquet]]}, projection=[time]"
          - "   UnionExec"
          - "     ParquetExec: file_groups={1 group: [[4.parquet]]}, projection=[time]"
        output:
          Ok:
            - " UnionExec"
            - "   ParquetExec: file_groups={2 groups: [[1.parquet], [2.parquet]]}, projection=[time]"
            - "   FilterExec: false"
            - "     UnionExec"
            - "       ParquetExec: file_groups={1 group: [[3.parquet]]}, projection=[time]"
            - "   ParquetExec: file_groups={1 group: [[4.parquet]]}, projection=[time]"
        "###
        );
    }

    #[test]
    fn test_combine_some_union_arms() {
        let chunk1 = TestChunk::new("table").with_id(1).with_dummy_parquet_file();
        let chunk2 = TestChunk::new("table").with_id(1).with_dummy_parquet_file();
        let chunk3 = TestChunk::new("table").with_id(1).with_dummy_parquet_file();
        let schema = chunk1.schema().as_arrow();
        let plan = Arc::new(UnionExec::new(vec![
            chunks_to_physical_nodes(&schema, None, vec![Arc::new(chunk1)], 2),
            chunks_to_physical_nodes(&schema, None, vec![Arc::new(chunk2)], 2),
            Arc::new(
                FilterExec::try_new(
                    Arc::new(Literal::new(ScalarValue::from(false))),
                    chunks_to_physical_nodes(&schema, None, vec![Arc::new(chunk3)], 2),
                )
                .unwrap(),
            ),
        ]));
        let opt = CombineChunks;
        insta::assert_yaml_snapshot!(
            OptimizationTest::new(plan, opt),
            @r###"
        ---
        input:
          - " UnionExec"
          - "   UnionExec"
          - "     ParquetExec: file_groups={1 group: [[1.parquet]]}"
          - "   UnionExec"
          - "     ParquetExec: file_groups={1 group: [[1.parquet]]}"
          - "   FilterExec: false"
          - "     UnionExec"
          - "       ParquetExec: file_groups={1 group: [[1.parquet]]}"
        output:
          Ok:
            - " UnionExec"
            - "   ParquetExec: file_groups={2 groups: [[1.parquet], [1.parquet]]}"
            - "   FilterExec: false"
            - "     UnionExec"
            - "       ParquetExec: file_groups={1 group: [[1.parquet]]}"
        "###
        );
    }

    #[test]
    fn test_no_chunks() {
        let chunk1 = TestChunk::new("table").with_id(1);
        let schema = chunk1.schema().as_arrow();
        let plan = chunks_to_physical_nodes(&schema, None, vec![], 2);
        let opt = CombineChunks;
        let mut config = ConfigOptions::default();
        config.execution.target_partitions = 2;
        insta::assert_yaml_snapshot!(
            OptimizationTest::new_with_config(plan, opt, &config),
            @r###"
        ---
        input:
          - " EmptyExec"
        output:
          Ok:
            - " EmptyExec"
        "###
        );
    }

    #[test]
    fn test_no_valid_arms() {
        let chunk1 = TestChunk::new("table").with_id(1);
        let schema = chunk1.schema().as_arrow();
        let plan = Arc::new(UnionExec::new(vec![Arc::new(
            FilterExec::try_new(
                Arc::new(Literal::new(ScalarValue::from(false))),
                chunks_to_physical_nodes(&schema, None, vec![Arc::new(chunk1)], 2),
            )
            .unwrap(),
        )]));
        let opt = CombineChunks;
        insta::assert_yaml_snapshot!(
            OptimizationTest::new(plan, opt),
            @r###"
        ---
        input:
          - " UnionExec"
          - "   FilterExec: false"
          - "     UnionExec"
          - "       RecordBatchesExec: chunks=1"
        output:
          Ok:
            - " UnionExec"
            - "   FilterExec: false"
            - "     UnionExec"
            - "       RecordBatchesExec: chunks=1"
        "###
        );
    }
}
