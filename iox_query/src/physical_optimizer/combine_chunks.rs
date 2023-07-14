use std::sync::Arc;

use datafusion::{
    common::tree_node::{Transformed, TreeNode},
    config::ConfigOptions,
    error::{DataFusionError, Result},
    physical_optimizer::PhysicalOptimizerRule,
    physical_plan::{union::UnionExec, ExecutionPlan},
};

use crate::{
    physical_optimizer::chunk_extraction::extract_chunks, provider::chunks_to_physical_nodes,
};

/// Collects [`QueryChunk`]s and re-creates a appropriate physical nodes.
///
/// This only works if there no filters, projections, sorts, or de-duplicate operations in the affected subtree.
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
                let (inputs_with_chunks, inputs_other): (Vec<_>, Vec<_>) = union_exec
                    .inputs()
                    .iter()
                    .cloned()
                    .partition(|plan| {
                        extract_chunks(plan.as_ref()).is_some()
                    });

                if inputs_with_chunks.is_empty() {
                    return Ok(Transformed::No(plan));
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
                        return Err(DataFusionError::External(format!("Expected chunks_to_physical_nodes to produce UnionExec but got {union_of_chunks:?}").into()));
                    };
                    let final_union = UnionExec::new(union_of_chunks.inputs().iter().cloned().chain(inputs_other.into_iter()).collect());
                    return Ok(Transformed::Yes(Arc::new(final_union)));
                }
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
        let chunk1 = TestChunk::new("table").with_id(1);
        let chunk2 = TestChunk::new("table").with_id(2).with_dummy_parquet_file();
        let chunk3 = TestChunk::new("table").with_id(3);
        let chunk4 = TestChunk::new("table").with_id(4).with_dummy_parquet_file();
        let chunk5 = TestChunk::new("table").with_id(5).with_dummy_parquet_file();
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
          - "     RecordBatchesExec: batches_groups=1 batches=0 total_rows=0"
          - "     ParquetExec: file_groups={1 group: [[2.parquet]]}"
          - "   UnionExec"
          - "     RecordBatchesExec: batches_groups=1 batches=0 total_rows=0"
          - "     ParquetExec: file_groups={2 groups: [[4.parquet], [5.parquet]]}"
        output:
          Ok:
            - " UnionExec"
            - "   RecordBatchesExec: batches_groups=2 batches=0 total_rows=0"
            - "   ParquetExec: file_groups={2 groups: [[2.parquet, 5.parquet], [4.parquet]]}"
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
          - " EmptyExec: produce_one_row=false"
        output:
          Ok:
            - " EmptyExec: produce_one_row=false"
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
          - "       RecordBatchesExec: batches_groups=1 batches=0 total_rows=0"
        output:
          Ok:
            - " UnionExec"
            - "   FilterExec: false"
            - "     UnionExec"
            - "       RecordBatchesExec: batches_groups=1 batches=0 total_rows=0"
        "###
        );
    }
}
