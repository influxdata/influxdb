use std::sync::Arc;

use datafusion::{
    config::ConfigOptions,
    error::Result,
    physical_optimizer::PhysicalOptimizerRule,
    physical_plan::{rewrite::TreeNodeRewritable, ExecutionPlan},
};
use predicate::Predicate;

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
            if let Some((iox_schema, chunks)) = extract_chunks(plan.as_ref()) {
                return Ok(Some(chunks_to_physical_nodes(
                    &iox_schema,
                    None,
                    chunks,
                    Predicate::new(),
                    config.execution.target_partitions,
                )));
            }

            Ok(None)
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
    use datafusion::physical_plan::union::UnionExec;

    use crate::{physical_optimizer::test_util::OptimizationTest, test::TestChunk, QueryChunkMeta};

    use super::*;

    #[test]
    fn test_combine() {
        let chunk1 = TestChunk::new("table").with_id(1);
        let chunk2 = TestChunk::new("table").with_id(2).with_dummy_parquet_file();
        let chunk3 = TestChunk::new("table").with_id(3);
        let chunk4 = TestChunk::new("table").with_id(4).with_dummy_parquet_file();
        let chunk5 = TestChunk::new("table").with_id(5).with_dummy_parquet_file();
        let schema = chunk1.schema().clone();
        let plan = Arc::new(UnionExec::new(vec![
            chunks_to_physical_nodes(
                &schema,
                None,
                vec![Arc::new(chunk1), Arc::new(chunk2)],
                Predicate::new(),
                2,
            ),
            chunks_to_physical_nodes(
                &schema,
                None,
                vec![Arc::new(chunk3), Arc::new(chunk4), Arc::new(chunk5)],
                Predicate::new(),
                2,
            ),
        ]));
        let opt = CombineChunks::default();
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
          - "     ParquetExec: limit=None, partitions={1 group: [[2.parquet]]}, projection=[]"
          - "   UnionExec"
          - "     RecordBatchesExec: batches_groups=1 batches=0 total_rows=0"
          - "     ParquetExec: limit=None, partitions={2 groups: [[4.parquet], [5.parquet]]}, projection=[]"
        output:
          Ok:
            - " UnionExec"
            - "   RecordBatchesExec: batches_groups=2 batches=0 total_rows=0"
            - "   ParquetExec: limit=None, partitions={2 groups: [[2.parquet, 5.parquet], [4.parquet]]}, projection=[]"
        "###
        );
    }
}
