use crate::{
    config::IoxConfigExt,
    physical_optimizer::chunk_extraction::extract_chunks,
    provider::{chunks_to_physical_nodes, DeduplicateExec},
    QueryChunk,
};
use datafusion::{
    common::tree_node::{Transformed, TreeNode},
    config::ConfigOptions,
    error::Result,
    physical_optimizer::PhysicalOptimizerRule,
    physical_plan::{union::UnionExec, ExecutionPlan},
};
use hashbrown::HashMap;
use observability_deps::tracing::warn;
use std::sync::Arc;

/// Split de-duplication operations based on partitons.
///
/// This should usually be more cost-efficient.
#[derive(Debug, Default)]
pub struct PartitionSplit;

impl PhysicalOptimizerRule for PartitionSplit {
    fn optimize(
        &self,
        plan: Arc<dyn ExecutionPlan>,
        config: &ConfigOptions,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        plan.transform_up(&|plan| {
            let plan_any = plan.as_any();

            if let Some(dedup_exec) = plan_any.downcast_ref::<DeduplicateExec>() {
                let mut children = dedup_exec.children();
                assert_eq!(children.len(), 1);
                let child = children.remove(0);
                let Some((schema, chunks, output_sort_key)) = extract_chunks(child.as_ref()) else {
                    return Ok(Transformed::No(plan));
                };

                let mut chunks_by_partition: HashMap<_, Vec<Arc<dyn QueryChunk>>> =
                    Default::default();
                for chunk in chunks {
                    chunks_by_partition
                        .entry(chunk.partition_id().clone())
                        .or_default()
                        .push(chunk);
                }

                // If there not multiple partitions (0 or 1), then this optimizer is a no-op. Signal that to the
                // optimizer framework.
                if chunks_by_partition.len() < 2 {
                    return Ok(Transformed::No(plan));
                }

                // Protect against degenerative plans
                let max_dedup_partition_split = config
                    .extensions
                    .get::<IoxConfigExt>()
                    .cloned()
                    .unwrap_or_default()
                    .max_dedup_partition_split;
                if chunks_by_partition.len() > max_dedup_partition_split {
                    warn!(
                        n_partitions = chunks_by_partition.len(),
                        max_dedup_partition_split,
                        "cannot split dedup operation based on partition, too many partitions"
                    );
                    return Ok(Transformed::No(plan));
                }

                // ensure deterministic order
                let mut chunks_by_partition = chunks_by_partition.into_iter().collect::<Vec<_>>();
                chunks_by_partition.sort_by(|a, b| a.0.cmp(&b.0));

                let out = UnionExec::new(
                    chunks_by_partition
                        .into_iter()
                        .map(|(_p_id, chunks)| {
                            Arc::new(DeduplicateExec::new(
                                chunks_to_physical_nodes(
                                    &schema,
                                    output_sort_key.as_ref(),
                                    chunks,
                                    config.execution.target_partitions,
                                ),
                                dedup_exec.sort_keys().to_vec(),
                                dedup_exec.use_chunk_order_col(),
                            )) as _
                        })
                        .collect(),
                );
                return Ok(Transformed::Yes(Arc::new(out)));
            }

            Ok(Transformed::No(plan))
        })
    }

    fn name(&self) -> &str {
        "partition_split"
    }

    fn schema_check(&self) -> bool {
        true
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::physical_optimizer::{
        dedup::test_util::{chunk, dedup_plan},
        test_util::OptimizationTest,
    };
    use data_types::{PartitionHashId, PartitionId, TransitionPartitionId};

    #[test]
    fn test_no_chunks() {
        let schema = chunk(1).schema().clone();
        let plan = dedup_plan(schema, vec![]);
        let opt = PartitionSplit;
        insta::assert_yaml_snapshot!(
            OptimizationTest::new(plan, opt),
            @r###"
        ---
        input:
          - " DeduplicateExec: [tag1@1 ASC,tag2@2 ASC,time@3 ASC]"
          - "   EmptyExec: produce_one_row=false"
        output:
          Ok:
            - " DeduplicateExec: [tag1@1 ASC,tag2@2 ASC,time@3 ASC]"
            - "   EmptyExec: produce_one_row=false"
        "###
        );
    }

    #[test]
    fn test_same_partition() {
        let chunk1 = chunk(1);
        let chunk2 = chunk(2);
        let chunk3 = chunk(3).with_dummy_parquet_file();
        let schema = chunk1.schema().clone();
        let plan = dedup_plan(schema, vec![chunk1, chunk2, chunk3]);
        let opt = PartitionSplit;
        insta::assert_yaml_snapshot!(
            OptimizationTest::new(plan, opt),
            @r###"
        ---
        input:
          - " DeduplicateExec: [tag1@1 ASC,tag2@2 ASC,time@3 ASC]"
          - "   UnionExec"
          - "     RecordBatchesExec: batches_groups=2 batches=0 total_rows=0"
          - "     ParquetExec: file_groups={1 group: [[3.parquet]]}, projection=[field, tag1, tag2, time]"
        output:
          Ok:
            - " DeduplicateExec: [tag1@1 ASC,tag2@2 ASC,time@3 ASC]"
            - "   UnionExec"
            - "     RecordBatchesExec: batches_groups=2 batches=0 total_rows=0"
            - "     ParquetExec: file_groups={1 group: [[3.parquet]]}, projection=[field, tag1, tag2, time]"
        "###
        );
    }

    #[test]
    fn test_different_partitions() {
        let chunk1 = chunk(1).with_partition(1);
        let chunk2 = chunk(2).with_partition(2);
        // use at least 3 parquet files for one of the two partitions to validate that `target_partitions` is forwared correctly
        let chunk3 = chunk(3).with_dummy_parquet_file().with_partition(1);
        let chunk4 = chunk(4).with_dummy_parquet_file().with_partition(2);
        let chunk5 = chunk(5).with_dummy_parquet_file().with_partition(1);
        let chunk6 = chunk(6).with_dummy_parquet_file().with_partition(1);
        let schema = chunk1.schema().clone();
        let plan = dedup_plan(schema, vec![chunk1, chunk2, chunk3, chunk4, chunk5, chunk6]);
        let opt = PartitionSplit;
        let mut config = ConfigOptions::default();
        config.execution.target_partitions = 2;
        insta::assert_yaml_snapshot!(
            OptimizationTest::new_with_config(plan, opt, &config),
            @r###"
        ---
        input:
          - " DeduplicateExec: [tag1@1 ASC,tag2@2 ASC,time@3 ASC]"
          - "   UnionExec"
          - "     RecordBatchesExec: batches_groups=2 batches=0 total_rows=0"
          - "     ParquetExec: file_groups={2 groups: [[3.parquet, 5.parquet], [4.parquet, 6.parquet]]}, projection=[field, tag1, tag2, time]"
        output:
          Ok:
            - " UnionExec"
            - "   DeduplicateExec: [tag1@1 ASC,tag2@2 ASC,time@3 ASC]"
            - "     UnionExec"
            - "       RecordBatchesExec: batches_groups=1 batches=0 total_rows=0"
            - "       ParquetExec: file_groups={2 groups: [[3.parquet, 6.parquet], [5.parquet]]}, projection=[field, tag1, tag2, time]"
            - "   DeduplicateExec: [tag1@1 ASC,tag2@2 ASC,time@3 ASC]"
            - "     UnionExec"
            - "       RecordBatchesExec: batches_groups=1 batches=0 total_rows=0"
            - "       ParquetExec: file_groups={1 group: [[4.parquet]]}, projection=[field, tag1, tag2, time]"
        "###
        );
    }

    #[test]
    fn test_different_partitions_with_and_without_hash_ids() {
        // Partition without hash ID in the catalog
        let legacy_partition_id = 1;
        let legacy_transition_partition_id =
            TransitionPartitionId::Deprecated(PartitionId::new(legacy_partition_id));

        // Partition with hash ID in the catalog
        let transition_partition_id =
            TransitionPartitionId::Deterministic(PartitionHashId::arbitrary_for_testing());

        let chunk1 = chunk(1).with_partition_id(legacy_transition_partition_id.clone());
        let chunk2 = chunk(2).with_partition_id(transition_partition_id.clone());

        let chunk3 = chunk(3)
            .with_dummy_parquet_file()
            .with_partition_id(legacy_transition_partition_id.clone());
        let chunk4 = chunk(4)
            .with_dummy_parquet_file()
            .with_partition_id(transition_partition_id.clone());
        let chunk5 = chunk(5)
            .with_dummy_parquet_file()
            .with_partition_id(legacy_transition_partition_id.clone());
        let chunk6 = chunk(6)
            .with_dummy_parquet_file()
            .with_partition_id(legacy_transition_partition_id.clone());
        let schema = chunk1.schema().clone();
        let plan = dedup_plan(schema, vec![chunk1, chunk2, chunk3, chunk4, chunk5, chunk6]);
        let opt = PartitionSplit;
        let mut config = ConfigOptions::default();
        config.execution.target_partitions = 2;
        insta::assert_yaml_snapshot!(
            OptimizationTest::new_with_config(plan, opt, &config),
            @r###"
        ---
        input:
          - " DeduplicateExec: [tag1@1 ASC,tag2@2 ASC,time@3 ASC]"
          - "   UnionExec"
          - "     RecordBatchesExec: batches_groups=2 batches=0 total_rows=0"
          - "     ParquetExec: file_groups={2 groups: [[3.parquet, 5.parquet], [4.parquet, 6.parquet]]}, projection=[field, tag1, tag2, time]"
        output:
          Ok:
            - " UnionExec"
            - "   DeduplicateExec: [tag1@1 ASC,tag2@2 ASC,time@3 ASC]"
            - "     UnionExec"
            - "       RecordBatchesExec: batches_groups=1 batches=0 total_rows=0"
            - "       ParquetExec: file_groups={2 groups: [[3.parquet, 6.parquet], [5.parquet]]}, projection=[field, tag1, tag2, time]"
            - "   DeduplicateExec: [tag1@1 ASC,tag2@2 ASC,time@3 ASC]"
            - "     UnionExec"
            - "       RecordBatchesExec: batches_groups=1 batches=0 total_rows=0"
            - "       ParquetExec: file_groups={1 group: [[4.parquet]]}, projection=[field, tag1, tag2, time]"
        "###
        );
    }

    #[test]
    fn test_max_split() {
        let chunk1 = chunk(1).with_partition(1);
        let chunk2 = chunk(2).with_partition(2);
        let chunk3 = chunk(3).with_partition(3);
        let schema = chunk1.schema().clone();
        let plan = dedup_plan(schema, vec![chunk1, chunk2, chunk3]);
        let opt = PartitionSplit;
        let mut config = ConfigOptions::default();
        config.extensions.insert(IoxConfigExt {
            max_dedup_partition_split: 2,
            ..Default::default()
        });
        insta::assert_yaml_snapshot!(
            OptimizationTest::new_with_config(plan, opt, &config),
            @r###"
        ---
        input:
          - " DeduplicateExec: [tag1@1 ASC,tag2@2 ASC,time@3 ASC]"
          - "   UnionExec"
          - "     RecordBatchesExec: batches_groups=3 batches=0 total_rows=0"
        output:
          Ok:
            - " DeduplicateExec: [tag1@1 ASC,tag2@2 ASC,time@3 ASC]"
            - "   UnionExec"
            - "     RecordBatchesExec: batches_groups=3 batches=0 total_rows=0"
        "###
        );
    }
}
