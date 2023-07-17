use std::sync::Arc;

use datafusion::{
    common::tree_node::{Transformed, TreeNode},
    config::ConfigOptions,
    error::Result,
    physical_optimizer::PhysicalOptimizerRule,
    physical_plan::{union::UnionExec, ExecutionPlan},
};
use observability_deps::tracing::warn;

use crate::{
    config::IoxConfigExt,
    physical_optimizer::chunk_extraction::extract_chunks,
    provider::{chunks_to_physical_nodes, group_potential_duplicates, DeduplicateExec},
};

/// Split de-duplication operations based on time.
///
/// Chunks that overlap will be part of the same de-dup group.
///
/// This should usually be more cost-efficient.
#[derive(Debug, Default)]
pub struct TimeSplit;

impl PhysicalOptimizerRule for TimeSplit {
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

                let groups = group_potential_duplicates(chunks);

                // if there are no chunks or there is only one group, we don't need to split
                if groups.len() < 2 {
                    return Ok(Transformed::No(plan));
                }

                // Protect against degenerative plans
                let max_dedup_time_split = config
                    .extensions
                    .get::<IoxConfigExt>()
                    .cloned()
                    .unwrap_or_default()
                    .max_dedup_time_split;
                if groups.len() > max_dedup_time_split {
                    warn!(
                        n_groups = groups.len(),
                        max_dedup_time_split,
                        "cannot split dedup operation based on time overlaps, too many groups"
                    );
                    return Ok(Transformed::No(plan));
                }

                let out = UnionExec::new(
                    groups
                        .into_iter()
                        .map(|chunks| {
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
        "time_split"
    }

    fn schema_check(&self) -> bool {
        true
    }
}

#[cfg(test)]
mod tests {
    use crate::{
        physical_optimizer::{
            dedup::test_util::{chunk, dedup_plan},
            test_util::OptimizationTest,
        },
        QueryChunk,
    };

    use super::*;

    #[test]
    fn test_no_chunks() {
        let schema = chunk(1).schema().clone();
        let plan = dedup_plan(schema, vec![]);
        let opt = TimeSplit;
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
    fn test_all_overlap() {
        let chunk1 = chunk(1).with_timestamp_min_max(5, 10);
        let chunk2 = chunk(2).with_timestamp_min_max(3, 5);
        let chunk3 = chunk(3)
            .with_dummy_parquet_file()
            .with_timestamp_min_max(8, 9);
        let schema = chunk1.schema().clone();
        let plan = dedup_plan(schema, vec![chunk1, chunk2, chunk3]);
        let opt = TimeSplit;
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
    fn test_different_groups() {
        let chunk1 = chunk(1).with_timestamp_min_max(0, 10);
        let chunk2 = chunk(2).with_timestamp_min_max(11, 12);
        // use at least 3 parquet files for one of the two partitions to validate that `target_partitions` is forwarded correctly
        let chunk3 = chunk(3)
            .with_dummy_parquet_file()
            .with_timestamp_min_max(1, 5);
        let chunk4 = chunk(4)
            .with_dummy_parquet_file()
            .with_timestamp_min_max(11, 11);
        let chunk5 = chunk(5)
            .with_dummy_parquet_file()
            .with_timestamp_min_max(7, 8);
        let chunk6 = chunk(6)
            .with_dummy_parquet_file()
            .with_timestamp_min_max(0, 0);
        let schema = chunk1.schema().clone();
        let plan = dedup_plan(schema, vec![chunk1, chunk2, chunk3, chunk4, chunk5, chunk6]);
        let opt = TimeSplit;
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
            - "       ParquetExec: file_groups={2 groups: [[6.parquet, 5.parquet], [3.parquet]]}, projection=[field, tag1, tag2, time]"
            - "   DeduplicateExec: [tag1@1 ASC,tag2@2 ASC,time@3 ASC]"
            - "     UnionExec"
            - "       RecordBatchesExec: batches_groups=1 batches=0 total_rows=0"
            - "       ParquetExec: file_groups={1 group: [[4.parquet]]}, projection=[field, tag1, tag2, time]"
        "###
        );
    }

    #[test]
    fn test_max_split() {
        let chunk1 = chunk(1).with_timestamp_min_max(1, 1);
        let chunk2 = chunk(2).with_timestamp_min_max(2, 2);
        let chunk3 = chunk(3).with_timestamp_min_max(3, 3);
        let schema = chunk1.schema().clone();
        let plan = dedup_plan(schema, vec![chunk1, chunk2, chunk3]);
        let opt = TimeSplit;
        let mut config = ConfigOptions::default();
        config.extensions.insert(IoxConfigExt {
            max_dedup_time_split: 2,
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
