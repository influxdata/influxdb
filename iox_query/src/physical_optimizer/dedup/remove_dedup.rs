use std::sync::Arc;

use datafusion::{
    common::tree_node::{Transformed, TreeNode},
    config::ConfigOptions,
    error::Result,
    physical_optimizer::PhysicalOptimizerRule,
    physical_plan::ExecutionPlan,
};

use crate::{
    physical_optimizer::chunk_extraction::extract_chunks,
    provider::{chunks_to_physical_nodes, DeduplicateExec},
};

/// Removes de-duplication operation if there are at most 1 chunks and this chunk does NOT contain primary-key duplicates.
#[derive(Debug, Default)]
pub struct RemoveDedup;

impl PhysicalOptimizerRule for RemoveDedup {
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

                if (chunks.len() < 2) && chunks.iter().all(|c| !c.may_contain_pk_duplicates()) {
                    return Ok(Transformed::Yes(chunks_to_physical_nodes(
                        &schema,
                        output_sort_key.as_ref(),
                        chunks,
                        config.execution.target_partitions,
                    )));
                }
            }

            Ok(Transformed::No(plan))
        })
    }

    fn name(&self) -> &str {
        "remove_dedup"
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
        let opt = RemoveDedup;
        insta::assert_yaml_snapshot!(
            OptimizationTest::new(plan, opt),
            @r###"
        ---
        input:
          - " DeduplicateExec: [tag1@1 ASC,tag2@2 ASC,time@3 ASC]"
          - "   EmptyExec: produce_one_row=false"
        output:
          Ok:
            - " EmptyExec: produce_one_row=false"
        "###
        );
    }

    #[test]
    fn test_single_chunk_no_pk_dups() {
        let chunk1 = chunk(1).with_may_contain_pk_duplicates(false);
        let schema = chunk1.schema().clone();
        let plan = dedup_plan(schema, vec![chunk1]);
        let opt = RemoveDedup;
        insta::assert_yaml_snapshot!(
            OptimizationTest::new(plan, opt),
            @r###"
        ---
        input:
          - " DeduplicateExec: [tag1@1 ASC,tag2@2 ASC,time@3 ASC]"
          - "   UnionExec"
          - "     RecordBatchesExec: chunks=1"
        output:
          Ok:
            - " UnionExec"
            - "   RecordBatchesExec: chunks=1"
        "###
        );
    }

    #[test]
    fn test_single_chunk_with_pk_dups() {
        let chunk1 = chunk(1).with_may_contain_pk_duplicates(true);
        let schema = chunk1.schema().clone();
        let plan = dedup_plan(schema, vec![chunk1]);
        let opt = RemoveDedup;
        insta::assert_yaml_snapshot!(
            OptimizationTest::new(plan, opt),
            @r###"
        ---
        input:
          - " DeduplicateExec: [tag1@1 ASC,tag2@2 ASC,time@3 ASC]"
          - "   UnionExec"
          - "     RecordBatchesExec: chunks=1"
        output:
          Ok:
            - " DeduplicateExec: [tag1@1 ASC,tag2@2 ASC,time@3 ASC]"
            - "   UnionExec"
            - "     RecordBatchesExec: chunks=1"
        "###
        );
    }

    #[test]
    fn test_multiple_chunks() {
        let chunk1 = chunk(1).with_may_contain_pk_duplicates(false);
        let chunk2 = chunk(2).with_may_contain_pk_duplicates(false);
        let schema = chunk1.schema().clone();
        let plan = dedup_plan(schema, vec![chunk1, chunk2]);
        let opt = RemoveDedup;
        insta::assert_yaml_snapshot!(
            OptimizationTest::new(plan, opt),
            @r###"
        ---
        input:
          - " DeduplicateExec: [tag1@1 ASC,tag2@2 ASC,time@3 ASC]"
          - "   UnionExec"
          - "     RecordBatchesExec: chunks=2"
        output:
          Ok:
            - " DeduplicateExec: [tag1@1 ASC,tag2@2 ASC,time@3 ASC]"
            - "   UnionExec"
            - "     RecordBatchesExec: chunks=2"
        "###
        );
    }
}
