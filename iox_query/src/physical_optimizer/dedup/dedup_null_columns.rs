use std::{collections::HashSet, sync::Arc};

use datafusion::{
    common::tree_node::{Transformed, TreeNode},
    config::ConfigOptions,
    error::Result,
    physical_optimizer::PhysicalOptimizerRule,
    physical_plan::ExecutionPlan,
};
use schema::{sort::SortKeyBuilder, TIME_COLUMN_NAME};

use crate::{
    physical_optimizer::chunk_extraction::extract_chunks,
    provider::{chunks_to_physical_nodes, DeduplicateExec},
    util::arrow_sort_key_exprs,
};

/// Determine sort key set of [`DeduplicateExec`] by elimating all-NULL columns.
///
/// This finds a good sort key for [`DeduplicateExec`] based on the [`QueryChunk`]s covered by the deduplication.
///
/// We assume that, columns that are NOT present in any chunks and hence are only created as pure NULL-columns are
/// not relevant for deduplication since they are effectively constant.
///
///
/// [`QueryChunk`]: crate::QueryChunk
#[derive(Debug, Default)]
pub struct DedupNullColumns;

impl PhysicalOptimizerRule for DedupNullColumns {
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
                let Some((schema, chunks, _output_sort_key)) = extract_chunks(child.as_ref())
                else {
                    return Ok(Transformed::No(plan));
                };

                let pk_cols = dedup_exec.sort_columns();

                let mut used_pk_cols = HashSet::new();
                for chunk in &chunks {
                    for (_type, field) in chunk.schema().iter() {
                        if pk_cols.contains(field.name().as_str()) {
                            used_pk_cols.insert(field.name().as_str());
                        }
                    }
                }

                let mut used_pk_cols = used_pk_cols.into_iter().collect::<Vec<_>>();
                used_pk_cols.sort_by_key(|col| (*col == TIME_COLUMN_NAME, *col));

                let mut sort_key_builder = SortKeyBuilder::new();
                for col in used_pk_cols {
                    sort_key_builder = sort_key_builder.with_col(col);
                }

                let sort_key = sort_key_builder.build();
                let child = chunks_to_physical_nodes(
                    &schema,
                    (!sort_key.is_empty()).then_some(&sort_key),
                    chunks,
                    config.execution.target_partitions,
                );

                let sort_exprs = arrow_sort_key_exprs(&sort_key, &schema);
                return Ok(Transformed::Yes(Arc::new(DeduplicateExec::new(
                    child,
                    sort_exprs,
                    dedup_exec.use_chunk_order_col(),
                ))));
            }

            Ok(Transformed::No(plan))
        })
    }

    fn name(&self) -> &str {
        "dedup_null_columns"
    }

    fn schema_check(&self) -> bool {
        true
    }
}

#[cfg(test)]
mod tests {
    use schema::SchemaBuilder;

    use crate::{
        physical_optimizer::{
            dedup::test_util::{chunk, dedup_plan, dedup_plan_with_chunk_order_col},
            test_util::OptimizationTest,
        },
        test::TestChunk,
        QueryChunk,
    };

    use super::*;

    #[test]
    fn test_no_chunks() {
        let schema = chunk(1).schema().clone();
        let plan = dedup_plan(schema, vec![]);
        let opt = DedupNullColumns;
        insta::assert_yaml_snapshot!(
            OptimizationTest::new(plan, opt),
            @r###"
        ---
        input:
          - " DeduplicateExec: [tag1@1 ASC,tag2@2 ASC,time@3 ASC]"
          - "   EmptyExec: produce_one_row=false"
        output:
          Ok:
            - " DeduplicateExec: []"
            - "   EmptyExec: produce_one_row=false"
        "###
        );
    }

    #[test]
    fn test_single_chunk_all_cols() {
        let chunk = chunk(1).with_dummy_parquet_file();
        let schema = chunk.schema().clone();
        let plan = dedup_plan(schema, vec![chunk]);
        let opt = DedupNullColumns;
        insta::assert_yaml_snapshot!(
            OptimizationTest::new(plan, opt),
            @r###"
        ---
        input:
          - " DeduplicateExec: [tag1@1 ASC,tag2@2 ASC,time@3 ASC]"
          - "   UnionExec"
          - "     ParquetExec: file_groups={1 group: [[1.parquet]]}, projection=[field, tag1, tag2, time]"
        output:
          Ok:
            - " DeduplicateExec: [tag1@1 ASC,tag2@2 ASC,time@3 ASC]"
            - "   UnionExec"
            - "     ParquetExec: file_groups={1 group: [[1.parquet]]}, projection=[field, tag1, tag2, time]"
        "###
        );
    }

    #[test]
    fn test_single_chunk_schema_has_chunk_order_col() {
        let chunk = chunk(1).with_dummy_parquet_file();
        let schema = chunk.schema().clone();
        let plan = dedup_plan_with_chunk_order_col(schema, vec![chunk]);
        let opt = DedupNullColumns;
        insta::assert_yaml_snapshot!(
            OptimizationTest::new(plan, opt),
            @r###"
        ---
        input:
          - " DeduplicateExec: [tag1@1 ASC,tag2@2 ASC,time@3 ASC]"
          - "   UnionExec"
          - "     ParquetExec: file_groups={1 group: [[1.parquet]]}, projection=[field, tag1, tag2, time, __chunk_order], output_ordering=[__chunk_order@4 ASC]"
        output:
          Ok:
            - " DeduplicateExec: [tag1@1 ASC,tag2@2 ASC,time@3 ASC]"
            - "   UnionExec"
            - "     ParquetExec: file_groups={1 group: [[1.parquet]]}, projection=[field, tag1, tag2, time, __chunk_order], output_ordering=[__chunk_order@4 ASC]"
        "###
        );
    }

    #[test]
    fn test_single_chunk_misses_pk_cols() {
        let chunk = TestChunk::new("table")
            .with_id(1)
            .with_tag_column("tag1")
            .with_dummy_parquet_file();
        let schema = SchemaBuilder::new()
            .tag("tag1")
            .tag("tag2")
            .tag("zzz")
            .timestamp()
            .build()
            .unwrap();
        let plan = dedup_plan(schema, vec![chunk]);
        let opt = DedupNullColumns;
        insta::assert_yaml_snapshot!(
            OptimizationTest::new(plan, opt),
            @r###"
        ---
        input:
          - " DeduplicateExec: [tag1@0 ASC,tag2@1 ASC,zzz@2 ASC,time@3 ASC]"
          - "   UnionExec"
          - "     ParquetExec: file_groups={1 group: [[1.parquet]]}, projection=[tag1, tag2, zzz, time]"
        output:
          Ok:
            - " DeduplicateExec: [tag1@0 ASC]"
            - "   UnionExec"
            - "     ParquetExec: file_groups={1 group: [[1.parquet]]}, projection=[tag1, tag2, zzz, time]"
        "###
        );
    }

    #[test]
    fn test_two_chunks() {
        let chunk1 = TestChunk::new("table")
            .with_id(1)
            .with_tag_column("tag1")
            .with_tag_column("tag2")
            .with_time_column()
            .with_dummy_parquet_file();
        let chunk2 = TestChunk::new("table")
            .with_id(2)
            .with_tag_column("tag1")
            .with_tag_column("tag3")
            .with_time_column()
            .with_dummy_parquet_file();
        let schema = SchemaBuilder::new()
            .tag("tag1")
            .tag("tag2")
            .tag("tag3")
            .tag("tag4")
            .timestamp()
            .build()
            .unwrap();
        let plan = dedup_plan(schema, vec![chunk1, chunk2]);
        let opt = DedupNullColumns;
        insta::assert_yaml_snapshot!(
            OptimizationTest::new(plan, opt),
            @r###"
        ---
        input:
          - " DeduplicateExec: [tag1@0 ASC,tag2@1 ASC,tag3@2 ASC,tag4@3 ASC,time@4 ASC]"
          - "   UnionExec"
          - "     ParquetExec: file_groups={2 groups: [[1.parquet], [2.parquet]]}, projection=[tag1, tag2, tag3, tag4, time]"
        output:
          Ok:
            - " DeduplicateExec: [tag1@0 ASC,tag2@1 ASC,tag3@2 ASC,time@4 ASC]"
            - "   UnionExec"
            - "     ParquetExec: file_groups={2 groups: [[1.parquet], [2.parquet]]}, projection=[tag1, tag2, tag3, tag4, time]"
        "###
        );
    }
}
