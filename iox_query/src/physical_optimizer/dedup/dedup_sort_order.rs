use std::{cmp::Reverse, sync::Arc};

use arrow::compute::SortOptions;
use datafusion::{
    common::tree_node::{Transformed, TreeNode},
    config::ConfigOptions,
    error::Result,
    physical_optimizer::PhysicalOptimizerRule,
    physical_plan::ExecutionPlan,
};
use indexmap::IndexSet;
use schema::{sort::SortKeyBuilder, TIME_COLUMN_NAME};

use crate::{
    physical_optimizer::chunk_extraction::extract_chunks,
    provider::{chunks_to_physical_nodes, DeduplicateExec},
    util::arrow_sort_key_exprs,
    CHUNK_ORDER_COLUMN_NAME,
};

/// Determine sort key order of [`DeduplicateExec`].
///
/// This finds a cheap sort key order for [`DeduplicateExec`] based on the [`QueryChunk`]s covered by the deduplication.
/// This means that the sort key of the [`DeduplicateExec`] should be as close as possible to the pre-sorted chunks to
/// avoid resorting. If all chunks are pre-sorted (or not sorted at all), this is basically the joined merged sort key
/// of all of them. If the chunks do not agree on a single sort order[^different_orders], then we use a vote-based
/// system where we column-by-column pick the sort key order in the hope that this does the least harm.
///
/// The produces sort key MUST be the same set of columns as before, i.e. this rule does NOT change the column set, it
/// only changes the order.
///
/// We assume that the order of the sort key passed to [`DeduplicateExec`] is not relevant for correctness.
///
/// This optimizer makes no assumption about how the ingester or compaction tier work or how chunks relate to each
/// other. As a consequence, it does NOT use the partition sort key.
///
///
/// [^different_orders]: In an ideal system, all chunks that have a sort order should agree on a single one. However we
///     want to avoid that the querier disintegrates when the ingester or compactor are buggy or when manual
///     interventions (like manual file creations) insert files that are slightly off.
///
///
/// [`QueryChunk`]: crate::QueryChunk
#[derive(Debug, Default)]
pub struct DedupSortOrder;

impl PhysicalOptimizerRule for DedupSortOrder {
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
                let Some((schema, chunks, _output_sort_key)) = extract_chunks(child.as_ref()) else {
                    return Ok(Transformed::No(plan))
                };

                let mut chunk_sort_keys: Vec<IndexSet<_>> = chunks
                    .iter()
                    .map(|chunk| {
                        chunk
                            .sort_key()
                            .map(|sort_key| {
                                sort_key
                                    .iter()
                                    .map(|(col, opts)| {
                                        assert_eq!(opts, &SortOptions::default());
                                        col.as_ref()
                                    })
                                    .collect()
                            })
                            .unwrap_or_default()
                    })
                    .collect();

                let mut quorum_sort_key_builder = SortKeyBuilder::default();
                let mut todo_pk_columns = dedup_exec.sort_columns();
                todo_pk_columns.remove(CHUNK_ORDER_COLUMN_NAME);
                while !todo_pk_columns.is_empty() {
                    let candidate_counts = todo_pk_columns.iter().copied().map(|col| {
                        let count = chunk_sort_keys
                            .iter()
                            .filter(|sort_key| {
                                match sort_key.get_index_of(col) {
                                    Some(idx) if idx == 0 => {
                                        // Column next in sort order from this chunks PoV. This is good.
                                        true
                                    }
                                    Some(_) => {
                                        // Column part of the sort order but we have at least one more column before
                                        // that. Try to avoid an expensive resort for this chunk.
                                        false
                                    }
                                    None => {
                                        // Column is not in the sort order of this chunk at all. Hence we can place it
                                        // everywhere in the quorum sort key w/o having to worry about this particular
                                        // chunk.
                                        true
                                    }
                                }
                            })
                            .count();
                        (col, count)
                    });
                    let candidate_counts = sorted(
                        candidate_counts
                            .into_iter()
                            .map(|(col, count)| (Reverse(count), col == TIME_COLUMN_NAME, col)),
                    );
                    let next_key = candidate_counts.first().expect("all TODO cols inserted").2;

                    for chunk_sort_key in &mut chunk_sort_keys {
                        chunk_sort_key.shift_remove_full(next_key);
                    }

                    let was_present = todo_pk_columns.remove(next_key);
                    assert!(was_present);

                    quorum_sort_key_builder = quorum_sort_key_builder.with_col(next_key);
                }

                let quorum_sort_key = quorum_sort_key_builder.build();
                let child = chunks_to_physical_nodes(
                    &schema,
                    (!quorum_sort_key.is_empty()).then_some(&quorum_sort_key),
                    chunks,
                    config.execution.target_partitions,
                );

                let sort_exprs = arrow_sort_key_exprs(&quorum_sort_key, &schema);
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
        "dedup_sort_order"
    }

    fn schema_check(&self) -> bool {
        true
    }
}

/// Collect items into a sorted vector.
fn sorted<T>(it: impl IntoIterator<Item = T>) -> Vec<T>
where
    T: Ord,
{
    let mut items = it.into_iter().collect::<Vec<T>>();
    items.sort();
    items
}

#[cfg(test)]
mod tests {
    use schema::{sort::SortKey, SchemaBuilder, TIME_COLUMN_NAME};

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
        let opt = DedupSortOrder;
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
    fn test_single_chunk_no_sort_key() {
        let chunk = chunk(1).with_dummy_parquet_file();
        let schema = chunk.schema().clone();
        let plan = dedup_plan(schema, vec![chunk]);
        let opt = DedupSortOrder;
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
    fn test_single_chunk_order() {
        let chunk = chunk(1)
            .with_dummy_parquet_file()
            .with_sort_key(SortKey::from_columns([
                Arc::from("tag2"),
                Arc::from("tag1"),
                Arc::from(TIME_COLUMN_NAME),
            ]));
        let schema = chunk.schema().clone();
        let plan = dedup_plan(schema, vec![chunk]);
        let opt = DedupSortOrder;
        insta::assert_yaml_snapshot!(
            OptimizationTest::new(plan, opt),
            @r###"
        ---
        input:
          - " DeduplicateExec: [tag1@1 ASC,tag2@2 ASC,time@3 ASC]"
          - "   UnionExec"
          - "     ParquetExec: file_groups={1 group: [[1.parquet]]}, projection=[field, tag1, tag2, time], output_ordering=[tag2@2 ASC, tag1@1 ASC, time@3 ASC]"
        output:
          Ok:
            - " DeduplicateExec: [tag2@2 ASC,tag1@1 ASC,time@3 ASC]"
            - "   UnionExec"
            - "     ParquetExec: file_groups={1 group: [[1.parquet]]}, projection=[field, tag1, tag2, time], output_ordering=[tag2@2 ASC, tag1@1 ASC, time@3 ASC]"
        "###
        );
    }

    #[test]
    fn test_single_chunk_with_chunk_order_col() {
        let chunk = chunk(1)
            .with_dummy_parquet_file()
            .with_sort_key(SortKey::from_columns([
                Arc::from("tag2"),
                Arc::from("tag1"),
                Arc::from(TIME_COLUMN_NAME),
            ]));
        let schema = chunk.schema().clone();
        let plan = dedup_plan_with_chunk_order_col(schema, vec![chunk]);
        let opt = DedupSortOrder;
        insta::assert_yaml_snapshot!(
            OptimizationTest::new(plan, opt),
            @r###"
        ---
        input:
          - " DeduplicateExec: [tag1@1 ASC,tag2@2 ASC,time@3 ASC]"
          - "   UnionExec"
          - "     ParquetExec: file_groups={1 group: [[1.parquet]]}, projection=[field, tag1, tag2, time, __chunk_order], output_ordering=[tag2@2 ASC, tag1@1 ASC, time@3 ASC, __chunk_order@4 ASC]"
        output:
          Ok:
            - " DeduplicateExec: [tag2@2 ASC,tag1@1 ASC,time@3 ASC]"
            - "   UnionExec"
            - "     ParquetExec: file_groups={1 group: [[1.parquet]]}, projection=[field, tag1, tag2, time, __chunk_order], output_ordering=[tag2@2 ASC, tag1@1 ASC, time@3 ASC, __chunk_order@4 ASC]"
        "###
        );
    }

    #[test]
    fn test_unusual_time_order() {
        let chunk = chunk(1)
            .with_dummy_parquet_file()
            .with_sort_key(SortKey::from_columns([
                Arc::from(TIME_COLUMN_NAME),
                Arc::from("tag1"),
                Arc::from("tag2"),
            ]));
        let schema = chunk.schema().clone();
        let plan = dedup_plan(schema, vec![chunk]);
        let opt = DedupSortOrder;
        insta::assert_yaml_snapshot!(
            OptimizationTest::new(plan, opt),
            @r###"
        ---
        input:
          - " DeduplicateExec: [tag1@1 ASC,tag2@2 ASC,time@3 ASC]"
          - "   UnionExec"
          - "     ParquetExec: file_groups={1 group: [[1.parquet]]}, projection=[field, tag1, tag2, time], output_ordering=[time@3 ASC, tag1@1 ASC, tag2@2 ASC]"
        output:
          Ok:
            - " DeduplicateExec: [time@3 ASC,tag1@1 ASC,tag2@2 ASC]"
            - "   UnionExec"
            - "     ParquetExec: file_groups={1 group: [[1.parquet]]}, projection=[field, tag1, tag2, time], output_ordering=[time@3 ASC, tag1@1 ASC, tag2@2 ASC]"
        "###
        );
    }

    #[test]
    fn test_single_chunk_time_always_included() {
        let chunk = chunk(1)
            .with_tag_column("zzz")
            .with_dummy_parquet_file()
            .with_sort_key(SortKey::from_columns([
                Arc::from("tag2"),
                Arc::from("tag1"),
            ]));
        let schema = chunk.schema().clone();
        let plan = dedup_plan(schema, vec![chunk]);
        let opt = DedupSortOrder;
        insta::assert_yaml_snapshot!(
            OptimizationTest::new(plan, opt),
            @r###"
        ---
        input:
          - " DeduplicateExec: [tag1@1 ASC,tag2@2 ASC,zzz@4 ASC,time@3 ASC]"
          - "   UnionExec"
          - "     ParquetExec: file_groups={1 group: [[1.parquet]]}, projection=[field, tag1, tag2, time, zzz], output_ordering=[tag2@2 ASC, tag1@1 ASC]"
        output:
          Ok:
            - " DeduplicateExec: [tag2@2 ASC,tag1@1 ASC,zzz@4 ASC,time@3 ASC]"
            - "   UnionExec"
            - "     ParquetExec: file_groups={1 group: [[1.parquet]]}, projection=[field, tag1, tag2, time, zzz]"
        "###
        );
    }

    #[test]
    fn test_single_chunk_misses_pk_cols() {
        let chunk = TestChunk::new("table")
            .with_id(1)
            .with_tag_column("tag1")
            .with_dummy_parquet_file()
            .with_sort_key(SortKey::from_columns([Arc::from("tag1")]));
        let schema = SchemaBuilder::new()
            .tag("tag1")
            .tag("tag2")
            .tag("zzz")
            .timestamp()
            .build()
            .unwrap();
        let plan = dedup_plan(schema, vec![chunk]);
        let opt = DedupSortOrder;
        insta::assert_yaml_snapshot!(
            OptimizationTest::new(plan, opt),
            @r###"
        ---
        input:
          - " DeduplicateExec: [tag1@0 ASC,tag2@1 ASC,zzz@2 ASC,time@3 ASC]"
          - "   UnionExec"
          - "     ParquetExec: file_groups={1 group: [[1.parquet]]}, projection=[tag1, tag2, zzz, time], output_ordering=[tag1@0 ASC]"
        output:
          Ok:
            - " DeduplicateExec: [tag1@0 ASC,tag2@1 ASC,zzz@2 ASC,time@3 ASC]"
            - "   UnionExec"
            - "     ParquetExec: file_groups={1 group: [[1.parquet]]}, projection=[tag1, tag2, zzz, time], output_ordering=[tag1@0 ASC, tag2@1 ASC, zzz@2 ASC, time@3 ASC]"
        "###
        );
    }

    #[test]
    fn test_two_chunks_break_even_by_col_name() {
        let chunk1 = chunk(1)
            .with_dummy_parquet_file()
            .with_sort_key(SortKey::from_columns([
                Arc::from("tag1"),
                Arc::from("tag2"),
                Arc::from(TIME_COLUMN_NAME),
            ]));
        let chunk2 = chunk(2)
            .with_dummy_parquet_file()
            .with_sort_key(SortKey::from_columns([
                Arc::from("tag2"),
                Arc::from("tag1"),
                Arc::from(TIME_COLUMN_NAME),
            ]));
        let schema = chunk1.schema().clone();
        let plan = dedup_plan(schema, vec![chunk1, chunk2]);
        let opt = DedupSortOrder;
        insta::assert_yaml_snapshot!(
            OptimizationTest::new(plan, opt),
            @r###"
        ---
        input:
          - " DeduplicateExec: [tag1@1 ASC,tag2@2 ASC,time@3 ASC]"
          - "   UnionExec"
          - "     ParquetExec: file_groups={2 groups: [[1.parquet], [2.parquet]]}, projection=[field, tag1, tag2, time]"
        output:
          Ok:
            - " DeduplicateExec: [tag1@1 ASC,tag2@2 ASC,time@3 ASC]"
            - "   UnionExec"
            - "     ParquetExec: file_groups={2 groups: [[1.parquet], [2.parquet]]}, projection=[field, tag1, tag2, time]"
        "###
        );
    }

    #[test]
    fn test_two_chunks_sorted_ranks_higher_than_not_sorted() {
        let chunk1 = chunk(1)
            .with_dummy_parquet_file()
            .with_sort_key(SortKey::from_columns([
                Arc::from("tag2"),
                Arc::from(TIME_COLUMN_NAME),
            ]));
        let chunk2 = chunk(2)
            .with_dummy_parquet_file()
            .with_sort_key(SortKey::from_columns([
                Arc::from("tag2"),
                Arc::from("tag1"),
                Arc::from(TIME_COLUMN_NAME),
            ]));
        let schema = chunk1.schema().clone();
        let plan = dedup_plan(schema, vec![chunk1, chunk2]);
        let opt = DedupSortOrder;
        insta::assert_yaml_snapshot!(
            OptimizationTest::new(plan, opt),
            @r###"
        ---
        input:
          - " DeduplicateExec: [tag1@1 ASC,tag2@2 ASC,time@3 ASC]"
          - "   UnionExec"
          - "     ParquetExec: file_groups={2 groups: [[1.parquet], [2.parquet]]}, projection=[field, tag1, tag2, time], output_ordering=[tag2@2 ASC, tag1@1 ASC, time@3 ASC]"
        output:
          Ok:
            - " DeduplicateExec: [tag2@2 ASC,tag1@1 ASC,time@3 ASC]"
            - "   UnionExec"
            - "     ParquetExec: file_groups={2 groups: [[1.parquet], [2.parquet]]}, projection=[field, tag1, tag2, time]"
        "###
        );
    }

    #[test]
    fn test_two_chunks_one_without_sort_key() {
        let chunk1 = chunk(1)
            .with_dummy_parquet_file()
            .with_sort_key(SortKey::from_columns([
                Arc::from("tag2"),
                Arc::from("tag1"),
                Arc::from(TIME_COLUMN_NAME),
            ]));
        let chunk2 = chunk(2).with_dummy_parquet_file();
        let schema = chunk1.schema().clone();
        let plan = dedup_plan(schema, vec![chunk1, chunk2]);
        let opt = DedupSortOrder;
        insta::assert_yaml_snapshot!(
            OptimizationTest::new(plan, opt),
            @r###"
        ---
        input:
          - " DeduplicateExec: [tag1@1 ASC,tag2@2 ASC,time@3 ASC]"
          - "   UnionExec"
          - "     ParquetExec: file_groups={2 groups: [[1.parquet], [2.parquet]]}, projection=[field, tag1, tag2, time]"
        output:
          Ok:
            - " DeduplicateExec: [tag2@2 ASC,tag1@1 ASC,time@3 ASC]"
            - "   UnionExec"
            - "     ParquetExec: file_groups={2 groups: [[1.parquet], [2.parquet]]}, projection=[field, tag1, tag2, time]"
        "###
        );
    }

    #[test]
    fn test_three_chunks_different_subsets() {
        let chunk1 = TestChunk::new("table")
            .with_id(1)
            .with_tag_column("tag1")
            .with_tag_column("tag2")
            .with_time_column()
            .with_dummy_parquet_file()
            .with_sort_key(SortKey::from_columns([
                Arc::from("tag2"),
                Arc::from("tag1"),
                Arc::from(TIME_COLUMN_NAME),
            ]));
        let chunk2 = TestChunk::new("table")
            .with_id(2)
            .with_tag_column("tag1")
            .with_tag_column("tag3")
            .with_time_column()
            .with_dummy_parquet_file()
            .with_sort_key(SortKey::from_columns([
                Arc::from("tag3"),
                Arc::from("tag1"),
                Arc::from(TIME_COLUMN_NAME),
            ]));
        let chunk3 = TestChunk::new("table")
            .with_id(3)
            .with_tag_column("tag1")
            .with_tag_column("tag2")
            .with_tag_column("tag3")
            .with_time_column()
            .with_dummy_parquet_file()
            .with_sort_key(SortKey::from_columns([
                Arc::from("tag2"),
                Arc::from("tag3"),
                Arc::from("tag1"),
                Arc::from(TIME_COLUMN_NAME),
            ]));
        let schema = chunk3.schema().clone();
        let plan = dedup_plan(schema, vec![chunk1, chunk2, chunk3]);
        let opt = DedupSortOrder;
        insta::assert_yaml_snapshot!(
            OptimizationTest::new(plan, opt),
            @r###"
        ---
        input:
          - " DeduplicateExec: [tag1@0 ASC,tag2@1 ASC,tag3@2 ASC,time@3 ASC]"
          - "   UnionExec"
          - "     ParquetExec: file_groups={2 groups: [[1.parquet, 3.parquet], [2.parquet]]}, projection=[tag1, tag2, tag3, time]"
        output:
          Ok:
            - " DeduplicateExec: [tag2@1 ASC,tag3@2 ASC,tag1@0 ASC,time@3 ASC]"
            - "   UnionExec"
            - "     ParquetExec: file_groups={3 groups: [[1.parquet], [3.parquet], [2.parquet]]}, projection=[tag1, tag2, tag3, time], output_ordering=[tag2@1 ASC, tag3@2 ASC, tag1@0 ASC, time@3 ASC]"
        "###
        );
    }

    #[test]
    fn test_three_chunks_single_chunk_has_extra_col1() {
        let chunk1 = TestChunk::new("table")
            .with_id(1)
            .with_tag_column("tag1")
            .with_time_column()
            .with_dummy_parquet_file()
            .with_sort_key(SortKey::from_columns([
                Arc::from("tag1"),
                Arc::from(TIME_COLUMN_NAME),
            ]));
        let chunk2 = TestChunk::new("table")
            .with_id(2)
            .with_tag_column("tag1")
            .with_time_column()
            .with_dummy_parquet_file()
            .with_sort_key(SortKey::from_columns([
                Arc::from("tag1"),
                Arc::from(TIME_COLUMN_NAME),
            ]));
        let chunk3 = TestChunk::new("table")
            .with_id(3)
            .with_tag_column("tag1")
            .with_tag_column("tag2")
            .with_time_column()
            .with_dummy_parquet_file()
            .with_sort_key(SortKey::from_columns([
                Arc::from("tag2"),
                Arc::from("tag1"),
                Arc::from(TIME_COLUMN_NAME),
            ]));
        let schema = chunk3.schema().clone();
        let plan = dedup_plan(schema, vec![chunk1, chunk2, chunk3]);
        let opt = DedupSortOrder;
        insta::assert_yaml_snapshot!(
            OptimizationTest::new(plan, opt),
            @r###"
        ---
        input:
          - " DeduplicateExec: [tag1@0 ASC,tag2@1 ASC,time@2 ASC]"
          - "   UnionExec"
          - "     ParquetExec: file_groups={2 groups: [[1.parquet, 3.parquet], [2.parquet]]}, projection=[tag1, tag2, time], output_ordering=[tag2@1 ASC, tag1@0 ASC, time@2 ASC]"
        output:
          Ok:
            - " DeduplicateExec: [tag2@1 ASC,tag1@0 ASC,time@2 ASC]"
            - "   UnionExec"
            - "     ParquetExec: file_groups={3 groups: [[1.parquet], [3.parquet], [2.parquet]]}, projection=[tag1, tag2, time], output_ordering=[tag2@1 ASC, tag1@0 ASC, time@2 ASC]"
        "###
        );
    }

    #[test]
    fn test_three_chunks_single_chunk_has_extra_col2() {
        let chunk1 = TestChunk::new("table")
            .with_id(1)
            .with_tag_column("tag1")
            .with_tag_column("tag2")
            .with_time_column()
            .with_dummy_parquet_file()
            .with_sort_key(SortKey::from_columns([
                Arc::from("tag1"),
                Arc::from(TIME_COLUMN_NAME),
            ]));
        let chunk2 = TestChunk::new("table")
            .with_id(2)
            .with_tag_column("tag1")
            .with_tag_column("tag2")
            .with_time_column()
            .with_dummy_parquet_file()
            .with_sort_key(SortKey::from_columns([
                Arc::from("tag1"),
                Arc::from(TIME_COLUMN_NAME),
            ]));
        let chunk3 = TestChunk::new("table")
            .with_id(3)
            .with_tag_column("tag1")
            .with_tag_column("tag2")
            .with_time_column()
            .with_dummy_parquet_file()
            .with_sort_key(SortKey::from_columns([
                Arc::from("tag2"),
                Arc::from("tag1"),
                Arc::from(TIME_COLUMN_NAME),
            ]));
        let schema = chunk3.schema().clone();
        let plan = dedup_plan(schema, vec![chunk1, chunk2, chunk3]);
        let opt = DedupSortOrder;
        insta::assert_yaml_snapshot!(
            OptimizationTest::new(plan, opt),
            @r###"
        ---
        input:
          - " DeduplicateExec: [tag1@0 ASC,tag2@1 ASC,time@2 ASC]"
          - "   UnionExec"
          - "     ParquetExec: file_groups={2 groups: [[1.parquet, 3.parquet], [2.parquet]]}, projection=[tag1, tag2, time], output_ordering=[tag2@1 ASC, tag1@0 ASC, time@2 ASC]"
        output:
          Ok:
            - " DeduplicateExec: [tag2@1 ASC,tag1@0 ASC,time@2 ASC]"
            - "   UnionExec"
            - "     ParquetExec: file_groups={3 groups: [[1.parquet], [3.parquet], [2.parquet]]}, projection=[tag1, tag2, time]"
        "###
        );
    }
}
