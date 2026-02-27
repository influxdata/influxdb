use std::{cmp::Reverse, sync::Arc};

use arrow::compute::SortOptions;
use datafusion::{
    common::{
        plan_datafusion_err,
        tree_node::{Transformed, TreeNode},
    },
    config::ConfigOptions,
    error::Result,
    physical_optimizer::PhysicalOptimizerRule,
    physical_plan::ExecutionPlan,
};
use indexmap::IndexSet;
use schema::sort::SortKeyBuilder;

use crate::{
    CHUNK_ORDER_COLUMN_NAME,
    physical_optimizer::chunk_extraction::extract_chunks,
    provider::{DeduplicateExec, chunks_to_physical_nodes},
    util::arrow_sort_key_exprs,
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
/// The order of the sort key passed to [`DeduplicateExec`] does not affect correctness but will be used as a tie-breaker
/// whenever the vote-based mechanism does not agree on a single order.
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
        plan.transform_up(|plan| {
            let plan_any = plan.as_any();

            if let Some(dedup_exec) = plan_any.downcast_ref::<DeduplicateExec>() {
                let mut children = dedup_exec.children();
                assert_eq!(children.len(), 1);
                let child = children.remove(0);
                let Some((schema, chunks, _output_sort_key)) = extract_chunks(child.as_ref())
                else {
                    return Ok(Transformed::no(plan));
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
                let mut todo_pk_columns = dedup_exec.sort_columns_ordered();
                todo_pk_columns.remove(CHUNK_ORDER_COLUMN_NAME);
                while !todo_pk_columns.is_empty() {
                    let mut candidate_counts = todo_pk_columns
                        .iter()
                        .map(|(col, existing_ordinal)| {
                            let count = chunk_sort_keys
                                .iter()
                                .filter(|sort_key| {
                                    match sort_key.get_index_of(*col) {
                                        Some(0) => {
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
                            (*col, count, *existing_ordinal)
                        })
                        .collect::<Vec<_>>();
                    candidate_counts.sort_by_key(|(_col, count, existing_ordinal)| {
                        (Reverse(*count), *existing_ordinal)
                    });
                    let (next_key, _count, _priority_inverse) =
                        candidate_counts.first().expect("all TODO cols inserted");

                    for chunk_sort_key in &mut chunk_sort_keys {
                        chunk_sort_key.shift_remove_full(next_key);
                    }

                    let was_present = todo_pk_columns.remove(next_key).is_some();
                    assert!(was_present);

                    quorum_sort_key_builder = quorum_sort_key_builder.with_col(*next_key);
                }

                let quorum_sort_key = quorum_sort_key_builder.build();
                let child = chunks_to_physical_nodes(
                    &schema,
                    (!quorum_sort_key.is_empty()).then_some(&quorum_sort_key),
                    chunks,
                    config.execution.target_partitions,
                );

                let sort_exprs = arrow_sort_key_exprs(&quorum_sort_key, &schema)
                    .ok_or_else(|| plan_datafusion_err!("de-dup sort key empty"))?;
                return Ok(Transformed::yes(Arc::new(DeduplicateExec::new(
                    child,
                    sort_exprs,
                    dedup_exec.use_chunk_order_col(),
                ))));
            }

            Ok(Transformed::no(plan))
        })
        .map(|t| t.data)
    }

    fn name(&self) -> &str {
        "dedup_sort_order"
    }

    fn schema_check(&self) -> bool {
        true
    }
}

#[cfg(test)]
mod tests {
    use schema::{SchemaBuilder, TIME_COLUMN_NAME, sort::SortKey};
    use test_helpers::maybe_start_logging;

    use crate::{
        QueryChunk,
        physical_optimizer::{
            dedup::test_util::{chunk, dedup_plan, dedup_plan_with_chunk_order_col},
            test_util::OptimizationTest,
        },
        test::TestChunk,
    };

    use super::*;

    #[test]
    fn test_no_chunks() {
        let schema = chunk(1).schema().clone();
        let plan = dedup_plan(schema, vec![]);
        let opt = DedupSortOrder;
        insta::assert_yaml_snapshot!(
            OptimizationTest::new(plan, opt),
            @r#"
        input:
          - " DeduplicateExec: [tag1@1 ASC,tag2@2 ASC,time@3 ASC]"
          - "   EmptyExec"
        output:
          Ok:
            - " DeduplicateExec: [tag1@1 ASC,tag2@2 ASC,time@3 ASC]"
            - "   EmptyExec"
        "#
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
            @r#"
        input:
          - " DeduplicateExec: [tag1@1 ASC,tag2@2 ASC,time@3 ASC]"
          - "   DataSourceExec: file_groups={1 group: [[1.parquet]]}, projection=[field, tag1, tag2, time], file_type=parquet"
        output:
          Ok:
            - " DeduplicateExec: [tag1@1 ASC,tag2@2 ASC,time@3 ASC]"
            - "   DataSourceExec: file_groups={1 group: [[1.parquet]]}, projection=[field, tag1, tag2, time], file_type=parquet"
        "#
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
            @r#"
        input:
          - " DeduplicateExec: [tag1@1 ASC,tag2@2 ASC,time@3 ASC]"
          - "   DataSourceExec: file_groups={1 group: [[1.parquet]]}, projection=[field, tag1, tag2, time], output_ordering=[tag2@2 ASC, tag1@1 ASC, time@3 ASC], file_type=parquet"
        output:
          Ok:
            - " DeduplicateExec: [tag2@2 ASC,tag1@1 ASC,time@3 ASC]"
            - "   DataSourceExec: file_groups={1 group: [[1.parquet]]}, projection=[field, tag1, tag2, time], output_ordering=[tag2@2 ASC, tag1@1 ASC, time@3 ASC], file_type=parquet"
        "#
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
            @r#"
        input:
          - " DeduplicateExec: [tag1@1 ASC,tag2@2 ASC,time@3 ASC]"
          - "   DataSourceExec: file_groups={1 group: [[1.parquet]]}, projection=[field, tag1, tag2, time, __chunk_order], output_ordering=[tag2@2 ASC, tag1@1 ASC, time@3 ASC, __chunk_order@4 ASC], file_type=parquet"
        output:
          Ok:
            - " DeduplicateExec: [tag2@2 ASC,tag1@1 ASC,time@3 ASC]"
            - "   DataSourceExec: file_groups={1 group: [[1.parquet]]}, projection=[field, tag1, tag2, time, __chunk_order], output_ordering=[tag2@2 ASC, tag1@1 ASC, time@3 ASC, __chunk_order@4 ASC], file_type=parquet"
        "#
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
            @r#"
        input:
          - " DeduplicateExec: [tag1@1 ASC,tag2@2 ASC,time@3 ASC]"
          - "   DataSourceExec: file_groups={1 group: [[1.parquet]]}, projection=[field, tag1, tag2, time], output_ordering=[time@3 ASC, tag1@1 ASC, tag2@2 ASC], file_type=parquet"
        output:
          Ok:
            - " DeduplicateExec: [time@3 ASC,tag1@1 ASC,tag2@2 ASC]"
            - "   DataSourceExec: file_groups={1 group: [[1.parquet]]}, projection=[field, tag1, tag2, time], output_ordering=[time@3 ASC, tag1@1 ASC, tag2@2 ASC], file_type=parquet"
        "#
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
            @r#"
        input:
          - " DeduplicateExec: [tag1@1 ASC,tag2@2 ASC,zzz@4 ASC,time@3 ASC]"
          - "   DataSourceExec: file_groups={1 group: [[1.parquet]]}, projection=[field, tag1, tag2, time, zzz], output_ordering=[tag2@2 ASC, tag1@1 ASC], file_type=parquet"
        output:
          Ok:
            - " DeduplicateExec: [tag2@2 ASC,tag1@1 ASC,zzz@4 ASC,time@3 ASC]"
            - "   DataSourceExec: file_groups={1 group: [[1.parquet]]}, projection=[field, tag1, tag2, time, zzz], file_type=parquet"
        "#
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
            @r#"
        input:
          - " DeduplicateExec: [tag1@0 ASC,tag2@1 ASC,zzz@2 ASC,time@3 ASC]"
          - "   DataSourceExec: file_groups={1 group: [[1.parquet]]}, projection=[tag1, tag2, zzz, time], output_ordering=[tag1@0 ASC], file_type=parquet"
        output:
          Ok:
            - " DeduplicateExec: [tag1@0 ASC,tag2@1 ASC,zzz@2 ASC,time@3 ASC]"
            - "   DataSourceExec: file_groups={1 group: [[1.parquet]]}, projection=[tag1, tag2, zzz, time], output_ordering=[tag1@0 ASC, tag2@1 ASC, zzz@2 ASC, time@3 ASC], file_type=parquet"
        "#
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
            @r#"
        input:
          - " DeduplicateExec: [tag1@1 ASC,tag2@2 ASC,time@3 ASC]"
          - "   DataSourceExec: file_groups={2 groups: [[1.parquet], [2.parquet]]}, projection=[field, tag1, tag2, time], file_type=parquet"
        output:
          Ok:
            - " DeduplicateExec: [tag1@1 ASC,tag2@2 ASC,time@3 ASC]"
            - "   DataSourceExec: file_groups={2 groups: [[1.parquet], [2.parquet]]}, projection=[field, tag1, tag2, time], file_type=parquet"
        "#
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
            @r#"
        input:
          - " DeduplicateExec: [tag1@1 ASC,tag2@2 ASC,time@3 ASC]"
          - "   DataSourceExec: file_groups={2 groups: [[1.parquet], [2.parquet]]}, projection=[field, tag1, tag2, time], output_ordering=[tag2@2 ASC, tag1@1 ASC, time@3 ASC], file_type=parquet"
        output:
          Ok:
            - " DeduplicateExec: [tag2@2 ASC,tag1@1 ASC,time@3 ASC]"
            - "   DataSourceExec: file_groups={2 groups: [[1.parquet], [2.parquet]]}, projection=[field, tag1, tag2, time], file_type=parquet"
        "#
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
            @r#"
        input:
          - " DeduplicateExec: [tag1@1 ASC,tag2@2 ASC,time@3 ASC]"
          - "   DataSourceExec: file_groups={2 groups: [[1.parquet], [2.parquet]]}, projection=[field, tag1, tag2, time], file_type=parquet"
        output:
          Ok:
            - " DeduplicateExec: [tag2@2 ASC,tag1@1 ASC,time@3 ASC]"
            - "   DataSourceExec: file_groups={2 groups: [[1.parquet], [2.parquet]]}, projection=[field, tag1, tag2, time], file_type=parquet"
        "#
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
            @r#"
        input:
          - " DeduplicateExec: [tag1@0 ASC,tag2@1 ASC,tag3@2 ASC,time@3 ASC]"
          - "   DataSourceExec: file_groups={2 groups: [[1.parquet, 3.parquet], [2.parquet]]}, projection=[tag1, tag2, tag3, time], file_type=parquet"
        output:
          Ok:
            - " DeduplicateExec: [tag2@1 ASC,tag3@2 ASC,tag1@0 ASC,time@3 ASC]"
            - "   DataSourceExec: file_groups={3 groups: [[1.parquet], [3.parquet], [2.parquet]]}, projection=[tag1, tag2, tag3, time], output_ordering=[tag2@1 ASC, tag3@2 ASC, tag1@0 ASC, time@3 ASC], file_type=parquet"
        "#
        );
    }

    #[test]
    fn test_three_chunks_single_chunk_has_extra_col1() {
        maybe_start_logging();

        let chunk1 = TestChunk::new("table")
            .with_id(1)
            .with_tag_column_with_stats("tag1", Some("a"), Some("a"))
            .with_time_column_with_stats(Some(1), Some(1))
            .with_dummy_parquet_file()
            .with_sort_key(SortKey::from_columns([
                Arc::from("tag1"),
                Arc::from(TIME_COLUMN_NAME),
            ]));
        let chunk2 = TestChunk::new("table")
            .with_id(2)
            .with_tag_column_with_stats("tag1", Some("b"), Some("b"))
            .with_time_column_with_stats(Some(2), Some(2))
            .with_dummy_parquet_file()
            .with_sort_key(SortKey::from_columns([
                Arc::from("tag1"),
                Arc::from(TIME_COLUMN_NAME),
            ]));
        let chunk3 = TestChunk::new("table")
            .with_id(3)
            .with_tag_column_with_stats("tag1", Some("c"), Some("c"))
            .with_tag_column_with_stats("tag2", Some("c"), Some("c"))
            .with_time_column_with_stats(Some(3), Some(3))
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
            @r#"
        input:
          - " DeduplicateExec: [tag1@0 ASC,tag2@1 ASC,time@2 ASC]"
          - "   DataSourceExec: file_groups={2 groups: [[1.parquet, 3.parquet], [2.parquet]]}, projection=[tag1, tag2, time], output_ordering=[tag2@1 ASC, tag1@0 ASC, time@2 ASC], file_type=parquet"
        output:
          Ok:
            - " DeduplicateExec: [tag2@1 ASC,tag1@0 ASC,time@2 ASC]"
            - "   DataSourceExec: file_groups={3 groups: [[1.parquet], [3.parquet], [2.parquet]]}, projection=[tag1, tag2, time], output_ordering=[tag2@1 ASC, tag1@0 ASC, time@2 ASC], file_type=parquet"
        "#
        );
    }

    #[test]
    fn test_three_chunks_single_chunk_has_extra_col2() {
        let chunk1 = TestChunk::new("table")
            .with_id(1)
            .with_tag_column_with_stats("tag1", Some("a1"), Some("a1"))
            .with_tag_column_with_stats("tag2", Some("a2"), Some("a2"))
            .with_time_column_with_stats(Some(1), Some(1))
            .with_dummy_parquet_file()
            .with_sort_key(SortKey::from_columns([
                Arc::from("tag1"),
                Arc::from(TIME_COLUMN_NAME),
            ]));
        let chunk2 = TestChunk::new("table")
            .with_id(2)
            .with_tag_column_with_stats("tag1", Some("b1"), Some("b1"))
            .with_tag_column_with_stats("tag2", Some("b2"), Some("b2"))
            .with_time_column_with_stats(Some(2), Some(2))
            .with_dummy_parquet_file()
            .with_sort_key(SortKey::from_columns([
                Arc::from("tag1"),
                Arc::from(TIME_COLUMN_NAME),
            ]));
        let chunk3 = TestChunk::new("table")
            .with_id(3)
            .with_tag_column_with_stats("tag1", Some("c1"), Some("c1"))
            .with_tag_column_with_stats("tag2", Some("c2"), Some("c2"))
            .with_time_column_with_stats(Some(3), Some(3))
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
            @r#"
        input:
          - " DeduplicateExec: [tag1@0 ASC,tag2@1 ASC,time@2 ASC]"
          - "   DataSourceExec: file_groups={2 groups: [[1.parquet, 3.parquet], [2.parquet]]}, projection=[tag1, tag2, time], output_ordering=[tag2@1 ASC, tag1@0 ASC, time@2 ASC], file_type=parquet"
        output:
          Ok:
            - " DeduplicateExec: [tag2@1 ASC,tag1@0 ASC,time@2 ASC]"
            - "   DataSourceExec: file_groups={3 groups: [[1.parquet], [3.parquet], [2.parquet]]}, projection=[tag1, tag2, time], file_type=parquet"
        "#
        );
    }

    #[test]
    fn test_tie_breaker() {
        let chunk1 = TestChunk::new("table")
            .with_id(1)
            .with_tag_column("tag1")
            .with_tag_column("tag2")
            .with_time_column()
            .with_dummy_parquet_file()
            .with_sort_key(SortKey::from_columns([
                Arc::from("tag1"),
                Arc::from("tag2"),
                Arc::from(TIME_COLUMN_NAME),
            ]));
        let chunk2 = TestChunk::new("table")
            .with_id(2)
            .with_tag_column("tag1")
            .with_tag_column("tag3")
            .with_time_column()
            .with_dummy_parquet_file()
            .with_sort_key(SortKey::from_columns([
                Arc::from("tag1"),
                Arc::from("tag3"),
                Arc::from(TIME_COLUMN_NAME),
            ]));
        let schema = SchemaBuilder::new()
            .tag("tag1")
            .tag("tag3")
            .tag("tag2")
            .timestamp()
            .build()
            .unwrap();
        let plan = chunks_to_physical_nodes(
            &schema.as_arrow(),
            None,
            vec![Arc::new(chunk1), Arc::new(chunk2)],
            2,
        );

        let sort_key = schema::sort::SortKey::from_columns(["tag1", "tag3", "tag2", "time"]);
        let sort_exprs = arrow_sort_key_exprs(&sort_key, &plan.schema()).unwrap();
        let plan = Arc::new(DeduplicateExec::new(plan, sort_exprs, false));
        let opt = DedupSortOrder;
        insta::assert_yaml_snapshot!(
            OptimizationTest::new(plan, opt),
            @r#"
        input:
          - " DeduplicateExec: [tag1@0 ASC,tag3@1 ASC,tag2@2 ASC,time@3 ASC]"
          - "   DataSourceExec: file_groups={2 groups: [[1.parquet], [2.parquet]]}, projection=[tag1, tag3, tag2, time], file_type=parquet"
        output:
          Ok:
            - " DeduplicateExec: [tag1@0 ASC,tag3@1 ASC,tag2@2 ASC,time@3 ASC]"
            - "   DataSourceExec: file_groups={2 groups: [[1.parquet], [2.parquet]]}, projection=[tag1, tag3, tag2, time], output_ordering=[tag1@0 ASC, tag3@1 ASC, tag2@2 ASC, time@3 ASC], file_type=parquet"
        "#
        );
    }
}
