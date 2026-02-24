use crate::{
    QueryChunk,
    config::IoxConfigExt,
    physical_optimizer::chunk_extraction::extract_chunks,
    provider::{
        DeduplicateExec, chunks_to_physical_nodes, group_potential_duplicates,
        overlap::timestamp_min_max,
    },
    util::union_multiple_children,
};
use datafusion::{
    common::tree_node::{Transformed, TreeNode},
    config::ConfigOptions,
    error::Result,
    physical_optimizer::PhysicalOptimizerRule,
    physical_plan::{ExecutionPlan, empty::EmptyExec},
};
use hashbrown::HashMap;
use std::sync::Arc;
use tracing::warn;

#[derive(Debug, Default)]
pub struct SplitDedup;

impl PhysicalOptimizerRule for SplitDedup {
    fn optimize(
        &self,
        plan: Arc<dyn ExecutionPlan>,
        config: &ConfigOptions,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        plan.transform_up(|plan| {
            let Some(dedup_exec) = plan.as_any().downcast_ref::<DeduplicateExec>() else {
                return Ok(Transformed::no(plan));
            };

            let mut children = dedup_exec.children();
            assert_eq!(children.len(), 1);
            let child = children.remove(0);
            let Some((schema, chunks, output_sort_key)) = extract_chunks(child.as_ref()) else {
                return Ok(Transformed::no(plan));
            };

            let groups = split_by_partition(chunks);
            // split by time
            let groups = groups
                .into_iter()
                .flat_map(group_potential_duplicates)
                .collect::<Vec<_>>();
            let groups = detect_dedup_requirement(groups);
            let groups = sort_groups(groups);
            let groups = fuse_groups(groups);

            // Protect against degenerative plans
            let max_dedup_split = config
                .extensions
                .get::<IoxConfigExt>()
                .cloned()
                .unwrap_or_default()
                .max_dedup_split;
            let n_split = groups.len();
            if n_split == 0 {
                return Ok(Transformed::yes(Arc::new(EmptyExec::new(schema))));
            }
            if n_split > max_dedup_split {
                warn!(
                    n_split,
                    max_dedup_split, "cannot split dedup operation, fanout too wide"
                );
                return Ok(Transformed::no(plan));
            }

            let out = union_multiple_children(
                groups
                    .into_iter()
                    .map(|GroupWithDedupFlag { group, needs_dedup }| {
                        let plan = chunks_to_physical_nodes(
                            &schema,
                            output_sort_key.as_ref(),
                            group,
                            config.execution.target_partitions,
                        );

                        if needs_dedup {
                            Arc::new(DeduplicateExec::new(
                                plan,
                                dedup_exec.sort_keys().clone(),
                                dedup_exec.use_chunk_order_col(),
                            )) as _
                        } else {
                            plan
                        }
                    })
                    .collect(),
            )?;

            Ok(Transformed::yes(out))
        })
        .map(|t| t.data)
    }

    fn name(&self) -> &str {
        "split_dedup"
    }

    fn schema_check(&self) -> bool {
        true
    }
}

/// A group of [`QueryChunk`]s.
type Group = Vec<Arc<dyn QueryChunk>>;

/// A [`Group`] with a flag that indicates if de-duplication is required.
struct GroupWithDedupFlag {
    group: Group,
    needs_dedup: bool,
}

/// Split group into sub-groups based on partition.
///
/// That works because we know that -- by design -- partitions have disjunct primary key spaces.
fn split_by_partition(chunks: Group) -> Vec<Group> {
    let mut chunks_by_partition: HashMap<_, Group> = Default::default();
    for chunk in chunks {
        chunks_by_partition
            .entry(chunk.partition_id().clone())
            .or_default()
            .push(chunk);
    }

    // ensure deterministic order
    let mut chunks_by_partition = chunks_by_partition.into_iter().collect::<Vec<_>>();
    chunks_by_partition.sort_by(|a, b| a.0.cmp(&b.0));

    chunks_by_partition
        .into_iter()
        .map(|(_p_id, chunks)| chunks)
        .collect()
}

/// Add flag to group to detect if they need de-duplication.
fn detect_dedup_requirement(groups: Vec<Group>) -> Vec<GroupWithDedupFlag> {
    groups
        .into_iter()
        .map(|group| {
            let no_dedup =
                (group.len() < 2) && group.iter().all(|c| !c.may_contain_pk_duplicates());
            GroupWithDedupFlag {
                group,
                needs_dedup: !no_dedup,
            }
        })
        .collect()
}

/// Sort groups in some order that we think is nice.
fn sort_groups(mut groups: Vec<GroupWithDedupFlag>) -> Vec<GroupWithDedupFlag> {
    groups.sort_by_cached_key(|g| {
        // order by time
        let min_time = g
            .group
            .iter()
            .filter_map(|c| timestamp_min_max(c.as_ref()).map(|min_max| min_max.min))
            .min();

        // order by chunk order
        let min_order = g.group.iter().map(|c| c.order()).min();

        // use partition ID as first tie-breaker
        let min_partition_id = g.group.iter().map(|c| c.partition_id().clone()).min();

        // use chunk ID as second tie-breaker
        let min_chunk_id = g.group.iter().map(|c| c.id()).min();

        (min_time, min_order, min_partition_id, min_chunk_id)
    });

    groups
}

/// Fuse neighboring groups if they don't require de-duplication.
fn fuse_groups(groups: Vec<GroupWithDedupFlag>) -> Vec<GroupWithDedupFlag> {
    let mut out = Vec::new();
    let mut current_group: Option<GroupWithDedupFlag> = None;
    for mut group in groups {
        if let Some(mut current_group_taken) = current_group.take() {
            if current_group_taken.needs_dedup || group.needs_dedup {
                out.push(current_group_taken);
                current_group = Some(group);
            } else {
                current_group_taken.group.append(&mut group.group);
                current_group = Some(current_group_taken);
            }
        } else {
            current_group = Some(group);
        }
    }
    if let Some(current_group) = current_group {
        out.push(current_group);
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        physical_optimizer::{
            dedup::test_util::{chunk, dedup_plan},
            test_util::OptimizationTest,
        },
        test::TestChunk,
        util::arrow_sort_key_exprs,
    };
    use datafusion::{
        physical_plan::{expressions::Literal, filter::FilterExec},
        scalar::ScalarValue,
    };

    #[test]
    fn test_no_chunks() {
        let schema = chunk(1).schema().clone();
        let plan = dedup_plan(schema, vec![]);
        insta::assert_yaml_snapshot!(
            OptimizationTest::new(plan, SplitDedup),
            @r#"
        input:
          - " DeduplicateExec: [tag1@1 ASC,tag2@2 ASC,time@3 ASC]"
          - "   EmptyExec"
        output:
          Ok:
            - " EmptyExec"
        "#
        );
    }

    #[test]
    fn test_no_valid_arms() {
        let chunk1 = TestChunk::new("table")
            .with_tag_column("tag")
            .with_time_column()
            .with_id(1);
        let schema = chunk1.schema();
        let sort_key = schema::sort::SortKey::from_columns(schema.primary_key());
        let sort_exprs = arrow_sort_key_exprs(&sort_key, &schema.as_arrow()).unwrap();
        let plan = Arc::new(DeduplicateExec::new(
            Arc::new(
                FilterExec::try_new(
                    Arc::new(Literal::new(ScalarValue::from(false))),
                    chunks_to_physical_nodes(&schema.as_arrow(), None, vec![Arc::new(chunk1)], 2),
                )
                .unwrap(),
            ),
            sort_exprs,
            false,
        ));
        insta::assert_yaml_snapshot!(
            OptimizationTest::new(plan, SplitDedup),
            @r#"
        input:
          - " DeduplicateExec: [tag@0 ASC,time@1 ASC]"
          - "   FilterExec: false"
          - "     RecordBatchesExec: chunks=1, projection=[tag, time]"
        output:
          Ok:
            - " DeduplicateExec: [tag@0 ASC,time@1 ASC]"
            - "   FilterExec: false"
            - "     RecordBatchesExec: chunks=1, projection=[tag, time]"
        "#
        );
    }

    mod partition_split {
        use super::*;

        #[test]
        fn test_same_partition() {
            let chunk1 = chunk(1);
            let chunk2 = chunk(2);
            let chunk3 = chunk(3).with_dummy_parquet_file();
            let schema = chunk1.schema().clone();
            let plan = dedup_plan(schema, vec![chunk1, chunk2, chunk3]);
            insta::assert_yaml_snapshot!(
                OptimizationTest::new(plan, SplitDedup),
                @r#"
            input:
              - " DeduplicateExec: [tag1@1 ASC,tag2@2 ASC,time@3 ASC]"
              - "   UnionExec"
              - "     RecordBatchesExec: chunks=2, projection=[field, tag1, tag2, time]"
              - "     DataSourceExec: file_groups={1 group: [[3.parquet]]}, projection=[field, tag1, tag2, time], file_type=parquet"
            output:
              Ok:
                - " DeduplicateExec: [tag1@1 ASC,tag2@2 ASC,time@3 ASC]"
                - "   UnionExec"
                - "     RecordBatchesExec: chunks=2, projection=[field, tag1, tag2, time]"
                - "     DataSourceExec: file_groups={1 group: [[3.parquet]]}, projection=[field, tag1, tag2, time], file_type=parquet"
            "#
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
            let mut config = ConfigOptions::default();
            config.execution.target_partitions = 2;
            insta::assert_yaml_snapshot!(
                OptimizationTest::new_with_config(plan, SplitDedup, &config),
                @r#"
            input:
              - " DeduplicateExec: [tag1@1 ASC,tag2@2 ASC,time@3 ASC]"
              - "   UnionExec"
              - "     RecordBatchesExec: chunks=2, projection=[field, tag1, tag2, time]"
              - "     DataSourceExec: file_groups={2 groups: [[3.parquet, 5.parquet], [4.parquet, 6.parquet]]}, projection=[field, tag1, tag2, time], file_type=parquet"
            output:
              Ok:
                - " UnionExec"
                - "   DeduplicateExec: [tag1@1 ASC,tag2@2 ASC,time@3 ASC]"
                - "     UnionExec"
                - "       RecordBatchesExec: chunks=1, projection=[field, tag1, tag2, time]"
                - "       DataSourceExec: file_groups={2 groups: [[3.parquet, 6.parquet], [5.parquet]]}, projection=[field, tag1, tag2, time], file_type=parquet"
                - "   DeduplicateExec: [tag1@1 ASC,tag2@2 ASC,time@3 ASC]"
                - "     UnionExec"
                - "       RecordBatchesExec: chunks=1, projection=[field, tag1, tag2, time]"
                - "       DataSourceExec: file_groups={1 group: [[4.parquet]]}, projection=[field, tag1, tag2, time], file_type=parquet"
            "#
            );
        }

        #[test]
        fn test_max_split() {
            let chunk1 = chunk(1)
                .with_partition(1)
                .with_may_contain_pk_duplicates(true);
            let chunk2 = chunk(2)
                .with_partition(2)
                .with_may_contain_pk_duplicates(true);
            let chunk3 = chunk(3)
                .with_partition(3)
                .with_may_contain_pk_duplicates(true);
            let schema = chunk1.schema().clone();
            let plan = dedup_plan(schema, vec![chunk1, chunk2, chunk3]);
            let mut config = ConfigOptions::default();
            config.extensions.insert(IoxConfigExt {
                max_dedup_split: 2,
                ..Default::default()
            });
            insta::assert_yaml_snapshot!(
                OptimizationTest::new_with_config(plan, SplitDedup, &config),
                @r#"
            input:
              - " DeduplicateExec: [tag1@1 ASC,tag2@2 ASC,time@3 ASC]"
              - "   RecordBatchesExec: chunks=3, projection=[field, tag1, tag2, time]"
            output:
              Ok:
                - " DeduplicateExec: [tag1@1 ASC,tag2@2 ASC,time@3 ASC]"
                - "   RecordBatchesExec: chunks=3, projection=[field, tag1, tag2, time]"
            "#
            );
        }
    }

    mod time_split {
        use super::*;

        #[test]
        fn test_all_overlap() {
            let chunk1 = chunk(1).with_timestamp_min_max(5, 10);
            let chunk2 = chunk(2).with_timestamp_min_max(3, 5);
            let chunk3 = chunk(3)
                .with_dummy_parquet_file()
                .with_timestamp_min_max(8, 9);
            let schema = chunk1.schema().clone();
            let plan = dedup_plan(schema, vec![chunk1, chunk2, chunk3]);
            insta::assert_yaml_snapshot!(
                OptimizationTest::new(plan, SplitDedup),
                @r#"
            input:
              - " DeduplicateExec: [tag1@1 ASC,tag2@2 ASC,time@3 ASC]"
              - "   UnionExec"
              - "     RecordBatchesExec: chunks=2, projection=[field, tag1, tag2, time]"
              - "     DataSourceExec: file_groups={1 group: [[3.parquet]]}, projection=[field, tag1, tag2, time], file_type=parquet"
            output:
              Ok:
                - " DeduplicateExec: [tag1@1 ASC,tag2@2 ASC,time@3 ASC]"
                - "   UnionExec"
                - "     RecordBatchesExec: chunks=2, projection=[field, tag1, tag2, time]"
                - "     DataSourceExec: file_groups={1 group: [[3.parquet]]}, projection=[field, tag1, tag2, time], file_type=parquet"
            "#
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
            let mut config = ConfigOptions::default();
            config.execution.target_partitions = 2;
            insta::assert_yaml_snapshot!(
                OptimizationTest::new_with_config(plan, SplitDedup, &config),
                @r#"
            input:
              - " DeduplicateExec: [tag1@1 ASC,tag2@2 ASC,time@3 ASC]"
              - "   UnionExec"
              - "     RecordBatchesExec: chunks=2, projection=[field, tag1, tag2, time]"
              - "     DataSourceExec: file_groups={2 groups: [[3.parquet, 5.parquet], [4.parquet, 6.parquet]]}, projection=[field, tag1, tag2, time], file_type=parquet"
            output:
              Ok:
                - " UnionExec"
                - "   DeduplicateExec: [tag1@1 ASC,tag2@2 ASC,time@3 ASC]"
                - "     UnionExec"
                - "       RecordBatchesExec: chunks=1, projection=[field, tag1, tag2, time]"
                - "       DataSourceExec: file_groups={2 groups: [[6.parquet, 5.parquet], [3.parquet]]}, projection=[field, tag1, tag2, time], file_type=parquet"
                - "   DeduplicateExec: [tag1@1 ASC,tag2@2 ASC,time@3 ASC]"
                - "     UnionExec"
                - "       RecordBatchesExec: chunks=1, projection=[field, tag1, tag2, time]"
                - "       DataSourceExec: file_groups={1 group: [[4.parquet]]}, projection=[field, tag1, tag2, time], file_type=parquet"
            "#
            );
        }

        #[test]
        fn test_max_split() {
            let chunk1 = chunk(1)
                .with_timestamp_min_max(1, 1)
                .with_may_contain_pk_duplicates(true);
            let chunk2 = chunk(2)
                .with_timestamp_min_max(2, 2)
                .with_may_contain_pk_duplicates(true);
            let chunk3 = chunk(3)
                .with_timestamp_min_max(3, 3)
                .with_may_contain_pk_duplicates(true);
            let schema = chunk1.schema().clone();
            let plan = dedup_plan(schema, vec![chunk1, chunk2, chunk3]);
            let mut config = ConfigOptions::default();
            config.extensions.insert(IoxConfigExt {
                max_dedup_split: 2,
                ..Default::default()
            });
            insta::assert_yaml_snapshot!(
                OptimizationTest::new_with_config(plan, SplitDedup, &config),
                @r#"
            input:
              - " DeduplicateExec: [tag1@1 ASC,tag2@2 ASC,time@3 ASC]"
              - "   RecordBatchesExec: chunks=3, projection=[field, tag1, tag2, time]"
            output:
              Ok:
                - " DeduplicateExec: [tag1@1 ASC,tag2@2 ASC,time@3 ASC]"
                - "   RecordBatchesExec: chunks=3, projection=[field, tag1, tag2, time]"
            "#
            );
        }
    }

    mod remove_dedup {
        use super::*;

        #[test]
        fn test_single_chunk_no_pk_dups() {
            let chunk1 = chunk(1).with_may_contain_pk_duplicates(false);
            let schema = chunk1.schema().clone();
            let plan = dedup_plan(schema, vec![chunk1]);
            insta::assert_yaml_snapshot!(
                OptimizationTest::new(plan, SplitDedup),
                @r#"
            input:
              - " DeduplicateExec: [tag1@1 ASC,tag2@2 ASC,time@3 ASC]"
              - "   RecordBatchesExec: chunks=1, projection=[field, tag1, tag2, time]"
            output:
              Ok:
                - " RecordBatchesExec: chunks=1, projection=[field, tag1, tag2, time]"
            "#
            );
        }

        #[test]
        fn test_single_chunk_with_pk_dups() {
            let chunk1 = chunk(1).with_may_contain_pk_duplicates(true);
            let schema = chunk1.schema().clone();
            let plan = dedup_plan(schema, vec![chunk1]);
            insta::assert_yaml_snapshot!(
                OptimizationTest::new(plan, SplitDedup),
                @r#"
            input:
              - " DeduplicateExec: [tag1@1 ASC,tag2@2 ASC,time@3 ASC]"
              - "   RecordBatchesExec: chunks=1, projection=[field, tag1, tag2, time]"
            output:
              Ok:
                - " DeduplicateExec: [tag1@1 ASC,tag2@2 ASC,time@3 ASC]"
                - "   RecordBatchesExec: chunks=1, projection=[field, tag1, tag2, time]"
            "#
            );
        }

        #[test]
        fn test_multiple_chunks() {
            let chunk1 = chunk(1).with_may_contain_pk_duplicates(false);
            let chunk2 = chunk(2).with_may_contain_pk_duplicates(false);
            let schema = chunk1.schema().clone();
            let plan = dedup_plan(schema, vec![chunk1, chunk2]);
            insta::assert_yaml_snapshot!(
                OptimizationTest::new(plan, SplitDedup),
                @r#"
            input:
              - " DeduplicateExec: [tag1@1 ASC,tag2@2 ASC,time@3 ASC]"
              - "   RecordBatchesExec: chunks=2, projection=[field, tag1, tag2, time]"
            output:
              Ok:
                - " DeduplicateExec: [tag1@1 ASC,tag2@2 ASC,time@3 ASC]"
                - "   RecordBatchesExec: chunks=2, projection=[field, tag1, tag2, time]"
            "#
            );
        }
    }

    mod combine {
        use super::*;

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
                .with_time_column_with_stats(Some(5), Some(6))
                .with_may_contain_pk_duplicates(true);
            let chunk4 = TestChunk::new("table")
                .with_id(4)
                .with_dummy_parquet_file()
                .with_time_column_with_stats(Some(7), Some(8));
            let chunk5 = TestChunk::new("table")
                .with_id(5)
                .with_dummy_parquet_file()
                .with_time_column_with_stats(Some(9), Some(10));
            let schema = chunk1.schema().clone();
            let plan = dedup_plan(schema, vec![chunk1, chunk3, chunk2, chunk5, chunk4]);
            insta::assert_yaml_snapshot!(
                OptimizationTest::new(plan, SplitDedup),
                @r#"
            input:
              - " DeduplicateExec: [time@0 ASC]"
              - "   DataSourceExec: file_groups={2 groups: [[1.parquet, 2.parquet, 4.parquet], [3.parquet, 5.parquet]]}, projection=[time], file_type=parquet"
            output:
              Ok:
                - " UnionExec"
                - "   DataSourceExec: file_groups={2 groups: [[1.parquet], [2.parquet]]}, projection=[time], file_type=parquet"
                - "   DeduplicateExec: [time@0 ASC]"
                - "     DataSourceExec: file_groups={1 group: [[3.parquet]]}, projection=[time], file_type=parquet"
                - "   DataSourceExec: file_groups={2 groups: [[4.parquet], [5.parquet]]}, projection=[time], file_type=parquet"
            "#
            );
        }

        #[test]
        fn max_split() {
            let chunk1 = TestChunk::new("table")
                .with_id(1)
                .with_dummy_parquet_file()
                .with_time_column_with_stats(Some(1), Some(2));
            let chunk2 = TestChunk::new("table")
                .with_id(2)
                .with_dummy_parquet_file()
                .with_time_column_with_stats(Some(3), Some(4))
                .with_may_contain_pk_duplicates(true);
            let chunk3 = TestChunk::new("table")
                .with_id(3)
                .with_dummy_parquet_file()
                .with_time_column_with_stats(Some(5), Some(6));
            let schema = chunk1.schema().clone();
            let plan = dedup_plan(schema, vec![chunk1, chunk2, chunk3]);
            let mut config = ConfigOptions::default();
            config.extensions.insert(IoxConfigExt {
                max_dedup_split: 2,
                ..Default::default()
            });
            insta::assert_yaml_snapshot!(
                OptimizationTest::new_with_config(plan, SplitDedup, &config),
                @r#"
            input:
              - " DeduplicateExec: [time@0 ASC]"
              - "   DataSourceExec: file_groups={2 groups: [[1.parquet, 3.parquet], [2.parquet]]}, projection=[time], file_type=parquet"
            output:
              Ok:
                - " DeduplicateExec: [time@0 ASC]"
                - "   DataSourceExec: file_groups={2 groups: [[1.parquet, 3.parquet], [2.parquet]]}, projection=[time], file_type=parquet"
            "#
            );
        }
    }
}
