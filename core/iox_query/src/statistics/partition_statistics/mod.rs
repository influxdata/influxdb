use std::sync::Arc;

use datafusion::{
    common::{ColumnStatistics, Result, Statistics, internal_datafusion_err, stats::Precision},
    datasource::{physical_plan::FileScanConfig, source::DataSourceExec},
    error::DataFusionError,
    physical_plan::{
        ExecutionPlan, PhysicalExpr,
        aggregates::AggregateExec,
        coalesce_batches::CoalesceBatchesExec,
        coalesce_partitions::CoalescePartitionsExec,
        coop::CooperativeExec,
        empty::EmptyExec,
        expressions::Column,
        filter::FilterExec,
        limit::{GlobalLimitExec, LocalLimitExec},
        placeholder_row::PlaceholderRowExec,
        projection::ProjectionExec,
        repartition::RepartitionExec,
        sorts::{sort::SortExec, sort_preserving_merge::SortPreservingMergeExec},
        union::UnionExec,
    },
};
use fetch::apply_fetch;
use filter::apply_filter;
use itertools::Itertools;
use project_schema::{
    proj_exec_stats, project_select_subset_of_column_statistics,
    project_statistics_with_chunk_order,
};
use util::{
    children_partition_count, make_column_statistics_inexact, merge_stats_collection,
    partition_count,
};

use crate::provider::{
    DeduplicateExec, RecordBatchesExec, progressive_eval::ProgressiveEvalExec,
    reorder_partitions::ReorderPartitionsExec,
};

mod fetch;
mod filter;
mod project_schema;
pub(crate) use project_schema::project_schema_onto_datasrc_statistics;
pub(crate) mod util;

/// This matches the planned API for DataFusion's `PartitionedStatistics`.
pub type PartitionedStatistics = Vec<Arc<Statistics>>;

pub(crate) trait PartitionStatistics {
    /// Returns a [`Statistics`] per partition.
    ///
    /// This function matches the planned API for DataFusion's `ExecutionPlan::statistics_by_partition`.
    fn statistics_by_partition(&self) -> Result<PartitionedStatistics>;
}

impl PartitionStatistics for EmptyExec {
    fn statistics_by_partition(&self) -> Result<PartitionedStatistics> {
        let mut stats = Statistics::new_unknown(&self.schema());
        stats.num_rows = Precision::Exact(0); // tis empty
        let data = Arc::new(stats);

        Ok(vec![data; partition_count(self)])
    }
}

impl PartitionStatistics for DataSourceExec {
    fn statistics_by_partition(&self) -> Result<PartitionedStatistics> {
        let Some(cfg) = self.data_source().as_any().downcast_ref::<FileScanConfig>() else {
            return Ok(unknown_statistics_by_partition(self));
        };

        let file_schema = &cfg.file_schema;
        let target_schema = Arc::clone(&self.schema());

        // get per partition (a.k.a. file group)
        let per_partition = cfg
            .file_groups
            .iter()
            .map(|file_group| {
                file_group
                    .iter()
                    .map(|file| {
                        // per file, get projected statistics
                        let statistics = if let Some(file_stats) = &file.statistics {
                            project_schema_onto_datasrc_statistics(
                                file_stats,
                                file_schema,
                                &target_schema,
                            )?
                        } else {
                            // doesn't have file stats
                            Arc::new(Statistics::new_unknown(&target_schema))
                        };
                        Ok::<Arc<Statistics>, DataFusionError>(statistics)
                    })
                    .process_results(|stats| {
                        Arc::new(merge_stats_collection(stats.into_iter(), &target_schema))
                    })
            })
            .try_collect()?;

        Ok(per_partition)
    }
}

impl PartitionStatistics for PlaceholderRowExec {
    fn statistics_by_partition(&self) -> Result<PartitionedStatistics> {
        // all partitions have the same single row, and same stats.
        // refer to `PlaceholderRowExec::execute`.
        let data = self.partition_statistics(Some(0))?.into();
        Ok(vec![data; partition_count(self)])
    }
}

impl PartitionStatistics for UnionExec {
    fn statistics_by_partition(&self) -> Result<PartitionedStatistics> {
        preserve_partitioning_no_projection(self)
    }
}

impl PartitionStatistics for CoalesceBatchesExec {
    fn statistics_by_partition(&self) -> Result<PartitionedStatistics> {
        preserve_partitioning_no_projection_apply_fetch(self, self.fetch(), 0)
    }
}

impl PartitionStatistics for LocalLimitExec {
    fn statistics_by_partition(&self) -> Result<PartitionedStatistics> {
        preserve_partitioning_no_projection_apply_fetch(self, Some(self.fetch()), 0)
    }
}

impl PartitionStatistics for GlobalLimitExec {
    fn statistics_by_partition(&self) -> Result<PartitionedStatistics> {
        preserve_partitioning_no_projection_apply_fetch(self, self.fetch(), self.skip())
    }
}

impl PartitionStatistics for FilterExec {
    fn statistics_by_partition(&self) -> Result<PartitionedStatistics> {
        preserve_partitioning_with_schema_projection_apply_filter(
            self,
            self.projection(),
            self.predicate(),
            self.default_selectivity(),
        )
    }
}

impl PartitionStatistics for ProjectionExec {
    fn statistics_by_partition(&self) -> Result<PartitionedStatistics> {
        // use the specific `proj_exec_stats` to project schema on every incoming partition
        let partition_cnt = partition_count(self);

        let all_partition_stats = self.children().into_iter().try_fold(
            Vec::with_capacity(partition_cnt),
            |mut acc, child| {
                let child_stats = statistics_by_partition(child.as_ref())?;

                let child_stats_with_project_exec_projected: Result<Vec<_>, DataFusionError> =
                    child_stats
                        .into_iter()
                        .map(|stats| {
                            proj_exec_stats(
                                Arc::unwrap_or_clone(stats),
                                self.expr().iter(),
                                &self.schema(),
                            )
                        })
                        .collect();

                acc.extend(child_stats_with_project_exec_projected?);
                Ok::<PartitionedStatistics, DataFusionError>(acc)
            },
        )?;

        Ok(all_partition_stats)
    }
}

impl PartitionStatistics for DeduplicateExec {
    fn statistics_by_partition(&self) -> Result<PartitionedStatistics> {
        if children_partition_count(self) != 1 {
            return Err(internal_datafusion_err!(
                "DeduplicateExec only supports a single input stream"
            ));
        }

        let child = self.children()[0];
        let single_child_partition_stats = statistics_by_partition(child.as_ref())?;
        Ok(vec![project_statistics_with_chunk_order(
            &single_child_partition_stats[0],
            &child.schema(),
            &self.schema(),
        )?])
    }
}

impl PartitionStatistics for SortPreservingMergeExec {
    fn statistics_by_partition(&self) -> Result<PartitionedStatistics> {
        merge_partitions_no_projection_apply_fetch(self, self.fetch(), 0)
    }
}

impl PartitionStatistics for ProgressiveEvalExec {
    fn statistics_by_partition(&self) -> Result<PartitionedStatistics> {
        merge_partitions_no_projection_apply_fetch(self, self.fetch(), 0)
    }
}

impl PartitionStatistics for ReorderPartitionsExec {
    fn statistics_by_partition(&self) -> Result<PartitionedStatistics> {
        let child = self.children()[0];
        let single_child_partition_stats = statistics_by_partition(child.as_ref())?;

        // re-order with partition mapping
        let mapped_partition_indices = self.mapped_partition_indices();
        let reordered_stats_for_all_partitions = single_child_partition_stats
            .into_iter()
            .enumerate()
            .map(|(input_idx, partition_stats)| {
                let out_idx = mapped_partition_indices
                    .iter()
                    .position(|in_idx| *in_idx == input_idx);
                (out_idx, partition_stats)
            })
            .sorted_by_key(|(out_idx, _)| *out_idx)
            .map(|(_, partition_stats)| partition_stats)
            .collect_vec();

        Ok(reordered_stats_for_all_partitions)
    }
}

impl PartitionStatistics for CoalescePartitionsExec {
    fn statistics_by_partition(&self) -> Result<PartitionedStatistics> {
        merge_partitions_no_projection(self)
    }
}

impl PartitionStatistics for SortExec {
    fn statistics_by_partition(&self) -> Result<PartitionedStatistics> {
        if self.preserve_partitioning() {
            preserve_partitioning_no_projection_apply_fetch(self, self.fetch(), 0)
        } else {
            merge_partitions_no_projection_apply_fetch(self, self.fetch(), 0)
        }
    }
}

impl PartitionStatistics for RepartitionExec {
    fn statistics_by_partition(&self) -> Result<PartitionedStatistics> {
        // takes N input partitions, returning M output partitions.

        // the statistics are "merged" into the same value (because any input can go to any output)
        let mut merged = merge_partitions_no_projection(self)?;
        let merged_single = merged.pop().expect("should have a single merged statistic");
        let merged_single = Arc::unwrap_or_clone(merged_single);

        // then the merged stat is turned to inexact (since unknown division across output partitions)
        let inexact_merged = Arc::new(Statistics {
            num_rows: Precision::Absent,
            total_byte_size: Precision::Absent,
            column_statistics: make_column_statistics_inexact(merged_single.column_statistics),
        });

        // finally, all output partitions have the same merged stat
        Ok(vec![inexact_merged; partition_count(self)])
    }
}

impl PartitionStatistics for AggregateExec {
    fn statistics_by_partition(&self) -> Result<PartitionedStatistics> {
        if self.aggr_expr().is_empty() {
            let inner_stats_per_partition = statistics_by_partition(self.input.as_ref())?;
            let input_schema = self.input.schema();

            inner_stats_per_partition
                .iter()
                .map(|stats| {
                    // Create column statistics for each output GROUP BY expression
                    let column_statistics: Result<Vec<ColumnStatistics>, DataFusionError> = self
                        .output_group_expr()
                        .iter()
                        .map(|group_expr| {
                            // Check if this group expression corresponds to an input column
                            if let Some(input_col_idx) = group_expr
                                .as_any()
                                .downcast_ref::<Column>()
                                .and_then(|col| input_schema.index_of(col.name()).ok())
                            {
                                // This is a direct column reference, use existing statistics
                                if input_col_idx < stats.column_statistics.len() {
                                    let col_stats = &stats.column_statistics[input_col_idx];
                                    Ok(ColumnStatistics {
                                        min_value: col_stats.min_value.clone(),
                                        max_value: col_stats.max_value.clone(),
                                        ..Default::default()
                                    })
                                } else {
                                    // Input column index out of bounds - this should not happen
                                    Err(internal_datafusion_err!(
                                        "Column index {input_col_idx} out of bounds in partition statistics (available columns: {}, column found in schema)",
                                        stats.column_statistics.len()
                                    ))
                                }
                            } else {
                                // This is a computed expression (like date_part), return unknown stats
                                Ok(ColumnStatistics::default())
                            }
                        })
                        .collect();

                    let column_statistics = column_statistics?;
                    Ok(Arc::new(Statistics {
                        num_rows: Precision::Absent,
                        total_byte_size: Precision::Absent,
                        column_statistics,
                    }))
                })
                .collect()
        } else {
            // if aggr expr is not empty, then the projected values (per column) could be different
            Ok(unknown_statistics_by_partition(self))
        }
    }
}

impl PartitionStatistics for CooperativeExec {
    fn statistics_by_partition(&self) -> Result<PartitionedStatistics> {
        statistics_by_partition(self.input().as_ref())
    }
}

/// Handle downcasting of the `dyn ExecutionPlan` to the specific execution nodes which
/// have implemented [`PartitionStatistics`].
pub fn statistics_by_partition(plan: &dyn ExecutionPlan) -> Result<PartitionedStatistics> {
    if let Some(exec) = plan.as_any().downcast_ref::<EmptyExec>() {
        exec.statistics_by_partition()
    } else if let Some(exec) = plan.as_any().downcast_ref::<PlaceholderRowExec>() {
        exec.statistics_by_partition()
    } else if let Some(exec) = plan.as_any().downcast_ref::<RecordBatchesExec>() {
        exec.statistics_by_partition()
    } else if let Some(exec) = plan.as_any().downcast_ref::<DataSourceExec>() {
        exec.statistics_by_partition()
    } else if let Some(exec) = plan.as_any().downcast_ref::<UnionExec>() {
        exec.statistics_by_partition()
    } else if let Some(exec) = plan.as_any().downcast_ref::<CoalesceBatchesExec>() {
        exec.statistics_by_partition()
    } else if let Some(exec) = plan.as_any().downcast_ref::<LocalLimitExec>() {
        exec.statistics_by_partition()
    } else if let Some(exec) = plan.as_any().downcast_ref::<GlobalLimitExec>() {
        exec.statistics_by_partition()
    } else if let Some(exec) = plan.as_any().downcast_ref::<FilterExec>() {
        exec.statistics_by_partition()
    } else if let Some(exec) = plan.as_any().downcast_ref::<ProjectionExec>() {
        exec.statistics_by_partition()
    } else if let Some(exec) = plan.as_any().downcast_ref::<DeduplicateExec>() {
        exec.statistics_by_partition()
    } else if let Some(exec) = plan.as_any().downcast_ref::<SortPreservingMergeExec>() {
        exec.statistics_by_partition()
    } else if let Some(exec) = plan.as_any().downcast_ref::<ProgressiveEvalExec>() {
        exec.statistics_by_partition()
    } else if let Some(exec) = plan.as_any().downcast_ref::<CoalescePartitionsExec>() {
        exec.statistics_by_partition()
    } else if let Some(exec) = plan.as_any().downcast_ref::<SortExec>() {
        exec.statistics_by_partition()
    } else if let Some(exec) = plan.as_any().downcast_ref::<RepartitionExec>() {
        exec.statistics_by_partition()
    } else if let Some(exec) = plan.as_any().downcast_ref::<AggregateExec>() {
        exec.statistics_by_partition()
    } else if let Some(exec) = plan.as_any().downcast_ref::<CooperativeExec>() {
        exec.statistics_by_partition()
    } else {
        /* These include, but not limited to, the following operators used in iox:
                WindowAggExec
                SymmetricHashJoinExec
                HashJoinExec
                NestedLoopJoinExec
                ValuesExec
                GapFillExec
                FieldsPivotExec
                SeriesPivotExec
                SchemaPivotExec
                StreamSplitExec
        */
        Ok(unknown_statistics_by_partition(plan))
    }
}

/// Provide unknown/absent statistics for all partitions in the plan.
fn unknown_statistics_by_partition(plan: &dyn ExecutionPlan) -> PartitionedStatistics {
    let data = Arc::new(Statistics::new_unknown(&plan.schema()));
    vec![data; partition_count(plan)]
}

/// Preserve partitioning from plan inputs.
///
/// This does not perform any schema projection.
fn preserve_partitioning_no_projection(plan: &dyn ExecutionPlan) -> Result<PartitionedStatistics> {
    preserve_partitioning_with_schema_projection(plan, None)
}

/// Preserve partitioning from plan inputs.
/// Apply fetch variables (fetch/skip) to modify the [`Statistics::num_rows`].
///
/// This does not perform any schema projection.
fn preserve_partitioning_no_projection_apply_fetch(
    plan: &dyn ExecutionPlan,
    fetch: Option<usize>,
    skip: usize,
) -> Result<PartitionedStatistics> {
    let partition_cnt = partition_count(plan);
    let all_partition_stats = plan.children().into_iter().try_fold(
        Vec::with_capacity(partition_cnt),
        |mut acc, child| {
            let child_stats = statistics_by_partition(child.as_ref())?;

            let child_stats_with_fetch = child_stats
                .into_iter()
                .map(|stats| apply_fetch(stats, fetch, skip));

            acc.extend(child_stats_with_fetch);
            Ok::<PartitionedStatistics, DataFusionError>(acc)
        },
    )?;

    Ok(all_partition_stats)
}

/// Preserve partitioning from plan inputs.
/// Performs a schema projection to modify the [`Statistics::column_statistics`].
///
/// A schema projection is only required if either a subset of input fields are projected
/// into the plan output (e.g. it has a `self.projection`), or we need to add column_statistics
/// for a chunk order column.
fn preserve_partitioning_with_schema_projection(
    plan: &dyn ExecutionPlan,
    subset_selected: Option<&Vec<usize>>,
) -> Result<PartitionedStatistics> {
    let target_schema = plan.schema();
    let mut all_partition_stats = Vec::with_capacity(partition_count(plan));

    for child in plan.children() {
        let child_stats = statistics_by_partition(child.as_ref())?;

        child_stats
            .into_iter()
            .map(|stats| {
                if let Some(proj_idx) = subset_selected {
                    // apply a schema projection
                    project_select_subset_of_column_statistics(
                        &stats,
                        &child.schema(),
                        proj_idx,
                        &target_schema,
                    )
                } else {
                    Ok(stats)
                }
            })
            .process_results(|iter| all_partition_stats.extend(iter))?;
    }

    Ok(all_partition_stats)
}

/// Preserve partitioning from plan inputs.
/// Apply filter variables to modify the num_rows, total_byte_size, column_statistics in [`Statistics`].
///
/// Then performs a schema projection to modify the [`Statistics::column_statistics`].
fn preserve_partitioning_with_schema_projection_apply_filter(
    plan: &dyn ExecutionPlan,
    project: Option<&Vec<usize>>,
    predicate: &Arc<dyn PhysicalExpr>,
    default_selectivity: u8,
) -> Result<PartitionedStatistics> {
    let target_schema = plan.schema();
    let mut all_partition_stats = Vec::with_capacity(partition_count(plan));

    for child in plan.children() {
        let child_stats = statistics_by_partition(child.as_ref())?;

        child_stats
            .into_iter()
            .map(|stats| {
                // apply filter first on input child
                apply_filter(
                    Arc::unwrap_or_clone(stats),
                    &child.schema(),
                    predicate,
                    default_selectivity,
                )
            })
            // then apply schema projection on output
            .map_ok(|stats| {
                if let Some(proj_idx) = project {
                    project_select_subset_of_column_statistics(
                        &stats,
                        &child.schema(),
                        proj_idx,
                        &target_schema,
                    )
                } else {
                    Ok(Arc::new(stats))
                }
            })
            .flatten_ok()
            .process_results(|iter| all_partition_stats.extend(iter))?;
    }

    Ok(all_partition_stats)
}

/// Merge partition stats across plan inputs.
///
/// This does not perform any schema projection.
fn merge_partitions_no_projection(plan: &dyn ExecutionPlan) -> Result<PartitionedStatistics> {
    merge_partitions_no_projection_apply_fetch(plan, None, 0)
}

/// Merge partition stats across plan inputs.
/// Apply fetch variables (fetch/skip) to modify the [`Statistics::num_rows`].
///
/// This does not perform any schema projection.
fn merge_partitions_no_projection_apply_fetch(
    plan: &dyn ExecutionPlan,
    fetch: Option<usize>,
    skip: usize,
) -> Result<PartitionedStatistics> {
    let merged_partition_stats = plan
        .children()
        .into_iter()
        .map(|child| {
            let child_stats = statistics_by_partition(child.as_ref())?;

            // merge stats for all partitions in each child
            let merged_child_partitions = if fetch.is_some() || skip > 0 {
                let child_stats_with_fetch = child_stats
                    .into_iter()
                    .map(|stats| apply_fetch(stats, fetch, skip));

                merge_stats_collection(child_stats_with_fetch, &plan.schema())
            } else {
                merge_stats_collection(child_stats.into_iter(), &plan.schema())
            };

            Ok::<Statistics, DataFusionError>(merged_child_partitions)
        })
        .process_results(|stats| {
            // merge stats across children
            Arc::new(merge_stats_collection(stats, &plan.schema()))
        })?;

    Ok(vec![merged_partition_stats])
}

#[cfg(test)]
mod test {
    use std::fmt::{Display, Formatter};

    use crate::test::test_utils::{
        PartitionedFilesAndRanges, SortKeyRange, coalesce_exec, crossjoin_exec,
        data_source_exec_parquet_with_sort_with_statistics, dedupe_exec, file_scan_config_builder,
        filter_exec, limit_exec, proj_exec, repartition_exec, single_column_schema, sort_exec,
        spm_exec, union_exec,
    };

    use super::*;
    use arrow::{
        array::{ArrayRef, Int64Array, RecordBatch},
        datatypes::{DataType, Field},
    };
    use datafusion::{
        common::ColumnStatistics,
        datasource::memory::MemorySourceConfig,
        error::DataFusionError,
        logical_expr::Operator,
        physical_expr::{LexOrdering, PhysicalSortExpr},
        physical_plan::{
            Partitioning, displayable,
            expressions::{BinaryExpr, IsNullExpr, NoOp, col, lit},
            union::InterleaveExec,
        },
    };
    use insta::assert_snapshot;
    use itertools::Itertools;

    /// For running test cases on the [`statistics_by_partition`].
    #[derive(Debug)]
    struct TestCase<'a> {
        /// Input place to test.
        input_plan: &'a Arc<dyn ExecutionPlan>,
        /// Column to extract.
        col_name: &'a str,
        /// Expected column statistics, per partition.
        expected_ranges_per_partition: Option<&'a [&'a SortKeyRange]>,
        /// Actual results per partition, populated after [`TestCase::run`].
        result_per_partition: PartitionedStatistics,
    }

    impl<'a> TestCase<'a> {
        fn new(
            input_plan: &'a Arc<dyn ExecutionPlan>,
            col_name: &'a str,
            expected_ranges_per_partition: Option<&'a [&'a SortKeyRange]>,
        ) -> Self {
            Self {
                input_plan,
                col_name,
                expected_ranges_per_partition,
                result_per_partition: vec![],
            }
        }

        /// Run the test cases, and populate the results in [`TestCase::result_per_partition`].
        fn run(mut self) -> Result<Self, DataFusionError> {
            let partition_cnt = self.input_partition_cnt();

            if let Some(per_partition) = &self.expected_ranges_per_partition {
                assert_eq!(
                    per_partition.len(),
                    partition_cnt,
                    "failure in test setup, the count of expected ranges should equal the partition count"
                );
            };

            // run test case with PartitionStatisitics implementations
            self.result_per_partition = statistics_by_partition(self.input_plan.as_ref())?;

            Ok(self)
        }

        fn input_partition_cnt(&self) -> usize {
            self.input_plan
                .properties()
                .output_partitioning()
                .partition_count()
        }

        /// Resultant [`ColumnStatistics`] per partition, extracted from the schema [`Statistics`],
        /// for the test column.
        ///
        /// If the schema does not have the resultant test column (e.g. the output plan projection is `select another-col`)
        /// Then the result is None.
        fn results_for_col_name(&self) -> Vec<Option<ColumnStatistics>> {
            if let Ok(col_idx) = self.input_plan.schema().index_of(self.col_name) {
                self.result_per_partition
                    .iter()
                    .map(|stats| Some(stats.column_statistics[col_idx].clone()))
                    .collect_vec()
            } else {
                vec![None; self.input_partition_cnt()]
            }
        }
    }

    impl Display for TestCase<'_> {
        fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
            let displayable_plan = displayable(self.input_plan.as_ref()).indent(false);
            writeln!(f, "{displayable_plan}")?;

            writeln!(f, "Expected column statistics per partition:")?;
            for partition in 0..self.input_partition_cnt() {
                if let Some(expected_per_partition) = self.expected_ranges_per_partition {
                    writeln!(
                        f,
                        "    partition {:?}:  {}",
                        partition, expected_per_partition[partition]
                    )?;
                } else {
                    writeln!(f, "    partition {partition:?}:  None")?;
                }
            }

            writeln!(f, "\nActual column statistics per partition:")?;
            for (partition, actual_stats) in self.results_for_col_name().iter().enumerate() {
                writeln!(
                    f,
                    "    partition {:?}:  {}",
                    partition,
                    str_column_stats(actual_stats)
                )?;
            }

            Ok(())
        }
    }

    /// Provide the [`ColumnStatistics`] as a string for insta.
    fn str_column_stats(stats: &Option<ColumnStatistics>) -> String {
        let Some(stats) = stats else {
            return "None".into();
        };
        if matches!(stats.min_value, Precision::Absent)
            && matches!(stats.max_value, Precision::Absent)
        {
            return "None".into();
        }

        if stats.null_count.get_value().cloned().unwrap_or_default() > 0 {
            format!(
                "({:?})->({:?}) null_count={}",
                stats.min_value.get_value().unwrap(),
                stats.max_value.get_value().unwrap(),
                stats.null_count.get_value().unwrap()
            )
        } else {
            format!(
                "({:?})->({:?})",
                stats.min_value.get_value().unwrap(),
                stats.max_value.get_value().unwrap()
            )
        }
    }

    /// Plan nodes which generate the original statistics (a.k.a. the data sources).
    #[test]
    fn test_handles_datasources_parquet() -> Result<(), DataFusionError> {
        let col_name = "a";
        let lex_ordering = LexOrdering::new(vec![PhysicalSortExpr::new(
            col(col_name, &single_column_schema())?,
            Default::default(),
        )])
        .unwrap();
        let ranges_per_partition = &[
            &SortKeyRange {
                min: Some(1000),
                max: Some(2000),
                null_count: 0,
            },
            &SortKeyRange {
                min: Some(2001),
                max: Some(3000),
                null_count: 0,
            },
            &SortKeyRange {
                min: Some(3001),
                max: Some(3500),
                null_count: 0,
            },
        ];

        /* Test Case: parquet */
        let plan = data_source_exec_parquet_with_sort_with_statistics(
            vec![lex_ordering.clone()],
            ranges_per_partition,
        );
        let test_case = TestCase::new(&plan, col_name, Some(ranges_per_partition));
        assert_snapshot!(
            test_case.run()?,
            @r"
        DataSourceExec: file_groups={3 groups: [[0.parquet], [1.parquet], [2.parquet]]}, projection=[a], output_ordering=[a@0 ASC], file_type=parquet

        Expected column statistics per partition:
            partition 0:  (Some(1000))->(Some(2000))
            partition 1:  (Some(2001))->(Some(3000))
            partition 2:  (Some(3001))->(Some(3500))

        Actual column statistics per partition:
            partition 0:  (Int32(1000))->(Int32(2000))
            partition 1:  (Int32(2001))->(Int32(3000))
            partition 2:  (Int32(3001))->(Int32(3500))
        ");
        Ok(())
    }

    #[test]
    fn test_handles_datasources_empty() -> Result<(), DataFusionError> {
        let col_name = "a";
        /* Test Case: empty exec */
        // note: empty exec always has only 1 partition, and has absent stats (except for 0 num_rows)
        let plan = Arc::new(EmptyExec::new(single_column_schema())) as _;
        let test_case = TestCase::new(&plan, col_name, None);
        let result = test_case.run()?;
        assert_snapshot!(
            result,
            @r"
        EmptyExec

        Expected column statistics per partition:
            partition 0:  None

        Actual column statistics per partition:
            partition 0:  None
        ");
        assert_eq!(
            result.result_per_partition[0].num_rows.get_value(),
            Some(&0),
            "empty exec should have zero rows"
        );
        Ok(())
    }

    #[test]
    fn test_handles_datasources_memory() -> Result<(), DataFusionError> {
        let col_name = "a";

        /* Test Case: memory exec */
        // note: memory exec is a collection of record batches, and has absent stats (because batches are not read at planning time)
        let a: ArrayRef = Arc::new(Int64Array::from(vec![10, 20]));
        let batch = RecordBatch::try_from_iter(vec![("a", a)])?;
        let partitions = &[vec![batch.clone()], vec![batch]];
        assert_eq!(partitions.len(), 2, "should have 2 partitions");
        let plan = Arc::new(DataSourceExec::new(Arc::new(
            MemorySourceConfig::try_new(partitions, single_column_schema(), Some(vec![0])).unwrap(),
        ))) as _;
        let test_case = TestCase::new(&plan, col_name, None);
        assert_snapshot!(
            test_case.run()?,
            @r"
        DataSourceExec: partitions=2, partition_sizes=[1, 1]

        Expected column statistics per partition:
            partition 0:  None
            partition 1:  None

        Actual column statistics per partition:
            partition 0:  None
            partition 1:  None
        ");
        Ok(())
    }

    #[test]
    fn test_handles_datasources_placeholder() -> Result<(), DataFusionError> {
        let col_name = "a";

        /* Test Case: placeholder row */
        // note: placeholder row is 1 row with null values for all columns
        let plan =
            Arc::new(PlaceholderRowExec::new(single_column_schema()).with_partitions(2)) as _;
        let test_case = TestCase::new(&plan, col_name, None);
        let result = test_case.run()?;
        assert_snapshot!(
            result,
            @r"
        PlaceholderRowExec

        Expected column statistics per partition:
            partition 0:  None
            partition 1:  None

        Actual column statistics per partition:
            partition 0:  None
            partition 1:  None
        ");
        assert_eq!(
            result.result_per_partition.len(),
            2,
            "should have stats for 2 partitions"
        );
        let [p0, p1] = &result.result_per_partition[..] else {
            unreachable!()
        };
        assert_eq!(
            p0.num_rows.get_value(),
            Some(1).as_ref(),
            "should have only 1 row"
        );
        assert_eq!(
            p0.column_statistics[0].null_count.get_value(),
            Some(1).as_ref(),
            "should be a null value"
        );
        assert_eq!(
            p1.num_rows.get_value(),
            Some(1).as_ref(),
            "should have only 1 row"
        );
        assert_eq!(
            p1.column_statistics[0].null_count.get_value(),
            Some(1).as_ref(),
            "should be a null value"
        );

        Ok(())
    }

    /// Plan nodes which pass through the partitions without impacting range.
    #[test]
    fn test_handles_partition_pass_thru() -> Result<(), DataFusionError> {
        let col_name = "a";
        let lex_ordering = LexOrdering::new(vec![PhysicalSortExpr::new(
            col(col_name, &single_column_schema())?,
            Default::default(),
        )])
        .unwrap();
        let ranges_per_partition = &[
            &SortKeyRange {
                min: Some(1000),
                max: Some(2000),
                null_count: 0,
            },
            &SortKeyRange {
                min: Some(2001),
                max: Some(3000),
                null_count: 0,
            },
            &SortKeyRange {
                min: Some(3001),
                max: Some(3500),
                null_count: 0,
            },
        ];

        /* Test Case: union */
        let left_plan = data_source_exec_parquet_with_sort_with_statistics(
            vec![lex_ordering.clone()],
            &ranges_per_partition[0..1],
        );
        let right_plan = data_source_exec_parquet_with_sort_with_statistics(
            vec![lex_ordering.clone()],
            &ranges_per_partition[1..],
        );
        let union = union_exec(vec![left_plan, right_plan]);
        let test_case = TestCase::new(&union, col_name, Some(ranges_per_partition));
        assert_snapshot!(
            test_case.run()?,
            @r"
        UnionExec
          DataSourceExec: file_groups={1 group: [[0.parquet]]}, projection=[a], output_ordering=[a@0 ASC], file_type=parquet
          DataSourceExec: file_groups={2 groups: [[0.parquet], [1.parquet]]}, projection=[a], output_ordering=[a@0 ASC], file_type=parquet

        Expected column statistics per partition:
            partition 0:  (Some(1000))->(Some(2000))
            partition 1:  (Some(2001))->(Some(3000))
            partition 2:  (Some(3001))->(Some(3500))

        Actual column statistics per partition:
            partition 0:  (Int32(1000))->(Int32(2000))
            partition 1:  (Int32(2001))->(Int32(3000))
            partition 2:  (Int32(3001))->(Int32(3500))
        ");

        /* Test Case: sorts, with preserve partitioning */
        let preserve_partitioning = true;
        let input = data_source_exec_parquet_with_sort_with_statistics(
            vec![lex_ordering.clone()],
            ranges_per_partition,
        );
        let sort = sort_exec(&lex_ordering, &input, preserve_partitioning);
        let test_case = TestCase::new(&sort, col_name, Some(ranges_per_partition));
        assert_snapshot!(
            test_case.run()?,
            @r"
        SortExec: expr=[a@0 ASC], preserve_partitioning=[true]
          DataSourceExec: file_groups={3 groups: [[0.parquet], [1.parquet], [2.parquet]]}, projection=[a], output_ordering=[a@0 ASC], file_type=parquet

        Expected column statistics per partition:
            partition 0:  (Some(1000))->(Some(2000))
            partition 1:  (Some(2001))->(Some(3000))
            partition 2:  (Some(3001))->(Some(3500))

        Actual column statistics per partition:
            partition 0:  (Int32(1000))->(Int32(2000))
            partition 1:  (Int32(2001))->(Int32(3000))
            partition 2:  (Int32(3001))->(Int32(3500))
        ");

        /* Test Case: coalesce */
        let coalesce = coalesce_exec(&input);
        let test_case = TestCase::new(&coalesce, col_name, Some(ranges_per_partition));
        assert_snapshot!(
            test_case.run()?,
            @r"
        CoalesceBatchesExec: target_batch_size=10
          DataSourceExec: file_groups={3 groups: [[0.parquet], [1.parquet], [2.parquet]]}, projection=[a], output_ordering=[a@0 ASC], file_type=parquet

        Expected column statistics per partition:
            partition 0:  (Some(1000))->(Some(2000))
            partition 1:  (Some(2001))->(Some(3000))
            partition 2:  (Some(3001))->(Some(3500))

        Actual column statistics per partition:
            partition 0:  (Int32(1000))->(Int32(2000))
            partition 1:  (Int32(2001))->(Int32(3000))
            partition 2:  (Int32(3001))->(Int32(3500))
        ");

        /* Test Case: limit */
        let limit = limit_exec(&input, 2);
        let test_case = TestCase::new(&limit, col_name, Some(ranges_per_partition));
        assert_snapshot!(
            test_case.run()?,
            @r"
        LocalLimitExec: fetch=2
          DataSourceExec: file_groups={3 groups: [[0.parquet], [1.parquet], [2.parquet]]}, projection=[a], output_ordering=[a@0 ASC], file_type=parquet

        Expected column statistics per partition:
            partition 0:  (Some(1000))->(Some(2000))
            partition 1:  (Some(2001))->(Some(3000))
            partition 2:  (Some(3001))->(Some(3500))

        Actual column statistics per partition:
            partition 0:  (Int32(1000))->(Int32(2000))
            partition 1:  (Int32(2001))->(Int32(3000))
            partition 2:  (Int32(3001))->(Int32(3500))
        ");

        /* Test Case: empty projection */
        let proj = proj_exec(
            &input,
            vec![(col(col_name, &single_column_schema())?, "a".into())],
        );
        let test_case = TestCase::new(&proj, col_name, Some(ranges_per_partition));
        assert_snapshot!(
            test_case.run()?,
            @r"
        ProjectionExec: expr=[a@0 as a]
          DataSourceExec: file_groups={3 groups: [[0.parquet], [1.parquet], [2.parquet]]}, projection=[a], output_ordering=[a@0 ASC], file_type=parquet

        Expected column statistics per partition:
            partition 0:  (Some(1000))->(Some(2000))
            partition 1:  (Some(2001))->(Some(3000))
            partition 2:  (Some(3001))->(Some(3500))

        Actual column statistics per partition:
            partition 0:  (Int32(1000))->(Int32(2000))
            partition 1:  (Int32(2001))->(Int32(3000))
            partition 2:  (Int32(3001))->(Int32(3500))
        ");

        /* Test Case: pass thru projection */
        let pass_thru_single_col = col("a", &single_column_schema())?;
        let proj = proj_exec(
            &input,
            vec![(Arc::clone(&pass_thru_single_col), "a".into())],
        );
        let test_case = TestCase::new(&proj, col_name, Some(ranges_per_partition));
        assert_snapshot!(
            test_case.run()?,
            @r"
        ProjectionExec: expr=[a@0 as a]
          DataSourceExec: file_groups={3 groups: [[0.parquet], [1.parquet], [2.parquet]]}, projection=[a], output_ordering=[a@0 ASC], file_type=parquet

        Expected column statistics per partition:
            partition 0:  (Some(1000))->(Some(2000))
            partition 1:  (Some(2001))->(Some(3000))
            partition 2:  (Some(3001))->(Some(3500))

        Actual column statistics per partition:
            partition 0:  (Int32(1000))->(Int32(2000))
            partition 1:  (Int32(2001))->(Int32(3000))
            partition 2:  (Int32(3001))->(Int32(3500))
        ");

        /* Test Case: projection that modifies -> will not pass thru*/
        let col_plus_2 = Arc::new(BinaryExpr::new(
            pass_thru_single_col,
            Operator::Plus,
            lit(2),
        ));
        let proj = proj_exec(&input, vec![(col_plus_2, "foo".into())]);
        let test_case = TestCase::new(&proj, col_name, Some(ranges_per_partition));
        assert_snapshot!(
            test_case.run()?,
            @r"
        ProjectionExec: expr=[a@0 + 2 as foo]
          DataSourceExec: file_groups={3 groups: [[0.parquet], [1.parquet], [2.parquet]]}, projection=[a], output_ordering=[a@0 ASC], file_type=parquet

        Expected column statistics per partition:
            partition 0:  (Some(1000))->(Some(2000))
            partition 1:  (Some(2001))->(Some(3000))
            partition 2:  (Some(3001))->(Some(3500))

        Actual column statistics per partition:
            partition 0:  None
            partition 1:  None
            partition 2:  None
        ");

        /* Test Case: filter (for now, we don't narrow the range further) */
        let filter = filter_exec(&input, Arc::new(IsNullExpr::new(Arc::new(NoOp::new()))));
        let test_case = TestCase::new(&filter, col_name, Some(ranges_per_partition));
        assert_snapshot!(
            test_case.run()?,
            @r"
        FilterExec: NoOp IS NULL
          DataSourceExec: file_groups={3 groups: [[0.parquet], [1.parquet], [2.parquet]]}, projection=[a], output_ordering=[a@0 ASC], file_type=parquet

        Expected column statistics per partition:
            partition 0:  (Some(1000))->(Some(2000))
            partition 1:  (Some(2001))->(Some(3000))
            partition 2:  (Some(3001))->(Some(3500))

        Actual column statistics per partition:
            partition 0:  (Int32(1000))->(Int32(2000))
            partition 1:  (Int32(2001))->(Int32(3000))
            partition 2:  (Int32(3001))->(Int32(3500))
        ");

        /* Test Case: deduplicate -- if we give single-partitioned input */
        // THIS IS HOW WE CONSTRUCT OUR PLANS.
        let input_partition = &ranges_per_partition[0..1];
        let partitioned_input = data_source_exec_parquet_with_sort_with_statistics(
            vec![lex_ordering.clone()],
            &ranges_per_partition[0..1],
        );
        let dedupe = dedupe_exec(&partitioned_input, &lex_ordering);
        let expected = input_partition; // passes thru
        let test_case = TestCase::new(&dedupe, col_name, Some(expected));
        assert_snapshot!(
            test_case.run()?,
            @r"
        DeduplicateExec: [a@0 ASC]
          DataSourceExec: file_groups={1 group: [[0.parquet]]}, projection=[a], output_ordering=[a@0 ASC], file_type=parquet

        Expected column statistics per partition:
            partition 0:  (Some(1000))->(Some(2000))

        Actual column statistics per partition:
            partition 0:  (Int32(1000))->(Int32(2000))
        ");

        Ok(())
    }

    /// Plan nodes which merge the partitions into a single output partition, producing a range
    /// predictable from the merged input partitions.
    #[test]
    fn test_handles_partition_merging() -> Result<(), DataFusionError> {
        let col_name = "a";
        let lex_ordering = LexOrdering::new(vec![PhysicalSortExpr::new(
            col(col_name, &single_column_schema())?,
            Default::default(),
        )])
        .unwrap();
        let ranges_per_partition = &[
            &SortKeyRange {
                min: Some(1000),
                max: Some(2000),
                null_count: 0,
            },
            &SortKeyRange {
                min: Some(2001),
                max: Some(3000),
                null_count: 0,
            },
            &SortKeyRange {
                min: Some(3001),
                max: Some(3500),
                null_count: 0,
            },
        ];

        // Expected result from all test cases.
        let partitioned_input = data_source_exec_parquet_with_sort_with_statistics(
            vec![lex_ordering.clone()],
            ranges_per_partition,
        );
        let expect_merged = &[&SortKeyRange {
            min: Some(1000),
            max: Some(3500),
            null_count: 0,
        }];

        /* Test Case: SPM */
        let spm = spm_exec(&partitioned_input, &lex_ordering);
        let test_case = TestCase::new(&spm, col_name, Some(expect_merged));
        assert_snapshot!(
            test_case.run()?,
            @r"
        SortPreservingMergeExec: [a@0 ASC]
          DataSourceExec: file_groups={3 groups: [[0.parquet], [1.parquet], [2.parquet]]}, projection=[a], output_ordering=[a@0 ASC], file_type=parquet

        Expected column statistics per partition:
            partition 0:  (Some(1000))->(Some(3500))

        Actual column statistics per partition:
            partition 0:  (Int32(1000))->(Int32(3500))
        ");

        /* Test Case: sorts, without preserve partitioning */
        let sort = sort_exec(&lex_ordering, &partitioned_input, false);
        let test_case = TestCase::new(&sort, col_name, Some(expect_merged));
        assert_snapshot!(
            test_case.run()?,
            @r"
        SortExec: expr=[a@0 ASC], preserve_partitioning=[false]
          DataSourceExec: file_groups={3 groups: [[0.parquet], [1.parquet], [2.parquet]]}, projection=[a], output_ordering=[a@0 ASC], file_type=parquet

        Expected column statistics per partition:
            partition 0:  (Some(1000))->(Some(3500))

        Actual column statistics per partition:
            partition 0:  (Int32(1000))->(Int32(3500))
        ");

        Ok(())
    }

    /// Plan nodes which has N input partitions and M output partitions, where N may not equal M
    /// and partitions are reshuffled.
    #[test]
    fn test_handles_repartitioning() -> Result<(), DataFusionError> {
        let col_name = "a";
        let lex_ordering = LexOrdering::new(vec![PhysicalSortExpr::new(
            col(col_name, &single_column_schema())?,
            Default::default(),
        )])
        .unwrap();
        let ranges_per_partition = &[
            &SortKeyRange {
                min: Some(1000),
                max: Some(2000),
                null_count: 0,
            },
            &SortKeyRange {
                min: Some(2001),
                max: Some(3000),
                null_count: 0,
            },
            &SortKeyRange {
                min: Some(3001),
                max: Some(3500),
                null_count: 0,
            },
        ];

        // Expected result from all test cases.
        let expect_merged = &[&SortKeyRange {
            min: Some(1000),
            max: Some(3500),
            null_count: 0,
        }];

        /* Test Case: Repartitioning */
        let partitioned_input = data_source_exec_parquet_with_sort_with_statistics(
            vec![lex_ordering.clone()],
            ranges_per_partition,
        );
        let partitioning = Partitioning::Hash(vec![], 4);
        let repartition = repartition_exec(&partitioned_input, partitioning);
        // expect all 4 hashed partitions to potentially cover the same range
        let expected = std::iter::repeat_n(expect_merged[0], 4).collect_vec();
        let test_case = TestCase::new(&repartition, col_name, Some(&expected));
        assert_snapshot!(
            test_case.run()?,
            @r"
        RepartitionExec: partitioning=Hash([], 4), input_partitions=3
          DataSourceExec: file_groups={3 groups: [[0.parquet], [1.parquet], [2.parquet]]}, projection=[a], output_ordering=[a@0 ASC], file_type=parquet

        Expected column statistics per partition:
            partition 0:  (Some(1000))->(Some(3500))
            partition 1:  (Some(1000))->(Some(3500))
            partition 2:  (Some(1000))->(Some(3500))
            partition 3:  (Some(1000))->(Some(3500))

        Actual column statistics per partition:
            partition 0:  (Int32(1000))->(Int32(3500))
            partition 1:  (Int32(1000))->(Int32(3500))
            partition 2:  (Int32(1000))->(Int32(3500))
            partition 3:  (Int32(1000))->(Int32(3500))
        ");

        Ok(())
    }

    fn build_interleave_plan(
        lex_ordering: LexOrdering,
        ranges_per_partition: &[&SortKeyRange],
    ) -> Result<Arc<dyn ExecutionPlan>, DataFusionError> {
        let partitioning = Partitioning::Hash(vec![], 4);
        let left_plan = repartition_exec(
            &data_source_exec_parquet_with_sort_with_statistics(
                vec![lex_ordering.clone()],
                &ranges_per_partition[0..1],
            ),
            partitioning.clone(),
        );
        let right_plan = repartition_exec(
            &data_source_exec_parquet_with_sort_with_statistics(
                vec![lex_ordering],
                &ranges_per_partition[1..],
            ),
            partitioning,
        );
        Ok(Arc::new(InterleaveExec::try_new(vec![
            left_plan, right_plan,
        ])?))
    }

    /// Plan nodes which short circuit the statistics_by_partition, returning None (or absent) statistics.
    #[test]
    fn test_returns_none() -> Result<(), DataFusionError> {
        let col_name = "a";
        let lex_ordering = LexOrdering::new(vec![PhysicalSortExpr::new(
            col(col_name, &single_column_schema())?,
            Default::default(),
        )])
        .unwrap();
        let ranges_per_partition = &[
            &SortKeyRange {
                min: Some(1000),
                max: Some(2000),
                null_count: 0,
            },
            &SortKeyRange {
                min: Some(2001),
                max: Some(3000),
                null_count: 0,
            },
            &SortKeyRange {
                min: Some(3001),
                max: Some(3500),
                null_count: 0,
            },
        ];

        /* Test Case: Joins */
        let left_plan = data_source_exec_parquet_with_sort_with_statistics(
            vec![lex_ordering.clone()],
            &ranges_per_partition[0..1],
        );
        let right_plan = data_source_exec_parquet_with_sort_with_statistics(
            vec![lex_ordering.clone()],
            &ranges_per_partition[1..],
        );
        let crossjoin = crossjoin_exec(&left_plan, &right_plan);
        let test_case = TestCase::new(&crossjoin, col_name, None);
        assert_snapshot!(
            test_case.run()?,
            @r"
        CrossJoinExec
          DataSourceExec: file_groups={1 group: [[0.parquet]]}, projection=[a], output_ordering=[a@0 ASC], file_type=parquet
          DataSourceExec: file_groups={2 groups: [[0.parquet], [1.parquet]]}, projection=[a], output_ordering=[a@0 ASC], file_type=parquet

        Expected column statistics per partition:
            partition 0:  None
            partition 1:  None

        Actual column statistics per partition:
            partition 0:  None
            partition 1:  None
        ");

        /* Test Case: interleave */
        let interleave = build_interleave_plan(lex_ordering.clone(), ranges_per_partition)?;
        let test_case = TestCase::new(&interleave, col_name, None);
        assert_snapshot!(
            test_case.run()?,
            @r"
        InterleaveExec
          RepartitionExec: partitioning=Hash([], 4), input_partitions=1
            DataSourceExec: file_groups={1 group: [[0.parquet]]}, projection=[a], output_ordering=[a@0 ASC], file_type=parquet
          RepartitionExec: partitioning=Hash([], 4), input_partitions=2
            DataSourceExec: file_groups={2 groups: [[0.parquet], [1.parquet]]}, projection=[a], output_ordering=[a@0 ASC], file_type=parquet

        Expected column statistics per partition:
            partition 0:  None
            partition 1:  None
            partition 2:  None
            partition 3:  None

        Actual column statistics per partition:
            partition 0:  None
            partition 1:  None
            partition 2:  None
            partition 3:  None
        ");

        /* Test Case: None will override later merges with partitions having stats */
        // (because None means cannot determine all of the subplan stats)
        let partitioned_input = data_source_exec_parquet_with_sort_with_statistics(
            vec![lex_ordering.clone()],
            ranges_per_partition,
        );
        let spm = spm_exec(
            &union_exec(vec![partitioned_input, interleave]),
            &lex_ordering,
        );
        let test_case = TestCase::new(&spm, col_name, None);
        assert_snapshot!(
            test_case.run()?,
            @r"
        SortPreservingMergeExec: [a@0 ASC]
          UnionExec
            DataSourceExec: file_groups={3 groups: [[0.parquet], [1.parquet], [2.parquet]]}, projection=[a], output_ordering=[a@0 ASC], file_type=parquet
            InterleaveExec
              RepartitionExec: partitioning=Hash([], 4), input_partitions=1
                DataSourceExec: file_groups={1 group: [[0.parquet]]}, projection=[a], output_ordering=[a@0 ASC], file_type=parquet
              RepartitionExec: partitioning=Hash([], 4), input_partitions=2
                DataSourceExec: file_groups={2 groups: [[0.parquet], [1.parquet]]}, projection=[a], output_ordering=[a@0 ASC], file_type=parquet

        Expected column statistics per partition:
            partition 0:  None

        Actual column statistics per partition:
            partition 0:  None
        ");

        Ok(())
    }

    /// These are our in-house plans which only interact properly with the visitor based upon
    /// how we construct our plans.
    #[test]
    fn test_special_cases() {
        let col_name = "a";
        let lex_ordering = LexOrdering::new(vec![PhysicalSortExpr::new(
            col(col_name, &single_column_schema()).unwrap(),
            Default::default(),
        )])
        .unwrap();
        let ranges_per_partition = &[
            &SortKeyRange {
                min: Some(1000),
                max: Some(2000),
                null_count: 0,
            },
            &SortKeyRange {
                min: Some(2001),
                max: Some(3000),
                null_count: 0,
            },
            &SortKeyRange {
                min: Some(3001),
                max: Some(3500),
                null_count: 0,
            },
        ];

        /* Test Case: deduplicate -- if we give multiple-partitioned input */
        // This is an invalid plan (refer to DeduplicateExec::execute).
        let partitioned_input = data_source_exec_parquet_with_sort_with_statistics(
            vec![lex_ordering.clone()],
            ranges_per_partition,
        );
        let dedupe = dedupe_exec(&partitioned_input, &lex_ordering);
        let test_case = TestCase::new(&dedupe, col_name, None);
        let err = test_case.run().unwrap_err();
        assert!(
            err.message()
                .contains("DeduplicateExec only supports a single input stream")
        );
    }

    /// How null counts are handled.
    #[test]
    fn test_null_counts() -> Result<(), DataFusionError> {
        let col_name = "a";
        let lex_ordering = LexOrdering::new(vec![PhysicalSortExpr::new(
            col(col_name, &single_column_schema())?,
            Default::default(),
        )])
        .unwrap();
        let ranges_per_partition = &[
            &SortKeyRange {
                min: Some(1000),
                max: Some(2000),
                null_count: 1,
            },
            &SortKeyRange {
                min: Some(2001),
                max: Some(3000),
                null_count: 4,
            },
            &SortKeyRange {
                min: Some(3001),
                max: Some(3500),
                null_count: 3,
            },
        ];

        /* Test Case: keeps null counts separate with pass thru */
        let left_plan = data_source_exec_parquet_with_sort_with_statistics(
            vec![lex_ordering.clone()],
            &ranges_per_partition[0..1],
        );
        let right_plan = data_source_exec_parquet_with_sort_with_statistics(
            vec![lex_ordering.clone()],
            &ranges_per_partition[1..],
        );
        let union = union_exec(vec![left_plan, right_plan]);
        let test_case = TestCase::new(&union, col_name, Some(ranges_per_partition));
        assert_snapshot!(
            test_case.run()?,
            @r"
        UnionExec
          DataSourceExec: file_groups={1 group: [[0.parquet]]}, projection=[a], output_ordering=[a@0 ASC], file_type=parquet
          DataSourceExec: file_groups={2 groups: [[0.parquet], [1.parquet]]}, projection=[a], output_ordering=[a@0 ASC], file_type=parquet

        Expected column statistics per partition:
            partition 0:  (Some(1000))->(Some(2000)) null_count=1
            partition 1:  (Some(2001))->(Some(3000)) null_count=4
            partition 2:  (Some(3001))->(Some(3500)) null_count=3

        Actual column statistics per partition:
            partition 0:  (Int32(1000))->(Int32(2000)) null_count=1
            partition 1:  (Int32(2001))->(Int32(3000)) null_count=4
            partition 2:  (Int32(3001))->(Int32(3500)) null_count=3
        ");

        /* Test Case: merges null counts */
        let spm = spm_exec(&union, &lex_ordering);
        let expect_merged = &[&SortKeyRange {
            min: Some(1000),
            max: Some(3500),
            null_count: 8,
        }];
        let test_case = TestCase::new(&spm, col_name, Some(expect_merged));
        assert_snapshot!(
            test_case.run()?,
            @r"
        SortPreservingMergeExec: [a@0 ASC]
          UnionExec
            DataSourceExec: file_groups={1 group: [[0.parquet]]}, projection=[a], output_ordering=[a@0 ASC], file_type=parquet
            DataSourceExec: file_groups={2 groups: [[0.parquet], [1.parquet]]}, projection=[a], output_ordering=[a@0 ASC], file_type=parquet

        Expected column statistics per partition:
            partition 0:  (Some(1000))->(Some(3500)) null_count=8

        Actual column statistics per partition:
            partition 0:  (Int32(1000))->(Int32(3500)) null_count=8
        ");

        Ok(())
    }

    /// Build test case for:
    ///
    /// Union
    ///     DeduplicateExec
    ///         SPM
    ///             Union
    ///                 DataSourceExec (ranges_per_partition_for_DataSourceExec_1)
    ///                 DataSourceExec (ranges_per_partition_for_DataSourceExec_2)
    ///     DataSourceExec (ranges_per_partition_for_DataSourceExec_3)
    ///
    fn build_test_case_two_unions_and_dedupe(
        ranges_per_partition_for_data_source_exec_1: &[&SortKeyRange],
        ranges_per_partition_for_data_source_exec_2: &[&SortKeyRange],
        ranges_per_partition_for_data_source_exec_3: &[&SortKeyRange],
        lex_ordering: &LexOrdering,
    ) -> Arc<dyn ExecutionPlan> {
        // first union, of `DataSourceExec`s 1 and 2
        let left_plan = data_source_exec_parquet_with_sort_with_statistics(
            vec![lex_ordering.clone()],
            ranges_per_partition_for_data_source_exec_1,
        );
        let right_plan = data_source_exec_parquet_with_sort_with_statistics(
            vec![lex_ordering.clone()],
            ranges_per_partition_for_data_source_exec_2,
        );
        let union = union_exec(vec![left_plan, right_plan]);

        // Add a dedupe exec, with an SPM beforehand.
        // Since the DeduplicateExec::execute() requires only 1 input partition.
        //
        //  DeduplicateExec
        //      SPM
        //          Union
        //              DataSourceExec
        //              DataSourceExec
        let union_then_spm_then_dedupe = dedupe_exec(&spm_exec(&union, lex_ordering), lex_ordering);

        // second union, with the 3rd `DataSourceExec`
        union_exec(vec![
            union_then_spm_then_dedupe,
            data_source_exec_parquet_with_sort_with_statistics(
                vec![lex_ordering.clone()],
                ranges_per_partition_for_data_source_exec_3,
            ),
        ])
    }

    /// Test using a common query plan seen in iox.
    #[test]
    fn test_with_two_unions_and_dedupe() -> Result<(), DataFusionError> {
        let col_name = "a";
        let lex_ordering = LexOrdering::new(vec![PhysicalSortExpr::new(
            col(col_name, &single_column_schema())?,
            Default::default(),
        )])
        .unwrap();

        /* Test Case: the SPM+Deduped `DataSourceExec` are overlapping each other, but are disjoint from the 3rd partion exec */
        let ranges_per_partition_for_data_source_exec_1 = &[
            &SortKeyRange {
                min: Some(1000),
                max: Some(2000),
                null_count: 4,
            },
            &SortKeyRange {
                min: Some(3001),
                max: Some(3500),
                null_count: 2,
            },
        ];
        let ranges_per_partition_for_data_source_exec_2 = &[&SortKeyRange {
            min: Some(2001), // DataSourceExec 1 and DataSourceExec 2 overlap, and will be deduped
            max: Some(3000),
            null_count: 1,
        }];
        let ranges_per_partition_for_data_source_exec_3 = &[&SortKeyRange {
            min: Some(4001), // parquet 3 is not overlapping with the others
            max: Some(4500),
            null_count: 1,
        }];
        let plan = build_test_case_two_unions_and_dedupe(
            ranges_per_partition_for_data_source_exec_1,
            ranges_per_partition_for_data_source_exec_2,
            ranges_per_partition_for_data_source_exec_3,
            &lex_ordering,
        );

        // Expected: output of uppermost union should shown non-overlapping left vs right side,
        // such that the SPM would be seen as having disjoint ranges.
        let expected = &[
            // merged & deduped from left side of uppermost/final union
            &SortKeyRange {
                min: Some(1000),
                max: Some(3500),
                null_count: 7,
            },
            // parquet 3, from right side of uppermost/final union
            &SortKeyRange {
                min: Some(4001),
                max: Some(4500),
                null_count: 1,
            },
        ];
        let test_case = TestCase::new(&plan, col_name, Some(expected));
        assert_snapshot!(
            test_case.run()?,
            @r"
        UnionExec
          DeduplicateExec: [a@0 ASC]
            SortPreservingMergeExec: [a@0 ASC]
              UnionExec
                DataSourceExec: file_groups={2 groups: [[0.parquet], [1.parquet]]}, projection=[a], output_ordering=[a@0 ASC], file_type=parquet
                DataSourceExec: file_groups={1 group: [[0.parquet]]}, projection=[a], output_ordering=[a@0 ASC], file_type=parquet
          DataSourceExec: file_groups={1 group: [[0.parquet]]}, projection=[a], output_ordering=[a@0 ASC], file_type=parquet

        Expected column statistics per partition:
            partition 0:  (Some(1000))->(Some(3500)) null_count=7
            partition 1:  (Some(4001))->(Some(4500)) null_count=1

        Actual column statistics per partition:
            partition 0:  (Int32(1000))->(Int32(3500)) null_count=7
            partition 1:  (Int32(4001))->(Int32(4500)) null_count=1
        ");

        Ok(())
    }

    /// Test we are using the proper file schema
    #[test]
    fn test_file_group() -> Result<(), DataFusionError> {
        // target column
        let col_name = "C";

        // File schema, vs plan schema.
        let file_schema = Arc::new(arrow::datatypes::Schema::new(vec![
            Field::new("a", DataType::Int64, true),
            Field::new("b", DataType::Int64, true),
            Field::new(col_name, DataType::Int64, true),
        ]));
        let plan_schema = Arc::new(arrow::datatypes::Schema::new(vec![Field::new(
            col_name,
            DataType::Int64,
            true,
        )]));

        // File scan config uses the file schema and ALL columns in file.
        let ranges_for_file_0 = vec![
            // col a
            SortKeyRange {
                min: Some(20),
                max: Some(30),
                null_count: 1,
            },
            // col b
            SortKeyRange {
                min: Some(2000),
                max: Some(3000),
                null_count: 1,
            },
            // col C (our tested key to extract)
            SortKeyRange {
                min: Some(200_000),
                max: Some(300_000),
                null_count: 1,
            },
        ];
        let ranges_for_file_1 = vec![
            // col a
            SortKeyRange {
                min: Some(30),
                max: Some(40),
                null_count: 1,
            },
            // col b
            SortKeyRange {
                min: Some(1000),
                max: Some(2000),
                null_count: 1,
            },
            // col C (our tested key to extract)
            SortKeyRange {
                min: Some(500_000),
                max: Some(700_000),
                null_count: 1,
            },
        ];
        let lex_ordering_on_c = LexOrdering::new(vec![PhysicalSortExpr::new(
            col(col_name, &file_schema)?, // file_schema
            Default::default(),
        )])
        .unwrap();
        let multiple_column_key_ranges_per_file = PartitionedFilesAndRanges {
            per_file: vec![ranges_for_file_0.clone(), ranges_for_file_1.clone()],
        };
        let filegroups_config = file_scan_config_builder(
            &file_schema,
            vec![lex_ordering_on_c],
            multiple_column_key_ranges_per_file,
        );

        // use a plan with only col C
        let data_source_exec = DataSourceExec::from_data_source(
            filegroups_config.with_projection(Some(vec![2])).build(),
        ) as _;
        let projected = col(col_name, &plan_schema)?; // plan_schema
        let plan = proj_exec(&data_source_exec, vec![(projected, "C".into())]);
        insta::assert_snapshot!(
            displayable(plan.as_ref()).indent(true),
            @r"
        ProjectionExec: expr=[C@0 as C]
          DataSourceExec: file_groups={2 groups: [[0.parquet], [1.parquet]]}, projection=[C], output_ordering=[C@0 ASC], file_type=parquet
        ",
        );

        // Test Case: show that plan schema is different from file schema
        assert_ne!(
            &plan_schema, &file_schema,
            "plan and file schema are not equivalent"
        );
        assert_eq!(
            plan.schema().fields().len(),
            1,
            "plan schema should have only 1 field"
        );
        assert_eq!(
            plan.schema(),
            plan_schema,
            "plan schema should only have col C"
        );
        let data_source_exec = plan.children()[0]
            .as_any()
            .downcast_ref::<DataSourceExec>()
            .unwrap();
        assert_eq!(
            data_source_exec.schema(),
            plan_schema,
            "`DataSourceExec` plan schema should only have col C"
        );

        /* Test Case: the statistics_by_partition will still extract the proper file_stats for col C */
        let actual = statistics_by_partition(plan.as_ref())?;
        let [actual_partition_0, actual_partition_1] = &actual[..] else {
            panic!("should have stats for 2 partitions");
        };
        assert_eq!(
            actual_partition_0.column_statistics.len(),
            1,
            "should have only 1 column for the ProjectExec C@0"
        );
        assert_eq!(
            actual_partition_1.column_statistics.len(),
            1,
            "should have only 1 column for the ProjectExec C@0"
        );

        // partition 0 == ranges_for_file_0
        let expected = ranges_for_file_0[2];
        assert_eq!(actual_partition_0.column_statistics[0], expected.into());

        // partition 1 == ranges_for_file_1
        let expected = ranges_for_file_1[2];
        assert_eq!(actual_partition_1.column_statistics[0], expected.into());

        Ok(())
    }

    #[test]
    fn test_extracts_multiple_cols_at_once() -> Result<(), DataFusionError> {
        // plan with multiple fields
        let mut fields = vec![
            Field::new("_not_used_file_col", DataType::Int64, true),
            Field::new("b", DataType::Int64, true),
            Field::new("c", DataType::Int64, true),
        ];
        let file_schema = Arc::new(arrow::datatypes::Schema::new(fields.clone()));
        let plan_schema = Arc::new(arrow::datatypes::Schema::new(fields.split_off(1)));

        // File scan config uses the file schema and ALL columns in file.
        let ranges_for_file_0 = vec![
            // col _not_used_file_col
            SortKeyRange {
                min: Some(20),
                max: Some(30),
                null_count: 1,
            },
            // col b
            SortKeyRange {
                min: Some(2000),
                max: Some(3000),
                null_count: 1,
            },
            // col c
            SortKeyRange {
                min: Some(200_000),
                max: Some(300_000),
                null_count: 1,
            },
        ];
        let ranges_for_file_1 = vec![
            // col _not_used_file_col
            SortKeyRange {
                min: Some(30),
                max: Some(40),
                null_count: 1,
            },
            // col b
            SortKeyRange {
                min: Some(1000),
                max: Some(2000),
                null_count: 1,
            },
            // col c
            SortKeyRange {
                min: Some(500_000),
                max: Some(700_000),
                null_count: 1,
            },
        ];

        // make file config, using file schema
        let lex_ordering_on_c = LexOrdering::new(vec![
            PhysicalSortExpr::new(col("c", &file_schema)?, Default::default()),
            PhysicalSortExpr::new(col("b", &file_schema)?, Default::default()),
        ])
        .unwrap();
        let multiple_column_key_ranges_per_file = PartitionedFilesAndRanges {
            per_file: vec![ranges_for_file_0.clone(), ranges_for_file_1.clone()],
        };
        let filegroups_config_builder = file_scan_config_builder(
            &file_schema, // file_schema
            vec![lex_ordering_on_c],
            multiple_column_key_ranges_per_file,
        );

        // make plan config, using a plan with only cols b & c
        let data_source_exec = DataSourceExec::from_data_source(
            filegroups_config_builder
                .with_projection(Some(vec![1, 2]))
                .build(),
        ) as _;
        let proj_c = col("c", &plan_schema)?; // plan_schema
        let proj_b = col("b", &plan_schema)?; // plan_schema
        // plan reverses the 2 cols, c then b
        let plan = proj_exec(
            &data_source_exec,
            vec![(proj_c, "c".into()), (proj_b, "b".into())],
        );
        insta::assert_snapshot!(
            displayable(plan.as_ref()).indent(true),
            @r"
        ProjectionExec: expr=[c@1 as c, b@0 as b]
          DataSourceExec: file_groups={2 groups: [[0.parquet], [1.parquet]]}, projection=[b, c], output_ordering=[c@1 ASC, b@0 ASC], file_type=parquet
        ",
        );

        /* Test Case: the statistics_by_partition will still extract the proper file_stats for both cols c and b */
        let actual = statistics_by_partition(plan.as_ref())?;
        let [actual_partition_0, actual_partition_1] = &actual[..] else {
            panic!("should have stats for 2 partitions");
        };

        // partition 0 == ranges_for_file_0
        // use cols c then b, in reverse order for the projection [2..=1]
        let expected: Vec<ColumnStatistics> = ranges_for_file_0[1..=2]
            .iter()
            .rev()
            .map(|sort_range| (*sort_range).into())
            .collect_vec();
        assert_eq!(
            &expected,
            &[
                // col c, partition 0
                ColumnStatistics {
                    null_count: Precision::Exact(1),
                    min_value: Precision::Exact(datafusion::scalar::ScalarValue::Int32(Some(
                        200_000
                    ))),
                    max_value: Precision::Exact(datafusion::scalar::ScalarValue::Int32(Some(
                        300_000
                    ))),
                    distinct_count: Precision::Absent,
                    sum_value: Precision::Absent,
                },
                // col b, partition 0
                ColumnStatistics {
                    null_count: Precision::Exact(1),
                    min_value: Precision::Exact(datafusion::scalar::ScalarValue::Int32(Some(2000))),
                    max_value: Precision::Exact(datafusion::scalar::ScalarValue::Int32(Some(3000))),
                    distinct_count: Precision::Absent,
                    sum_value: Precision::Absent,
                },
            ]
        );
        assert_eq!(actual_partition_0.column_statistics, expected);

        // partition 1 == ranges_for_file_1
        // use cols c then b, in reverse order for the projection [2..=1]
        let expected: Vec<ColumnStatistics> = ranges_for_file_1[1..=2]
            .iter()
            .rev()
            .map(|sort_range| (*sort_range).into())
            .collect_vec();
        assert_eq!(
            &expected,
            &[
                // col c, partition 1
                ColumnStatistics {
                    null_count: Precision::Exact(1),
                    min_value: Precision::Exact(datafusion::scalar::ScalarValue::Int32(Some(
                        500_000
                    ))),
                    max_value: Precision::Exact(datafusion::scalar::ScalarValue::Int32(Some(
                        700_000
                    ))),
                    distinct_count: Precision::Absent,
                    sum_value: Precision::Absent,
                },
                // col b, partition 1
                ColumnStatistics {
                    null_count: Precision::Exact(1),
                    min_value: Precision::Exact(datafusion::scalar::ScalarValue::Int32(Some(1000))),
                    max_value: Precision::Exact(datafusion::scalar::ScalarValue::Int32(Some(2000))),
                    distinct_count: Precision::Absent,
                    sum_value: Precision::Absent,
                },
            ]
        );
        assert_eq!(actual_partition_1.column_statistics, expected);

        Ok(())
    }

    #[test]
    fn test_will_not_extract_for_non_passthru_projections() -> Result<(), DataFusionError> {
        // plan with multiple fields
        let file_schema = Arc::new(arrow::datatypes::Schema::new(vec![
            Field::new("a", DataType::Int64, true),
            Field::new("b", DataType::Int64, true),
            Field::new("c", DataType::Int64, true),
        ]));
        let plan_schema = Arc::new(arrow::datatypes::Schema::new(vec![
            Field::new("b", DataType::Int64, true), // will be `a + 1 as b``
            Field::new("c", DataType::Int64, true),
        ]));

        // file stats for 3 columns in file_schema
        let ranges_for_file = vec![
            // col a
            SortKeyRange {
                min: Some(20),
                max: Some(30),
                null_count: 1,
            },
            // col b
            SortKeyRange {
                min: Some(2000),
                max: Some(3000),
                null_count: 1,
            },
            // col c
            SortKeyRange {
                min: Some(200_000),
                max: Some(300_000),
                null_count: 1,
            },
        ];

        // make scan plan
        let multiple_column_key_ranges_per_file = PartitionedFilesAndRanges {
            per_file: vec![ranges_for_file],
        };
        let filegroups_config_builder = file_scan_config_builder(
            &file_schema, // file_schema
            vec![],
            multiple_column_key_ranges_per_file,
        );
        let scan = DataSourceExec::from_data_source(
            filegroups_config_builder
                .with_projection(Some(vec![0, 1, 2]))
                .build(),
        ) as _;

        // make projection which modifies columns and aliases to an existing columns
        let pass_thru_c = col("c", &file_schema)?;
        let make_new = Arc::new(BinaryExpr::new(
            col("a", &file_schema)?,
            Operator::Minus,
            lit(1),
        )) as _;
        let plan = proj_exec(
            &scan,
            vec![(make_new, "b".into()), (pass_thru_c, "c".into())],
        );
        insta::assert_snapshot!(
            displayable(plan.as_ref()).indent(true),
            @r"
        ProjectionExec: expr=[a@0 - 1 as b, c@2 as c]
          DataSourceExec: file_groups={1 group: [[0.parquet]]}, projection=[a, b, c], file_type=parquet
        ",
        );
        assert_eq!(
            plan.schema(),
            plan_schema,
            "should have plan schema with the 2 final projected columns"
        );

        /* Test Case: the statistics_by_partition will still extract the proper file_stats for both cols c and b */
        let actual = statistics_by_partition(plan.as_ref())?;

        // will selectively detect which projection exprs are not a pass thru, and return absent statistics
        assert_eq!(
            &actual[0].column_statistics,
            &[
                // a + 1 as b, partition 0
                ColumnStatistics::new_unknown(),
                // col c, partition 0
                ColumnStatistics {
                    null_count: Precision::Exact(1),
                    min_value: Precision::Exact(datafusion::scalar::ScalarValue::Int32(Some(
                        200_000
                    ))),
                    max_value: Precision::Exact(datafusion::scalar::ScalarValue::Int32(Some(
                        300_000
                    ))),
                    distinct_count: Precision::Absent,
                    sum_value: Precision::Absent,
                },
            ]
        );

        Ok(())
    }
}
