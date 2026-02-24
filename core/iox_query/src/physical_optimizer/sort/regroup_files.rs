use std::{cmp::Ordering, sync::Arc};

use crate::{
    physical_optimizer::sort::extract_ranges::extract_ranges_from_files, provider::DeduplicateExec,
};
use datafusion::{
    common::{
        HashMap, internal_datafusion_err,
        tree_node::{Transformed, TreeNodeRecursion},
    },
    datasource::{
        listing::{FileRange, PartitionedFile},
        physical_plan::{FileGroup, FileScanConfig},
        source::DataSourceExec,
    },
    error::Result,
    physical_expr::LexOrdering,
    physical_plan::{ExecutionPlan, sorts::sort::SortExec},
};
use itertools::Itertools;
use object_store::path::Path;
use tracing::trace;

/// This function is to split all files in the same ParquetSource into different groups/DF partitions and
/// set the `preserve_partitioning` so they will be executed sequentially. The files will later be re-ordered
/// (if non-overlapping) by lexical range.
///
/// For [`PartitionedFile`]s from the same file source, these have the same (per-file) lexical range.
/// Therefore regroup these files together.
pub(crate) fn split_and_regroup_parquet_files(
    plan: Arc<dyn ExecutionPlan>,
    ordering_req: &LexOrdering,
) -> Result<Transformed<Arc<dyn ExecutionPlan>>> {
    if let Some(sort_exec) = plan.as_any().downcast_ref::<SortExec>()
        && (!sort_exec
            .properties()
            .equivalence_properties()
            .ordering_satisfy(ordering_req.iter().cloned())?
            || !sort_exec.preserve_partitioning())
    {
        // halt on DAG branch
        Ok(Transformed::new(plan, false, TreeNodeRecursion::Jump))
    } else if plan.as_any().downcast_ref::<DeduplicateExec>().is_some() {
        Ok(Transformed::new(plan, false, TreeNodeRecursion::Jump))
    } else if let Some(data_source_exec) = plan.as_any().downcast_ref::<DataSourceExec>()
        && let Some(transformed) =
            transform_data_source_exec_to_regrouped_disjoint_ranges(data_source_exec, ordering_req)?
    {
        Ok(Transformed::yes(transformed))
    } else {
        Ok(Transformed::no(plan))
    }
}

/// Transform a DataSourceExec with N files in various groupings,
/// into a DataSourceExec into N groups each include one file. This singular file
/// may actually be multiple ranged-scans of the same file.
///
/// This function will return None if
///   - There are no statsitics for the given column (including the when the column is missing from the file
///     and produce null values that leads to absent statistics)
///   - Some files overlap (the min/max ranges cannot be made disjoint)
///
/// The output DataSourceExec's are ordered to match the provided [`LexOrdering`].
///
/// For example:
/// ```text
/// DataSourceExec(groups=[[file1,file2], [file3]])
/// ```
/// Is rewritten so each file is in its own group and the files are lex ordered.
/// ```text
/// DataSourceExec(groups=[[file1], [file2], [file3]])
/// ```
///
/// For example with ranged scans:
/// ```text
/// DataSourceExec(groups=[[file1:0..10,file2], [file1:10..20]])
/// ```
/// Is rewritten to group the ranged scans together.
/// ```text
/// DataSourceExec(groups=[[file1:0..10,file1:10..20], [file2]])
/// ```
pub(crate) fn transform_data_source_exec_to_regrouped_disjoint_ranges(
    data_source_exec: &DataSourceExec,
    ordering_req: &LexOrdering,
) -> Result<Option<Arc<dyn ExecutionPlan>>> {
    let Some(cfg) = data_source_exec
        .data_source()
        .as_any()
        .downcast_ref::<FileScanConfig>()
    else {
        return Ok(None);
    };

    // Extract partitioned files from the DataSourceExec
    let files = cfg
        .file_groups
        .iter()
        .flat_map(|g| g.iter())
        .collect::<Vec<_>>();
    let schema = Arc::clone(&cfg.file_schema);

    // Regroup partitioned files from same file source
    let regrouped_files = group_same_file_sources(files)?;

    // extract disjoint lexical ranges
    // if cannot find, then is not disjoint
    let Some(lexical_ranges) = extract_ranges_from_files(ordering_req, &regrouped_files, schema)?
    else {
        return Ok(None);
    };

    trace!(
        "disjoint ranges from regrouped parquet files\n  {}",
        lexical_ranges
            .ordered_ranges()
            .map(|r| r.to_string())
            .collect::<Vec<_>>()
            .join("\n  ")
    );

    // reorder partitioned files by lexical indices
    let indices = lexical_ranges.indices();
    if indices.len() != regrouped_files.len() {
        return Err(internal_datafusion_err!(
            "should have every file group listed in the sorted indices"
        ));
    }
    let mut new_partitioned_file_groups = regrouped_files
        .into_iter()
        .enumerate()
        .map(|(file_idx, filegroup)| {
            (
                indices
                    .iter()
                    .position(|sorted_idx| *sorted_idx == file_idx)
                    .expect("file should be listed in indices"),
                filegroup,
            )
        })
        .collect::<Vec<_>>();
    new_partitioned_file_groups.sort_by_key(|(idx, _)| *idx);
    let new_partitioned_file_groups = new_partitioned_file_groups
        .into_iter()
        .map(|(_, filegroup)| filegroup)
        .collect_vec();

    // Assigned new partitioned file groups to the new base config,
    // and update the output ordering to match the transformed ordering.
    let mut new_file_scan_config = cfg.clone();
    new_file_scan_config.file_groups = new_partitioned_file_groups;

    Ok(Some(DataSourceExec::from_data_source(new_file_scan_config)))
}

/// Group [`PartitionedFile`] which come from the same file source, into the same
/// [`Vec<PartitionedFile>`] group.
///
/// If the partitioned files (a.k.a. scanned ranges) are adjacent to each other, then combine
/// into a single range scan.
///
/// Returns an error if the file partitions, from the same file, are overlapping.
fn group_same_file_sources(files: Vec<&PartitionedFile>) -> Result<Vec<FileGroup>> {
    let mut map: HashMap<&Path, Vec<PartitionedFile>> = HashMap::with_capacity(files.len());

    for file in files {
        let group = map
            .entry_ref(&file.object_meta.location)
            .or_insert_with(Vec::new);

        if !group.is_empty() {
            let (Some(file_range0), Some(file_range1)) =
                (&group[group.len() - 1].range, &file.range)
            else {
                return Err(internal_datafusion_err!(
                    "should not be rescanning the same file, unless scanning a range only"
                ));
            };

            if overlaps(file_range0, file_range1) {
                return Err(internal_datafusion_err!(
                    "should not perform a file scan of overlapping ranges within same file"
                ));
            }
        }
        group.push(file.clone());
    }

    Ok(map
        .into_values()
        .map(combine_adjacent_ranges_from_same_filegroup)
        .map(FileGroup::new)
        .collect())
}

/// If the partitioned files (a.k.a. scanned ranges) are adjacent to each other, than combine
/// into a single range scan.
///
/// This is required to handle the limitation that scanned ranges are each given the statistics for the entire file:
///     * The FileScanConfig uses the same [`FileScanConfig::file_schema`](datafusion::datasource::physical_plan::FileScanConfig::file_schema)
///         for each partitioned file in the group.
///     * Therefore the MinMaxStatistics will be the same for each partitioned file.
///     * Therefore the `MinMaxStatistics::is_sorted` will return false.
///     * Therefore the [`FileScanConfig::project`](datafusion::datasource::physical_plan::FileScanConfig::project)
///         will not find a sort ordering for the group. Refer to:
///         <https://github.com/apache/datafusion/blob/9d06baf45298bc736966d0239fa78ec7a434ca54/datafusion/datasource/src/file_scan_config.rs#L1470>
///
fn combine_adjacent_ranges_from_same_filegroup(
    filegroup: Vec<PartitionedFile>,
) -> Vec<PartitionedFile> {
    filegroup.into_iter().fold(vec![], |mut acc, file| {
        if acc.is_empty() {
            return vec![file];
        }
        let last = acc.len() - 1;
        if let Some(merged) = merge_adjacent_partitioned_files(&acc[last], &file) {
            acc[last] = merged;
        } else {
            acc.push(file);
        }
        acc
    })
}

fn overlaps(range0: &FileRange, range1: &FileRange) -> bool {
    match range0.start.cmp(&range1.start) {
        Ordering::Equal => true,
        Ordering::Less => range0.end > range1.start, // range.end is not inclusive
        Ordering::Greater => range1.end > range0.start,
    }
}

/// Returns a merged file, if the partitioned files are adjacent table scans.
///
/// Returns None if the ranged scans cannot be considered as an adjacent, continous singular scan.
fn merge_adjacent_partitioned_files(
    f0: &PartitionedFile,
    f1: &PartitionedFile,
) -> Option<PartitionedFile> {
    if is_adjacent(&f0.range, &f1.range) && eq_partitioned_files(f0, f1) {
        let mut merged = f0.clone();
        merged.range = Some(FileRange {
            start: merged.range.expect("exists").start,
            end: f1.range.clone().expect("exists").end,
        });
        Some(merged)
    } else {
        None
    }
}

fn is_adjacent(range0: &Option<FileRange>, range1: &Option<FileRange>) -> bool {
    matches!((range0, range1), (Some(range0), Some(range1)) if range0.end == range1.start)
}

/// Returns true if the [`PartitionedFile`]s are equal, excluding consideration of ranges.
fn eq_partitioned_files(f0: &PartitionedFile, f1: &PartitionedFile) -> bool {
    f0.object_meta == f1.object_meta // same file location (unique ID)
        && f0.partition_values == f1.partition_values // appended values are the same
}

#[cfg(test)]
mod tests {
    use super::*;

    use std::sync::Arc;

    use arrow::{compute::SortOptions, datatypes::Schema};
    use datafusion::{
        datasource::provider_as_source,
        logical_expr::LogicalPlanBuilder,
        physical_expr::{LexOrdering, PhysicalSortExpr},
        physical_plan::expressions::col,
        scalar::ScalarValue,
    };
    use executor::DedicatedExecutor;
    use schema::sort::SortKey;

    use crate::{
        QueryChunk,
        exec::{Executor, ExecutorConfig},
        physical_optimizer::sort::extract_ranges::min_max_for_partitioned_filegroup,
        provider::ProviderBuilder,
        test::{TestChunk, format_execution_plan},
    };

    fn pretty_str_mins_maxes(mins_maxs: &[Option<(ScalarValue, ScalarValue)>]) -> String {
        mins_maxs
            .iter()
            .map(|maybe_min_max| {
                if let Some((min, max)) = maybe_min_max {
                    format!("({min})->({max})")
                } else {
                    "None".to_string()
                }
            })
            .join(", ")
    }

    /// Create 5 parquet files,
    /// which are scanned in 8 partitioned file bits,
    /// and group into 4 starting file groups:
    ///     * [4.parquet:0..100000000, 8.parquet:0..25000000]
    ///     * [8.parquet:25000000..100000000, 5.parquet:0..50000000]
    ///     * [5.parquet:50000000..100000000, 6.parquet:0..75000000]
    ///     * [6.parquet:75000000..100000000, 7.parquet:0..100000000]
    ///
    /// These partitioned file scans are nonoverlapping since each scan stops where the next starts.
    ///     e.g. 8.parquet:0..25000000 then 8.parquet:25000000..100000000
    ///     `range.end` is exclusive
    ///
    async fn build_test_case_with_nonoverlapping_ranged_scans()
    -> (Arc<Schema>, LexOrdering, Arc<dyn ExecutionPlan>) {
        // DF session setup
        let config = ExecutorConfig {
            target_query_partitions: 4.try_into().unwrap(),
            ..ExecutorConfig::testing()
        };
        let exec = Executor::new_with_config_and_executor(config, DedicatedExecutor::new_testing());
        let ctx = exec.new_context();
        let state = ctx.inner().state();

        // chunks
        let c = TestChunk::new("t")
            .with_row_count(1_000_000)
            .with_tag_column("tag")
            .with_sort_key(SortKey::from_columns([Arc::from("tag"), Arc::from("time")]))
            .with_dummy_parquet_file_and_size(100000000)
            .with_may_contain_pk_duplicates(false);

        // add 5 non-overlapping files
        let c_file_3 = c
            .clone()
            .with_time_column_with_full_stats(Some(65), Some(69), None);
        let c_file_4 = c
            .clone()
            .with_time_column_with_full_stats(Some(60), Some(64), None);
        let c_file_5 = c
            .clone()
            .with_time_column_with_full_stats(Some(55), Some(58), None);
        let c_file_6 = c
            .clone()
            .with_time_column_with_full_stats(Some(50), Some(54), None);
        let c_file_7 = c
            .clone()
            .with_time_column_with_full_stats(Some(45), Some(49), None);

        // Schema & provider
        let schema = c_file_3.schema().clone();
        let provider = ProviderBuilder::new("t".into(), schema.clone())
            // add non-overlapped chunks in random order
            .add_chunk(Arc::new(c_file_7.with_id(8).with_order(8)))
            .add_chunk(Arc::new(c_file_3.with_id(4).with_order(4)))
            .add_chunk(Arc::new(c_file_5.with_id(6).with_order(6)))
            .add_chunk(Arc::new(c_file_6.with_id(7).with_order(7)))
            .add_chunk(Arc::new(c_file_4.with_id(5).with_order(5)))
            .build()
            .unwrap();

        // build logical plan
        let plan = LogicalPlanBuilder::scan(
            "t".to_owned(),
            provider_as_source(Arc::new(provider.clone())),
            None,
        )
        .unwrap()
        .build()
        .unwrap();

        // Reproducer: use the same starting DataSourceExec as the test case
        let plan = state.create_physical_plan(&plan).await.unwrap();

        // Reproducer: use the same ordering as the test case
        let arrow_schema = schema.as_arrow();
        let sort_ordering = LexOrdering::new(vec![PhysicalSortExpr::new(
            col("time", &arrow_schema).unwrap(),
            SortOptions::default(),
        )])
        .unwrap();

        (arrow_schema, sort_ordering, plan)
    }

    /// Reproducer for the starting point of the `DataSourceExec` (without regrouping) for
    /// the test case [`order_union_sorted_inputs::tests::test_many_partition_files`].
    #[tokio::test]
    async fn test_repartitioned_files_are_overlapping_without_regrouping() {
        // Reproducer: the same starting DataSourceExec as the test case
        let (arrow_schema, _, plan) = build_test_case_with_nonoverlapping_ranged_scans().await;
        insta::assert_yaml_snapshot!(
            format_execution_plan(&plan),
            @r#"- " DataSourceExec: file_groups={4 groups: [[4.parquet:0..100000000, 8.parquet:0..25000000], [8.parquet:25000000..100000000, 5.parquet:0..50000000], [5.parquet:50000000..100000000, 6.parquet:0..75000000], [6.parquet:75000000..100000000, 7.parquet:0..100000000]]}, projection=[tag, time], file_type=parquet""#
        );

        // Reproducer: the DataSourceExec has a total of 8 partitioned file scans.
        let data_source_exec = plan.as_any().downcast_ref::<DataSourceExec>().unwrap();
        let file_scan_config = data_source_exec
            .data_source()
            .as_any()
            .downcast_ref::<FileScanConfig>()
            .unwrap();
        let partitioned_files = file_scan_config
            .file_groups
            .iter()
            .flat_map(|g| g.iter())
            .collect::<Vec<_>>();
        assert_eq!(partitioned_files.len(), 8);

        // Test Case: the partitioned file fragements provide min/max stats that are overlapped,
        // since the col stats are for the entire file (not the partitioned range).
        let min_maxes = partitioned_files
            .into_iter()
            .map(|f| {
                min_max_for_partitioned_filegroup(
                    "time",
                    &FileGroup::new(vec![f.clone()]),
                    &arrow_schema,
                )
                .expect("should get min/max")
            })
            .collect_vec();
        insta::assert_yaml_snapshot!(
            pretty_str_mins_maxes(&min_maxes),
            @r#"
        "(65)->(69), (45)->(49), (45)->(49), (60)->(64), (60)->(64), (55)->(58), (55)->(58), (50)->(54)"
        "#
        );
    }

    /// Reproducer for the solution required (regrouping) for
    /// the test case [`order_union_sorted_inputs::tests::test_many_partition_files`].
    ///
    /// This demonstrates that regrouping can occur,
    /// which is required to create a nonoverlapping range.
    /// Such that the SPM can be replaced with a progressive eval.
    #[tokio::test]
    async fn test_after_regrouping_have_nonoverlapping_file_groups() {
        // Reproducer: the same starting DataSourceExec as the test case
        let (arrow_schema, sort_ordering, plan) =
            build_test_case_with_nonoverlapping_ranged_scans().await;
        insta::assert_yaml_snapshot!(
            format_execution_plan(&plan),
            @r#"- " DataSourceExec: file_groups={4 groups: [[4.parquet:0..100000000, 8.parquet:0..25000000], [8.parquet:25000000..100000000, 5.parquet:0..50000000], [5.parquet:50000000..100000000, 6.parquet:0..75000000], [6.parquet:75000000..100000000, 7.parquet:0..100000000]]}, projection=[tag, time], file_type=parquet""#
        );

        // Test Case: partitioned files will be regrouped based on same file source
        // e.g. file `8.parquet` range scans are together: `[8.parquet:0..25000000, 8.parquet:25000000..100000000]`
        let data_source_exec = plan.as_any().downcast_ref::<DataSourceExec>().unwrap();
        let Some(regrouped_plan) = transform_data_source_exec_to_regrouped_disjoint_ranges(
            data_source_exec,
            &sort_ordering,
        )
        .unwrap() else {
            panic!("should regroup into nonoverlapping (disjoint) file groups");
        };
        insta::assert_yaml_snapshot!(
            format_execution_plan(&regrouped_plan),
            @r#"- " DataSourceExec: file_groups={5 groups: [[8.parquet:0..100000000], [7.parquet:0..100000000], [6.parquet:0..100000000], [5.parquet:0..100000000], [4.parquet:0..100000000]]}, projection=[tag, time], output_ordering=[tag@0 ASC, time@1 ASC], file_type=parquet""#
        );

        // Test Case: the regrouped filegroups are non-overlapped.
        let regrouped_data_source_exec = regrouped_plan
            .as_any()
            .downcast_ref::<DataSourceExec>()
            .unwrap();
        let file_scan_config = regrouped_data_source_exec
            .data_source()
            .as_any()
            .downcast_ref::<FileScanConfig>()
            .unwrap();
        let min_maxes = file_scan_config
            .file_groups
            .iter()
            .map(|file_group| {
                min_max_for_partitioned_filegroup("time", file_group, &arrow_schema)
                    .expect("should get min/max")
            })
            .collect_vec();
        insta::assert_yaml_snapshot!(
            pretty_str_mins_maxes(&min_maxes),
            @r#"
        "(45)->(49), (50)->(54), (55)->(58), (60)->(64), (65)->(69)"
        "#
        );
    }
}
