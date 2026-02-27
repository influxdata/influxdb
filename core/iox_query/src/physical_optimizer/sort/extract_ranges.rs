//! Extract [`NonOverlappingOrderedLexicalRanges`] from different sources.

use arrow::compute::SortOptions;
use arrow::datatypes::Schema;
use datafusion::common::{
    ColumnStatistics, Result, ScalarValue, Statistics, internal_datafusion_err,
};
use datafusion::datasource::physical_plan::FileGroup;
use datafusion::physical_expr::LexOrdering;
use datafusion::physical_expr::expressions::Column;
use datafusion::physical_plan::{ExecutionPlan, ExecutionPlanProperties, displayable};
use itertools::FoldWhile;
use itertools::Itertools;
use std::sync::Arc;
use tracing::trace;

use crate::physical_optimizer::sort::lexical_range::{
    LexicalRange, NonOverlappingOrderedLexicalRanges,
};
use crate::statistics::partition_statistics::util::merge_col_stats;
use crate::statistics::{column_statistics_min_max, statistics_by_partition};

/// Attempt to extract LexicalRanges for the given sort keys and input plan
///
/// Output will have N ranges where N is the number of output partitions
///
/// Returns None if not possible to determine ranges, or if not disjoint.
///
/// Note that this method confirms that between each partition it is properly ordered, and has non-overlapping ranges.
/// It does not ensure that within each partition the data is properly sorted.
/// This nuance is important to support plans where the within parition data get immediately sorted:
/// ```text
/// ProgressiveEvalExec
///   SortExec: expr=[time@1 DESC], preserve_partitioning=[true]
///      DataSourceExec: file_groups={2 groups: [[newer_data.parquet], [old_data.parquet]]}, output_ordering=[time@1 ASC]
/// ```
pub(crate) fn extract_disjoint_ranges_from_plan(
    exprs: &LexOrdering,
    input_plan: &Arc<dyn ExecutionPlan>,
) -> Result<Option<NonOverlappingOrderedLexicalRanges>> {
    let Some(ranges_per_partition) = extract_ranges_from_plan(exprs, input_plan)? else {
        return Ok(None);
    };

    // determine if using a partial sortkey, due to absence of stats.
    let cnt_sort_keys_used = ranges_per_partition[0].num_values();
    let is_partial_sortkey = cnt_sort_keys_used < exprs.len();
    if cnt_sort_keys_used == 0 {
        return Ok(None);
    }

    // collect the sort_options for the keys used
    let sort_options = exprs
        .iter()
        .take(cnt_sort_keys_used)
        .map(|e| e.options)
        .collect::<Vec<_>>();

    let Some(nonoverlapping_ranges) =
        NonOverlappingOrderedLexicalRanges::try_new(&sort_options, ranges_per_partition)?
    else {
        return Ok(None);
    };

    // cannot use only a partial sort key comparison, if the sort key comparison is only constants.
    // e.g. for sort_keys [col0 desc, col1 desc, col2 asc], applied to 3 partitions:
    // if we have known lexical ranges for col0 & col1 of:
    //    * partition1= (a,10)->(a,10)
    //    * partition2= (a,10)->(a,10)  // touches, but is nonoverlapping
    //    * partition3= (b,10)->(b,10)  // disjoint
    // The partial key comparison [col0 desc, col1 desc] looks disjoint (a.k.a. can use ProgressiveEval).
    //
    // However, since this is only the partial sort key, we cannot know the correct ordering of partition1 and partition2
    // since their actual complete sort key values are (a,10,unknown)->(a,10,unknown).
    if is_partial_sortkey && nonoverlapping_ranges.has_neighbors_with_same_sortkey() {
        Ok(None)
    } else {
        Ok(Some(nonoverlapping_ranges))
    }
}

/// Extract [`Vec<LexicalRanges>`] from plan, per partition, regardless of whether disjoint or not.
pub(crate) fn extract_ranges_from_plan(
    exprs: &LexOrdering,
    input_plan: &Arc<dyn ExecutionPlan>,
) -> Result<Option<Vec<LexicalRange>>> {
    trace!(
        "Extracting lexical ranges for input plan: \n{}",
        displayable(input_plan.as_ref()).indent(true)
    );

    if !input_plan
        .properties()
        .equivalence_properties()
        .ordering_satisfy(exprs.iter().cloned())?
    {
        return Ok(None);
    }

    let num_input_partitions = partition_count(input_plan);

    // One builder for each output partition.
    // Each builder will contain multiple sort keys
    let mut builders = vec![LexicalRange::builder(); num_input_partitions];

    // get partitioned stats for the plan
    let partitioned_stats = statistics_by_partition(input_plan.as_ref())?;

    // add per sort key
    //
    // halting once we hit a sort key with missing statistics. This allows us to still detect non-overlapping
    // based upon the start of a sort key, if this partial sort key is sufficient to cause disjointness (nonoverlapping).
    for sort_expr in exprs {
        let Some(column) = sort_expr.expr.as_any().downcast_ref::<Column>() else {
            return Ok(None);
        };
        let Ok(col_idx) = input_plan.schema().index_of(column.name()) else {
            return Ok(None);
        };

        // gather min & max stats per partition
        let mut minmax_values = Vec::with_capacity(partitioned_stats.len());
        for stats_for_partition in &partitioned_stats {
            let Some((min, max)) = min_max_for_stats_with_sort_options(
                col_idx,
                &sort_expr.options,
                stats_for_partition,
            )?
            else {
                // no stats found. halt at this sort key.
                minmax_values = vec![];
                break;
            };

            if min.is_null() && max.is_null() {
                // stats are NULL. halt at this sort key.
                minmax_values = vec![];
                break;
            }

            minmax_values.push((min, max));
        }

        if minmax_values.is_empty() {
            // do not add for this sort key, and halt.
            break;
        } else {
            // add partition min/max stats to the comparison sort key builder.
            for (builder, (min, max)) in builders.iter_mut().zip(minmax_values) {
                builder.push(min, max);
            }
        }
    }

    let ranges_per_partition = builders
        .into_iter()
        .map(|builder| builder.build())
        .collect::<Vec<_>>();
    trace!(
        "Found lexical ranges: {:?}",
        ranges_per_partition
            .iter()
            .map(|lr| lr.to_string())
            .join(", ")
    );

    Ok(Some(ranges_per_partition))
}

fn partition_count(plan: &Arc<dyn ExecutionPlan>) -> usize {
    plan.output_partitioning().partition_count()
}

/// Returns the min and max value for the [`Statistics`],
/// and handles null values based on [`SortOptions`].
///
/// Returns None if statistics are absent/unknown.
fn min_max_for_stats_with_sort_options(
    col_idx: usize,
    sort_options: &SortOptions,
    stats: &Statistics,
) -> Result<Option<(ScalarValue, ScalarValue)>> {
    // Check if the column is a constant value according to the equivalence properties (TODO)

    let Some(ColumnStatistics {
        max_value,
        min_value,
        null_count,
        ..
    }) = stats.column_statistics.get(col_idx)
    else {
        return Err(internal_datafusion_err!(
            "extracted statistics is missing @{:?}, only has {:?} columns",
            col_idx,
            stats.column_statistics.len()
        ));
    };

    let (Some(min), Some(max)) = (min_value.get_value(), max_value.get_value()) else {
        return Ok(None);
    };

    let mut min = min.clone();
    let mut max = max.clone();
    if *null_count.get_value().unwrap_or(&0) > 0 {
        let nulls_as_min = !sort_options.descending && sort_options.nulls_first // ASC nulls first
    || sort_options.descending && !sort_options.nulls_first; // DESC nulls last

        // Get the typed null value for the data type of min/max
        let null: ScalarValue = min.data_type().try_into()?;

        if nulls_as_min {
            min = null;
        } else {
            max = null;
        }
    }

    Ok(Some((min, max)))
}

/// Attempt to extract LexicalRanges for the given sort keys and partitioned file groups.
///
/// Since the goal is to have as many non-overlapping ranges as possible, by determinining
/// the set of ranges. Each range may contain >1 file.
///
/// Therefore we expect M disjoint ranges for N files.
///
/// Returns None if not possible to determine disjoint ranges.
pub(crate) fn extract_ranges_from_files(
    exprs: &LexOrdering,
    file_groups: &[FileGroup],
    schema: Arc<Schema>,
) -> Result<Option<NonOverlappingOrderedLexicalRanges>> {
    trace!(
        "Extracting lexical ranges for {:?} file groups",
        file_groups.len()
    );
    let num_input_partitions = file_groups.len();

    // one builder for each output partition
    let mut builders = vec![LexicalRange::builder(); num_input_partitions];
    for sort_expr in exprs {
        let Some(column) = sort_expr.expr.as_any().downcast_ref::<Column>() else {
            return Ok(None);
        };
        let col_name = column.name();

        for (grouped_partitioned_files, builder) in file_groups.iter().zip(builders.iter_mut()) {
            let Some((min, max)) =
                min_max_for_partitioned_filegroup(col_name, grouped_partitioned_files, &schema)?
            else {
                return Ok(None);
            };
            builder.push(min, max);
        }
    }

    let sort_options = exprs.iter().map(|e| e.options).collect::<Vec<_>>();
    let ranges_per_partition = builders
        .into_iter()
        .map(|builder| builder.build())
        .collect::<Vec<_>>();

    trace!("Found lexical ranges: {:?}", ranges_per_partition);

    NonOverlappingOrderedLexicalRanges::try_new(&sort_options, ranges_per_partition)
}

/// Return the min and max value for the specified group of partitioned files.
pub(crate) fn min_max_for_partitioned_filegroup(
    col_name: &str,
    filegroup: &FileGroup,
    schema: &Arc<Schema>,
) -> Result<Option<(ScalarValue, ScalarValue)>> {
    let Some((col_idx, _)) = schema.fields().find(col_name) else {
        return Ok(None);
    };

    // from all files in group
    let Some(merged_col_stats) = filegroup
        .iter()
        .map(|file| {
            file.statistics
                .as_ref()
                .map(|has_stats| has_stats.column_statistics[col_idx].clone())
        })
        .fold_while(None, |acc, maybe_col_stats| match (acc, maybe_col_stats) {
            (_, None) => FoldWhile::Done(None),
            (None, Some(col_stats)) => FoldWhile::Continue(Some(col_stats)),
            (Some(acc_col_stats), Some(col_stats)) => {
                FoldWhile::Continue(Some(merge_col_stats(acc_col_stats, &col_stats)))
            }
        })
        .into_inner()
    else {
        // missing col_stats on one of the partitioned files in the group
        return Ok(None);
    };

    // `column_statistics_min_max` considers precision
    Ok(column_statistics_min_max(merged_col_stats))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::test::test_utils::{
        SortKeyRange, data_source_exec_parquet_with_sort_with_statistics,
        data_source_exec_parquet_with_sort_with_statistics_and_schema, sort_exec, union_exec,
    };
    use std::fmt::{Debug, Display, Formatter};

    use arrow::compute::SortOptions;
    use arrow::datatypes::{DataType, Field};
    use datafusion::{
        physical_expr::{LexOrdering, PhysicalSortExpr},
        physical_plan::expressions::col,
    };
    use insta::assert_snapshot;

    /// test with three partition ranges that are disjoint (non overlapping)
    fn test_case_disjoint_3() -> TestCaseBuilder {
        TestCaseBuilder::new()
            .with_key_range(SortKeyRange {
                min: Some(1000),
                max: Some(2000),
                null_count: 0,
            })
            .with_key_range(SortKeyRange {
                min: Some(2001),
                max: Some(3000),
                null_count: 0,
            })
            .with_key_range(SortKeyRange {
                min: Some(3001),
                max: Some(3500),
                null_count: 0,
            })
    }

    /// test with three partition ranges that are NOT disjoint (are overlapping)
    fn test_case_overlapping_3() -> TestCaseBuilder {
        TestCaseBuilder::new()
            .with_key_range(SortKeyRange {
                min: Some(1000),
                max: Some(2010),
                null_count: 0,
            })
            .with_key_range(SortKeyRange {
                min: Some(2001),
                max: Some(3000),
                null_count: 0,
            })
            .with_key_range(SortKeyRange {
                min: Some(3001),
                max: Some(3500),
                null_count: 0,
            })
    }

    #[test]
    fn test_union_sort_union_disjoint_ranges_asc() {
        assert_snapshot!(
        test_case_disjoint_3()
            .with_descending(false)
            .with_sort_expr("a")
            .union_sort_union_plan(),
         @r"
        UnionExec
          DataSourceExec: file_groups={1 group: [[0.parquet]]}, projection=[a], output_ordering=[a@0 ASC], file_type=parquet
          SortExec: expr=[a@0 ASC], preserve_partitioning=[false]
            UnionExec
              DataSourceExec: file_groups={1 group: [[0.parquet]]}, projection=[a], output_ordering=[a@0 ASC], file_type=parquet
              DataSourceExec: file_groups={1 group: [[0.parquet]]}, projection=[a], output_ordering=[a@0 ASC], file_type=parquet

        Input Ranges
          (Some(1000))->(Some(2000))
          (Some(2001))->(Some(3000))
          (Some(3001))->(Some(3500))
        Output Ranges: [0, 1]
          (1000)->(2000)
          (2001)->(3500)
        ");
    }

    #[test]
    fn test_union_sort_union_overlapping_ranges_asc() {
        assert_snapshot!(
         test_case_overlapping_3()
            .with_descending(false)
            .with_sort_expr("a")
            .union_sort_union_plan(),
         @r"
        UnionExec
          DataSourceExec: file_groups={1 group: [[0.parquet]]}, projection=[a], output_ordering=[a@0 ASC], file_type=parquet
          SortExec: expr=[a@0 ASC], preserve_partitioning=[false]
            UnionExec
              DataSourceExec: file_groups={1 group: [[0.parquet]]}, projection=[a], output_ordering=[a@0 ASC], file_type=parquet
              DataSourceExec: file_groups={1 group: [[0.parquet]]}, projection=[a], output_ordering=[a@0 ASC], file_type=parquet

        Input Ranges
          (Some(1000))->(Some(2010))
          (Some(2001))->(Some(3000))
          (Some(3001))->(Some(3500))
        No disjoint ranges found
        ");
    }

    #[test]
    fn test_union_sort_union_disjoint_ranges_desc_nulls_first() {
        assert_snapshot!(
        test_case_disjoint_3()
            .with_descending(true)
            .with_sort_expr("a")
            .union_sort_union_plan(),
         @r"
        UnionExec
          DataSourceExec: file_groups={1 group: [[0.parquet]]}, projection=[a], output_ordering=[a@0 DESC], file_type=parquet
          SortExec: expr=[a@0 DESC], preserve_partitioning=[false]
            UnionExec
              DataSourceExec: file_groups={1 group: [[0.parquet]]}, projection=[a], output_ordering=[a@0 DESC], file_type=parquet
              DataSourceExec: file_groups={1 group: [[0.parquet]]}, projection=[a], output_ordering=[a@0 DESC], file_type=parquet

        Input Ranges
          (Some(1000))->(Some(2000))
          (Some(2001))->(Some(3000))
          (Some(3001))->(Some(3500))
        Output Ranges: [1, 0]
          (2001)->(3500)
          (1000)->(2000)
        ");
    }

    #[test]
    fn test_union_sort_union_overlapping_ranges_desc_nulls_first() {
        assert_snapshot!(
         test_case_overlapping_3()
            .with_descending(true)
            .with_sort_expr("a")
            .union_sort_union_plan(),
         @r"
        UnionExec
          DataSourceExec: file_groups={1 group: [[0.parquet]]}, projection=[a], output_ordering=[a@0 DESC], file_type=parquet
          SortExec: expr=[a@0 DESC], preserve_partitioning=[false]
            UnionExec
              DataSourceExec: file_groups={1 group: [[0.parquet]]}, projection=[a], output_ordering=[a@0 DESC], file_type=parquet
              DataSourceExec: file_groups={1 group: [[0.parquet]]}, projection=[a], output_ordering=[a@0 DESC], file_type=parquet

        Input Ranges
          (Some(1000))->(Some(2010))
          (Some(2001))->(Some(3000))
          (Some(3001))->(Some(3500))
        No disjoint ranges found
        ");
    }

    // default is NULLS FIRST so try NULLS LAST
    #[test]
    fn test_union_sort_union_disjoint_ranges_asc_nulls_last() {
        assert_snapshot!(
        test_case_disjoint_3()
            .with_descending(false)
            .with_nulls_first(false)
            .with_sort_expr("a")
            .union_sort_union_plan(),
         @r"
        UnionExec
          DataSourceExec: file_groups={1 group: [[0.parquet]]}, projection=[a], output_ordering=[a@0 ASC NULLS LAST], file_type=parquet
          SortExec: expr=[a@0 ASC NULLS LAST], preserve_partitioning=[false]
            UnionExec
              DataSourceExec: file_groups={1 group: [[0.parquet]]}, projection=[a], output_ordering=[a@0 ASC NULLS LAST], file_type=parquet
              DataSourceExec: file_groups={1 group: [[0.parquet]]}, projection=[a], output_ordering=[a@0 ASC NULLS LAST], file_type=parquet

        Input Ranges
          (Some(1000))->(Some(2000))
          (Some(2001))->(Some(3000))
          (Some(3001))->(Some(3500))
        Output Ranges: [0, 1]
          (1000)->(2000)
          (2001)->(3500)
        ");
    }

    // default is NULLS FIRST so try NULLS LAST
    #[test]
    fn test_union_sort_union_overlapping_ranges_asc_nulls_last() {
        assert_snapshot!(
         test_case_overlapping_3()
            .with_descending(false)
            .with_nulls_first(false)
            .with_sort_expr("a")
            .union_sort_union_plan(),
         @r"
        UnionExec
          DataSourceExec: file_groups={1 group: [[0.parquet]]}, projection=[a], output_ordering=[a@0 ASC NULLS LAST], file_type=parquet
          SortExec: expr=[a@0 ASC NULLS LAST], preserve_partitioning=[false]
            UnionExec
              DataSourceExec: file_groups={1 group: [[0.parquet]]}, projection=[a], output_ordering=[a@0 ASC NULLS LAST], file_type=parquet
              DataSourceExec: file_groups={1 group: [[0.parquet]]}, projection=[a], output_ordering=[a@0 ASC NULLS LAST], file_type=parquet

        Input Ranges
          (Some(1000))->(Some(2010))
          (Some(2001))->(Some(3000))
          (Some(3001))->(Some(3500))
        No disjoint ranges found
        ");
    }

    #[test]
    fn test_union_disjoint_ranges_asc() {
        assert_snapshot!(
         test_case_disjoint_3()
            .with_descending(false)
            .with_sort_expr("a")
            .union_plan(),
         @r"
        UnionExec
          DataSourceExec: file_groups={1 group: [[0.parquet]]}, projection=[a], output_ordering=[a@0 ASC], file_type=parquet
          DataSourceExec: file_groups={2 groups: [[0.parquet], [1.parquet]]}, projection=[a], output_ordering=[a@0 ASC], file_type=parquet

        Input Ranges
          (Some(1000))->(Some(2000))
          (Some(2001))->(Some(3000))
          (Some(3001))->(Some(3500))
        Output Ranges: [0, 1, 2]
          (1000)->(2000)
          (2001)->(3000)
          (3001)->(3500)
        ");
    }

    #[test]
    fn test_union_overlapping_ranges_asc() {
        assert_snapshot!(
         test_case_overlapping_3()
            .with_descending(false)
            .with_sort_expr("a")
            .union_plan(),
         @r"
        UnionExec
          DataSourceExec: file_groups={1 group: [[0.parquet]]}, projection=[a], output_ordering=[a@0 ASC], file_type=parquet
          DataSourceExec: file_groups={2 groups: [[0.parquet], [1.parquet]]}, projection=[a], output_ordering=[a@0 ASC], file_type=parquet

        Input Ranges
          (Some(1000))->(Some(2010))
          (Some(2001))->(Some(3000))
          (Some(3001))->(Some(3500))
        No disjoint ranges found
        ");
    }

    #[test]
    fn test_union_disjoint_ranges_desc() {
        assert_snapshot!(
         test_case_disjoint_3()
            .with_descending(true)
            .with_sort_expr("a")
            .union_plan(),
         @r"
        UnionExec
          DataSourceExec: file_groups={1 group: [[0.parquet]]}, projection=[a], output_ordering=[a@0 DESC], file_type=parquet
          DataSourceExec: file_groups={2 groups: [[0.parquet], [1.parquet]]}, projection=[a], output_ordering=[a@0 DESC], file_type=parquet

        Input Ranges
          (Some(1000))->(Some(2000))
          (Some(2001))->(Some(3000))
          (Some(3001))->(Some(3500))
        Output Ranges: [2, 1, 0]
          (3001)->(3500)
          (2001)->(3000)
          (1000)->(2000)
        ");
    }

    #[test]
    fn test_union_overlapping_ranges_desc() {
        assert_snapshot!(
         test_case_overlapping_3()
            .with_descending(true)
            .with_sort_expr("a")
            .union_plan(),
         @r"
        UnionExec
          DataSourceExec: file_groups={1 group: [[0.parquet]]}, projection=[a], output_ordering=[a@0 DESC], file_type=parquet
          DataSourceExec: file_groups={2 groups: [[0.parquet], [1.parquet]]}, projection=[a], output_ordering=[a@0 DESC], file_type=parquet

        Input Ranges
          (Some(1000))->(Some(2010))
          (Some(2001))->(Some(3000))
          (Some(3001))->(Some(3500))
        No disjoint ranges found
        ");
    }

    /// Helper for building up patterns for testing statistics extraction from
    /// ExecutionPlans
    #[derive(Debug)]
    struct TestCaseBuilder {
        input_ranges: Vec<SortKeyRange>,
        sort_options: SortOptions,
        sort_exprs: Vec<PhysicalSortExpr>,
        schema: Arc<Schema>,
    }

    impl TestCaseBuilder {
        /// Creates a new `TestCaseBuilder` instance with default values.
        fn new() -> Self {
            let schema = Arc::new(Schema::new(vec![Field::new("a", DataType::Int64, true)]));

            Self {
                input_ranges: vec![],
                sort_options: SortOptions::default(),
                sort_exprs: vec![],
                schema,
            }
        }

        /// Add a key range
        pub fn with_key_range(mut self, key_range: SortKeyRange) -> Self {
            self.input_ranges.push(key_range);
            self
        }

        /// set SortOptions::descending flag
        pub fn with_descending(mut self, descending: bool) -> Self {
            self.sort_options.descending = descending;
            self
        }

        /// set SortOptions::nulls_first flag
        pub fn with_nulls_first(mut self, nulls_first: bool) -> Self {
            self.sort_options.nulls_first = nulls_first;
            self
        }

        /// Add a sort expression to the ordering (created with the current SortOptions)
        pub fn with_sort_expr(mut self, column_name: &str) -> Self {
            let expr =
                PhysicalSortExpr::new(col(column_name, &self.schema).unwrap(), self.sort_options);
            self.sort_exprs.push(expr);
            self
        }

        /// Build a test physical plan like the following, and extract disjoint ranges from it:
        ///
        /// ```text
        /// UNION
        ///     DataSourceExec (key_ranges[0])            (range_a)
        ///     SORT
        ///         UNION
        ///             DataSourceExec (key_ranges[1])    (range_b_1)
        ///             DataSourceExec (key_ranges[2])    (range_b_2)
        /// ```
        fn union_sort_union_plan(self) -> TestResult {
            let Self {
                input_ranges,
                sort_options: _, // used to create sort exprs, nothere
                sort_exprs,
                schema,
            } = self;
            let lex_ordering = LexOrdering::new(sort_exprs).unwrap();

            assert_eq!(input_ranges.len(), 3);
            let range_a = &input_ranges[0];
            let range_b_1 = &input_ranges[1];
            let range_b_2 = &input_ranges[2];

            let datasrc_a = data_source_exec_parquet_with_sort_with_statistics_and_schema(
                &schema,
                vec![lex_ordering.clone()],
                &[range_a],
            );

            let datasrc_b1 = data_source_exec_parquet_with_sort_with_statistics_and_schema(
                &schema,
                vec![lex_ordering.clone()],
                &[range_b_1],
            );
            let datasrc_b2 = data_source_exec_parquet_with_sort_with_statistics_and_schema(
                &schema,
                vec![lex_ordering.clone()],
                &[range_b_2],
            );
            let b = sort_exec(
                &lex_ordering,
                &union_exec(vec![datasrc_b1, datasrc_b2]),
                false,
            );

            let plan = union_exec(vec![datasrc_a, b]);

            let actual = extract_disjoint_ranges_from_plan(&lex_ordering, &plan)
                .expect("Error extracting disjoint ranges from plan");
            TestResult {
                input_ranges,
                plan,
                actual,
            }
        }

        /// Build a test physical plan like the following, and extract disjoint ranges from it:
        ///
        /// ```text
        /// UNION
        ///     DataSourceExec (key_ranges[0])                (range_a)
        ///     DataSourceExec (key_ranges[1], key_ranges[2]) (range_b_1, range_b_2)
        /// ```
        fn union_plan(self) -> TestResult {
            let Self {
                input_ranges,
                sort_options: _, // used to create sort exprs, nothere
                sort_exprs,
                schema,
            } = self;

            let lex_ordering = LexOrdering::new(sort_exprs).unwrap();

            assert_eq!(input_ranges.len(), 3);
            let range_a = &input_ranges[0];
            let range_b_1 = &input_ranges[1];
            let range_b_2 = &input_ranges[2];
            let datasrc_a = data_source_exec_parquet_with_sort_with_statistics(
                vec![lex_ordering.clone()],
                &[range_a],
            );
            let datasrc_b = data_source_exec_parquet_with_sort_with_statistics_and_schema(
                &schema,
                vec![lex_ordering.clone()],
                &[range_b_1, range_b_2],
            );

            let plan = union_exec(vec![datasrc_a, datasrc_b]);

            let actual = extract_disjoint_ranges_from_plan(&lex_ordering, &plan)
                .expect("Error extracting disjoint ranges from plan");
            TestResult {
                input_ranges,
                plan,
                actual,
            }
        }
    }

    /// Result of running a test case, including the input ranges, the execution
    /// plan, and the actual disjoint ranges found.
    ///
    /// This struct implements `Display` to provide a formatted output of the
    /// test case results that can be easily compared using `insta` snapshots.
    struct TestResult {
        input_ranges: Vec<SortKeyRange>,
        plan: Arc<dyn ExecutionPlan>,
        actual: Option<NonOverlappingOrderedLexicalRanges>,
    }

    impl TestResult {}

    impl Display for TestResult {
        fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
            let displayable_plan = displayable(self.plan.as_ref()).indent(false);
            writeln!(f, "{displayable_plan}")?;

            writeln!(f, "Input Ranges")?;
            for range in &self.input_ranges {
                writeln!(f, "  {range}")?;
            }

            match self.actual.as_ref() {
                Some(actual) => {
                    writeln!(f, "Output Ranges: {:?}", actual.indices())?;
                    for range in actual.ordered_ranges() {
                        writeln!(f, "  {range}")?;
                    }
                }
                None => {
                    writeln!(f, "No disjoint ranges found")?;
                }
            }
            Ok(())
        }
    }
}
