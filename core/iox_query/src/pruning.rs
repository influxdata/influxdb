//! Implementation of statistics based pruning

use crate::QueryChunk;
use crate::pruning_oracle::BucketPartitionPruningOracle;

use arrow::{
    array::{ArrayRef, BooleanArray, UInt64Array, new_empty_array},
    datatypes::{DataType, SchemaRef},
};
use datafusion::{
    physical_expr::execution_props::ExecutionProps,
    physical_optimizer::pruning::PruningStatistics,
    physical_plan::{ColumnStatistics, Statistics},
    prelude::{Column, Expr, col},
    scalar::ScalarValue,
};
use datafusion_util::{create_pruning_predicate, lit_timestamptz_nano};
use query_functions::group_by::Aggregate;
use schema::{Schema, TIME_COLUMN_NAME};
use std::collections::HashSet;
use std::sync::Arc;
use tracing::{debug, trace, warn};

/// Reason why a chunk could not be pruned.
///
/// Also see [`PruningObserver::could_not_prune`].
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum NotPrunedReason {
    /// No expression on predicate
    NoExpressionOnPredicate,

    /// Can not create pruning predicate
    CanNotCreatePruningPredicate,

    /// DataFusion pruning failed
    DataFusionPruningFailed,
}

impl NotPrunedReason {
    /// Human-readable string representation.
    pub fn name(&self) -> &'static str {
        match self {
            Self::NoExpressionOnPredicate => "No expression on predicate",
            Self::CanNotCreatePruningPredicate => "Can not create pruning predicate",
            Self::DataFusionPruningFailed => "DataFusion pruning failed",
        }
    }
}

impl std::fmt::Display for NotPrunedReason {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.name())
    }
}

/// Something that cares to be notified when pruning of chunks occurs
pub trait PruningObserver {
    /// Called when the specified chunk was pruned
    fn was_pruned(&self, _chunk: &dyn QueryChunk) {}

    /// Called when a chunk was not pruned.
    fn was_not_pruned(&self, _chunk: &dyn QueryChunk) {}

    /// Called when no pruning can happen at all for some reason.
    ///
    /// Since pruning is optional and _only_ improves performance but its lack does not affect correctness, this will
    /// NOT lead to a query error.
    ///
    /// In this case, statistical pruning will not happen and neither [`was_pruned`](Self::was_pruned) nor
    /// [`was_not_pruned`](Self::was_not_pruned) will be called.
    fn could_not_prune(&self, _reason: NotPrunedReason, _chunk: &dyn QueryChunk) {}
}

/// Given a Vec of prunable items, returns a possibly smaller set
/// filtering those where the predicate can be proven to evaluate to
/// `false` for every single row.
pub fn prune_chunks(
    table_schema: &Schema,
    chunks: &[Arc<dyn QueryChunk>],
    filters: &[Expr],
) -> Result<Vec<bool>, NotPrunedReason> {
    let num_chunks = chunks.len();
    debug!(num_chunks, ?filters, "Pruning chunks");
    let summaries: Vec<_> = chunks
        .iter()
        .map(|c| Summary {
            stats: c.stats(),
            schema_ref: c.schema().as_arrow(),
            pruning_oracle: None,
        })
        .collect();

    let filter_expr = match filters.iter().cloned().reduce(|a, b| a.and(b)) {
        Some(expr) => expr,
        None => {
            debug!("No expression on predicate");
            return Err(NotPrunedReason::NoExpressionOnPredicate);
        }
    };

    prune_summaries(
        ChunkPruningStatistics::new(table_schema, &summaries),
        &filter_expr,
    )
}

/// An opaque [`PruningStatistics`] implementation which exposes the associated table schema.
pub trait SchemaPruningStatistics: PruningStatistics {
    fn table_schema(&self) -> &Schema;
}

/// Given a `Vec` of pruning summaries, return a `Vec<bool>` where `false` indicates that the
/// predicate can be proven to evaluate to `false` for every single row.
pub fn prune_summaries(
    pruning_statistics: impl SchemaPruningStatistics,
    filter_expr: &Expr,
) -> Result<Vec<bool>, NotPrunedReason> {
    trace!(%filter_expr, "Filter_expr of pruning chunks");

    // no information about the queries here
    let props = ExecutionProps::new();
    let pruning_predicate = match create_pruning_predicate(
        &props,
        filter_expr,
        &pruning_statistics.table_schema().as_arrow(),
    ) {
        Ok(p) => p,
        Err(e) => {
            warn!(%e, ?filter_expr, "Can not create pruning predicate");
            return Err(NotPrunedReason::CanNotCreatePruningPredicate);
        }
    };

    let results = match pruning_predicate.prune(&pruning_statistics) {
        Ok(results) => results,
        Err(e) => {
            warn!(%e, ?filter_expr, "DataFusion pruning failed");
            return Err(NotPrunedReason::DataFusionPruningFailed);
        }
    };
    Ok(results)
}

/// Summary of statistics and optional server-side bucketing info for pruning
#[derive(Debug)]
pub struct Summary {
    pub stats: Arc<Statistics>,
    pub schema_ref: SchemaRef,
    pub pruning_oracle: Option<Arc<BucketPartitionPruningOracle>>,
}

/// Wraps a collection of pruning summaries and implements the [`PruningStatistics`]
/// interface required to use them for pruning [`QueryChunk`]s
#[derive(Debug)]
pub struct ChunkPruningStatistics<'a> {
    table_schema: &'a Schema,
    summaries: &'a [Summary],
}

impl<'a> ChunkPruningStatistics<'a> {
    pub fn new(table_schema: &'a Schema, summaries: &'a [Summary]) -> Self {
        Self {
            table_schema,
            summaries,
        }
    }

    /// Returns the [`DataType`] for `column`
    fn column_type(&self, column: &Column) -> Option<&DataType> {
        let index = self.table_schema.find_index_of(&column.name)?;
        Some(self.table_schema.field(index).1.data_type())
    }

    /// Returns an iterator that for each chunk returns the [`Statistics`]
    /// for the provided `column` if any
    fn column_summaries<'b: 'a, 'c: 'a>(
        &'c self,
        column: &'b Column,
    ) -> impl Iterator<Item = Option<&'a ColumnStatistics>> + 'a {
        self.summaries.iter().map(|summary| {
            let Summary {
                stats,
                schema_ref,
                pruning_oracle: _,
            } = summary;
            let idx = schema_ref.index_of(&column.name).ok()?;
            Some(&stats.column_statistics[idx])
        })
    }
}

impl<'a> SchemaPruningStatistics for ChunkPruningStatistics<'a> {
    fn table_schema(&self) -> &'a Schema {
        self.table_schema
    }
}

impl PruningStatistics for ChunkPruningStatistics<'_> {
    fn min_values(&self, column: &Column) -> Option<ArrayRef> {
        let data_type = self.column_type(column)?;
        let summaries = self.column_summaries(column);
        collect_pruning_stats(&column.name, data_type, summaries, Aggregate::Min)
    }

    fn max_values(&self, column: &Column) -> Option<ArrayRef> {
        let data_type = self.column_type(column)?;
        let summaries = self.column_summaries(column);
        collect_pruning_stats(&column.name, data_type, summaries, Aggregate::Max)
    }

    fn num_containers(&self) -> usize {
        self.summaries.len()
    }

    fn null_counts(&self, column: &Column) -> Option<ArrayRef> {
        let null_counts = self
            .column_summaries(column)
            .map(|stats| stats.and_then(|stats| stats.null_count.get_value()))
            .map(|x| x.map(|x| *x as u64));

        Some(Arc::new(UInt64Array::from_iter(null_counts)))
    }

    fn row_counts(&self, _column: &Column) -> Option<ArrayRef> {
        let row_counts = self
            .summaries
            .iter()
            .map(|summary| match summary.stats.num_rows {
                datafusion::common::stats::Precision::Absent => None,
                datafusion::common::stats::Precision::Inexact(rows) => Some(rows as u64),
                datafusion::common::stats::Precision::Exact(rows) => Some(rows as u64),
            });

        Some(Arc::new(UInt64Array::from_iter(row_counts)))
    }

    fn contained(&self, column: &Column, values: &HashSet<ScalarValue>) -> Option<BooleanArray> {
        Some(
            self.summaries
                .iter()
                .map(|summary| match summary.pruning_oracle {
                    Some(ref pruning_oracle) => pruning_oracle.could_contain_values(column, values),
                    None => None,
                })
                .collect(),
        )
    }
}

/// Collects an [`ArrayRef`] containing the aggregate statistic corresponding to
/// `aggregate` for each of the provided [`Statistics`]
fn collect_pruning_stats<'a>(
    column_name: &str,
    data_type: &DataType,
    statistics: impl Iterator<Item = Option<&'a ColumnStatistics>>,
    aggregate: Aggregate,
) -> Option<ArrayRef> {
    let mut statistics = statistics.peekable();
    if statistics.peek().is_none() {
        // empty
        return Some(new_empty_array(data_type));
    }

    let null = ScalarValue::try_from(data_type).ok()?;

    let stats = ScalarValue::iter_to_array(statistics.map(|stats| {
        stats
            .and_then(|stats| get_aggregate(stats, aggregate).cloned())
            .unwrap_or_else(|| null.clone())
    }));

    if let Err(e) = &stats {
        warn!(%e, %column_name, "Failed to create pruning stats");
    }
    // Ignore the error and return None to allow the query to proceed (though pruning may be
    // less effective as it won't have access to these statistics)

    stats.ok()
}

/// Returns the aggregate statistic corresponding to `aggregate` from `stats`
fn get_aggregate(stats: &ColumnStatistics, aggregate: Aggregate) -> Option<&ScalarValue> {
    match aggregate {
        Aggregate::Min => stats.min_value.get_value(),
        Aggregate::Max => stats.max_value.get_value(),
        _ => None,
    }
}

/// Retention time expression, "time > retention_time".
pub fn retention_expr(retention_time: i64) -> Expr {
    col(TIME_COLUMN_NAME).gt(lit_timestamptz_nano(retention_time))
}

#[cfg(test)]
mod test {
    use std::{ops::Not, sync::Arc};

    use data_types::partition_template::bucket_for_tag_value;
    use datafusion::prelude::{col, lit};
    use datafusion_util::lit_dict;
    use schema::merge::SchemaMerger;
    use test_helpers::{assert_not_contains, tracing::TracingCapture};

    use crate::{
        QueryChunk,
        pruning_oracle::{BucketInfo, BucketPartitionPruningOracleBuilder},
        test::TestChunk,
    };

    use super::*;

    #[test]
    fn test_empty_schema() {
        test_helpers::maybe_start_logging();
        let capture = TracingCapture::new();
        let c1 = Arc::new(TestChunk::new("chunk1"));

        let result = prune_chunks(&c1.schema().clone(), &[Arc::clone(&c1) as _], &[]);
        assert_eq!(result, Err(NotPrunedReason::NoExpressionOnPredicate));

        let result = prune_chunks(
            &c1.schema().clone(),
            &[Arc::clone(&c1) as _],
            &[lit(1).eq(lit(1))],
        );
        assert_eq!(result.expect("pruning succeeds"), vec![true]);

        assert_not_contains!(capture.to_string().to_lowercase(), "warn");
    }

    #[test]
    fn test_empty_chunks() {
        test_helpers::maybe_start_logging();
        let capture = TracingCapture::new();
        let c1 = Arc::new(TestChunk::new("chunk1").with_f64_field_column("column1"));

        let result = prune_chunks(&c1.schema().clone(), &[], &[col("column1").gt(lit(1.0f64))]);
        assert_eq!(result.expect("pruning succeeds"), Vec::<bool>::new());

        assert_not_contains!(capture.to_string().to_lowercase(), "warn");
    }

    #[test]
    fn test_pruned_f64() {
        test_helpers::maybe_start_logging();
        // column1 > 100.0 where
        //   c1: [0.0, 10.0] --> pruned
        let c1 = Arc::new(TestChunk::new("chunk1").with_f64_field_column_with_stats(
            "column1",
            Some(0.0),
            Some(10.0),
        ));

        let filters = vec![col("column1").gt(lit(100.0f64))];

        let result = prune_chunks(&c1.schema().clone(), &[c1], &filters);
        assert_eq!(result.expect("pruning succeeds"), vec![false]);
    }

    #[test]
    fn test_pruned_i64() {
        test_helpers::maybe_start_logging();
        // column1 > 100 where
        //   c1: [0, 10] --> pruned

        let c1 = Arc::new(TestChunk::new("chunk1").with_i64_field_column_with_stats(
            "column1",
            Some(0),
            Some(10),
        ));

        let filters = vec![col("column1").gt(lit(100i64))];

        let result = prune_chunks(&c1.schema().clone(), &[c1], &filters);

        assert_eq!(result.expect("pruning succeeds"), vec![false]);
    }

    #[test]
    fn test_pruned_u64() {
        test_helpers::maybe_start_logging();
        // column1 > 100 where
        //   c1: [0, 10] --> pruned

        let c1 = Arc::new(TestChunk::new("chunk1").with_u64_field_column_with_stats(
            "column1",
            Some(0),
            Some(10),
        ));

        let filters = vec![col("column1").gt(lit(100u64))];

        let result = prune_chunks(&c1.schema().clone(), &[c1], &filters);
        assert_eq!(result.expect("pruning succeeds"), vec![false]);
    }

    #[test]
    fn test_pruned_bool() {
        test_helpers::maybe_start_logging();
        // column1 where
        //   c1: [false, false] --> pruned
        let c1 = Arc::new(TestChunk::new("chunk1").with_bool_field_column_with_stats(
            "column1",
            Some(false),
            Some(false),
        ));

        let filters = vec![col("column1")];

        let result = prune_chunks(&c1.schema().clone(), &[c1], &filters);
        assert_eq!(result.expect("pruning succeeds"), vec![false; 1]);
    }

    #[test]
    fn test_pruned_string() {
        test_helpers::maybe_start_logging();
        // column1 > "z" where
        //   c1: ["a", "q"] --> pruned

        let c1 = Arc::new(
            TestChunk::new("chunk1").with_string_field_column_with_stats(
                "column1",
                Some("a"),
                Some("q"),
            ),
        );

        let filters = vec![col("column1").gt(lit("z"))];

        let result = prune_chunks(&c1.schema().clone(), &[c1], &filters);
        assert_eq!(result.expect("pruning succeeds"), vec![false]);
    }

    #[test]
    fn test_not_pruned_f64() {
        test_helpers::maybe_start_logging();
        // column1 < 100.0 where
        //   c1: [0.0, 10.0] --> not pruned
        let c1 = Arc::new(TestChunk::new("chunk1").with_f64_field_column_with_stats(
            "column1",
            Some(0.0),
            Some(10.0),
        ));

        let filters = vec![col("column1").lt(lit(100.0f64))];

        let result = prune_chunks(&c1.schema().clone(), &[c1], &filters);
        assert_eq!(result.expect("pruning succeeds"), vec![true]);
    }

    #[test]
    fn test_not_pruned_i64() {
        test_helpers::maybe_start_logging();
        // column1 < 100 where
        //   c1: [0, 10] --> not pruned

        let c1 = Arc::new(TestChunk::new("chunk1").with_i64_field_column_with_stats(
            "column1",
            Some(0),
            Some(10),
        ));

        let filters = vec![col("column1").lt(lit(100i64))];

        let result = prune_chunks(&c1.schema().clone(), &[c1], &filters);
        assert_eq!(result.expect("pruning succeeds"), vec![true]);
    }

    #[test]
    fn test_not_pruned_u64() {
        test_helpers::maybe_start_logging();
        // column1 < 100 where
        //   c1: [0, 10] --> not pruned

        let c1 = Arc::new(TestChunk::new("chunk1").with_u64_field_column_with_stats(
            "column1",
            Some(0),
            Some(10),
        ));

        let filters = vec![col("column1").lt(lit(100u64))];

        let result = prune_chunks(&c1.schema().clone(), &[c1], &filters);
        assert_eq!(result.expect("pruning succeeds"), vec![true]);
    }

    #[test]
    fn test_not_pruned_bool() {
        test_helpers::maybe_start_logging();
        // column1
        //   c1: [false, true] --> not pruned

        let c1 = Arc::new(TestChunk::new("chunk1").with_bool_field_column_with_stats(
            "column1",
            Some(false),
            Some(true),
        ));

        let filters = vec![col("column1")];

        let result = prune_chunks(&c1.schema().clone(), &[c1], &filters);
        assert_eq!(result.expect("pruning succeeds"), vec![true]);
    }

    #[test]
    fn test_not_pruned_string() {
        test_helpers::maybe_start_logging();
        // column1 < "z" where
        //   c1: ["a", "q"] --> not pruned

        let c1 = Arc::new(
            TestChunk::new("chunk1").with_string_field_column_with_stats(
                "column1",
                Some("a"),
                Some("q"),
            ),
        );

        let filters = vec![col("column1").lt(lit("z"))];

        let result = prune_chunks(&c1.schema().clone(), &[c1], &filters);
        assert_eq!(result.expect("pruning succeeds"), vec![true]);
    }

    fn merge_schema(chunks: &[Arc<dyn QueryChunk>]) -> Schema {
        let mut merger = SchemaMerger::new();
        for chunk in chunks {
            merger = merger.merge(chunk.schema()).unwrap();
        }
        merger.build()
    }

    #[test]
    fn test_pruned_null() {
        test_helpers::maybe_start_logging();
        // column1 > 100 where
        //   c1: [Null, 10] --> pruned
        //   c2: [0, Null] --> not pruned
        //   c3: [Null, Null] --> not pruned (min/max are not known in chunk 3)
        //   c4: Null --> not pruned (no statistics at all)

        let c1 = Arc::new(TestChunk::new("chunk1").with_i64_field_column_with_stats(
            "column1",
            None,
            Some(10),
        )) as Arc<dyn QueryChunk>;

        let c2 = Arc::new(TestChunk::new("chunk2").with_i64_field_column_with_stats(
            "column1",
            Some(0),
            None,
        )) as Arc<dyn QueryChunk>;

        let c3 = Arc::new(
            TestChunk::new("chunk3").with_i64_field_column_with_stats("column1", None, None),
        ) as Arc<dyn QueryChunk>;

        let c4 = Arc::new(TestChunk::new("chunk4").with_i64_field_column("column1"))
            as Arc<dyn QueryChunk>;

        let filters = vec![col("column1").gt(lit(100i64))];

        let chunks = vec![c1, c2, c3, c4];
        let schema = merge_schema(&chunks);

        let result = prune_chunks(&schema, &chunks, &filters);

        assert_eq!(
            result.expect("pruning succeeds"),
            vec![false, true, true, true]
        );
    }

    #[test]
    fn test_pruned_multi_chunk() {
        test_helpers::maybe_start_logging();
        // column1 > 100 where
        //   c1: [0, 10] --> pruned
        //   c2: [0, 1000] --> not pruned
        //   c3: [10, 20] --> pruned
        //   c4: [None, None] --> not pruned
        //   c5: [10, None] --> not pruned
        //   c6: [None, 10] --> pruned

        let c1 = Arc::new(TestChunk::new("chunk1").with_i64_field_column_with_stats(
            "column1",
            Some(0),
            Some(10),
        )) as Arc<dyn QueryChunk>;

        let c2 = Arc::new(TestChunk::new("chunk2").with_i64_field_column_with_stats(
            "column1",
            Some(0),
            Some(1000),
        )) as Arc<dyn QueryChunk>;

        let c3 = Arc::new(TestChunk::new("chunk3").with_i64_field_column_with_stats(
            "column1",
            Some(10),
            Some(20),
        )) as Arc<dyn QueryChunk>;

        let c4 = Arc::new(
            TestChunk::new("chunk4").with_i64_field_column_with_stats("column1", None, None),
        ) as Arc<dyn QueryChunk>;

        let c5 = Arc::new(TestChunk::new("chunk5").with_i64_field_column_with_stats(
            "column1",
            Some(10),
            None,
        )) as Arc<dyn QueryChunk>;

        let c6 = Arc::new(TestChunk::new("chunk6").with_i64_field_column_with_stats(
            "column1",
            None,
            Some(20),
        )) as Arc<dyn QueryChunk>;

        let filters = vec![col("column1").gt(lit(100i64))];

        let chunks = vec![c1, c2, c3, c4, c5, c6];
        let schema = merge_schema(&chunks);

        let result = prune_chunks(&schema, &chunks, &filters);

        assert_eq!(
            result.expect("pruning succeeds"),
            vec![false, true, false, true, true, false]
        );
    }

    #[test]
    fn test_pruned_different_schema() {
        test_helpers::maybe_start_logging();
        // column1 > 100 where
        //   c1: column1 [0, 100], column2 [0, 4] --> pruned (in range, column2 ignored)
        //   c2: column1 [0, 1000], column2 [0, 4] --> not pruned (in range, column2 ignored)
        //   c3: None, column2 [0, 4] --> not pruned (no stats for column1)
        let c1 = Arc::new(
            TestChunk::new("chunk1")
                .with_i64_field_column_with_stats("column1", Some(0), Some(100))
                .with_i64_field_column_with_stats("column2", Some(0), Some(4)),
        ) as Arc<dyn QueryChunk>;

        let c2 = Arc::new(
            TestChunk::new("chunk2")
                .with_i64_field_column_with_stats("column1", Some(0), Some(1000))
                .with_i64_field_column_with_stats("column2", Some(0), Some(4)),
        ) as Arc<dyn QueryChunk>;

        let c3 = Arc::new(TestChunk::new("chunk3").with_i64_field_column_with_stats(
            "column2",
            Some(0),
            Some(4),
        )) as Arc<dyn QueryChunk>;

        let filters = vec![col("column1").gt(lit(100i64))];

        let chunks = vec![c1, c2, c3];
        let schema = merge_schema(&chunks);

        let result = prune_chunks(&schema, &chunks, &filters);

        assert_eq!(result.expect("pruning succeeds"), vec![false, true, true]);
    }

    #[test]
    fn test_pruned_is_null() {
        test_helpers::maybe_start_logging();
        // Verify that type of predicate is pruned if column1 is null
        // (this is a common predicate type created by the INfluxRPC planner)
        // (NOT column1 IS NULL) AND (column1 = 'bar')

        // No nulls, can't prune as it has values that are more and less than 'bar'
        let c1 = Arc::new(
            TestChunk::new("chunk1")
                .with_row_count(100)
                .with_tag_column_with_nulls_and_full_stats(
                    "column1",
                    Some("a"),
                    Some("z"),
                    None,
                    0,
                ),
        ) as Arc<dyn QueryChunk>;

        // Has no nulls, can prune it out based on statistics alone
        let c2 = Arc::new(
            TestChunk::new("chunk2")
                .with_row_count(100)
                .with_tag_column_with_nulls_and_full_stats(
                    "column1",
                    Some("a"),
                    Some("b"),
                    None,
                    0,
                ),
        ) as Arc<dyn QueryChunk>;

        // Has nulls, can still can prune it out based on statistics alone
        let c3 = Arc::new(
            TestChunk::new("chunk3")
                .with_row_count(100)
                .with_tag_column_with_nulls_and_full_stats(
                    "column1",
                    Some("a"),
                    Some("b"),
                    None,
                    1, // that one peksy null!
                ),
        ) as Arc<dyn QueryChunk>;

        let filters = vec![
            col("column1")
                .is_null()
                .not()
                .and(col("column1").eq(lit_dict("bar"))),
        ];

        let chunks = vec![c1, c2, c3];
        let schema = merge_schema(&chunks);

        let result = prune_chunks(&schema, &chunks, &filters);

        assert_eq!(result.expect("pruning succeeds"), vec![true, false, false]);
    }

    #[test]
    fn test_pruned_is_not_null() {
        test_helpers::maybe_start_logging();

        // Initialise statistics that represent three partitions:
        //
        //   - my_tag=A (with no NULL values)
        //   - my_tag=<NULL>
        //   - my_tag=B (with 1 NULL value)
        //
        // The pruning predicate is `my_tag IS NOT NULL`.

        let partitions: [Arc<dyn QueryChunk>; 3] = [
            // Statistics for the my_tag=A partition.
            //
            // No nulls, so can't prune
            Arc::new(
                TestChunk::new("bananas_table")
                    .with_row_count(100)
                    .with_tag_column_with_nulls_and_full_stats(
                        "my_tag",
                        Some("A"),
                        Some("A"),
                        None,
                        0,
                    ),
            ),
            // Statistics for the "my_tag=NULL" partition.
            //
            // It necessarily contains only NULL values (in this case, 10 rows with 10 nulls).
            // All nulls, should be pruned
            Arc::new(
                TestChunk::new("bananas_table")
                    .with_row_count(10)
                    .with_tag_column_with_nulls_and_full_stats("my_tag", None, None, None, 10),
            ),
            // Statistics for the my_tag=B partition
            //
            // Has a single NULL value, but not all the values are NULL, so can't prune
            Arc::new(
                TestChunk::new("bananas_table")
                    .with_row_count(100)
                    .with_tag_column_with_nulls_and_full_stats(
                        "my_tag",
                        Some("B"),
                        Some("B"),
                        None,
                        1,
                    ),
            ),
        ];

        // Convert the chunks into pruning statistics.
        let summaries = partitions
            .iter()
            .map(|c| Summary {
                stats: c.stats(),
                schema_ref: c.schema().as_arrow(),
                pruning_oracle: None,
            })
            .collect::<Vec<_>>();

        let table_schema = merge_schema(&partitions);
        let pruning_stats = ChunkPruningStatistics::new(&table_schema, &summaries);

        // Construct a predicate that should filter out all partitions except
        // "my_tag=NULL".
        let expr = col("my_tag").is_not_null();

        let got = prune_summaries(pruning_stats, &expr).unwrap();

        // Only the second partition should be pruned out.
        assert_eq!(got.as_slice(), [true, false, true]);
    }

    #[test]
    fn test_pruned_multi_column() {
        test_helpers::maybe_start_logging();
        // column1 > 100 AND column2 < 5 where
        //   c1: column1 [0, 1000], column2 [0, 4] --> not pruned (both in range)
        //   c2: column1 [0, 10], column2 [0, 4] --> pruned (column1 and column2 out of range)
        //   c3: column1 [0, 10], column2 [5, 10] --> pruned (column1 out of range, column2 in of range)
        //   c4: column1 [1000, 2000], column2 [0, 4] --> not pruned (column1 in range, column2 in range)
        //   c5: column1 [0, 10], column2 Null --> pruned (column1 out of range, but column2 has no stats)
        //   c6: column1 Null, column2 [0, 4] --> not pruned (column1 has no stats, column2 out of range)

        let c1 = Arc::new(
            TestChunk::new("chunk1")
                .with_i64_field_column_with_stats("column1", Some(0), Some(1000))
                .with_i64_field_column_with_stats("column2", Some(0), Some(4)),
        ) as Arc<dyn QueryChunk>;

        let c2 = Arc::new(
            TestChunk::new("chunk2")
                .with_i64_field_column_with_stats("column1", Some(0), Some(10))
                .with_i64_field_column_with_stats("column2", Some(0), Some(4)),
        ) as Arc<dyn QueryChunk>;

        let c3 = Arc::new(
            TestChunk::new("chunk3")
                .with_i64_field_column_with_stats("column1", Some(0), Some(10))
                .with_i64_field_column_with_stats("column2", Some(5), Some(10)),
        ) as Arc<dyn QueryChunk>;

        let c4 = Arc::new(
            TestChunk::new("chunk4")
                .with_i64_field_column_with_stats("column1", Some(1000), Some(2000))
                .with_i64_field_column_with_stats("column2", Some(0), Some(4)),
        ) as Arc<dyn QueryChunk>;

        let c5 = Arc::new(
            TestChunk::new("chunk5")
                .with_i64_field_column_with_stats("column1", Some(0), Some(10))
                .with_i64_field_column("column2"),
        ) as Arc<dyn QueryChunk>;

        let c6 = Arc::new(
            TestChunk::new("chunk6")
                .with_i64_field_column("column1")
                .with_i64_field_column_with_stats("column2", Some(0), Some(4)),
        ) as Arc<dyn QueryChunk>;

        let filters = vec![
            col("column1")
                .gt(lit(100i64))
                .and(col("column2").lt(lit(5i64))),
        ];

        let chunks = vec![c1, c2, c3, c4, c5, c6];
        let schema = merge_schema(&chunks);

        let result = prune_chunks(&schema, &chunks, &filters);

        assert_eq!(
            result.expect("Pruning succeeds"),
            vec![true, false, false, true, false, true]
        );
    }

    #[test]
    fn test_pruned_server_side_bucketing() {
        test_helpers::maybe_start_logging();
        // column1=foo where
        //  c1: BucketInfo { id: hash("bar"), num_buckets: 10 } --> pruned

        const NUM_BUCKETS: u32 = 10;

        let c1 = Arc::new(
            TestChunk::new("chunk1").with_string_field_column_with_stats("column1", None, None),
        ) as Arc<dyn QueryChunk>;

        let schema = c1.schema().clone();

        let summaries: Vec<_> = [c1]
            .iter()
            .map(|c| {
                let mut oracle_builder = BucketPartitionPruningOracleBuilder::default();
                oracle_builder.insert(
                    Arc::from("column1"),
                    BucketInfo {
                        id: Some(bucket_for_tag_value("bar", NUM_BUCKETS)),
                        num_buckets: NUM_BUCKETS,
                    },
                );
                Summary {
                    stats: c.stats(),
                    schema_ref: c.schema().as_arrow(),
                    pruning_oracle: Some(Arc::new(oracle_builder.build(None))),
                }
            })
            .collect();

        let filter = col("column1").eq(lit_dict("foo"));

        let result = prune_summaries(ChunkPruningStatistics::new(&schema, &summaries), &filter);
        assert_eq!(result.expect("pruning succeeds"), vec![false]);
    }

    #[test]
    fn test_pruned_server_side_bucketing_multi_chunk() {
        test_helpers::maybe_start_logging();
        // column1=foo where
        //  c1: BucketInfo { id: hash("foo"), num_buckets: 4 } --> not pruned
        //  c2: BucketInfo { id: hash("bar"), num_buckets: 4 } --> pruned
        //  c3: BucketInfo { id: hash("baz"), num_buckets: 4 } --> pruned
        //  c4: BucketInfo { id: hash("mia"), num_buckets: 4 } --> not pruned (this chunk share the same bucket with c1, i.e. foo)

        const NUM_BUCKETS: u32 = 4;

        let c1 = Arc::new(
            TestChunk::new("chunk1").with_string_field_column_with_stats("column1", None, None),
        ) as Arc<dyn QueryChunk>;
        let chunks = vec![c1; 4];
        let schema = merge_schema(&chunks);

        let tag_values = ["foo", "bar", "baz", "mia"];
        let summaries: Vec<_> = chunks
            .iter()
            .zip(tag_values.iter())
            .map(|(c, &tag_value)| {
                let mut oracle_builder = BucketPartitionPruningOracleBuilder::default();
                oracle_builder.insert(
                    Arc::from("column1"),
                    BucketInfo {
                        id: Some(bucket_for_tag_value(tag_value, NUM_BUCKETS)),
                        num_buckets: NUM_BUCKETS,
                    },
                );
                Summary {
                    stats: c.stats(),
                    schema_ref: c.schema().as_arrow(),
                    pruning_oracle: Some(Arc::new(oracle_builder.build(None))),
                }
            })
            .collect();

        let filter = col("column1").eq(lit_dict("foo"));

        let result = prune_summaries(ChunkPruningStatistics::new(&schema, &summaries), &filter);
        assert_eq!(
            result.expect("pruning succeeds"),
            vec![true, false, false, true]
        );
    }

    #[test]
    fn test_not_pruned_server_side_bucketing_same_tag_value() {
        test_helpers::maybe_start_logging();
        // column1=foo where
        //  c1: BucketInfo { id: hash("foo"), num_buckets: 10 } --> not pruned

        const NUM_BUCKETS: u32 = 10;

        let c1 = Arc::new(
            TestChunk::new("chunk1").with_string_field_column_with_stats("column1", None, None),
        ) as Arc<dyn QueryChunk>;

        let table_schema = c1.schema().clone();

        let summaries: Vec<_> = [c1]
            .iter()
            .map(|c| {
                let mut oracle_builder = BucketPartitionPruningOracleBuilder::default();
                oracle_builder.insert(
                    Arc::from("column1"),
                    BucketInfo {
                        id: Some(bucket_for_tag_value("foo", NUM_BUCKETS)),
                        num_buckets: NUM_BUCKETS,
                    },
                );
                Summary {
                    stats: c.stats(),
                    schema_ref: c.schema().as_arrow(),
                    pruning_oracle: Some(Arc::new(oracle_builder.build(None))),
                }
            })
            .collect();

        let filter = col("column1").eq(lit_dict("foo"));

        let result = prune_summaries(
            ChunkPruningStatistics::new(&table_schema, &summaries),
            &filter,
        );
        assert_eq!(result.expect("pruning succeeds"), vec![true]);
    }

    #[test]
    fn test_not_pruned_server_side_bucketing_different_tag_value() {
        test_helpers::maybe_start_logging();
        // column1=foo where
        //  c1: BucketInfo { id: hash("bar"), num_buckets: 1 } --> not pruned

        // Only one bucket, so both foo and bar will be in the same bucket
        const NUM_BUCKETS: u32 = 1;

        let c1 = Arc::new(
            TestChunk::new("chunk1").with_string_field_column_with_stats("column1", None, None),
        ) as Arc<dyn QueryChunk>;

        let table_schema = c1.schema().clone();

        let summaries: Vec<_> = [c1]
            .iter()
            .map(|c| {
                let mut oracle_builder = BucketPartitionPruningOracleBuilder::default();
                oracle_builder.insert(
                    Arc::from("column1"),
                    BucketInfo {
                        id: Some(bucket_for_tag_value("bar", NUM_BUCKETS)),
                        num_buckets: NUM_BUCKETS,
                    },
                );
                Summary {
                    stats: c.stats(),
                    schema_ref: c.schema().as_arrow(),
                    pruning_oracle: Some(Arc::new(oracle_builder.build(None))),
                }
            })
            .collect();

        let filter = col("column1").eq(lit_dict("foo"));

        let result = prune_summaries(
            ChunkPruningStatistics::new(&table_schema, &summaries),
            &filter,
        );
        assert_eq!(result.expect("pruning succeeds"), vec![true]);
    }

    /// This test simulates a table with a tag-based partition template:
    ///
    /// ```
    /// TagValue("my_tag")
    /// ```
    ///
    /// Such that partitions are created for each distinct value of "my_tag".
    ///
    /// When these partitions are pruned with a predicate of "my_tag=A", only
    /// one partition can be part of the result set, and the pruner can / should
    /// determine that only this one partition (the one that contains my_tag=A
    /// values) should be considered, discarding any others.
    ///
    /// Unfortunately this doesn't happen if a partition exists for
    /// "my_tag=NULL".
    #[test]
    fn test_tag_partitioning_with_one_partition_value_null() {
        // When one partition has a NULL value, the pruning doesn't work as
        // expected.
        //
        // Initialise statistics that represent three partitions:
        //
        //   - my_tag=A
        //   - my_tag=<NULL>
        //   - my_tag=B
        //
        // Then query with a predicate of my_tag=A below.
        //
        // The pruning should only return the first partition, but it returns
        // all three.
        let partitions: [Arc<dyn QueryChunk>; 3] = [
            // Statistics for the my_tag=A partition
            Arc::new(
                TestChunk::new("bananas_table")
                    .with_row_count(100)
                    .with_tag_column_with_nulls_and_full_stats(
                        "my_tag",
                        Some("A"),
                        Some("A"),
                        None,
                        0,
                    ),
            ),
            // These statistics are for the "my_tag=NULL" partition.
            //
            // It necessarily contains only NULL values (in this case, 1 row).
            Arc::new(
                TestChunk::new("bananas_table")
                    .with_row_count(1)
                    .with_tag_column_with_nulls_and_full_stats("my_tag", None, None, None, 1),
            ),
            // Statistics for the my_tag=B partition
            Arc::new(
                TestChunk::new("bananas_table")
                    .with_row_count(100)
                    .with_tag_column_with_nulls_and_full_stats(
                        "my_tag",
                        Some("B"),
                        Some("B"),
                        None,
                        0,
                    ),
            ),
        ];

        // Convert the chunks into pruning statistics.
        let summaries = partitions
            .iter()
            .map(|c| Summary {
                stats: c.stats(),
                schema_ref: c.schema().as_arrow(),
                pruning_oracle: None,
            })
            .collect::<Vec<_>>();

        let table_schema = merge_schema(&partitions);
        let pruning_stats = ChunkPruningStatistics::new(&table_schema, &summaries);

        // Construct a predicate that should filter out all partitions except
        // "my_tag=A".
        let expr = col("my_tag").eq(lit_dict("A"));

        let got = prune_summaries(pruning_stats, &expr).unwrap();

        // Only the first partition should be returned.
        assert_eq!(got.as_slice(), [true, false, false]);

        // When all the partitions has values, i.e. no NULL values, the pruning
        // works as expected.
        //
        // Initialise statistics that represent three partitions:
        //
        //   - my_tag=A
        //   - my_tag=B
        //   - my_tag=C
        //
        // Then query with a predicate of my_tag=A below.
        let partitions: [Arc<dyn QueryChunk>; 3] = [
            // Statistics for the my_tag=A partition
            Arc::new(
                TestChunk::new("bananas_table")
                    .with_row_count(100)
                    .with_tag_column_with_nulls_and_full_stats(
                        "my_tag",
                        Some("A"),
                        Some("A"),
                        None,
                        0,
                    ),
            ),
            // Statistics for the my_tag=B partition
            Arc::new(
                TestChunk::new("bananas_table")
                    .with_row_count(100)
                    .with_tag_column_with_nulls_and_full_stats(
                        "my_tag",
                        Some("B"),
                        Some("B"),
                        None,
                        0,
                    ),
            ),
            // Statistics for the my_tag=C partition
            Arc::new(
                TestChunk::new("bananas_table")
                    .with_row_count(100)
                    .with_tag_column_with_nulls_and_full_stats(
                        "my_tag",
                        Some("C"),
                        Some("C"),
                        None,
                        0,
                    ),
            ),
        ];

        // Convert the chunks into pruning statistics.
        let summaries = partitions
            .iter()
            .map(|c| Summary {
                stats: c.stats(),
                schema_ref: c.schema().as_arrow(),
                pruning_oracle: None,
            })
            .collect::<Vec<_>>();

        let table_schema = merge_schema(&partitions);
        let pruning_stats = ChunkPruningStatistics::new(&table_schema, &summaries);

        let expr = col("my_tag").eq(lit_dict("A"));

        let got = prune_summaries(pruning_stats, &expr).unwrap();

        assert_eq!(got.as_slice(), [true, false, false]);
    }
}
