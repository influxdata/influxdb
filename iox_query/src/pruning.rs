//! Implementation of statistics based pruning

use crate::QueryChunk;
use arrow::{
    array::{ArrayRef, UInt64Array},
    datatypes::{DataType, SchemaRef},
};
use datafusion::{
    physical_expr::execution_props::ExecutionProps,
    physical_optimizer::pruning::PruningStatistics,
    physical_plan::{ColumnStatistics, Statistics},
    prelude::Column,
    scalar::ScalarValue,
};
use datafusion_util::create_pruning_predicate;
use observability_deps::tracing::{debug, trace, warn};
use predicate::Predicate;
use query_functions::group_by::Aggregate;
use schema::Schema;
use std::sync::Arc;

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
    predicate: &Predicate,
) -> Result<Vec<bool>, NotPrunedReason> {
    let num_chunks = chunks.len();
    debug!(num_chunks, %predicate, "Pruning chunks");
    let summaries: Vec<_> = chunks
        .iter()
        .map(|c| (c.stats(), c.schema().as_arrow()))
        .collect();
    prune_summaries(table_schema, &summaries, predicate)
}

/// Given a `Vec` of pruning summaries, return a `Vec<bool>` where `false` indicates that the
/// predicate can be proven to evaluate to `false` for every single row.
pub fn prune_summaries(
    table_schema: &Schema,
    summaries: &[(Arc<Statistics>, SchemaRef)],
    predicate: &Predicate,
) -> Result<Vec<bool>, NotPrunedReason> {
    let filter_expr = match predicate.filter_expr() {
        Some(expr) => expr,
        None => {
            debug!("No expression on predicate");
            return Err(NotPrunedReason::NoExpressionOnPredicate);
        }
    };
    trace!(%filter_expr, "Filter_expr of pruning chunks");

    // no information about the queries here
    let props = ExecutionProps::new();
    let pruning_predicate =
        match create_pruning_predicate(&props, &filter_expr, &table_schema.as_arrow()) {
            Ok(p) => p,
            Err(e) => {
                warn!(%e, ?filter_expr, "Can not create pruning predicate");
                return Err(NotPrunedReason::CanNotCreatePruningPredicate);
            }
        };

    let statistics = ChunkPruningStatistics {
        table_schema,
        summaries,
    };

    let results = match pruning_predicate.prune(&statistics) {
        Ok(results) => results,
        Err(e) => {
            warn!(%e, ?filter_expr, "DataFusion pruning failed");
            return Err(NotPrunedReason::DataFusionPruningFailed);
        }
    };
    Ok(results)
}

/// Wraps a collection of [`QueryChunk`] and implements the [`PruningStatistics`]
/// interface required for pruning
struct ChunkPruningStatistics<'a> {
    table_schema: &'a Schema,
    summaries: &'a [(Arc<Statistics>, SchemaRef)],
}

impl<'a> ChunkPruningStatistics<'a> {
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
        self.summaries.iter().map(|(stats, schema)| {
            let stats = stats.column_statistics.as_ref()?;
            let idx = schema.index_of(&column.name).ok()?;
            Some(&stats[idx])
        })
    }
}

impl<'a> PruningStatistics for ChunkPruningStatistics<'a> {
    fn min_values(&self, column: &Column) -> Option<ArrayRef> {
        let data_type = self.column_type(column)?;
        let summaries = self.column_summaries(column);
        collect_pruning_stats(data_type, summaries, Aggregate::Min)
    }

    fn max_values(&self, column: &Column) -> Option<ArrayRef> {
        let data_type = self.column_type(column)?;
        let summaries = self.column_summaries(column);
        collect_pruning_stats(data_type, summaries, Aggregate::Max)
    }

    fn num_containers(&self) -> usize {
        self.summaries.len()
    }

    fn null_counts(&self, column: &Column) -> Option<ArrayRef> {
        let null_counts = self
            .column_summaries(column)
            .map(|x| x.and_then(|s| s.null_count.map(|x| x as u64)));

        Some(Arc::new(UInt64Array::from_iter(null_counts)))
    }
}

/// Collects an [`ArrayRef`] containing the aggregate statistic corresponding to
/// `aggregate` for each of the provided [`Statistics`]
fn collect_pruning_stats<'a>(
    data_type: &DataType,
    statistics: impl Iterator<Item = Option<&'a ColumnStatistics>>,
    aggregate: Aggregate,
) -> Option<ArrayRef> {
    let null = ScalarValue::try_from(data_type).ok()?;

    ScalarValue::iter_to_array(statistics.map(|stats| {
        stats
            .and_then(|stats| get_aggregate(stats, aggregate).cloned())
            .unwrap_or_else(|| null.clone())
    }))
    .ok()
}

/// Returns the aggregate statistic corresponding to `aggregate` from `stats`
fn get_aggregate(stats: &ColumnStatistics, aggregate: Aggregate) -> Option<&ScalarValue> {
    match aggregate {
        Aggregate::Min => stats.min_value.as_ref(),
        Aggregate::Max => stats.max_value.as_ref(),
        _ => None,
    }
}

#[cfg(test)]
mod test {
    use std::{ops::Not, sync::Arc};

    use datafusion::prelude::{col, lit};
    use datafusion_util::lit_dict;
    use predicate::Predicate;
    use schema::merge::SchemaMerger;

    use crate::{test::TestChunk, QueryChunk};

    use super::*;

    #[test]
    fn test_empty() {
        test_helpers::maybe_start_logging();
        let c1 = Arc::new(TestChunk::new("chunk1"));

        let predicate = Predicate::new();
        let result = prune_chunks(&c1.schema().clone(), &[c1], &predicate);

        assert_eq!(result, Err(NotPrunedReason::NoExpressionOnPredicate));
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

        let predicate = Predicate::new().with_expr(col("column1").gt(lit(100.0f64)));

        let result = prune_chunks(&c1.schema().clone(), &[c1], &predicate);
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

        let predicate = Predicate::new().with_expr(col("column1").gt(lit(100i64)));

        let result = prune_chunks(&c1.schema().clone(), &[c1], &predicate);

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

        let predicate = Predicate::new().with_expr(col("column1").gt(lit(100u64)));

        let result = prune_chunks(&c1.schema().clone(), &[c1], &predicate);
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

        let predicate = Predicate::new().with_expr(col("column1"));

        let result = prune_chunks(&c1.schema().clone(), &[c1], &predicate);
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

        let predicate = Predicate::new().with_expr(col("column1").gt(lit("z")));

        let result = prune_chunks(&c1.schema().clone(), &[c1], &predicate);
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

        let predicate = Predicate::new().with_expr(col("column1").lt(lit(100.0f64)));

        let result = prune_chunks(&c1.schema().clone(), &[c1], &predicate);
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

        let predicate = Predicate::new().with_expr(col("column1").lt(lit(100i64)));

        let result = prune_chunks(&c1.schema().clone(), &[c1], &predicate);
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

        let predicate = Predicate::new().with_expr(col("column1").lt(lit(100u64)));

        let result = prune_chunks(&c1.schema().clone(), &[c1], &predicate);
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

        let predicate = Predicate::new().with_expr(col("column1"));

        let result = prune_chunks(&c1.schema().clone(), &[c1], &predicate);
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

        let predicate = Predicate::new().with_expr(col("column1").lt(lit("z")));

        let result = prune_chunks(&c1.schema().clone(), &[c1], &predicate);
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

        let predicate = Predicate::new().with_expr(col("column1").gt(lit(100i64)));

        let chunks = vec![c1, c2, c3, c4];
        let schema = merge_schema(&chunks);

        let result = prune_chunks(&schema, &chunks, &predicate);

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

        let predicate = Predicate::new().with_expr(col("column1").gt(lit(100i64)));

        let chunks = vec![c1, c2, c3, c4, c5, c6];
        let schema = merge_schema(&chunks);

        let result = prune_chunks(&schema, &chunks, &predicate);

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

        let predicate = Predicate::new().with_expr(col("column1").gt(lit(100i64)));

        let chunks = vec![c1, c2, c3];
        let schema = merge_schema(&chunks);

        let result = prune_chunks(&schema, &chunks, &predicate);

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
            TestChunk::new("chunk1").with_tag_column_with_nulls_and_full_stats(
                "column1",
                Some("a"),
                Some("z"),
                100,
                None,
                0,
            ),
        ) as Arc<dyn QueryChunk>;

        // Has no nulls, can prune it out based on statistics alone
        let c2 = Arc::new(
            TestChunk::new("chunk2").with_tag_column_with_nulls_and_full_stats(
                "column1",
                Some("a"),
                Some("b"),
                100,
                None,
                0,
            ),
        ) as Arc<dyn QueryChunk>;

        // Has nulls, can still can prune it out based on statistics alone
        let c3 = Arc::new(
            TestChunk::new("chunk3").with_tag_column_with_nulls_and_full_stats(
                "column1",
                Some("a"),
                Some("b"),
                100,
                None,
                1, // that one peksy null!
            ),
        ) as Arc<dyn QueryChunk>;

        let predicate = Predicate::new().with_expr(
            col("column1")
                .is_null()
                .not()
                .and(col("column1").eq(lit_dict("bar"))),
        );

        let chunks = vec![c1, c2, c3];
        let schema = merge_schema(&chunks);

        let result = prune_chunks(&schema, &chunks, &predicate);

        assert_eq!(result.expect("pruning succeeds"), vec![true, false, false]);
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

        let predicate = Predicate::new().with_expr(
            col("column1")
                .gt(lit(100i64))
                .and(col("column2").lt(lit(5i64))),
        );

        let chunks = vec![c1, c2, c3, c4, c5, c6];
        let schema = merge_schema(&chunks);

        let result = prune_chunks(&schema, &chunks, &predicate);

        assert_eq!(
            result.expect("Pruning succeeds"),
            vec![true, false, false, true, false, true]
        );
    }
}
