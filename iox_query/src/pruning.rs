//! Implementation of statistics based pruning

use crate::QueryChunk;
use arrow::{
    array::{
        ArrayRef, BooleanArray, DictionaryArray, Float64Array, Int64Array, StringArray, UInt64Array,
    },
    datatypes::{DataType, Int32Type, TimeUnit},
};
use data_types::{StatValues, Statistics};
use datafusion::{
    logical_plan::Column,
    physical_optimizer::pruning::{PruningPredicate, PruningStatistics},
};
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
    /// Called when the specified chunk was pruned from observation.
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
///
/// TODO(raphael): Perhaps this should return `Result<Vec<bool>>` instead of
/// the [`PruningObserver`] plumbing
pub fn prune_chunks<O>(
    observer: &O,
    table_schema: Arc<Schema>,
    chunks: Vec<Arc<dyn QueryChunk>>,
    predicate: &Predicate,
) -> Vec<Arc<dyn QueryChunk>>
where
    O: PruningObserver,
{
    let num_chunks = chunks.len();
    trace!(num_chunks, %predicate, "Pruning chunks");

    let filter_expr = match predicate.filter_expr() {
        Some(expr) => expr,
        None => {
            for chunk in &chunks {
                observer.could_not_prune(NotPrunedReason::NoExpressionOnPredicate, chunk.as_ref());
            }
            return chunks;
        }
    };
    trace!(%filter_expr, "Filter_expr of pruning chunks");

    let pruning_predicate =
        match PruningPredicate::try_new(filter_expr.clone(), table_schema.as_arrow()) {
            Ok(p) => p,
            Err(e) => {
                for chunk in &chunks {
                    observer.could_not_prune(
                        NotPrunedReason::CanNotCreatePruningPredicate,
                        chunk.as_ref(),
                    );
                }
                warn!(%e, ?filter_expr, "Can not create pruning predicate");
                return chunks;
            }
        };

    let statistics = ChunkPruningStatistics {
        table_schema: table_schema.as_ref(),
        chunks: chunks.as_slice(),
    };

    let results = match pruning_predicate.prune(&statistics) {
        Ok(results) => results,
        Err(e) => {
            for chunk in &chunks {
                observer.could_not_prune(NotPrunedReason::DataFusionPruningFailed, chunk.as_ref());
            }
            warn!(%e, ?filter_expr, "DataFusion pruning failed");
            return chunks;
        }
    };

    assert_eq!(chunks.len(), results.len());

    let mut pruned_chunks = Vec::with_capacity(chunks.len());
    for (chunk, keep) in chunks.into_iter().zip(results) {
        match keep {
            true => {
                observer.was_not_pruned(chunk.as_ref());
                pruned_chunks.push(chunk);
            }
            false => {
                observer.was_pruned(chunk.as_ref());
            }
        }
    }

    let num_remaining_chunks = pruned_chunks.len();
    debug!(
        %predicate,
        num_chunks,
        num_pruned_chunks = num_chunks - num_remaining_chunks,
        num_remaining_chunks,
        "Pruned chunks"
    );
    pruned_chunks
}

/// Wraps a collection of [`QueryChunk`] and implements the [`PruningStatistics`]
/// interface required by [`PruningPredicate`]
struct ChunkPruningStatistics<'a> {
    table_schema: &'a Schema,
    chunks: &'a [Arc<dyn QueryChunk>],
}

impl<'a> ChunkPruningStatistics<'a> {
    /// Returns the [`DataType`] for `column`
    fn column_type(&self, column: &Column) -> Option<&DataType> {
        let index = self.table_schema.find_index_of(&column.name)?;
        Some(self.table_schema.field(index).1.data_type())
    }

    /// Returns an iterator that for each chunk returns the [`Statistics`]
    /// for the provided `column` if any
    fn column_summaries<'b: 'a>(
        &self,
        column: &'b Column,
    ) -> impl Iterator<Item = Option<Statistics>> + 'a {
        self.chunks
            .iter()
            .map(|chunk| Some(chunk.summary()?.column(&column.name)?.stats.clone()))
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
        self.chunks.len()
    }

    fn null_counts(&self, column: &Column) -> Option<ArrayRef> {
        let null_counts = self
            .column_summaries(column)
            .map(|x| x.and_then(|s| s.null_count()));

        Some(Arc::new(UInt64Array::from_iter(null_counts)))
    }
}

/// Collects an [`ArrayRef`] containing the aggregate statistic corresponding to
/// `aggregate` for each of the provided [`Statistics`]
fn collect_pruning_stats(
    data_type: &DataType,
    statistics: impl Iterator<Item = Option<Statistics>>,
    aggregate: Aggregate,
) -> Option<ArrayRef> {
    match data_type {
        DataType::Int64 | DataType::Timestamp(TimeUnit::Nanosecond, None) => {
            let values = statistics.map(|s| match s {
                Some(Statistics::I64(v)) => get_aggregate(v, aggregate),
                _ => None,
            });
            Some(Arc::new(Int64Array::from_iter(values)))
        }
        DataType::UInt64 => {
            let values = statistics.map(|s| match s {
                Some(Statistics::U64(v)) => get_aggregate(v, aggregate),
                _ => None,
            });
            Some(Arc::new(UInt64Array::from_iter(values)))
        }
        DataType::Float64 => {
            let values = statistics.map(|s| match s {
                Some(Statistics::F64(v)) => get_aggregate(v, aggregate),
                _ => None,
            });
            Some(Arc::new(Float64Array::from_iter(values)))
        }
        DataType::Boolean => {
            let values = statistics.map(|s| match s {
                Some(Statistics::Bool(v)) => get_aggregate(v, aggregate),
                _ => None,
            });
            Some(Arc::new(BooleanArray::from_iter(values)))
        }
        DataType::Utf8 => {
            let values = statistics.map(|s| match s {
                Some(Statistics::String(v)) => get_aggregate(v, aggregate),
                _ => None,
            });
            Some(Arc::new(StringArray::from_iter(values)))
        }
        DataType::Dictionary(key, value)
            if key.as_ref() == &DataType::Int32 && value.as_ref() == &DataType::Utf8 =>
        {
            let values = statistics.map(|s| match s {
                Some(Statistics::String(v)) => get_aggregate(v, aggregate),
                _ => None,
            });

            // DictionaryArray can only be built from string references (`str`), not from owned strings (`String`), so
            // we need to collect the strings first
            let values: Vec<_> = values.collect();
            let values = values.iter().map(|s| s.as_deref());
            Some(Arc::new(DictionaryArray::<Int32Type>::from_iter(values)))
        }
        _ => None,
    }
}

/// Returns the aggregate statistic corresponding to `aggregate` from `stats`
fn get_aggregate<T>(stats: StatValues<T>, aggregate: Aggregate) -> Option<T> {
    match aggregate {
        Aggregate::Min => stats.min,
        Aggregate::Max => stats.max,
        _ => None,
    }
}

#[cfg(test)]
mod test {
    use std::{cell::RefCell, sync::Arc};

    use datafusion::logical_plan::{col, lit};
    use predicate::Predicate;
    use schema::merge::SchemaMerger;

    use crate::{test::TestChunk, QueryChunk, QueryChunkMeta};

    use super::*;

    #[test]
    fn test_empty() {
        test_helpers::maybe_start_logging();
        let observer = TestObserver::new();
        let c1 = Arc::new(TestChunk::new("chunk1"));

        let predicate = Predicate::new();
        let pruned = prune_chunks(&observer, c1.schema(), vec![c1], &predicate);

        assert_eq!(
            observer.events(),
            vec!["chunk1: Could not prune: No expression on predicate"]
        );
        assert_eq!(names(&pruned), vec!["chunk1"]);
    }

    #[test]
    fn test_pruned_f64() {
        test_helpers::maybe_start_logging();
        // column1 > 100.0 where
        //   c1: [0.0, 10.0] --> pruned
        let observer = TestObserver::new();
        let c1 = Arc::new(TestChunk::new("chunk1").with_f64_field_column_with_stats(
            "column1",
            Some(0.0),
            Some(10.0),
        ));

        let predicate = Predicate::new().with_expr(col("column1").gt(lit(100.0)));

        let pruned = prune_chunks(&observer, c1.schema(), vec![c1], &predicate);
        assert_eq!(observer.events(), vec!["chunk1: Pruned"]);
        assert!(pruned.is_empty())
    }

    #[test]
    fn test_pruned_i64() {
        test_helpers::maybe_start_logging();
        // column1 > 100 where
        //   c1: [0, 10] --> pruned

        let observer = TestObserver::new();
        let c1 = Arc::new(TestChunk::new("chunk1").with_i64_field_column_with_stats(
            "column1",
            Some(0),
            Some(10),
        ));

        let predicate = Predicate::new().with_expr(col("column1").gt(lit(100)));

        let pruned = prune_chunks(&observer, c1.schema(), vec![c1], &predicate);

        assert_eq!(observer.events(), vec!["chunk1: Pruned"]);
        assert!(pruned.is_empty())
    }

    #[test]
    fn test_pruned_u64() {
        test_helpers::maybe_start_logging();
        // column1 > 100 where
        //   c1: [0, 10] --> pruned

        let observer = TestObserver::new();
        let c1 = Arc::new(TestChunk::new("chunk1").with_u64_field_column_with_stats(
            "column1",
            Some(0),
            Some(10),
        ));

        let predicate = Predicate::new().with_expr(col("column1").gt(lit(100)));

        let pruned = prune_chunks(&observer, c1.schema(), vec![c1], &predicate);

        assert_eq!(observer.events(), vec!["chunk1: Pruned"]);
        assert!(pruned.is_empty())
    }

    #[test]
    fn test_pruned_bool() {
        test_helpers::maybe_start_logging();
        // column1 where
        //   c1: [false, false] --> pruned

        let observer = TestObserver::new();
        let c1 = Arc::new(TestChunk::new("chunk1").with_bool_field_column_with_stats(
            "column1",
            Some(false),
            Some(false),
        ));

        let predicate = Predicate::new().with_expr(col("column1"));

        let pruned = prune_chunks(&observer, c1.schema(), vec![c1], &predicate);

        assert_eq!(observer.events(), vec!["chunk1: Pruned"]);
        assert!(pruned.is_empty())
    }

    #[test]
    fn test_pruned_string() {
        test_helpers::maybe_start_logging();
        // column1 > "z" where
        //   c1: ["a", "q"] --> pruned

        let observer = TestObserver::new();
        let c1 = Arc::new(
            TestChunk::new("chunk1").with_string_field_column_with_stats(
                "column1",
                Some("a"),
                Some("q"),
            ),
        );

        let predicate = Predicate::new().with_expr(col("column1").gt(lit("z")));

        let pruned = prune_chunks(&observer, c1.schema(), vec![c1], &predicate);

        assert_eq!(observer.events(), vec!["chunk1: Pruned"]);
        assert!(pruned.is_empty())
    }

    #[test]
    fn test_not_pruned_f64() {
        test_helpers::maybe_start_logging();
        // column1 < 100.0 where
        //   c1: [0.0, 10.0] --> not pruned
        let observer = TestObserver::new();
        let c1 = Arc::new(TestChunk::new("chunk1").with_f64_field_column_with_stats(
            "column1",
            Some(0.0),
            Some(10.0),
        ));

        let predicate = Predicate::new().with_expr(col("column1").lt(lit(100.0)));

        let pruned = prune_chunks(&observer, c1.schema(), vec![c1], &predicate);
        assert_eq!(observer.events(), vec!["chunk1: Not pruned"]);
        assert_eq!(names(&pruned), vec!["chunk1"]);
    }

    #[test]
    fn test_not_pruned_i64() {
        test_helpers::maybe_start_logging();
        // column1 < 100 where
        //   c1: [0, 10] --> not pruned

        let observer = TestObserver::new();
        let c1 = Arc::new(TestChunk::new("chunk1").with_i64_field_column_with_stats(
            "column1",
            Some(0),
            Some(10),
        ));

        let predicate = Predicate::new().with_expr(col("column1").lt(lit(100)));

        let pruned = prune_chunks(&observer, c1.schema(), vec![c1], &predicate);

        assert_eq!(observer.events(), vec!["chunk1: Not pruned"]);
        assert_eq!(names(&pruned), vec!["chunk1"]);
    }

    #[test]
    fn test_not_pruned_u64() {
        test_helpers::maybe_start_logging();
        // column1 < 100 where
        //   c1: [0, 10] --> not pruned

        let observer = TestObserver::new();
        let c1 = Arc::new(TestChunk::new("chunk1").with_u64_field_column_with_stats(
            "column1",
            Some(0),
            Some(10),
        ));

        let predicate = Predicate::new().with_expr(col("column1").lt(lit(100)));

        let pruned = prune_chunks(&observer, c1.schema(), vec![c1], &predicate);

        assert_eq!(observer.events(), vec!["chunk1: Not pruned"]);
        assert_eq!(names(&pruned), vec!["chunk1"]);
    }

    #[test]
    fn test_not_pruned_bool() {
        test_helpers::maybe_start_logging();
        // column1
        //   c1: [false, true] --> not pruned

        let observer = TestObserver::new();
        let c1 = Arc::new(TestChunk::new("chunk1").with_bool_field_column_with_stats(
            "column1",
            Some(false),
            Some(true),
        ));

        let predicate = Predicate::new().with_expr(col("column1"));

        let pruned = prune_chunks(&observer, c1.schema(), vec![c1], &predicate);

        assert_eq!(observer.events(), vec!["chunk1: Not pruned"]);
        assert_eq!(names(&pruned), vec!["chunk1"]);
    }

    #[test]
    fn test_not_pruned_string() {
        test_helpers::maybe_start_logging();
        // column1 < "z" where
        //   c1: ["a", "q"] --> not pruned

        let observer = TestObserver::new();
        let c1 = Arc::new(
            TestChunk::new("chunk1").with_string_field_column_with_stats(
                "column1",
                Some("a"),
                Some("q"),
            ),
        );

        let predicate = Predicate::new().with_expr(col("column1").lt(lit("z")));

        let pruned = prune_chunks(&observer, c1.schema(), vec![c1], &predicate);

        assert_eq!(observer.events(), vec!["chunk1: Not pruned"]);
        assert_eq!(names(&pruned), vec!["chunk1"]);
    }

    fn merge_schema(chunks: &[Arc<dyn QueryChunk>]) -> Arc<Schema> {
        let mut merger = SchemaMerger::new();
        for chunk in chunks {
            merger = merger.merge(chunk.schema().as_ref()).unwrap();
        }
        Arc::new(merger.build())
    }

    #[test]
    fn test_pruned_null() {
        test_helpers::maybe_start_logging();
        // column1 > 100 where
        //   c1: [Null, 10] --> pruned
        //   c2: [0, Null] --> not pruned
        //   c3: [Null, Null] --> not pruned (min/max are not known in chunk 3)
        //   c4: Null --> not pruned (no statistics at all)

        let observer = TestObserver::new();
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

        let c4 = Arc::new(TestChunk::new("chunk4").with_i64_field_column_no_stats("column1"))
            as Arc<dyn QueryChunk>;

        let predicate = Predicate::new().with_expr(col("column1").gt(lit(100)));

        let chunks = vec![c1, c2, c3, c4];
        let schema = merge_schema(&chunks);

        let pruned = prune_chunks(&observer, schema, chunks, &predicate);

        assert_eq!(
            observer.events(),
            vec![
                "chunk1: Pruned",
                "chunk2: Not pruned",
                "chunk3: Not pruned",
                "chunk4: Not pruned"
            ]
        );
        assert_eq!(names(&pruned), vec!["chunk2", "chunk3", "chunk4"]);
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

        let observer = TestObserver::new();
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

        let predicate = Predicate::new().with_expr(col("column1").gt(lit(100)));

        let chunks = vec![c1, c2, c3, c4, c5, c6];
        let schema = merge_schema(&chunks);

        let pruned = prune_chunks(&observer, schema, chunks, &predicate);

        assert_eq!(
            observer.events(),
            vec![
                "chunk1: Pruned",
                "chunk2: Not pruned",
                "chunk3: Pruned",
                "chunk4: Not pruned",
                "chunk5: Not pruned",
                "chunk6: Pruned"
            ]
        );
        assert_eq!(names(&pruned), vec!["chunk2", "chunk4", "chunk5"]);
    }

    #[test]
    fn test_pruned_different_schema() {
        test_helpers::maybe_start_logging();
        // column1 > 100 where
        //   c1: column1 [0, 100], column2 [0, 4] --> pruned (in range, column2 ignored)
        //   c2: column1 [0, 1000], column2 [0, 4] --> not pruned (in range, column2 ignored)
        //   c3: None, column2 [0, 4] --> not pruned (no stats for column1)
        let observer = TestObserver::new();
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

        let predicate = Predicate::new().with_expr(col("column1").gt(lit(100)));

        let chunks = vec![c1, c2, c3];
        let schema = merge_schema(&chunks);

        let pruned = prune_chunks(&observer, schema, chunks, &predicate);

        assert_eq!(
            observer.events(),
            vec!["chunk1: Pruned", "chunk2: Not pruned", "chunk3: Not pruned"]
        );
        assert_eq!(names(&pruned), vec!["chunk2", "chunk3"]);
    }

    #[test]
    fn test_pruned_is_null() {
        test_helpers::maybe_start_logging();
        // Verify that type of predicate is pruned if column1 is null
        // (this is a common predicate type created by the INfluxRPC planner)
        // (NOT column1 IS NULL) AND (column1 = 'bar')
        let observer = TestObserver::new();
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
                .and(col("column1").eq(lit("bar"))),
        );

        let chunks = vec![c1, c2, c3];
        let schema = merge_schema(&chunks);

        let pruned = prune_chunks(&observer, schema, chunks, &predicate);

        assert_eq!(
            observer.events(),
            vec!["chunk1: Not pruned", "chunk2: Pruned", "chunk3: Pruned"]
        );
        assert_eq!(names(&pruned), vec!["chunk1"]);
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

        let observer = TestObserver::new();
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
                .with_i64_field_column_no_stats("column2"),
        ) as Arc<dyn QueryChunk>;

        let c6 = Arc::new(
            TestChunk::new("chunk6")
                .with_i64_field_column_no_stats("column1")
                .with_i64_field_column_with_stats("column2", Some(0), Some(4)),
        ) as Arc<dyn QueryChunk>;

        let predicate =
            Predicate::new().with_expr(col("column1").gt(lit(100)).and(col("column2").lt(lit(5))));

        let chunks = vec![c1, c2, c3, c4, c5, c6];
        let schema = merge_schema(&chunks);

        let pruned = prune_chunks(&observer, schema, chunks, &predicate);

        assert_eq!(
            observer.events(),
            vec![
                "chunk1: Not pruned",
                "chunk2: Pruned",
                "chunk3: Pruned",
                "chunk4: Not pruned",
                "chunk5: Pruned",
                "chunk6: Not pruned"
            ]
        );
        assert_eq!(names(&pruned), vec!["chunk1", "chunk4", "chunk6"]);
    }

    fn names(pruned: &[Arc<dyn QueryChunk>]) -> Vec<&str> {
        pruned.iter().map(|p| p.table_name()).collect()
    }

    #[derive(Debug, Default)]
    struct TestObserver {
        events: RefCell<Vec<String>>,
    }

    impl TestObserver {
        fn new() -> Self {
            Self::default()
        }

        fn events(&self) -> Vec<String> {
            self.events.borrow().iter().cloned().collect()
        }
    }

    impl PruningObserver for TestObserver {
        fn was_pruned(&self, chunk: &dyn QueryChunk) {
            self.events
                .borrow_mut()
                .push(format!("{}: Pruned", chunk.table_name()))
        }

        fn was_not_pruned(&self, chunk: &dyn QueryChunk) {
            self.events
                .borrow_mut()
                .push(format!("{}: Not pruned", chunk.table_name()))
        }

        fn could_not_prune(&self, reason: NotPrunedReason, chunk: &dyn QueryChunk) {
            self.events.borrow_mut().push(format!(
                "{}: Could not prune: {}",
                chunk.table_name(),
                reason
            ))
        }
    }
}
