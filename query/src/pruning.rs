//! Implementation of statistics based pruning

use arrow::array::ArrayRef;
use data_types::partition_metadata::{ColumnSummary, TableSummary};
use datafusion::{
    logical_plan::{Column, Expr},
    physical_optimizer::pruning::{PruningPredicate, PruningStatistics},
};
use observability_deps::tracing::{debug, trace};
use predicate::predicate::Predicate;

use crate::{
    statistics::{max_to_scalar, min_to_scalar},
    QueryChunkMeta,
};

/// Something that cares to be notified when pruning of chunks occurs
pub trait PruningObserver {
    type Observed;

    /// Called when the specified chunk was pruned from observation
    fn was_pruned(&self, _chunk: &Self::Observed) {}

    /// Called when no pruning can happen at all for some reason
    fn could_not_prune(&self, _reason: &str) {}

    /// Called when the specified chunk could not be pruned, for some reason
    fn could_not_prune_chunk(&self, _chunk: &Self::Observed, _reason: &str) {}
}

/// Given a Vec of prunable items, returns a possibly smaller set
/// filtering those that can not pass the predicate.
pub fn prune_chunks<C, P, O>(observer: &O, summaries: Vec<C>, predicate: &Predicate) -> Vec<C>
where
    C: AsRef<P>,
    P: QueryChunkMeta,
    O: PruningObserver<Observed = P>,
{
    let num_chunks = summaries.len();
    trace!(num_chunks, %predicate, "Pruning chunks");

    let filter_expr = match predicate.filter_expr() {
        Some(expr) => expr,
        None => {
            observer.could_not_prune("No expression on predicate");
            return summaries;
        }
    };

    // TODO: performance optimization: batch the chunk pruning by
    // grouping the chunks with the same types for all columns
    // together and then creating a single PruningPredicate for each
    // group.
    let pruned_summaries: Vec<_> = summaries
        .into_iter()
        .filter(|c| must_keep(observer, c.as_ref(), &filter_expr))
        .collect();

    let num_remaining_chunks = pruned_summaries.len();
    debug!(
        %predicate,
        num_chunks,
        num_pruned_chunks = num_chunks - num_remaining_chunks,
        num_remaining_chunks,
        "Pruned chunks"
    );
    pruned_summaries
}

/// returns true if rows in chunk may pass the predicate
fn must_keep<P, O>(observer: &O, chunk: &P, filter_expr: &Expr) -> bool
where
    P: QueryChunkMeta,
    O: PruningObserver<Observed = P>,
{
    trace!(?filter_expr, schema=?chunk.schema(), "creating pruning predicate");

    let pruning_predicate = match PruningPredicate::try_new(filter_expr, chunk.schema().as_arrow())
    {
        Ok(p) => p,
        Err(e) => {
            observer.could_not_prune_chunk(chunk, "Can not create pruning predicate");
            trace!(%e, ?filter_expr, "Can not create pruning predicate");
            return true;
        }
    };

    let statistics = ChunkMetaStats {
        summary: chunk.summary(),
    };

    match pruning_predicate.prune(&statistics) {
        Ok(results) => {
            // Boolean array for each row in stats, false if the
            // stats could not pass the predicate
            let must_keep = results[0]; // 0 as ChunkMetaStats returns a single row
            if !must_keep {
                observer.was_pruned(chunk)
            }
            must_keep
        }
        Err(e) => {
            observer.could_not_prune_chunk(chunk, "Can not evaluate pruning predicate");
            trace!(%e, ?filter_expr, "Can not evauate pruning predicate");
            true
        }
    }
}

// struct to implement pruning
struct ChunkMetaStats<'a> {
    summary: &'a TableSummary,
}
impl<'a> ChunkMetaStats<'a> {
    fn column_summary(&self, column: &str) -> Option<&ColumnSummary> {
        let summary = self.summary.columns.iter().find(|c| c.name == column);
        summary
    }
}

impl<'a> PruningStatistics for ChunkMetaStats<'a> {
    fn min_values(&self, column: &Column) -> Option<ArrayRef> {
        let min = self
            .column_summary(&column.name)
            .and_then(|c| min_to_scalar(&c.stats))
            .map(|s| s.to_array_of_size(1));
        min
    }

    fn max_values(&self, column: &Column) -> Option<ArrayRef> {
        let max = self
            .column_summary(&column.name)
            .and_then(|c| max_to_scalar(&c.stats))
            .map(|s| s.to_array_of_size(1));
        max
    }

    fn num_containers(&self) -> usize {
        // We don't (yet) group multiple table summaries into a single
        // object, so we are always evaluating the pruning predicate
        // on a single chunk at a time
        1
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use crate::{test::TestChunk, QueryChunk};
    use datafusion::logical_plan::{col, lit};
    use predicate::predicate::PredicateBuilder;
    use std::{cell::RefCell, sync::Arc};

    #[test]
    fn test_empty() {
        test_helpers::maybe_start_logging();
        let observer = TestObserver::new();
        let c1 = Arc::new(TestChunk::new("chunk1"));

        let predicate = PredicateBuilder::new().build();
        let pruned = prune_chunks(&observer, vec![c1], &predicate);

        assert_eq!(
            observer.events(),
            vec!["Could not prune: No expression on predicate"]
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

        let predicate = PredicateBuilder::new()
            .add_expr(col("column1").gt(lit(100.0)))
            .build();

        let pruned = prune_chunks(&observer, vec![c1], &predicate);
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

        let predicate = PredicateBuilder::new()
            .add_expr(col("column1").gt(lit(100)))
            .build();

        let pruned = prune_chunks(&observer, vec![c1], &predicate);

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

        let predicate = PredicateBuilder::new()
            .add_expr(col("column1").gt(lit(100)))
            .build();

        let pruned = prune_chunks(&observer, vec![c1], &predicate);

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

        let predicate = PredicateBuilder::new().add_expr(col("column1")).build();

        let pruned = prune_chunks(&observer, vec![c1], &predicate);

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

        let predicate = PredicateBuilder::new()
            .add_expr(col("column1").gt(lit("z")))
            .build();

        let pruned = prune_chunks(&observer, vec![c1], &predicate);

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

        let predicate = PredicateBuilder::new()
            .add_expr(col("column1").lt(lit(100.0)))
            .build();

        let pruned = prune_chunks(&observer, vec![c1], &predicate);
        assert!(observer.events().is_empty());
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

        let predicate = PredicateBuilder::new()
            .add_expr(col("column1").lt(lit(100)))
            .build();

        let pruned = prune_chunks(&observer, vec![c1], &predicate);

        assert!(observer.events().is_empty());
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

        let predicate = PredicateBuilder::new()
            .add_expr(col("column1").lt(lit(100)))
            .build();

        let pruned = prune_chunks(&observer, vec![c1], &predicate);

        assert!(observer.events().is_empty());
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

        let predicate = PredicateBuilder::new().add_expr(col("column1")).build();

        let pruned = prune_chunks(&observer, vec![c1], &predicate);

        assert!(observer.events().is_empty());
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

        let predicate = PredicateBuilder::new()
            .add_expr(col("column1").lt(lit("z")))
            .build();

        let pruned = prune_chunks(&observer, vec![c1], &predicate);

        assert!(observer.events().is_empty());
        assert_eq!(names(&pruned), vec!["chunk1"]);
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
        ));

        let c2 = Arc::new(TestChunk::new("chunk2").with_i64_field_column_with_stats(
            "column1",
            Some(0),
            None,
        ));

        let c3 = Arc::new(
            TestChunk::new("chunk3").with_i64_field_column_with_stats("column1", None, None),
        );

        let c4 = Arc::new(TestChunk::new("chunk4").with_i64_field_column_no_stats("column1"));

        let predicate = PredicateBuilder::new()
            .add_expr(col("column1").gt(lit(100)))
            .build();

        let pruned = prune_chunks(&observer, vec![c1, c2, c3, c4], &predicate);

        assert_eq!(observer.events(), vec!["chunk1: Pruned"]);
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
        ));

        let c2 = Arc::new(TestChunk::new("chunk2").with_i64_field_column_with_stats(
            "column1",
            Some(0),
            Some(1000),
        ));

        let c3 = Arc::new(TestChunk::new("chunk3").with_i64_field_column_with_stats(
            "column1",
            Some(10),
            Some(20),
        ));

        let c4 = Arc::new(
            TestChunk::new("chunk4").with_i64_field_column_with_stats("column1", None, None),
        );

        let c5 = Arc::new(TestChunk::new("chunk5").with_i64_field_column_with_stats(
            "column1",
            Some(10),
            None,
        ));

        let c6 = Arc::new(TestChunk::new("chunk6").with_i64_field_column_with_stats(
            "column1",
            None,
            Some(20),
        ));

        let predicate = PredicateBuilder::new()
            .add_expr(col("column1").gt(lit(100)))
            .build();

        let pruned = prune_chunks(&observer, vec![c1, c2, c3, c4, c5, c6], &predicate);

        assert_eq!(
            observer.events(),
            vec!["chunk1: Pruned", "chunk3: Pruned", "chunk6: Pruned"]
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
        );

        let c2 = Arc::new(
            TestChunk::new("chunk2")
                .with_i64_field_column_with_stats("column1", Some(0), Some(1000))
                .with_i64_field_column_with_stats("column2", Some(0), Some(4)),
        );

        let c3 = Arc::new(TestChunk::new("chunk3").with_i64_field_column_with_stats(
            "column2",
            Some(0),
            Some(4),
        ));

        let predicate = PredicateBuilder::new()
            .add_expr(col("column1").gt(lit(100)))
            .build();

        let pruned = prune_chunks(&observer, vec![c1, c2, c3], &predicate);

        assert_eq!(
            observer.events(),
            vec![
                "chunk1: Pruned",
                "chunk3: Could not prune chunk: Can not evaluate pruning predicate"
            ]
        );
        assert_eq!(names(&pruned), vec!["chunk2", "chunk3"]);
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
        );

        let c2 = Arc::new(
            TestChunk::new("chunk2")
                .with_i64_field_column_with_stats("column1", Some(0), Some(10))
                .with_i64_field_column_with_stats("column2", Some(0), Some(4)),
        );

        let c3 = Arc::new(
            TestChunk::new("chunk3")
                .with_i64_field_column_with_stats("column1", Some(0), Some(10))
                .with_i64_field_column_with_stats("column2", Some(5), Some(10)),
        );

        let c4 = Arc::new(
            TestChunk::new("chunk4")
                .with_i64_field_column_with_stats("column1", Some(1000), Some(2000))
                .with_i64_field_column_with_stats("column2", Some(0), Some(4)),
        );

        let c5 = Arc::new(
            TestChunk::new("chunk5")
                .with_i64_field_column_with_stats("column1", Some(0), Some(10))
                .with_i64_field_column_no_stats("column2"),
        );

        let c6 = Arc::new(
            TestChunk::new("chunk6")
                .with_i64_field_column_no_stats("column1")
                .with_i64_field_column_with_stats("column2", Some(0), Some(4)),
        );

        let predicate = PredicateBuilder::new()
            .add_expr(col("column1").gt(lit(100)).and(col("column2").lt(lit(5))))
            .build();

        let pruned = prune_chunks(&observer, vec![c1, c2, c3, c4, c5, c6], &predicate);

        assert_eq!(
            observer.events(),
            vec!["chunk2: Pruned", "chunk3: Pruned", "chunk5: Pruned"]
        );
        assert_eq!(names(&pruned), vec!["chunk1", "chunk4", "chunk6"]);
    }

    #[test]
    fn test_pruned_incompatible_types() {
        test_helpers::maybe_start_logging();
        // Ensure pruning doesn't error / works when some chunks
        // return stats of incompatible types

        // column1 < 100
        //   c1: column1 ["0", "9"] --> not pruned (types are different)
        //   c2: column1 ["1000", "2000"] --> not pruned (types are still different)
        //   c3: column1 [1000, 2000] --> pruned (types are correct)

        let observer = TestObserver::new();
        let c1 = Arc::new(
            TestChunk::new("chunk1").with_string_field_column_with_stats(
                "column1",
                Some("0"),
                Some("9"),
            ),
        );

        let c2 = Arc::new(
            TestChunk::new("chunk2").with_string_field_column_with_stats(
                "column1",
                Some("1000"),
                Some("2000"),
            ),
        );

        let c3 = Arc::new(TestChunk::new("chunk3").with_i64_field_column_with_stats(
            "column1",
            Some(1000),
            Some(2000),
        ));

        let predicate = PredicateBuilder::new()
            .add_expr(col("column1").lt(lit(100)))
            .build();

        let pruned = prune_chunks(&observer, vec![c1, c2, c3], &predicate);

        assert_eq!(
            observer.events(),
            vec![
                "chunk1: Could not prune chunk: Can not create pruning predicate",
                "chunk2: Could not prune chunk: Can not create pruning predicate",
                "chunk3: Pruned",
            ]
        );
        assert_eq!(names(&pruned), vec!["chunk1", "chunk2"]);
    }

    #[test]
    fn test_pruned_different_types() {
        test_helpers::maybe_start_logging();
        // Ensure pruning works even when different chunks have
        // different types for the columns

        // column1 < 100
        //   c1: column1 [0i64, 1000i64]  --> not pruned (in range)
        //   c2: column1 [0u64, 1000u64] --> not pruned (note types are different)
        //   c3: column1 [1000i64, 2000i64] --> pruned (out of range)
        //   c4: column1 [1000u64, 2000u64] --> pruned (types are different)

        let observer = TestObserver::new();
        let c1 = Arc::new(TestChunk::new("chunk1").with_i64_field_column_with_stats(
            "column1",
            Some(0),
            Some(1000),
        ));

        let c2 = Arc::new(TestChunk::new("chunk2").with_u64_field_column_with_stats(
            "column1",
            Some(0),
            Some(1000),
        ));

        let c3 = Arc::new(TestChunk::new("chunk3").with_i64_field_column_with_stats(
            "column1",
            Some(1000),
            Some(2000),
        ));

        let c4 = Arc::new(TestChunk::new("chunk4").with_u64_field_column_with_stats(
            "column1",
            Some(1000),
            Some(2000),
        ));

        let predicate = PredicateBuilder::new()
            .add_expr(col("column1").lt(lit(100)))
            .build();

        let pruned = prune_chunks(&observer, vec![c1, c2, c3, c4], &predicate);

        assert_eq!(observer.events(), vec!["chunk3: Pruned", "chunk4: Pruned"]);
        assert_eq!(names(&pruned), vec!["chunk1", "chunk2"]);
    }

    fn names(pruned: &[Arc<TestChunk>]) -> Vec<&str> {
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
        type Observed = TestChunk;

        fn was_pruned(&self, chunk: &Self::Observed) {
            self.events.borrow_mut().push(format!("{}: Pruned", chunk))
        }

        fn could_not_prune(&self, reason: &str) {
            self.events
                .borrow_mut()
                .push(format!("Could not prune: {}", reason))
        }

        fn could_not_prune_chunk(&self, chunk: &Self::Observed, reason: &str) {
            self.events
                .borrow_mut()
                .push(format!("{}: Could not prune chunk: {}", chunk, reason))
        }
    }
}
