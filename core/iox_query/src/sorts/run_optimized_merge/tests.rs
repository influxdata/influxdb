//! Tests for the run-optimized streaming merge.
//!
//! Every test runs with the merge's `cfg(test)` `O(k)` self-check active, which
//! re-derives each emitted block against every other input the slow way. The
//! cases below therefore test not only that the output is sorted but that the
//! `log2(k)` comparisons the merge actually makes were enough to know it.

use std::{
    cmp::Ordering,
    sync::Arc,
    task::{Context, Poll},
};

use arrow::{
    array::{Array, ArrayRef, AsArray, DictionaryArray, Int64Array, RecordBatch, UInt64Array},
    buffer::Buffer,
    compute::SortOptions,
    datatypes::{DataType, Field, Int32Type, Int64Type, Schema, SchemaRef},
};
use datafusion::{
    error::{DataFusionError, Result},
    execution::memory_pool::{GreedyMemoryPool, MemoryConsumer, MemoryPool, UnboundedMemoryPool},
    physical_expr::{LexOrdering, PhysicalExpr, PhysicalSortExpr},
    physical_plan::{
        expressions::Column,
        metrics::{BaselineMetrics, ExecutionPlanMetricsSet, MetricsSet},
        stream::RecordBatchStreamAdapter,
    },
};
use futures::{TryStreamExt, stream};
use proptest::prelude::*;
use tokio_stream::wrappers::ReceiverStream;

use super::{
    RunOptimizedMergeMetrics, RunOptimizedStreamingMergeBuilder,
    compare::{ComparatorCache, Pair, RowComparator},
    input::MergeInput,
    merge::BlockMerger,
    sort_key::{evaluate_sort_keys, sort_options},
};

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

/// `tag` is a dictionary so the tests exercise the same key types IOx uses;
/// `__rowid` is unique across every input and identifies a row regardless of
/// which stream it came from.
fn test_schema() -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new(
            "tag",
            DataType::Dictionary(Box::new(DataType::Int32), Box::new(DataType::Utf8)),
            true,
        ),
        Field::new("time", DataType::Int64, true),
        Field::new("__rowid", DataType::UInt64, false),
    ]))
}

/// A row of test data: a tag, a time, and the id that tracks it through the
/// merge.
type Row = (Option<String>, Option<i64>, u64);

fn make_batch(rows: &[Row]) -> RecordBatch {
    let tags: DictionaryArray<Int32Type> = rows
        .iter()
        .map(|(t, _, _)| t.as_ref().map(String::as_str))
        .collect();
    let times = Int64Array::from_iter(rows.iter().map(|(_, t, _)| *t));
    let rowids = UInt64Array::from_iter_values(rows.iter().map(|(_, _, id)| *id));
    RecordBatch::try_new(
        test_schema(),
        vec![Arc::new(tags), Arc::new(times), Arc::new(rowids)],
    )
    .expect("valid batch")
}

/// Cut `rows` into batches of at most `batch_rows`.
fn make_batches(rows: &[Row], batch_rows: usize) -> Vec<RecordBatch> {
    if rows.is_empty() {
        return Vec::new();
    }
    rows.chunks(batch_rows).map(make_batch).collect()
}

fn col(schema: &SchemaRef, name: &str) -> Arc<dyn PhysicalExpr> {
    Arc::new(Column::new_with_schema(name, schema).expect("column exists")) as _
}

/// An ordering over `(name, descending, nulls_first)` triples.
fn ordering(cols: &[(&str, bool, bool)]) -> LexOrdering {
    let schema = test_schema();
    LexOrdering::new(cols.iter().map(|(name, descending, nulls_first)| {
        PhysicalSortExpr::new(
            col(&schema, name),
            SortOptions {
                descending: *descending,
                nulls_first: *nulls_first,
            },
        )
    }))
    .expect("non-empty ordering")
}

/// The default IOx shape: grouped by tag, ordered by time.
fn tag_time() -> LexOrdering {
    ordering(&[("tag", false, true), ("time", false, true)])
}

fn stream_of(batches: Vec<RecordBatch>) -> datafusion::execution::SendableRecordBatchStream {
    Box::pin(RecordBatchStreamAdapter::new(
        test_schema(),
        stream::iter(batches.into_iter().map(Ok)),
    ))
}

/// A stream that delivers its batches through a channel, so that the merge has
/// to cope with [`Poll::Pending`](std::task::Poll::Pending) while filling.
fn slow_stream(batches: Vec<RecordBatch>) -> datafusion::execution::SendableRecordBatchStream {
    let (tx, rx) = tokio::sync::mpsc::channel(1);
    tokio::spawn(async move {
        for batch in batches {
            tokio::task::yield_now().await;
            if tx.send(Ok(batch)).await.is_err() {
                break;
            }
        }
    });
    Box::pin(RecordBatchStreamAdapter::new(
        test_schema(),
        ReceiverStream::new(rx),
    ))
}

/// How a merge is set up and run.
struct Harness {
    streams: Vec<Vec<Row>>,
    expr: LexOrdering,
    batch_rows: usize,
    batch_size: usize,
    fetch: Option<usize>,
    threshold: Option<usize>,
    cache_entries: Option<usize>,
    memory_limit: Option<usize>,
    slow: bool,
}

impl Harness {
    fn new(streams: Vec<Vec<Row>>) -> Self {
        Self {
            streams,
            expr: tag_time(),
            batch_rows: 4,
            batch_size: 8192,
            fetch: None,
            threshold: None,
            cache_entries: None,
            memory_limit: None,
            slow: false,
        }
    }

    fn with_expr(mut self, expr: LexOrdering) -> Self {
        self.expr = expr;
        self
    }

    /// The number of rows in each input batch.
    fn with_batch_rows(mut self, batch_rows: usize) -> Self {
        self.batch_rows = batch_rows;
        self
    }

    /// The number of rows asked for in each output batch.
    fn with_batch_size(mut self, batch_size: usize) -> Self {
        self.batch_size = batch_size;
        self
    }

    fn with_fetch(mut self, fetch: usize) -> Self {
        self.fetch = Some(fetch);
        self
    }

    fn with_threshold(mut self, threshold: usize) -> Self {
        self.threshold = Some(threshold);
        self
    }

    fn with_comparator_cache_entries(mut self, entries: usize) -> Self {
        self.cache_entries = Some(entries);
        self
    }

    fn with_memory_limit(mut self, limit: usize) -> Self {
        self.memory_limit = Some(limit);
        self
    }

    fn with_slow_inputs(mut self) -> Self {
        self.slow = true;
        self
    }

    async fn run(&self) -> Result<(Vec<RecordBatch>, MetricsSet)> {
        let metrics_set = ExecutionPlanMetricsSet::new();
        let pool: Arc<dyn MemoryPool> = match self.memory_limit {
            None => Arc::new(UnboundedMemoryPool::default()),
            Some(limit) => Arc::new(GreedyMemoryPool::new(limit)),
        };
        let reservation = MemoryConsumer::new("test merge").register(&pool);

        let streams = self
            .streams
            .iter()
            .map(|rows| {
                let batches = make_batches(rows, self.batch_rows);
                if self.slow {
                    slow_stream(batches)
                } else {
                    stream_of(batches)
                }
            })
            .collect();

        let mut builder = RunOptimizedStreamingMergeBuilder::new()
            .with_streams(streams)
            .with_schema(test_schema())
            .with_expressions(&self.expr)
            .with_metrics(BaselineMetrics::new(&metrics_set, 0))
            .with_merge_metrics(RunOptimizedMergeMetrics::new(&metrics_set, 0))
            .with_batch_size(self.batch_size)
            .with_fetch(self.fetch)
            .with_reservation(reservation);
        if let Some(threshold) = self.threshold {
            builder = builder.with_degradation_threshold(threshold);
        }
        if let Some(entries) = self.cache_entries {
            builder = builder.with_comparator_cache_entries(entries);
        }

        let batches = builder.build()?.try_collect::<Vec<_>>().await?;
        Ok((batches, metrics_set.clone_inner().aggregate_by_name()))
    }

    /// Run the merge and check its output against the inputs.
    async fn check(&self) -> (Vec<RecordBatch>, MetricsSet) {
        let (batches, metrics) = self.run().await.expect("merge succeeded");
        assert_sorted(&batches, &self.expr);

        let mut expected = self
            .streams
            .iter()
            .flatten()
            .map(|(_, _, id)| *id)
            .collect::<Vec<_>>();
        let mut got = rowids(&batches);
        match self.fetch {
            None => {
                expected.sort_unstable();
                got.sort_unstable();
                assert_eq!(expected, got, "the merge did not emit every input row once");
            }
            Some(fetch) => {
                assert_eq!(fetch.min(expected.len()), got.len(), "wrong fetch count");
                got.sort_unstable();
                got.dedup();
                assert_eq!(rowids(&batches).len(), got.len(), "duplicate rows emitted");
            }
        }
        (batches, metrics)
    }
}

fn rowids(batches: &[RecordBatch]) -> Vec<u64> {
    batches
        .iter()
        .flat_map(|b| {
            let ids = b
                .column_by_name("__rowid")
                .expect("rowid column")
                .as_any()
                .downcast_ref::<UInt64Array>()
                .expect("u64 rowids");
            (0..ids.len()).map(|i| ids.value(i)).collect::<Vec<_>>()
        })
        .collect()
}

/// Assert the concatenation of `batches` is ordered by `expr`, comparing across
/// batch boundaries as well as within each batch.
fn assert_sorted(batches: &[RecordBatch], expr: &LexOrdering) {
    let options = sort_options(expr);
    let keys = batches
        .iter()
        .map(|b| evaluate_sort_keys(expr, b).expect("evaluated keys").arrays)
        .collect::<Vec<_>>();

    for (b, batch_keys) in keys.iter().enumerate() {
        let rows = batches[b].num_rows();
        assert_ne!(0, rows, "the merge emitted an empty batch");
        let within = RowComparator::try_new(batch_keys, batch_keys, &options).expect("comparator");
        for row in 1..rows {
            assert_ne!(
                Ordering::Greater,
                within.compare(row - 1, row),
                "batch {b} is not sorted at row {row}"
            );
        }
        if b > 0 {
            let across =
                RowComparator::try_new(&keys[b - 1], batch_keys, &options).expect("comparator");
            assert_ne!(
                Ordering::Greater,
                across.compare(batches[b - 1].num_rows() - 1, 0),
                "batch {b} does not follow batch {}",
                b - 1
            );
        }
    }
}

/// The value of a named metric.
///
/// `MetricsSet::sum_by_name` ignores both `OutputRows` and the custom values
/// the merge reports, so the set is scanned by name instead. It has already
/// been aggregated by name, so there is exactly one entry to find.
fn metric(metrics: &MetricsSet, name: &str) -> usize {
    metrics
        .iter()
        .find(|m| m.value().name() == name)
        .unwrap_or_else(|| panic!("{name} reported"))
        .value()
        .as_usize()
}

/// Rows carrying one tag at the given times, numbered from `first_id`.
fn rows(tag: &str, times: impl IntoIterator<Item = i64>, first_id: u64) -> Vec<Row> {
    times
        .into_iter()
        .enumerate()
        .map(|(i, t)| (Some(tag.to_owned()), Some(t), first_id + i as u64))
        .collect()
}

/// Split one sorted table into `streams` streams, giving each a chunk of
/// `chunk` consecutive rows in turn.
///
/// `chunk` is exactly the block length the merge should find: at `chunk = 1`
/// the streams are perfectly striped and at `chunk >= total / streams` they are
/// disjoint.
fn interleaved(total: usize, streams: usize, chunk: usize) -> Vec<Vec<Row>> {
    let mut out = vec![Vec::new(); streams];
    for id in 0..total {
        out[(id / chunk) % streams].push((Some("a".to_owned()), Some(id as i64), id as u64));
    }
    out
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[tokio::test]
async fn merges_disjoint_streams_a_batch_at_a_time() {
    // Two streams that do not overlap at all: the merge should need one round
    // per input batch, not one per row.
    let streams = interleaved(64, 2, 32);
    let (_, metrics) = Harness::new(streams).with_batch_rows(8).check().await;
    assert_eq!(8, metric(&metrics, "merge_rounds"));
    assert_eq!(8, metric(&metrics, "mean_block_rows"));
}

#[tokio::test]
async fn merges_striped_streams_a_row_at_a_time() {
    let streams = interleaved(64, 4, 1);
    let (_, metrics) = Harness::new(streams)
        .with_batch_rows(8)
        // Never hand over, so the block merge is the thing under test.
        .with_threshold(0)
        .check()
        .await;
    assert_eq!(64, metric(&metrics, "merge_rounds"));
    assert_eq!(1, metric(&metrics, "mean_block_rows"));
    assert_eq!(0, metric(&metrics, "fell_back_to_streaming_merge"));
}

#[tokio::test]
async fn merges_the_worked_example() {
    let streams = vec![
        rows("a", [10, 20, 30], 0),
        [rows("a", [15, 25, 35], 3), rows("a", [40, 50, 60], 6)].concat(),
    ];
    let (batches, _) = Harness::new(streams)
        .with_batch_rows(3)
        .with_threshold(0)
        .check()
        .await;
    assert_eq!(vec![0, 3, 1, 4, 2, 5, 6, 7, 8], rowids(&batches));
}

#[tokio::test]
async fn merges_many_streams() {
    // A count that is not a power of two makes the bottom row of the tournament
    // tree ragged, so a leaf plays an input against a subtree winner.
    for streams in [1usize, 2, 3, 5, 7, 8, 13, 17] {
        let harness = Harness::new(interleaved(128, streams, 3))
            .with_batch_rows(5)
            .with_threshold(0);
        harness.check().await;
    }
}

#[tokio::test]
async fn merges_streams_that_block() {
    let (_, metrics) = Harness::new(interleaved(64, 3, 4))
        .with_batch_rows(4)
        .with_slow_inputs()
        .check()
        .await;
    assert_eq!(64, metric(&metrics, "output_rows"));
}

#[tokio::test]
async fn a_single_stream_is_passed_straight_through() {
    let (batches, metrics) = Harness::new(vec![rows("a", 0..40, 0)])
        .with_batch_rows(10)
        .check()
        .await;
    assert_eq!(1, metric(&metrics, "merge_inputs"));
    // One round per input batch, with nothing to clip against.
    assert_eq!(4, metric(&metrics, "merge_rounds"));
    assert_eq!(1, batches.len());
}

#[tokio::test]
async fn output_batches_are_the_requested_size() {
    let (batches, _) = Harness::new(interleaved(100, 2, 7))
        .with_batch_rows(3)
        .with_batch_size(16)
        .check()
        .await;
    let sizes = batches.iter().map(|b| b.num_rows()).collect::<Vec<_>>();
    assert_eq!(vec![16, 16, 16, 16, 16, 16, 4], sizes);
}

#[tokio::test]
async fn empty_batches_are_skipped() {
    let metrics_set = ExecutionPlanMetricsSet::new();
    let pool: Arc<dyn MemoryPool> = Arc::new(UnboundedMemoryPool::default());
    let expr = tag_time();
    let empty = RecordBatch::new_empty(test_schema());
    let streams = vec![
        stream_of(vec![
            empty.clone(),
            make_batch(&rows("a", [1, 3], 0)),
            empty.clone(),
        ]),
        stream_of(vec![
            empty.clone(),
            make_batch(&rows("a", [2, 4], 2)),
            empty,
        ]),
    ];
    let merged = RunOptimizedStreamingMergeBuilder::new()
        .with_streams(streams)
        .with_schema(test_schema())
        .with_expressions(&expr)
        .with_metrics(BaselineMetrics::new(&metrics_set, 0))
        .with_batch_size(8192)
        .with_reservation(MemoryConsumer::new("test merge").register(&pool))
        .build()
        .expect("built")
        .try_collect::<Vec<_>>()
        .await
        .expect("merged");

    assert_eq!(vec![0, 2, 1, 3], rowids(&merged));
}

#[tokio::test]
async fn fetch_stops_the_merge_early() {
    for fetch in [1usize, 5, 17, 64, 200] {
        Harness::new(interleaved(64, 3, 2))
            .with_batch_rows(4)
            .with_batch_size(8)
            .with_fetch(fetch)
            .check()
            .await;
    }
}

#[tokio::test]
async fn fetch_is_honoured_after_handing_over() {
    let (batches, metrics) = Harness::new(interleaved(4096, 4, 1))
        .with_batch_rows(64)
        .with_threshold(usize::MAX)
        .with_fetch(700)
        .check()
        .await;
    assert_eq!(1, metric(&metrics, "fell_back_to_streaming_merge"));
    assert_eq!(700, batches.iter().map(|b| b.num_rows()).sum::<usize>());
}

#[tokio::test]
async fn hands_over_when_blocks_get_short() {
    // Striped streams give one-row blocks, which is what the guard is for.
    let (_, metrics) = Harness::new(interleaved(4096, 4, 1))
        .with_batch_rows(64)
        .check()
        .await;
    assert_eq!(1, metric(&metrics, "fell_back_to_streaming_merge"));
    // The guard only looks every few hundred blocks, so the block merge does
    // some of the work before handing over.
    assert!(
        metric(&metrics, "merge_rounds") < 4096,
        "the merge should not have finished by itself"
    );
}

#[tokio::test]
async fn does_not_hand_over_when_blocks_stay_long() {
    let (_, metrics) = Harness::new(interleaved(4096, 4, 256))
        .with_batch_rows(64)
        .check()
        .await;
    assert_eq!(0, metric(&metrics, "fell_back_to_streaming_merge"));
}

#[tokio::test]
async fn handing_over_preserves_the_order_across_the_switch() {
    // Long blocks until the tail, then stripes: the guard fires part-way
    // through, and the rows already in the coalescer have to come out before
    // anything the fallback produces.
    let mut streams = interleaved(2048, 2, 512);
    let tail = interleaved(2048, 2, 1);
    for (stream, extra) in streams.iter_mut().zip(tail) {
        stream.extend(
            extra
                .into_iter()
                .map(|(tag, time, id)| (tag, time.map(|t| t + 2048), id + 2048)),
        );
    }
    let (_, metrics) = Harness::new(streams).with_batch_rows(64).check().await;
    assert_eq!(1, metric(&metrics, "fell_back_to_streaming_merge"));
}

#[tokio::test]
async fn every_ordering_is_honoured() {
    for descending in [false, true] {
        for nulls_first in [false, true] {
            let expr = ordering(&[
                ("tag", descending, nulls_first),
                ("time", descending, nulls_first),
            ]);
            let options = sort_options(&expr);
            // One sorted table per ordering, cut into three streams.
            let mut table = (0..60)
                .map(|id| {
                    let tag = match id % 5 {
                        0 => None,
                        n => Some(format!("s{n}")),
                    };
                    let time = if id % 7 == 0 { None } else { Some(id as i64) };
                    (tag, time, id as u64)
                })
                .collect::<Vec<_>>();
            sort_rows(&mut table, &options);
            let streams = (0..3)
                .map(|s| table.iter().skip(s).step_by(3).cloned().collect::<Vec<_>>())
                .collect();
            Harness::new(streams)
                .with_expr(expr)
                .with_batch_rows(4)
                .with_threshold(0)
                .check()
                .await;
        }
    }
}

/// Sort rows the way the merge would, so a test can build sorted inputs.
fn sort_rows(rows: &mut [Row], options: &[SortOptions]) {
    let key = |row: &Row| (row.0.clone(), row.1);
    rows.sort_by(|a, b| {
        let (a_tag, a_time) = key(a);
        let (b_tag, b_time) = key(b);
        compare_option(&a_tag, &b_tag, options[0])
            .then_with(|| compare_option(&a_time, &b_time, options[1]))
            .then(a.2.cmp(&b.2))
    });
}

fn compare_option<T: Ord>(a: &Option<T>, b: &Option<T>, options: SortOptions) -> Ordering {
    match (a, b) {
        (None, None) => Ordering::Equal,
        (None, Some(_)) => nulls_order(options),
        (Some(_), None) => nulls_order(options).reverse(),
        (Some(a), Some(b)) => {
            let cmp = a.cmp(b);
            if options.descending {
                cmp.reverse()
            } else {
                cmp
            }
        }
    }
}

fn nulls_order(options: SortOptions) -> Ordering {
    if options.nulls_first {
        Ordering::Less
    } else {
        Ordering::Greater
    }
}

#[tokio::test]
async fn buffered_batches_are_charged_to_the_memory_pool() {
    let err = Harness::new(interleaved(4096, 8, 1))
        .with_batch_rows(512)
        .with_memory_limit(1024)
        .run()
        .await
        .expect_err("the merge should run out of memory");
    assert!(
        matches!(err, DataFusionError::ResourcesExhausted(_)),
        "unexpected error: {err}"
    );
}

#[tokio::test]
async fn memory_is_released_as_the_merge_consumes_it() {
    // Enough for a handful of batches but nowhere near all sixty-four, so this
    // only passes if each buffered batch is released as it is consumed.
    Harness::new(interleaved(4096, 4, 512))
        .with_batch_rows(64)
        .with_memory_limit(256 * 1024)
        .check()
        .await;
}

#[tokio::test]
async fn a_tight_pool_costs_the_comparator_cache_and_not_the_merge() {
    let harness = Harness::new(interleaved(256, 8, 1))
        .with_batch_rows(4)
        .with_threshold(0);
    let (_, metrics) = harness.check().await;
    let peak = metric(&metrics, "mem_used");

    // A byte less than the merge asked for with nothing in its way, so at least
    // one comparator it wanted to keep cannot be charged for. Keeping them is
    // discretionary, so the merge should give that up and still produce every
    // row in order.
    harness.with_memory_limit(peak - 1).check().await;
}

#[tokio::test]
async fn many_inputs_still_block_merge() {
    // Far more pairs of inputs than the cache can hold, so comparators are
    // displaced and rebuilt throughout. That costs time; it must not cost
    // correctness.
    let (_, metrics) = Harness::new(interleaved(2048, 96, 1))
        .with_batch_rows(4)
        .with_threshold(0)
        .with_comparator_cache_entries(32)
        .check()
        .await;
    assert_eq!(96, metric(&metrics, "merge_inputs"));
    assert_ne!(0, metric(&metrics, "merge_rounds"));
}

#[tokio::test]
async fn the_cache_can_be_turned_off() {
    Harness::new(interleaved(512, 8, 1))
        .with_batch_rows(4)
        .with_threshold(0)
        .with_comparator_cache_entries(0)
        .check()
        .await;
}

#[tokio::test]
async fn a_stale_comparator_is_not_reused() {
    // Every input reads many batches, so the comparators built against each
    // batch have to go when it does. Comparing against the wrong batch would
    // put rows out of order.
    Harness::new(interleaved(4096, 6, 3))
        .with_batch_rows(7)
        .with_threshold(0)
        .with_comparator_cache_entries(64)
        .check()
        .await;
}

/// Buffer a batch for every input that needs one.
fn fill(merger: &mut BlockMerger) {
    let waker = futures::task::noop_waker();
    let mut cx = Context::from_waker(&waker);
    match merger.poll_fill(&mut cx) {
        Poll::Ready(result) => result.expect("filled"),
        Poll::Pending => panic!("in-memory inputs never wait"),
    }
}

/// The buffer holding a batch's `time` values, which every comparator built
/// against the batch keeps a handle on.
fn time_buffer(batch: &RecordBatch) -> Buffer {
    batch
        .column_by_name("time")
        .expect("time column")
        .as_primitive::<Int64Type>()
        .values()
        .inner()
        .clone()
}

#[test]
fn a_comparator_does_not_outlive_its_batch() {
    // Input 0's first batch orders entirely before anything in input 1, so the
    // first round consumes it outright, after comparators have been built
    // against it.
    let first = make_batch(&rows("a", 0..4, 0));
    let held = time_buffer(&first);
    let inputs = vec![
        MergeInput::new(stream_of(vec![first, make_batch(&rows("a", 8..12, 4))])),
        MergeInput::new(stream_of(vec![
            make_batch(&rows("a", 4..8, 8)),
            make_batch(&rows("a", 12..16, 12)),
        ])),
    ];
    let pool: Arc<dyn MemoryPool> = Arc::new(UnboundedMemoryPool::default());
    let mut merger = BlockMerger::new(
        inputs,
        tag_time(),
        64,
        MemoryConsumer::new("test merge").register(&pool),
        RunOptimizedMergeMetrics::default(),
    );

    fill(&mut merger);
    assert_eq!(2, held.strong_count(), "the buffered batch, and this test");
    let block = merger.next(usize::MAX).expect("a round").expect("a block");
    assert_eq!(4, block.num_rows(), "the first batch goes out whole");
    drop(block);

    assert_eq!(
        1,
        held.strong_count(),
        "something still holds the key column of a batch the merge has released"
    );
}

#[test]
fn flushing_an_input_drops_exactly_the_comparators_it_has_a_part_in() {
    let keys: Vec<ArrayRef> = vec![Arc::new(Int64Array::from(vec![1, 2, 3]))];
    let comparator = || {
        Arc::new(
            RowComparator::try_new(&keys, &keys, &[SortOptions::default()]).expect("comparator"),
        )
    };
    let bytes = comparator().bytes();

    let mut cache = ComparatorCache::new(64, 6);
    for low in 0..6 {
        for high in low + 1..6 {
            assert_eq!(0, cache.insert(Pair::new(low, high), comparator()));
        }
    }

    assert_eq!(
        5 * bytes,
        cache.flush(2),
        "input 2 has a part in five pairs"
    );
    for low in 0..6 {
        for high in low + 1..6 {
            assert_eq!(
                low != 2 && high != 2,
                cache.get(Pair::new(low, high)).is_some(),
                "pair ({low}, {high})"
            );
        }
    }
    assert_eq!(0, cache.flush(2), "input 2 has nothing left to drop");
    assert_eq!(
        4 * bytes,
        cache.flush(3),
        "(2, 3) already went with input 2"
    );
}

#[test]
fn a_finished_merge_holds_no_comparators_in_its_reservation() {
    let inputs = interleaved(256, 4, 3)
        .iter()
        .map(|rows| MergeInput::new(stream_of(make_batches(rows, 7))))
        .collect::<Vec<_>>();
    let pool: Arc<dyn MemoryPool> = Arc::new(UnboundedMemoryPool::default());
    let mut merger = BlockMerger::new(
        inputs,
        tag_time(),
        64,
        MemoryConsumer::new("test merge").register(&pool),
        RunOptimizedMergeMetrics::default(),
    );

    loop {
        fill(&mut merger);
        if merger.next(usize::MAX).expect("a round").is_none() {
            break;
        }
    }
    assert_eq!(
        ComparatorCache::bytes_for(64, 4),
        pool.reserved(),
        "every batch is consumed, so only the empty table should be charged"
    );
}

#[tokio::test]
async fn an_empty_stream_list_is_rejected() {
    let pool: Arc<dyn MemoryPool> = Arc::new(UnboundedMemoryPool::default());
    let expr = tag_time();
    let built = RunOptimizedStreamingMergeBuilder::new()
        .with_streams(Vec::new())
        .with_schema(test_schema())
        .with_expressions(&expr)
        .with_metrics(BaselineMetrics::new(&ExecutionPlanMetricsSet::new(), 0))
        .with_batch_size(8192)
        .with_reservation(MemoryConsumer::new("test merge").register(&pool))
        .build();
    let Err(err) = built else {
        panic!("an empty stream list should be rejected");
    };
    assert!(err.to_string().contains("streams cannot be empty"), "{err}");
}

#[tokio::test]
async fn errors_from_an_input_are_propagated() {
    let metrics_set = ExecutionPlanMetricsSet::new();
    let pool: Arc<dyn MemoryPool> = Arc::new(UnboundedMemoryPool::default());
    let expr = tag_time();
    let failing = Box::pin(RecordBatchStreamAdapter::new(
        test_schema(),
        stream::iter([Err(DataFusionError::Internal("input failed".to_owned()))]),
    ));
    let err = RunOptimizedStreamingMergeBuilder::new()
        .with_streams(vec![
            stream_of(vec![make_batch(&rows("a", [1], 0))]),
            failing,
        ])
        .with_schema(test_schema())
        .with_expressions(&expr)
        .with_metrics(BaselineMetrics::new(&metrics_set, 0))
        .with_batch_size(8192)
        .with_reservation(MemoryConsumer::new("test merge").register(&pool))
        .build()
        .expect("built")
        .try_collect::<Vec<_>>()
        .await
        .expect_err("the input error surfaces");
    assert!(err.to_string().contains("input failed"), "{err}");
}

// ---------------------------------------------------------------------------
// Property tests
// ---------------------------------------------------------------------------

/// A set of sorted streams, with the batch sizes and threshold to merge them
/// with.
fn merge_case() -> impl Strategy<Value = (Vec<Vec<Row>>, usize, usize, usize)> {
    (
        prop::collection::vec(
            prop::collection::vec(
                (
                    prop::option::of(prop::sample::select(vec!["a", "b", "c"])),
                    prop::option::of(0i64..12),
                ),
                0..24,
            ),
            1..6,
        ),
        1usize..8,
        1usize..16,
        prop::sample::select(vec![0usize, 4, usize::MAX]),
    )
        .prop_map(|(streams, batch_rows, batch_size, threshold)| {
            let options = sort_options(&tag_time());
            let mut id = 0;
            let streams = streams
                .into_iter()
                .map(|rows| {
                    let mut rows = rows
                        .into_iter()
                        .map(|(tag, time)| {
                            id += 1;
                            (tag.map(str::to_owned), time, id - 1)
                        })
                        .collect::<Vec<_>>();
                    sort_rows(&mut rows, &options);
                    rows
                })
                .collect();
            (streams, batch_rows, batch_size, threshold)
        })
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(192))]

    /// Any set of sorted streams merges to a sorted stream holding exactly
    /// their rows, whatever the batch sizes and whether or not the merge hands
    /// over part-way.
    #[test]
    fn merges_arbitrary_sorted_streams(
        (streams, batch_rows, batch_size, threshold) in merge_case()
    ) {
        let runtime = tokio::runtime::Builder::new_current_thread()
            .build()
            .expect("runtime");
        runtime.block_on(
            Harness::new(streams)
                .with_batch_rows(batch_rows)
                .with_batch_size(batch_size)
                .with_threshold(threshold)
                .check(),
        );
    }
}
