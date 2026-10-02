//! The builder for a run-optimized streaming merge.

use std::fmt::{Debug, Formatter};

use arrow::datatypes::SchemaRef;
use datafusion::{
    common::internal_err,
    error::Result,
    execution::{SendableRecordBatchStream, memory_pool::MemoryReservation},
    physical_expr::LexOrdering,
    physical_plan::metrics::BaselineMetrics,
};

use super::{
    input::MergeInput, merge::BlockMerger, metrics::RunOptimizedMergeMetrics,
    stream::RunOptimizedMergeStream,
};

/// The default mean block length below which the merge hands over to a
/// row-at-a-time merge.
///
/// Measured with `benches/run_optimized_merge.rs`: four sorted streams holding
/// 64 batches of 8192 rows between them, interleaved so that the merge finds
/// blocks of exactly the given length, against the same streams merged by
/// DataFusion (ms):
///
/// ```text
/// block rows     1     2     4     8    16    32    64   128   256  1024  8192
/// this merge   281   176  87.8  45.0  22.4  11.5   6.1   3.3   1.9  0.83  0.18
/// DataFusion  11.9  11.4  12.4  13.1  12.2  11.7  11.4  11.3  11.2  10.8  10.5
/// ```
///
/// Across that sweep DataFusion's merge is flat: it copies every row once and
/// compares it `log2(k)` times however long the blocks are. This merge costs
/// about `0.5ms + 361ms/rows`, so the two cross at a block of roughly 32 rows.
/// DataFusion's time drifts between 10.5 and 13.1ms across the sweep, which
/// puts the crossing between 29 and 36 rows. At 32 itself the two measure
/// level, 11.5 against 11.7.
///
/// Both tails are steep, and near enough symmetric about the crossing: at a
/// mean block of 8 rows carrying on costs 3.4x, and at 128 handing over costs
/// 3.4x. Neither direction is the safe one to err in, so the threshold sits on
/// the crossing rather than to one side of it.
///
/// Handing over is one way, because it consumes the block merge: inputs that go
/// back to producing long blocks afterwards stay on DataFusion's flat curve. It
/// is not free either, because the guard only looks every few hundred blocks
/// and the merge does that much work the slow way first — at one-row blocks
/// that costs 4% over merging with DataFusion from the start, which is the
/// price of not having to know in advance.
pub const DEFAULT_DEGRADATION_THRESHOLD_ROWS: usize = 32;

/// The number of comparators the merge keeps by default.
///
/// Comparators are held per pair of inputs, and the pairs of `k` inputs grow as
/// `k^2`, so the number of them worth keeping has to be capped somewhere rather
/// than left to follow the input count. 2048 is room for every pair of 64
/// inputs, and costs under a megabyte for a `[tag, time]` key.
///
/// Room is not a promise of a place. A pair may only be held in the one set of
/// slots its key maps to, so a comparator can be displaced while much of the
/// cache stands empty, and even a cache sized for every pair will turn some of
/// them away. What that costs is a comparator built again when it is next
/// wanted; what it never costs is more memory.
pub const DEFAULT_COMPARATOR_CACHE_ENTRIES: usize = 2048;

/// Builds a sorted stream out of sorted streams, a *block* at a time.
///
/// This is an alternative to DataFusion's [`StreamingMergeBuilder`] with the
/// same contract — every input stream must already be sorted by `expressions`,
/// and the output is their order-preserving merge — and largely the same
/// builder methods.
///
/// [`StreamingMergeBuilder`]:
///     datafusion::physical_plan::sorts::streaming_merge::StreamingMergeBuilder
///
/// # What it is for
///
/// A row-at-a-time merge costs `O(log k)` comparisons and one row copy for
/// every row it emits, whatever the inputs look like. That is the right price
/// when the inputs are finely interleaved, and much too high when they are not.
///
/// IOx inputs usually are not. Pacha's merge path emits ascending runs and
/// parquet chunks carry their own sort keys, so the streams being merged tend
/// to be *disjoint in time*: long spans of one stream ordering entirely before
/// the next row of any other. This merge finds those spans and passes each one
/// on as a single zero-copy slice, paying `O(log k)` comparisons per *block*
/// rather than per row. When the assumption turns out to be wrong it hands its
/// inputs to DataFusion's merge, which is where it would have been all along.
///
/// # The tournament
///
/// The inputs are held in a *loser tree*, the usual structure for a k-way
/// merge. It is an array of `k` nodes over `k` inputs:
///
/// * node `0` holds the overall winner, the input whose next row is smallest;
/// * node `i`, for `i > 0`, holds the *loser* of the match played at that node.
///
/// Input `r` enters at node `(k + r) / 2`, and the parent of node `i` is
/// `i / 2`. Ties are broken by input index, so the tournament is a total
/// order.
///
/// Building the tree costs `k` insertions. After that a round costs one
/// leaf-to-root walk of `log2(k)` comparisons, because only one input has
/// changed: the previous winner has given up some rows, and replaying it from
/// its own leaf replays exactly the matches whose outcome that could change.
///
/// Comparing two inputs means comparing rows of two different batches, which
/// takes a comparator built from both — a closure per sort column, and for a
/// dictionary column a nested comparator over its values. Those are cached,
/// one per pair of inputs, so a round compares without allocating. Ordering is
/// antisymmetric, so a pair needs only the one comparator whichever way round
/// it is asked about. It is worth 30 to 60% wherever rounds are frequent, and
/// the more the deeper the tree; where a round emits a whole batch there is
/// nothing to amortise and it neither helps nor costs.
///
/// # The second walk: the biggest block that can be passed forward
///
/// A row-at-a-time merge stops there and emits the winner's single next row.
/// This merge instead asks how many of the winner's rows it can pass on before
/// another input could interleave.
///
/// It starts from the largest block it could possibly emit — the whole
/// unconsumed remainder of the winner's current batch. A round never emits
/// across a batch boundary, because the winner's next batch has not necessarily
/// been read yet. It then walks the winner's leaf-to-root path *a second time*
/// and clips that block against the loser stored at each node.
///
/// That walk is what makes the block safe, and it is short for a reason worth
/// spelling out. The loser stored at a node on the winner's path is the input
/// with the smallest next row in the sibling subtree hanging off that node,
/// because that is the input that won the sibling subtree and then lost here.
/// Those sibling subtrees partition every input except the winner. So clipping
/// against each stored loser in turn clips against the smallest next row of
/// every other input — in `log2(k)` comparisons rather than `k`.
///
/// Each clip is a binary search: the block is sorted, so the rows that order at
/// or before the bound form a prefix of it. The block's last row is tested
/// first, which settles the common case — the whole block precedes everything
/// else, the case disjoint inputs produce — in a single comparison and skips
/// the search entirely.
///
/// Clipping is cumulative, and that is what keeps the walk cheap. Each node
/// narrows the block the next one sees, so the first search is the only one
/// over the full block and the last-row check usually ends the rest outright.
/// The running minimum is carried as a row count in the winner's own block
/// rather than as a key, so no two losers are ever compared with each other.
///
/// Rows that compare *equal* to the bound are admitted. Equal rows may be
/// emitted in any order without breaking sortedness, and admitting them is what
/// guarantees progress: the winner's first row orders at or before every other
/// input's next row, so a block is never empty and the merge always advances.
///
/// The block is then handed on as a slice of its batch. Nothing is copied until
/// the coalescer concatenates whatever it has buffered into an output batch of
/// the requested size, so a run of blocks that are already the right size costs
/// one concatenation rather than a row-by-row interleave.
///
/// ## The two walks start in different places
///
/// The replay walk starts at the *previous* winner's leaf. The clipping walk
/// starts at the *new* winner's leaf, which is only known once the replay has
/// reached the root. Where the two winners differ the replay picks the new
/// winner up part-way up the tree, above the matches inside its own subtree —
/// the inputs most likely to interleave with it. Those matches are on the
/// clipping walk, and reaching them is what the second starting point is for.
///
/// # A worked example
///
/// Two inputs ordered by `time`:
///
/// ```text
/// input 0: 10 20 30
/// input 1: 15 25 35 | 40 50 60
/// ```
///
/// ```text
/// round 1: input 0 wins on 10. Its block is 10 20 30; 30 > 15, so the binary
///          search clips it to 10 alone.                    emits [10]
/// round 2: input 1 wins on 15, clipped against 20.         emits [15]
/// round 3: input 0 wins on 20, clipped against 25.         emits [20]
/// round 4: input 1 wins on 25, clipped against 30.         emits [25]
/// round 5: input 0 wins on 30 and is then exhausted, so the
///          whole block goes.                               emits [30]
/// round 6: input 1 wins with nothing left to clip against. emits [35]
///          and, once its next batch is read,               emits [40 50 60]
/// ```
///
/// Finely interleaved inputs cost a round per row, which is what the fallback
/// exists for. Disjoint inputs cost a round per batch.
///
/// # Giving up
///
/// Every few hundred blocks the merge compares the mean block length it has
/// been achieving against
/// [`with_degradation_threshold`](Self::with_degradation_threshold). Below it,
/// the unconsumed remainder of each input is put back together into a stream —
/// the buffered batch, sliced at the row the merge reached, in front of the
/// rest of the original stream — and those streams are handed to DataFusion's
/// merge, which finishes the job. The mean block length, rather than the number
/// of inputs, is what decides: a handful of perfectly interleaved inputs is as
/// bad as a thousand.
///
/// # What it costs
///
/// Against DataFusion's merge over the same streams, 64 batches of 8192 rows
/// keyed on `[tag, time]`. Streams that are disjoint — the shape IOx produces,
/// where a whole batch is emitted per round — against streams striped a row at
/// a time, with the fallback disabled so that the block merge is what is being
/// measured (ms):
///
/// ```text
/// streams              2      4      8     16     32
/// disjoint   this   0.173  0.186  0.195  0.204  0.211
///            DF     9.1   10.5   11.9   17.6   18.0
/// striped    this   268    274    295    317    337
///            DF      9.7   11.5   14.1   20.6   23.4
/// ```
///
/// Disjoint streams cost essentially nothing: a block is a whole batch, and a
/// batch that is already the requested size passes through the coalescer
/// without being copied, so the merge does `log2(k)` comparisons per batch and
/// no work per row at all. That is where the 50-85x comes from, and it widens
/// with the number of streams because DataFusion's per-row comparison count
/// grows while this merge's per-block count is lost in the noise.
///
/// Striped streams are the mirror image, and the reason the fallback exists.
///
/// # Memory
///
/// One batch per input is buffered, charged to the reservation as it is read
/// and released as it is consumed. That is the same high-water mark as
/// DataFusion's merge, which also holds one batch per input. Unlike a sort,
/// neither of them holds the whole input, and neither spills.
///
/// The comparators the merge caches to avoid rebuilding are charged to the
/// same reservation, and are dropped along with the batches they were built
/// from.
#[derive(Default)]
pub struct RunOptimizedStreamingMergeBuilder<'a> {
    streams: Vec<SendableRecordBatchStream>,
    schema: Option<SchemaRef>,
    expressions: Option<&'a LexOrdering>,
    metrics: Option<BaselineMetrics>,
    merge_metrics: Option<RunOptimizedMergeMetrics>,
    batch_size: Option<usize>,
    fetch: Option<usize>,
    reservation: Option<MemoryReservation>,
    degradation_threshold: Option<usize>,
    comparator_cache_entries: Option<usize>,
}

impl Debug for RunOptimizedStreamingMergeBuilder<'_> {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("RunOptimizedStreamingMergeBuilder")
            .field("streams", &self.streams.len())
            .field("expressions", &self.expressions)
            .field("batch_size", &self.batch_size)
            .field("fetch", &self.fetch)
            .field("degradation_threshold", &self.degradation_threshold)
            .field("comparator_cache_entries", &self.comparator_cache_entries)
            .finish_non_exhaustive()
    }
}

impl<'a> RunOptimizedStreamingMergeBuilder<'a> {
    pub fn new() -> Self {
        Self::default()
    }

    /// The streams to merge, each already sorted by
    /// [`with_expressions`](Self::with_expressions).
    pub fn with_streams(mut self, streams: Vec<SendableRecordBatchStream>) -> Self {
        self.streams = streams;
        self
    }

    pub fn with_schema(mut self, schema: SchemaRef) -> Self {
        self.schema = Some(schema);
        self
    }

    pub fn with_expressions(mut self, expressions: &'a LexOrdering) -> Self {
        self.expressions = Some(expressions);
        self
    }

    pub fn with_metrics(mut self, metrics: BaselineMetrics) -> Self {
        self.metrics = Some(metrics);
        self
    }

    /// The counters describing how the merge got on, which have no counterpart
    /// in DataFusion's builder.
    ///
    /// They are recorded whether or not this is called; without it they are
    /// registered against a metrics set nobody holds.
    pub fn with_merge_metrics(mut self, metrics: RunOptimizedMergeMetrics) -> Self {
        self.merge_metrics = Some(metrics);
        self
    }

    /// The number of rows to aim for in each output batch.
    pub fn with_batch_size(mut self, batch_size: usize) -> Self {
        self.batch_size = Some(batch_size);
        self
    }

    /// Stop after this many rows.
    pub fn with_fetch(mut self, fetch: Option<usize>) -> Self {
        self.fetch = fetch;
        self
    }

    /// The reservation covering the one batch buffered per input.
    pub fn with_reservation(mut self, reservation: MemoryReservation) -> Self {
        self.reservation = Some(reservation);
        self
    }

    /// The mean block length below which the merge hands its remaining inputs
    /// to DataFusion's row-at-a-time merge.
    ///
    /// Defaults to [`DEFAULT_DEGRADATION_THRESHOLD_ROWS`]. Zero never hands
    /// over; [`usize::MAX`] hands over at the first check.
    pub fn with_degradation_threshold(mut self, rows: usize) -> Self {
        self.degradation_threshold = Some(rows);
        self
    }

    /// The number of comparators the merge may keep at once.
    ///
    /// Rounded up to a power of two, so the merge has at least as much room as
    /// was asked for and at most twice as much. Defaults to
    /// [`DEFAULT_COMPARATOR_CACHE_ENTRIES`], which is a power of two already.
    /// Zero keeps no comparators at all, and builds every one it needs.
    pub fn with_comparator_cache_entries(mut self, entries: usize) -> Self {
        self.comparator_cache_entries = Some(entries);
        self
    }

    pub fn build(self) -> Result<SendableRecordBatchStream> {
        let Self {
            streams,
            schema,
            expressions,
            metrics,
            merge_metrics,
            batch_size,
            fetch,
            reservation,
            degradation_threshold,
            comparator_cache_entries,
        } = self;

        let Some(expressions) = expressions else {
            return internal_err!("sort expressions cannot be empty for a streaming merge");
        };
        if streams.is_empty() {
            return internal_err!("streams cannot be empty for a streaming merge");
        }
        let schema = schema.expect("schema cannot be empty for a streaming merge");
        let metrics = metrics.expect("metrics cannot be empty for a streaming merge");
        let batch_size = batch_size.expect("batch size cannot be empty for a streaming merge");
        let reservation = reservation.expect("reservation cannot be empty for a streaming merge");
        let merge_metrics = merge_metrics.unwrap_or_default();
        merge_metrics.merge_inputs.add(streams.len());

        let expr = expressions.clone();
        let merger = BlockMerger::new(
            streams.into_iter().map(MergeInput::new).collect(),
            expr.clone(),
            comparator_cache_entries.unwrap_or(DEFAULT_COMPARATOR_CACHE_ENTRIES),
            reservation,
            merge_metrics.clone(),
        );
        Ok(Box::pin(RunOptimizedMergeStream::new(
            merger,
            schema,
            expr,
            metrics,
            merge_metrics,
            batch_size,
            fetch,
            degradation_threshold.unwrap_or(DEFAULT_DEGRADATION_THRESHOLD_ROWS),
        )))
    }
}
