//! The block-at-a-time merge tournament.

use std::{
    cmp::Ordering,
    sync::Arc,
    task::{Context, Poll},
};

use arrow::{array::RecordBatch, compute::SortOptions, datatypes::SchemaRef};
use datafusion::{
    common::internal_err,
    error::Result,
    execution::{SendableRecordBatchStream, memory_pool::MemoryReservation},
    physical_expr::LexOrdering,
};

use super::{
    compare::{ComparatorCache, Pair, PairComparator, RowComparator},
    input::MergeInput,
    metrics::RunOptimizedMergeMetrics,
    sort_key::sort_options,
};

/// The number of blocks emitted between checks of the degradation guard.
const DEGRADATION_WINDOW: usize = 256;

/// Merges sorted inputs into a single sorted sequence, emitting as many rows at
/// a time as the inputs allow.
///
/// The algorithm is described on [`RunOptimizedStreamingMergeBuilder`].
///
/// [`RunOptimizedStreamingMergeBuilder`]:
///     super::RunOptimizedStreamingMergeBuilder
pub(super) struct BlockMerger {
    inputs: Vec<MergeInput>,

    /// The ordering the inputs are sorted by, used to evaluate the sort key of
    /// each batch as it is buffered.
    expr: LexOrdering,

    /// The sort direction and null placement of each sort column.
    options: Vec<SortOptions>,

    /// Covers the buffered batch of every input.
    reservation: MemoryReservation,

    metrics: RunOptimizedMergeMetrics,

    /// Comparators, kept so that a round does not have to build them again.
    cache: ComparatorCache,

    /// Node 0 holds the current winner; nodes `1..inputs.len()` hold the loser
    /// of the match played at that node.
    loser_tree: Vec<usize>,

    /// False until the first round has built the tree.
    initialised: bool,

    /// Blocks and rows emitted since the degradation guard last ran.
    window_blocks: usize,
    window_rows: usize,
}

impl BlockMerger {
    pub(super) fn new(
        inputs: Vec<MergeInput>,
        expr: LexOrdering,
        cache_entries: usize,
        mut reservation: MemoryReservation,
        metrics: RunOptimizedMergeMetrics,
    ) -> Self {
        // The cache is charged before it is built, so that a reservation that
        // will not cover it never sees it allocated. That is reason to do
        // without the cache, not to fail the merge.
        let bytes = ComparatorCache::bytes_for(cache_entries, inputs.len());
        let cache_entries = if reservation.try_grow(bytes).is_ok() {
            cache_entries
        } else {
            0
        };
        let cache = ComparatorCache::new(cache_entries, inputs.len());
        metrics.mem_used.set_max(reservation.size());
        Self {
            loser_tree: vec![usize::MAX; inputs.len()],
            cache,
            inputs,
            options: sort_options(&expr),
            expr,
            reservation,
            metrics,
            initialised: false,
            window_blocks: 0,
            window_rows: 0,
        }
    }

    /// An empty reservation against the same memory consumer, for handing to
    /// the fallback merge.
    pub(super) fn new_empty_reservation(&self) -> MemoryReservation {
        self.reservation.new_empty()
    }

    /// Buffer a batch for every input that has run out of one.
    ///
    /// A ready result means every input is either loaded or exhausted, which is
    /// what a round of the tournament needs. Only inputs that have run out are
    /// polled, so an input that yielded a large batch is left alone until the
    /// merge has worked its way through it.
    pub(super) fn poll_fill(&mut self, cx: &mut Context<'_>) -> Poll<Result<()>> {
        let mut pending = false;
        for input in &mut self.inputs {
            if !input.needs_batch() {
                continue;
            }
            match input.poll_fill(cx, &self.expr) {
                Poll::Pending => pending = true,
                Poll::Ready(Err(e)) => return Poll::Ready(Err(e)),
                Poll::Ready(Ok(bytes)) => {
                    if let Err(e) = self.reservation.try_grow(bytes) {
                        return Poll::Ready(Err(e));
                    }
                    self.metrics.mem_used.set_max(self.reservation.size());
                }
            }
        }
        if pending {
            Poll::Pending
        } else {
            Poll::Ready(Ok(()))
        }
    }

    /// The node at which `input` enters the tournament.
    fn leaf(&self, input: usize) -> usize {
        (self.inputs.len() + input) / 2
    }

    fn parent(node: usize) -> usize {
        node / 2
    }

    /// A comparator between the current batches of `left` and `right`, taken
    /// from the cache when it is there and built when it is not.
    fn comparator(&mut self, left: usize, right: usize) -> Result<PairComparator> {
        debug_assert_ne!(left, right, "an input is never compared with itself");
        let swapped = left > right;
        let (low, high) = if swapped {
            (right, left)
        } else {
            (left, right)
        };

        let pair = Pair::new(low, high);
        if let Some(comparator) = self.cache.get(pair) {
            return Ok(PairComparator::new(comparator, swapped));
        }
        let Some((low_keys, _)) = self.inputs[low].head() else {
            return internal_err!("run-optimized merge compared an exhausted input");
        };
        let Some((high_keys, _)) = self.inputs[high].head() else {
            return internal_err!("run-optimized merge compared an exhausted input");
        };
        let comparator = Arc::new(RowComparator::try_new(low_keys, high_keys, &self.options)?);

        // Keeping the comparator is discretionary, so a pool that will not
        // cover it means this one is used once and dropped rather than the
        // merge failing.
        let bytes = comparator.bytes();
        if self.cache.enabled() && self.reservation.try_grow(bytes).is_ok() {
            let displaced = self.cache.insert(pair, Arc::clone(&comparator));
            self.reservation.shrink(displaced);
            self.metrics.mem_used.set_max(self.reservation.size());
        }
        Ok(PairComparator::new(comparator, swapped))
    }

    /// True when the next row of input `a` orders after the next row of input
    /// `b`. Exhausted inputs order after everything.
    ///
    /// Ties between next rows are broken by input index so that selecting the
    /// next tournament winner is a total order.
    fn is_gt(&mut self, a: usize, b: usize) -> Result<bool> {
        let Some((_, a_pos)) = self.inputs[a].head() else {
            return Ok(true);
        };
        let Some((_, b_pos)) = self.inputs[b].head() else {
            return Ok(false);
        };
        let comparator = self.comparator(a, b)?;
        Ok(comparator.compare(a_pos, b_pos).then(a.cmp(&b)).is_gt())
    }

    fn init_loser_tree(&mut self) -> Result<()> {
        self.loser_tree = vec![usize::MAX; self.inputs.len()];
        for input in 0..self.inputs.len() {
            let mut winner = input;
            let mut node = self.leaf(input);
            while node != 0 && self.loser_tree[node] != usize::MAX {
                let challenger = self.loser_tree[node];
                if self.is_gt(winner, challenger)? {
                    self.loser_tree[node] = winner;
                    winner = challenger;
                }
                node = Self::parent(node);
            }
            self.loser_tree[node] = winner;
        }
        self.initialised = true;
        Ok(())
    }

    /// Replay the previous winner from its leaf to the root.
    fn update_loser_tree(&mut self) -> Result<()> {
        let mut winner = self.loser_tree[0];
        let mut node = self.leaf(winner);
        while node != 0 {
            let challenger = self.loser_tree[node];
            if self.is_gt(winner, challenger)? {
                self.loser_tree[node] = winner;
                winner = challenger;
            }
            node = Self::parent(node);
        }
        self.loser_tree[0] = winner;
        Ok(())
    }

    /// The number of the winner's leading `emit_len` rows that order at or
    /// before the next row of input `loser`.
    fn clip(&mut self, winner: usize, emit_len: usize, loser: usize) -> Result<usize> {
        debug_assert_ne!(winner, loser, "the winner beat everything on its own path");
        let Some((_, loser_pos)) = self.inputs[loser].head() else {
            // An exhausted input orders after everything.
            return Ok(emit_len);
        };
        let Some((_, offset)) = self.inputs[winner].head() else {
            return internal_err!("run-optimized merge clipped the block of an exhausted input");
        };

        let comparator = self.comparator(winner, loser)?;
        let compare = |row: usize| comparator.compare(row, loser_pos);

        // The overwhelmingly common case is a block that precedes everything
        // else outright, which one comparison settles.
        if compare(offset + emit_len - 1) != Ordering::Greater {
            return Ok(emit_len);
        }

        // The block is sorted, so the rows that order at or before the bound
        // are a prefix of it.
        let mut low = 0;
        let mut high = emit_len;
        while low < high {
            let mid = low + (high - low) / 2;
            if compare(offset + mid) != Ordering::Greater {
                low = mid + 1;
            } else {
                high = mid;
            }
        }
        Ok(low)
    }

    /// Play one round, returning at most `max_rows` rows.
    ///
    /// Every input must be loaded or exhausted, which
    /// [`poll_fill`](Self::poll_fill) establishes.
    pub(super) fn next(&mut self, max_rows: usize) -> Result<Option<RecordBatch>> {
        if self.inputs.is_empty() || max_rows == 0 {
            return Ok(None);
        }
        if self.initialised {
            self.update_loser_tree()?;
        } else {
            self.init_loser_tree()?;
        }

        let winner = self.loser_tree[0];
        if self.inputs[winner].is_exhausted() {
            return Ok(None);
        }

        let mut emit_len = self.inputs[winner].block_len();
        let mut node = self.leaf(winner);
        while node != 0 {
            let loser = self.loser_tree[node];
            emit_len = self.clip(winner, emit_len, loser)?;
            node = Self::parent(node);
        }

        if emit_len == 0 {
            // Emitting nothing would replay the tree unchanged for ever.
            return internal_err!("run-optimized merge round emitted no rows");
        }
        #[cfg(test)]
        self.verify_block(winner, emit_len)?;

        self.metrics.merge_rounds.add(1);
        self.metrics.mean_block_rows.add(emit_len);
        self.window_blocks += 1;
        self.window_rows += emit_len;

        // A fetch limit is applied after the tournament so that the guard and
        // the metrics see the block the merge actually found.
        let emit_len = emit_len.min(max_rows);
        let (block, freed) = self.inputs[winner].take_block(emit_len)?;
        if freed > 0 {
            // The winner's batch has just been released, and every comparator
            // built against it holds its key arrays, so they go with it.
            let comparators = self.cache.flush(winner);
            self.reservation.shrink(freed + comparators);
        }
        Ok(Some(block))
    }

    /// Check the block against every other input the slow way.
    ///
    /// The merge decides what it may emit from `log2(inputs)` comparisons, so
    /// running this over the whole test suite is what actually exercises the
    /// argument that those comparisons are enough.
    #[cfg(test)]
    fn verify_block(&self, winner: usize, emit_len: usize) -> Result<()> {
        let Some((keys, offset)) = self.inputs[winner].head() else {
            return internal_err!("run-optimized merge verified an exhausted input");
        };
        let clipped = emit_len < self.inputs[winner].block_len();
        let mut clip_forced = false;

        for other in 0..self.inputs.len() {
            if other == winner {
                continue;
            }
            let Some((other_keys, other_pos)) = self.inputs[other].head() else {
                continue;
            };
            let comparator = RowComparator::try_new(keys, other_keys, &self.options)?;

            assert_ne!(
                Ordering::Greater,
                comparator.compare(offset, other_pos),
                "input {winner} won the tournament but input {other} has a smaller row"
            );
            assert_ne!(
                Ordering::Greater,
                comparator.compare(offset + emit_len - 1, other_pos),
                "the block of input {winner} runs past the next row of input {other}"
            );
            if clipped && comparator.compare(offset + emit_len, other_pos) == Ordering::Greater {
                clip_forced = true;
            }
        }

        assert!(
            !clipped || clip_forced,
            "the block of input {winner} was clipped shorter than it needed to be"
        );
        Ok(())
    }

    /// True when the inputs are interleaved finely enough that a row-at-a-time
    /// merge is the better strategy.
    ///
    /// The mean length of a block, rather than the number of inputs, is what
    /// decides: a handful of perfectly interleaved inputs is as bad as a
    /// thousand.
    pub(super) fn is_degraded(&mut self, threshold: usize) -> bool {
        if self.window_blocks < DEGRADATION_WINDOW {
            return false;
        }
        let mean = self.window_rows / self.window_blocks;
        self.window_blocks = 0;
        self.window_rows = 0;
        mean < threshold
    }

    /// Turn the unconsumed remainder of each input back into a stream.
    pub(super) fn into_streams(self, schema: &SchemaRef) -> Vec<SendableRecordBatchStream> {
        self.inputs
            .into_iter()
            .filter_map(|input| input.into_stream(schema))
            .collect()
    }
}
