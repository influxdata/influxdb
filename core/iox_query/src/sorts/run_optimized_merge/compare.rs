//! Comparison of rows held in different batches, and the comparators the
//! merge keeps between rounds.

use std::{cmp::Ordering, mem::size_of_val, sync::Arc};

use arrow::{
    array::{ArrayRef, DynComparator, make_comparator},
    compute::SortOptions,
    datatypes::DataType,
};
use datafusion::error::Result;

use super::cache::NWayCache;

/// A lexicographic comparator between the rows of two sets of sort-key arrays.
///
/// The sort direction and null placement of each column are baked into the
/// per-column comparators, so the result of [`compare`](Self::compare) is
/// already in the requested order.
///
/// Building one costs a closure per column, and for a dictionary column a
/// nested comparator over the values as well, so the merge builds one per pair
/// of batches it puts against each other and keeps it for reuse.
pub(super) struct RowComparator {
    columns: Vec<DynComparator>,

    /// What this comparator occupies, worked out once while it is built.
    bytes: usize,
}

/// What a comparator over a nested column costs beyond the closure size its
/// vtable reports.
///
/// A dictionary, list, struct or map column builds a comparator over its
/// children inside its own closure, in a box `size_of_val` does not reach.
/// Measured at 240 bytes for a dictionary of strings, and it does not vary with
/// the number of distinct values: the comparator captures the values array,
/// not its contents.
const NESTED_COMPARATOR_BYTES: usize = 240;

impl RowComparator {
    /// Build a comparator between rows of `left` and rows of `right`.
    ///
    /// The two sides may be the same arrays, in which case the comparator
    /// orders rows within a single batch.
    pub(super) fn try_new(
        left: &[ArrayRef],
        right: &[ArrayRef],
        options: &[SortOptions],
    ) -> Result<Self> {
        let columns = left
            .iter()
            .zip(right)
            .zip(options)
            .map(|((l, r), o)| make_comparator(l.as_ref(), r.as_ref(), *o))
            .collect::<std::result::Result<Vec<_>, _>>()?;

        let nested = left
            .iter()
            .filter(|array| {
                let data_type = array.data_type();
                data_type.is_nested() || matches!(data_type, DataType::Dictionary(_, _))
            })
            .count();
        let bytes = size_of::<Self>()
            // The cache holds these behind an `Arc`.
            + 2 * size_of::<usize>()
            + columns.capacity() * size_of::<DynComparator>()
            + columns
                .iter()
                .map(|column| size_of_val(column.as_ref()))
                .sum::<usize>()
            + nested * NESTED_COMPARATOR_BYTES;

        Ok(Self { columns, bytes })
    }

    /// What this comparator occupies, to within a few percent.
    ///
    /// The arrays the closures hold are the batches' own and are charged along
    /// with them; only the closures themselves are counted here.
    pub(super) fn bytes(&self) -> usize {
        self.bytes
    }

    /// Compare row `left` of the left arrays with row `right` of the right
    /// arrays.
    pub(super) fn compare(&self, left: usize, right: usize) -> Ordering {
        for column in &self.columns {
            match column(left, right) {
                Ordering::Equal => continue,
                other => return other,
            }
        }
        Ordering::Equal
    }
}

/// A comparator between two inputs, taking its arguments in the order the
/// caller asked for.
pub(super) struct PairComparator {
    comparator: Arc<RowComparator>,

    /// True when `comparator` was built with the two inputs the other way
    /// round, so its arguments and its answer both have to be turned around.
    swapped: bool,
}

impl PairComparator {
    /// Present `comparator` in the order the caller asked for, turning its
    /// arguments and its answer around when it was built the other way round.
    pub(super) fn new(comparator: Arc<RowComparator>, swapped: bool) -> Self {
        Self {
            comparator,
            swapped,
        }
    }

    pub(super) fn compare(&self, left: usize, right: usize) -> Ordering {
        if self.swapped {
            self.comparator.compare(right, left).reverse()
        } else {
            self.comparator.compare(left, right)
        }
    }
}

/// The number of slots a comparator may be held in.
///
/// Comparators are looked up on the merge's critical path, so a set is kept
/// narrow. A round asks after about `2 * log2(inputs)` pairs, so at four ways
/// five of that handful would have to land in one set before any is displaced.
const CACHE_WAYS: usize = 4;

/// A pair of inputs, smaller index first.
#[derive(Clone, Copy)]
pub(super) struct Pair {
    low: u32,
    high: u32,
}

impl Pair {
    pub(super) fn new(low: usize, high: usize) -> Self {
        debug_assert!(
            low < high,
            "a pair is ordered and an input has no pair with itself"
        );
        Self {
            low: low as u32,
            high: high as u32,
        }
    }

    /// The pair of `a` and `b`, whichever way round they come.
    fn of(a: usize, b: usize) -> Self {
        if a < b {
            Self::new(a, b)
        } else {
            Self::new(b, a)
        }
    }
}

impl From<Pair> for u64 {
    fn from(pair: Pair) -> Self {
        (u64::from(pair.low) << 32) | u64::from(pair.high)
    }
}

/// Comparators between the current batches of pairs of inputs, kept so that a
/// round does not have to build them again.
///
/// One is held per *unordered* pair. Ordering is antisymmetric, so a comparator
/// built for `(low, high)` answers a `(high, low)` question by taking its
/// arguments the other way round and reversing its answer.
///
/// A comparator holds the key arrays of the two batches it was built from, and
/// those arrays are only charged to the merge's reservation for as long as
/// their batches are. So a comparator must not outlive either batch, and
/// [`flush`](Self::flush) drops every one an input has a part in when the
/// merge releases that input's batch. To find them it keeps a bit for each
/// pair of inputs, set when a comparator between the two goes in, so a flush
/// costs a word per 64 inputs plus a removal per comparator it drops.
pub(super) struct ComparatorCache {
    entries: NWayCache<Pair, Arc<RowComparator>>,

    /// A row of bits for each input, with bit `j` of row `i` set when a
    /// comparator between inputs `i` and `j` has gone into the cache since
    /// either was last flushed. Both rows record the pair, so that either input
    /// can find it in its own row.
    partners: Vec<u64>,

    /// The number of words in a row of `partners`.
    words: usize,
}

impl ComparatorCache {
    /// A cache of at least `entries` comparators between pairs drawn from
    /// `inputs` inputs.
    pub(super) fn new(entries: usize, inputs: usize) -> Self {
        let entries = NWayCache::new(entries, CACHE_WAYS);
        let words = inputs.div_ceil(64);
        let partners = if entries.capacity() > 0 {
            vec![0; inputs * words]
        } else {
            Vec::new()
        };
        Self {
            entries,
            partners,
            words,
        }
    }

    /// The bytes a cache built by [`new`](Self::new) with these arguments
    /// would occupy before anything is put in it, known without building it.
    pub(super) fn bytes_for(entries: usize, inputs: usize) -> usize {
        let slots = NWayCache::<Pair, Arc<RowComparator>>::bytes_for(entries, CACHE_WAYS);
        if slots == 0 {
            return 0;
        }
        slots + inputs * inputs.div_ceil(64) * size_of::<u64>()
    }

    pub(super) fn enabled(&self) -> bool {
        self.entries.capacity() > 0
    }

    pub(super) fn get(&mut self, pair: Pair) -> Option<Arc<RowComparator>> {
        self.entries.get(pair).cloned()
    }

    /// Keep `comparator`, which the caller has already charged for, returning
    /// the bytes of whatever it displaced.
    pub(super) fn insert(&mut self, pair: Pair, comparator: Arc<RowComparator>) -> usize {
        let (low, high) = (pair.low as usize, pair.high as usize);
        self.partners[low * self.words + high / 64] |= 1 << (high % 64);
        self.partners[high * self.words + low / 64] |= 1 << (low % 64);
        self.entries
            .insert(pair, comparator)
            .map_or(0, |displaced| displaced.bytes())
    }

    /// Drop every comparator built against `input`'s current batch, returning
    /// the bytes they were charged.
    ///
    /// A partner's bit may outlast its comparator, which the cache is free to
    /// displace; removing a comparator that is no longer there does nothing.
    pub(super) fn flush(&mut self, input: usize) -> usize {
        if !self.enabled() {
            return 0;
        }
        let mut released = 0;
        let row = input * self.words;
        for word in 0..self.words {
            let mut bits = std::mem::take(&mut self.partners[row + word]);
            while bits != 0 {
                let partner = word * 64 + bits.trailing_zeros() as usize;
                bits &= bits - 1;
                self.partners[partner * self.words + input / 64] &= !(1 << (input % 64));
                if let Some(comparator) = self.entries.remove(Pair::of(input, partner)) {
                    released += comparator.bytes();
                }
            }
        }
        released
    }
}
