//! A provider of ordered timestamps, exposed as a [`SequenceNumber`].

use std::sync::atomic::{AtomicU64, Ordering};

use crossbeam_utils::CachePadded;
use data_types::SequenceNumber;

/// A concurrency-safe provider of totally ordered [`SequenceNumber`] values.
///
/// Given a single [`TimestampOracle`] instance, the [`SequenceNumber`] values
/// returned by calling [`TimestampOracle::next()`] are guaranteed to be totally
/// ordered and consecutive.
///
/// No ordering exists across independent [`TimestampOracle`] instances.
#[derive(Debug)]
pub(crate) struct TimestampOracle(CachePadded<AtomicU64>);

impl TimestampOracle {
    /// Construct a [`TimestampOracle`] that returns values starting from
    /// `last_value + 1`.
    pub(crate) fn new(last_value: u64) -> Self {
        Self(CachePadded::new(AtomicU64::new(last_value + 1)))
    }

    /// Obtain the next [`SequenceNumber`].
    pub(crate) fn next(&self) -> SequenceNumber {
        // Correctness:
        //
        // A relaxed atomic store has a consistent modification order, with two
        // racing threads calling fetch_add resolving into a defined ordering of
        // one having called before the other. This ordering will never change
        // or diverge between threads.
        let v = self.0.fetch_add(1, Ordering::Relaxed);

        SequenceNumber::new(v)
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use super::*;

    /// Ensure the next value read from a newly initialised [`TimestampOracle`]
    /// is always one greater than the init value.
    ///
    /// This ensures that a total ordering is derived even if a caller provides
    /// the last actual value, or the next expected value (at the expense of a
    /// possible gap in the sequence). This helps avoid off-by-one bugs in the
    /// caller.
    #[test]
    fn test_init_next_val() {
        let oracle = TimestampOracle::new(41);
        assert_eq!(oracle.next().get(), 42);
    }

    /// A property test ensuring that for N threads competing to sequence M
    /// operations, a total order of operations is derived from consecutive
    /// timestamps returned by a single [`TimestampOracle`] instance.
    #[test]
    #[allow(clippy::needless_collect)]
    fn test_concurrency() {
        // The number of threads that will race to obtain N_SEQ each.
        const N_THREADS: usize = 10;

        // The number of SequenceNumber timestamps each thread will acquire.
        const N_SEQ: usize = 100;

        // The init value for the TimestampOracle
        const LAST_VALUE: usize = 0;

        // The total number of SequenceNumber to be acquired in this test.
        const TOTAL_SEQ: usize = N_SEQ * N_THREADS;

        let oracle = Arc::new(TimestampOracle::new(LAST_VALUE as u64));
        let barrier = Arc::new(std::sync::Barrier::new(N_THREADS));

        // Spawn the desired number of threads, synchronise their starting
        // point, and then race them to obtain TOTAL_SEQ number of timestamps.
        //
        // Each thread returns an vector of the timestamp values it acquired.
        let handles = (0..N_THREADS)
            .map(|_| {
                let oracle = Arc::clone(&oracle);
                let barrier = Arc::clone(&barrier);
                std::thread::spawn(move || {
                    barrier.wait(); // synchronise start time
                    (0..N_SEQ).map(|_| oracle.next().get()).collect::<Vec<_>>()
                })
            })
            .collect::<Vec<_>>();

        // Collect all the timestamps from all the threads
        let mut timestamps = handles
            .into_iter()
            .flat_map(|h| h.join().expect("thread panic"))
            .collect::<Vec<_>>();

        // Sort the timestamps
        timestamps.sort_unstable();

        // Assert the complete set of values from the expected range appear,
        // unduplicated, and totally ordered.
        let expected = (LAST_VALUE + 1)..TOTAL_SEQ + 1;
        timestamps
            .into_iter()
            .zip(expected)
            .for_each(|(got, want)| assert_eq!(got, want as u64));
    }
}
