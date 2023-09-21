use std::{
    num::NonZeroUsize,
    sync::atomic::{AtomicUsize, Ordering},
};

use crate::buffer_tree::BufferWriteError;

/// A counter typed for counting partitions and applying a configured limit.
///
/// This value is completely unsynchronised (in the context of memory ordering)
/// and relies on external thread synchronisation. Calls to change the value of
/// this counter may be arbitrarily reordered with other memory operations - the
/// caller is responsible for providing external ordering where necessary.
#[derive(Debug)]
pub(crate) struct PartitionCounter {
    current: AtomicUsize,
    max: usize,
}

impl PartitionCounter {
    pub(crate) fn new(max: NonZeroUsize) -> Self {
        Self {
            current: AtomicUsize::new(0),
            max: max.get(),
        }
    }

    /// Increment the counter by 1.
    ///
    /// Return [`Err`] if the configured limit has been reached, else returns
    /// [`Ok`] if the current value is below the limit.
    pub(crate) fn inc(&self) -> Result<(), BufferWriteError> {
        if self.current.fetch_add(1, Ordering::Relaxed) >= self.max {
            // The limit was exceeded, roll back the addition and return an
            // error.
            let count = self.current.fetch_sub(1, Ordering::Relaxed);
            return Err(BufferWriteError::PartitionLimit { count: count - 1 });
        }

        Ok(())
    }

    /// Decrement the counter by 1.
    ///
    /// # Panics
    ///
    /// This method MAY panic if the counter wraps around - never decrement more
    /// than previously incremented.
    pub(crate) fn dec(&self) {
        let v = self.current.fetch_sub(1, Ordering::Relaxed);

        // Correctness: the fetch operations are RMW, which are guaranteed to
        // see a monotonic history, therefore any prior fetch_add always happens
        // before the fetch_sub above.
        debug_assert_ne!(v, usize::MAX); // MUST never wraparound wraparound
    }

    /// Returns an error if the limit has been reached and a subsequent
    /// increment would fail.
    pub(crate) fn is_maxed(&self) -> Result<(), BufferWriteError> {
        let count = self.read();
        if count >= self.max {
            return Err(BufferWriteError::PartitionLimit { count });
        }

        Ok(())
    }

    /// Read the approximate counter value.
    pub(crate) fn read(&self) -> usize {
        self.current.load(Ordering::Relaxed)
    }

    #[cfg(test)]
    pub(crate) fn set(&self, v: usize) {
        self.current.store(v, Ordering::Relaxed)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use assert_matches::assert_matches;

    #[test]
    fn test_counter() {
        const N: usize = 100;

        let c = PartitionCounter::new(NonZeroUsize::new(N).unwrap());

        for v in 0..N {
            assert_eq!(c.read(), v);
            assert_matches!(c.is_maxed(), Ok(()));
            assert!(c.inc().is_ok());
        }

        assert_eq!(c.read(), N);
        assert_matches!(
            c.is_maxed(),
            Err(BufferWriteError::PartitionLimit { count: N })
        );
        assert_matches!(c.inc(), Err(BufferWriteError::PartitionLimit { count: N }));
        assert_eq!(c.read(), N); // Counter value unchanged.

        c.dec();
        assert_eq!(c.read(), N - 1);
        assert_matches!(c.is_maxed(), Ok(()));
        assert_matches!(c.inc(), Ok(()));
        assert_eq!(c.read(), N);
    }
}
