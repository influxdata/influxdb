use std::{
    num::NonZeroUsize,
    sync::atomic::{AtomicUsize, Ordering},
};

/// A counter typed for counting partitions and applying a configured limit.
///
/// No ordering is guaranteed - increments and reads may be arbitrarily
/// reordered w.r.t other memory operations (relaxed ordering).
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
    pub(crate) fn inc(&self) {
        self.current.fetch_add(1, Ordering::Relaxed);
    }

    /// Read the approximate counter value.
    ///
    /// Reads may return stale values, but will always be monotonic.
    pub(crate) fn read(&self) -> usize {
        self.current.load(Ordering::Relaxed)
    }

    /// Return `true` if the configured limit has been reached.
    pub(crate) fn is_maxed(&self) -> bool {
        self.read() >= self.max
    }

    #[cfg(test)]
    pub(crate) fn set(&self, v: usize) {
        self.current.store(v, Ordering::Relaxed)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_counter() {
        const N: usize = 100;

        let c = PartitionCounter::new(NonZeroUsize::new(N).unwrap());

        for v in 0..N {
            assert!(!c.is_maxed());
            assert_eq!(c.read(), v);
            c.inc();
        }

        assert_eq!(c.read(), N);
        assert!(c.is_maxed());
    }
}
