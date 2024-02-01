//! A memory limiter

use super::{Error, Result};
use std::sync::atomic::{AtomicUsize, Ordering};

#[derive(Debug)]
pub(crate) struct MemoryLimiter {
    current: AtomicUsize,
    limit: usize,
}

impl MemoryLimiter {
    /// Create a new [`MemoryLimiter`] limited to `limit` bytes
    pub(crate) fn new(limit: usize) -> Self {
        Self {
            current: AtomicUsize::new(0),
            limit,
        }
    }

    /// Reserve `size` bytes, returning an error if this would exceed the limit
    pub(crate) fn reserve(&self, size: usize) -> Result<()> {
        let limit = self.limit;
        let max = limit
            .checked_sub(size)
            .ok_or(Error::TooLarge { size, limit })?;

        // We can use relaxed ordering as not relying on this to
        // synchronise memory accesses beyond itself
        self.current
            .fetch_update(Ordering::Relaxed, Ordering::Relaxed, |current| {
                // This cannot overflow as current + size <= limit
                (current <= max).then_some(current + size)
            })
            .map_err(|current| Error::OutOfMemory {
                size,
                current,
                limit,
            })?;
        Ok(())
    }

    /// Free `size` bytes
    pub(crate) fn free(&self, size: usize) {
        self.current.fetch_sub(size, Ordering::Relaxed);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_limiter() {
        let limiter = MemoryLimiter::new(100);

        limiter.reserve(20).unwrap();
        limiter.reserve(70).unwrap();

        let err = limiter.reserve(20).unwrap_err().to_string();
        assert_eq!(err, "Cannot reserve additional 20 bytes for cache containing 90 bytes as would exceed limit of 100 bytes");

        limiter.reserve(10).unwrap();
        limiter.reserve(0).unwrap();

        let err = limiter.reserve(1).unwrap_err().to_string();
        assert_eq!(err, "Cannot reserve additional 1 bytes for cache containing 100 bytes as would exceed limit of 100 bytes");

        limiter.free(10);
        limiter.reserve(10).unwrap();

        limiter.free(100);

        // Can add single value taking entire range
        limiter.reserve(100).unwrap();
        limiter.free(100);

        // Protected against overflow
        let err = limiter.reserve(usize::MAX).unwrap_err();
        assert!(matches!(err, Error::TooLarge { .. }), "{err}");
    }
}
