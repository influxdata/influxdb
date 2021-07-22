use std::sync::atomic::{AtomicI64, Ordering};
use std::time::{Duration, Instant};

/// Provides the ability to perform atomic operations on an `Instant`
#[derive(Debug)]
pub struct AtomicInstant {
    /// The start instant to measure relative to
    start: Instant,
    /// An offset in nanoseconds from the start instant
    ///
    /// We use an offset from `start` as an Instant is an opaque type that
    /// cannot be mutated atomically
    offset: AtomicI64,
}

impl AtomicInstant {
    /// Creates a new AtomicInstant with `Instant::now` as the current value
    pub fn now() -> Self {
        Self::new(Instant::now())
    }

    /// Creates a new AtomicInstant with the provided value
    pub fn new(start: Instant) -> Self {
        Self {
            start,
            offset: AtomicI64::new(0),
        }
    }

    fn offset(&self, instant: Instant) -> i64 {
        use std::cmp::Ordering;

        match self.start.cmp(&instant) {
            Ordering::Greater => -(self.start.duration_since(instant).as_nanos() as i64),
            Ordering::Equal => 0,
            Ordering::Less => instant.duration_since(self.start).as_nanos() as i64,
        }
    }

    fn instant(&self, offset: i64) -> Instant {
        match offset > 0 {
            true => self.start + Duration::from_nanos(offset as u64),
            false => self.start - Duration::from_nanos((-offset) as u64),
        }
    }

    /// Gets the current Instant
    pub fn load(&self, ordering: Ordering) -> Instant {
        self.instant(self.offset.load(ordering))
    }

    ///  Stores the given Instant
    pub fn store(&self, instant: Instant, ordering: Ordering) {
        self.offset.store(self.offset(instant), ordering);
    }

    /// Sets the value to the maximum of the current value and the provided Instant
    ///
    /// Returns the previous value
    pub fn fetch_max(&self, instant: Instant, ordering: Ordering) -> Instant {
        let previous_offset = self.offset.fetch_max(self.offset(instant), ordering);
        self.instant(previous_offset)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_atomic_instant() {
        let start = Instant::now();
        let instant = AtomicInstant::new(start);

        instant.store(start - Duration::from_secs(5), Ordering::Relaxed);
        assert_eq!(
            instant.load(Ordering::Relaxed),
            start - Duration::from_secs(5)
        );

        instant.store(start - Duration::from_secs(1), Ordering::Relaxed);
        assert_eq!(
            instant.load(Ordering::Relaxed),
            start - Duration::from_secs(1)
        );

        instant.store(start + Duration::from_secs(1), Ordering::Relaxed);
        assert_eq!(
            instant.load(Ordering::Relaxed),
            start + Duration::from_secs(1)
        );

        instant.store(start + Duration::from_nanos(1), Ordering::Relaxed);
        assert_eq!(
            instant.load(Ordering::Relaxed),
            start + Duration::from_nanos(1)
        );

        let ret = instant.fetch_max(start + Duration::from_secs(2), Ordering::Relaxed);
        assert_eq!(ret, start + Duration::from_nanos(1));
        assert_eq!(
            instant.load(Ordering::Relaxed),
            start + Duration::from_secs(2)
        );

        let ret = instant.fetch_max(start + Duration::from_secs(1), Ordering::Relaxed);
        assert_eq!(ret, start + Duration::from_secs(2));
        assert_eq!(
            instant.load(Ordering::Relaxed),
            start + Duration::from_secs(2)
        );
    }
}
