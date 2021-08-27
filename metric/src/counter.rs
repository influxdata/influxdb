use crate::{MetricKind, MetricObserver, Observation};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

/// A monotonic counter
#[derive(Debug, Clone, Default)]
pub struct U64Counter {
    state: Arc<AtomicU64>,
}

impl U64Counter {
    pub fn inc(&self, count: u64) {
        self.state.fetch_add(count, Ordering::Relaxed);
    }

    pub fn fetch(&self) -> u64 {
        self.state.load(Ordering::Relaxed)
    }
}

impl MetricObserver for U64Counter {
    type Recorder = Self;

    fn kind() -> MetricKind {
        MetricKind::U64Counter
    }

    fn recorder(&self) -> Self::Recorder {
        self.clone()
    }

    fn observe(&self) -> Observation {
        Observation::U64Counter(self.fetch())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_counter() {
        let counter = U64Counter::default();
        assert_eq!(counter.fetch(), 0);
        counter.inc(12);
        assert_eq!(counter.fetch(), 12);
        counter.inc(34);
        assert_eq!(counter.fetch(), 46);

        assert_eq!(counter.observe(), Observation::U64Counter(46));

        // Expect counter to wrap around
        counter.inc(u64::MAX);
        assert_eq!(counter.observe(), Observation::U64Counter(45));
    }
}
