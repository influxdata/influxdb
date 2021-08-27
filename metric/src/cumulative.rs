use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

use crate::{MetricKind, MetricObserver, Observation};

/// A `CumulativeGauge` reports the total of the values reported by `CumulativeRecorder`
///
/// Unlike `U64Gauge` this means it is safe to use in multiple locations with the same
/// set of `Attributes`. The value reported will be the sum of all `CumulativeRecorder`.
///
/// Any contributions made by a `CumulativeRecorder` to the total reported by the
/// `CumulativeGauge` are lost when the `CumulativeRecorder` is dropped
///
/// The primary use-case for CumulativeGauge is reporting observations at a lower
/// granularity than the instrumented objects. For example, we might want
/// to instrument individual Chunks but report the total across all Chunks
/// in a partition or table.
#[derive(Debug, Default)]
pub struct CumulativeGauge {
    state: Arc<AtomicU64>,
}

impl MetricObserver for CumulativeGauge {
    type Recorder = CumulativeRecorder;

    fn kind() -> MetricKind {
        MetricKind::U64Gauge
    }

    fn recorder(&self) -> Self::Recorder {
        CumulativeRecorder {
            local: 0,
            state: Arc::clone(&self.state),
        }
    }

    fn observe(&self) -> Observation {
        Observation::U64Gauge(self.state.load(Ordering::Relaxed))
    }
}

#[derive(Debug)]
pub struct CumulativeRecorder {
    local: u64,
    state: Arc<AtomicU64>,
}

impl CumulativeRecorder {
    /// Gets the local contribution from this instance
    pub fn get_local(&self) -> u64 {
        self.local
    }

    /// Increment the local value for this CumulativeRecorder
    pub fn inc(&mut self, delta: u64) {
        self.local += delta;
        self.state.fetch_add(delta, Ordering::Relaxed);
    }

    /// Decrement the local value for this CumulativeRecorder
    pub fn decr(&mut self, delta: u64) {
        self.local -= delta;
        self.state.fetch_sub(delta, Ordering::Relaxed);
    }

    /// Sets the local value for this CumulativeRecorder
    pub fn set(&mut self, new: u64) {
        match new.cmp(&self.local) {
            std::cmp::Ordering::Less => self.decr(self.local - new),
            std::cmp::Ordering::Equal => {}
            std::cmp::Ordering::Greater => self.inc(new - self.local),
        }
    }
}

impl Drop for CumulativeRecorder {
    fn drop(&mut self) {
        self.state.fetch_sub(self.local, Ordering::Relaxed);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_gauge() {
        let gauge = CumulativeGauge::default();

        let mut r1 = gauge.recorder();
        assert_eq!(gauge.observe(), Observation::U64Gauge(0));

        r1.set(23);
        assert_eq!(gauge.observe(), Observation::U64Gauge(23));

        let mut r2 = gauge.recorder();
        assert_eq!(gauge.observe(), Observation::U64Gauge(23));

        r2.set(34);
        assert_eq!(gauge.observe(), Observation::U64Gauge(57));

        std::mem::drop(r2);

        assert_eq!(gauge.observe(), Observation::U64Gauge(23));

        let mut r3 = gauge.recorder();
        r3.set(7);

        assert_eq!(gauge.observe(), Observation::U64Gauge(30));

        r1.set(53);
        assert_eq!(gauge.observe(), Observation::U64Gauge(60));

        std::mem::drop(r1);
        assert_eq!(gauge.observe(), Observation::U64Gauge(7));

        std::mem::drop(r3);
        assert_eq!(gauge.observe(), Observation::U64Gauge(0));

        // Test overflow behaviour
        let mut r1 = gauge.recorder();
        let mut r2 = gauge.recorder();

        r1.set(u64::MAX);
        assert_eq!(gauge.observe(), Observation::U64Gauge(u64::MAX));
        r2.set(34);
        assert_eq!(gauge.observe(), Observation::U64Gauge(33));

        std::mem::drop(r1);
        assert_eq!(gauge.observe(), Observation::U64Gauge(34));

        std::mem::drop(r2);
        assert_eq!(gauge.observe(), Observation::U64Gauge(0));
    }
}
