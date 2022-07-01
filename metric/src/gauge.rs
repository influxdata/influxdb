use crate::{MetricKind, MetricObserver, Observation};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

/// An observation of a single u64 value
///
/// NOTE: If the same `U64Gauge` is used in multiple locations, e.g. a non-unique set
/// of attributes is provided to `Metric<U64Gauge>::recorder`, the reported value
/// will oscillate between those reported by the separate locations
#[derive(Debug, Clone, Default)]
pub struct U64Gauge {
    state: Arc<AtomicU64>,
}

impl U64Gauge {
    /// Sets the value of this U64Gauge
    pub fn set(&self, value: u64) {
        self.state.store(value, Ordering::Relaxed);
    }

    /// Increments the value of this U64Gauge by the specified amount.
    pub fn inc(&self, delta: u64) {
        self.state.fetch_add(delta, Ordering::Relaxed);
    }

    /// Decrements the value of this U64Gauge by the specified amount.
    ///
    /// # Underflow / Overflow
    ///
    /// This operation wraps around on over/underflow.
    pub fn dec(&self, delta: u64) {
        self.state.fetch_sub(delta, Ordering::Relaxed);
    }

    /// Adjusts the value of this U64Gauge by the specified delta.
    ///
    /// # Underflow / Overflow
    ///
    /// This operation wraps around on over/underflow.
    pub fn delta(&self, delta: i64) {
        if delta > 0 {
            self.inc(delta as _);
        } else {
            self.dec(delta.unsigned_abs());
        }
    }

    /// Fetches the value of this U64Gauge
    pub fn fetch(&self) -> u64 {
        self.state.load(Ordering::Relaxed)
    }
}

impl MetricObserver for U64Gauge {
    type Recorder = Self;

    fn kind() -> MetricKind {
        MetricKind::U64Gauge
    }

    fn recorder(&self) -> Self::Recorder {
        self.clone()
    }

    fn observe(&self) -> Observation {
        Observation::U64Gauge(self.fetch())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_gauge() {
        let gauge = U64Gauge::default();

        assert_eq!(gauge.observe(), Observation::U64Gauge(0));

        gauge.set(345);
        assert_eq!(gauge.observe(), Observation::U64Gauge(345));

        gauge.set(23);
        assert_eq!(gauge.observe(), Observation::U64Gauge(23));

        gauge.inc(10);
        assert_eq!(gauge.observe(), Observation::U64Gauge(33));

        gauge.dec(10);
        assert_eq!(gauge.observe(), Observation::U64Gauge(23));

        gauge.delta(19);
        assert_eq!(gauge.observe(), Observation::U64Gauge(42));

        gauge.delta(-19);
        assert_eq!(gauge.observe(), Observation::U64Gauge(23));

        let r2 = gauge.recorder();

        r2.set(34);
        assert_eq!(gauge.observe(), Observation::U64Gauge(34));

        std::mem::drop(r2);

        assert_eq!(gauge.observe(), Observation::U64Gauge(34));
    }
}
