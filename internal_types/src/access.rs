use crate::atomic_instant::AtomicInstant;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::Instant;

/// A struct that allows recording access by a query
#[derive(Debug, Clone)]
pub struct AccessRecorder {
    state: Arc<AccessRecorderInner>,
}

impl Default for AccessRecorder {
    fn default() -> Self {
        Self::new(Instant::now())
    }
}

#[derive(Debug)]
struct AccessRecorderInner {
    count: AtomicUsize,
    last_instant: AtomicInstant,
}

#[derive(Debug, Clone, Eq, PartialEq)]
pub struct AccessMetrics {
    /// The number of accesses that have been recorded
    pub count: usize,

    /// The instant of the last access or if none the
    /// instant when the `AccessRecorder` was created
    pub last_instant: Instant,
}

impl AccessMetrics {
    /// Returns the Instant of the last access if any
    pub fn last_access(&self) -> Option<Instant> {
        (self.count > 0).then(|| self.last_instant)
    }
}

impl AccessRecorder {
    /// Creates a new AccessRecorder with the provided creation Instant
    pub fn new(instant: Instant) -> Self {
        Self {
            state: Arc::new(AccessRecorderInner {
                count: AtomicUsize::new(0),
                last_instant: AtomicInstant::new(instant),
            }),
        }
    }

    /// Records an access at the given instant
    pub fn record_access(&self, instant: Instant) {
        self.state
            .last_instant
            .fetch_max(instant, Ordering::Relaxed);
        self.state.count.fetch_add(1, Ordering::Release);
    }

    /// Records an access at the current instant
    pub fn record_access_now(&self) {
        self.record_access(Instant::now())
    }

    /// Gets the access metrics
    pub fn get_metrics(&self) -> AccessMetrics {
        // Acquire and Release ensures that if we observe the count from an access,
        // we are guaranteed that the observed last_instant will be greater than
        // or equal to the time of this access
        let count = self.state.count.load(Ordering::Acquire);
        let last_instant = self.state.last_instant.load(Ordering::Relaxed);
        AccessMetrics {
            count,
            last_instant,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Duration;

    #[test]
    fn test_access() {
        let t1 = Instant::now();
        let t2 = t1 + Duration::from_nanos(1);
        let t3 = t1 + Duration::from_nanos(2);

        let access_recorder = AccessRecorder::new(t1);

        assert_eq!(
            access_recorder.get_metrics(),
            AccessMetrics {
                count: 0,
                last_instant: t1
            }
        );

        access_recorder.record_access(t3);
        assert_eq!(
            access_recorder.get_metrics(),
            AccessMetrics {
                count: 1,
                last_instant: t3
            }
        );

        access_recorder.record_access(t2);
        assert_eq!(
            access_recorder.get_metrics(),
            AccessMetrics {
                count: 2,
                last_instant: t3
            }
        );
    }
}
