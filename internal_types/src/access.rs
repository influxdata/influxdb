use parking_lot::RwLock;
use std::sync::Arc;
use time::{Time, TimeProvider};

/// A struct that allows recording access by a query
#[derive(Debug, Clone)]
pub struct AccessRecorder {
    time_provider: Arc<dyn TimeProvider>,
    state: Arc<RwLock<AccessMetrics>>,
}

#[derive(Debug, Clone, Eq, PartialEq)]
pub struct AccessMetrics {
    /// The number of accesses that have been recorded
    pub count: usize,

    /// The time of the last access or if none the
    /// time when the `AccessRecorder` was created
    pub last_access: Time,
}

impl AccessMetrics {
    /// Returns the time of the last access if any
    pub fn last_access(&self) -> Option<Time> {
        (self.count > 0).then(|| self.last_access)
    }
}

impl AccessRecorder {
    /// Creates a new AccessRecorder
    pub fn new(time_provider: Arc<dyn TimeProvider>) -> Self {
        let now = time_provider.now();
        Self {
            time_provider,
            state: Arc::new(RwLock::new(AccessMetrics {
                count: 0,
                last_access: now,
            })),
        }
    }

    /// Records an access
    pub fn record_access(&self) {
        let now = self.time_provider.now();
        let mut state = self.state.write();
        state.last_access = state.last_access.max(now);
        state.count += 1;
    }

    /// Gets the access metrics
    pub fn get_metrics(&self) -> AccessMetrics {
        self.state.read().clone()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Duration;

    #[test]
    fn test_access() {
        let t1 = Time::from_timestamp(3044, 2);
        let t2 = t1 + Duration::from_nanos(1);
        let t3 = t1 + Duration::from_nanos(2);

        let time = Arc::new(time::MockProvider::new(t1));
        let access_recorder = AccessRecorder::new(Arc::<time::MockProvider>::clone(&time));

        assert_eq!(
            access_recorder.get_metrics(),
            AccessMetrics {
                count: 0,
                last_access: t1
            }
        );

        time.set(t3);
        access_recorder.record_access();
        assert_eq!(
            access_recorder.get_metrics(),
            AccessMetrics {
                count: 1,
                last_access: t3
            }
        );

        time.set(t2);
        access_recorder.record_access();
        assert_eq!(
            access_recorder.get_metrics(),
            AccessMetrics {
                count: 2,
                last_access: t3
            }
        );
    }
}
