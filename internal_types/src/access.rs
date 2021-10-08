use chrono::{DateTime, Utc};
use parking_lot::RwLock;
use std::sync::Arc;

/// A struct that allows recording access by a query
#[derive(Debug, Clone)]
pub struct AccessRecorder {
    state: Arc<RwLock<AccessMetrics>>,
}

impl Default for AccessRecorder {
    fn default() -> Self {
        Self::new(Utc::now())
    }
}

#[derive(Debug, Clone, Eq, PartialEq)]
pub struct AccessMetrics {
    /// The number of accesses that have been recorded
    pub count: usize,

    /// The time of the last access or if none the
    /// time when the `AccessRecorder` was created
    pub last_access: DateTime<Utc>,
}

impl AccessMetrics {
    /// Returns the Instant of the last access if any
    pub fn last_access(&self) -> Option<DateTime<Utc>> {
        (self.count > 0).then(|| self.last_access)
    }
}

impl AccessRecorder {
    /// Creates a new AccessRecorder with the provided creation DateTime
    pub fn new(now: DateTime<Utc>) -> Self {
        Self {
            state: Arc::new(RwLock::new(AccessMetrics {
                count: 0,
                last_access: now,
            })),
        }
    }

    /// Records an access at the given DateTime
    pub fn record_access(&self, now: DateTime<Utc>) {
        let mut state = self.state.write();
        state.last_access = state.last_access.max(now);
        state.count += 1;
    }

    /// Records an access at the current time
    pub fn record_access_now(&self) {
        self.record_access(Utc::now())
    }

    /// Gets the access metrics
    pub fn get_metrics(&self) -> AccessMetrics {
        self.state.read().clone()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::Duration;

    #[test]
    fn test_access() {
        let t1 = Utc::now();
        let t2 = t1 + Duration::nanoseconds(1);
        let t3 = t1 + Duration::nanoseconds(2);

        let access_recorder = AccessRecorder::new(t1);

        assert_eq!(
            access_recorder.get_metrics(),
            AccessMetrics {
                count: 0,
                last_access: t1
            }
        );

        access_recorder.record_access(t3);
        assert_eq!(
            access_recorder.get_metrics(),
            AccessMetrics {
                count: 1,
                last_access: t3
            }
        );

        access_recorder.record_access(t2);
        assert_eq!(
            access_recorder.get_metrics(),
            AccessMetrics {
                count: 2,
                last_access: t3
            }
        );
    }
}
