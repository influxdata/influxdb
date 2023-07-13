use std::{sync::Mutex, time::Duration};

use tokio::time::Instant;

/// A [`RateLimit`] rate limiter that smooths `N` queries over a second.
#[derive(Debug)]
pub struct RateLimit {
    last_query: Mutex<Instant>,
    min_interval: Mutex<Duration>,
}

impl RateLimit {
    pub(crate) fn new(rps: usize) -> Self {
        Self {
            last_query: Mutex::new(Instant::now()),
            min_interval: Mutex::new(Duration::from_secs(1) / rps as u32),
        }
    }

    pub fn can_proceed(&self) -> Option<Duration> {
        let mut last_query = self.last_query.lock().unwrap();
        let now = Instant::now();

        // Has enough time passed since the last query was allowed?
        let next_allowed = last_query
            .checked_add(*self.min_interval.lock().unwrap())
            .unwrap();
        if now < next_allowed {
            return Some(next_allowed - now);
        }

        *last_query = now;
        None
    }

    pub fn update_rps(&self, rps: usize) {
        *self.min_interval.lock().unwrap() = Duration::from_secs(1) / rps as u32;
    }
}
