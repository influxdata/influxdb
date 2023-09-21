use std::{sync::Mutex, time::Duration};

use tokio::time::Instant;

/// A [`RateLimit`] rate limiter that smooths `N` queries over a second.
#[derive(Debug)]
pub struct RateLimit {
    last_query: Mutex<Instant>,
    min_interval: Mutex<Duration>,

    // if we compute a simple interval and enforce at least that much time between each, a variable query
    // rate will be unpredictably slower than the specified rate.  So when the delay between queries is more
    // than the average, allow it to 'bank' some extra credit - so a few can go quicker after a period of
    // inactivity.
    //
    // burst_balance is how many can proceed immediately, without delay, due to slowness of the previous.
    burst_balance: Mutex<usize>,
    // max_burst is the maximum burst balance we allow.  Without this, a large delay in queries could
    // allow too big of a burst of queries.
    max_burst: Mutex<usize>,
}

impl RateLimit {
    pub(crate) fn new(rps: usize, max_burst: usize) -> Self {
        Self {
            last_query: Mutex::new(Instant::now()),
            min_interval: Mutex::new(Duration::from_secs(1) / rps as u32),
            max_burst: Mutex::new(max_burst),
            burst_balance: Mutex::new(0),
        }
    }

    pub fn can_proceed(&self) -> Option<Duration> {
        let mut last_query = self.last_query.lock().unwrap();
        let mut burst_balance = self.burst_balance.lock().unwrap();
        let interval = *self.min_interval.lock().unwrap();
        let now = Instant::now();

        // Has enough time passed since the last query was allowed?
        let next_allowed = last_query.checked_add(interval).unwrap();
        if now < next_allowed {
            // This request came quickly after the prior one.  If we have burst balance, we can use it,
            // otherwise we'll have to wait.
            if *burst_balance > 0 {
                *burst_balance -= 1;
                return None;
            }
            return Some(next_allowed - now);
        }

        // For simpliicity, only add the burst balance in whole numbers.  So if the time since the last
        // request was more than 2x the min interval, we can add to the burst balance.
        let credits = now.duration_since(next_allowed).as_nanos() / interval.as_nanos();
        if credits > 1 {
            let max = *self.max_burst.lock().unwrap();
            *burst_balance += credits as usize - 1;

            if *burst_balance > max {
                *burst_balance = max;
            }
        }

        *last_query = now;
        None
    }

    pub fn update_rps(&self, rps: usize, max_burst: usize) {
        *self.min_interval.lock().unwrap() = Duration::from_secs(1) / rps as u32;
        *self.max_burst.lock().unwrap() = max_burst;
        let mut burst_balance = self.burst_balance.lock().unwrap();
        if *burst_balance > max_burst {
            *burst_balance = max_burst;
        }
    }
}
