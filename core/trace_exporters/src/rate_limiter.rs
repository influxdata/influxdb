use std::{num::NonZeroU64, sync::Arc, time::Duration};

use iox_time::{Time, TimeProvider};

/// Limits `send` actions to a specific "messages per second".
#[derive(Debug)]
pub struct RateLimiter {
    wait_time: Duration,
    last_msg: Option<Time>,
    time_provider: Arc<dyn TimeProvider>,
}

impl RateLimiter {
    /// Create new rate limiter using the given config.
    pub fn new(msgs_per_second: NonZeroU64, time_provider: Arc<dyn TimeProvider>) -> Self {
        Self {
            wait_time: Duration::from_secs_f64(1.0 / msgs_per_second.get() as f64),
            last_msg: None,
            time_provider,
        }
    }

    /// Record a send action.
    ///
    /// This may async-block if the rate limit was hit until the it is OK to send a message again.
    ///
    /// It is safe to cancel this method.
    pub async fn send(&mut self) {
        let mut now = self.time_provider.now();

        if let Some(last) = &self.last_msg {
            let wait_until = *last + self.wait_time;
            if wait_until > now {
                self.time_provider.sleep_until(wait_until).await;

                // refresh `now`
                now = self.time_provider.now();
            }
        }

        // modify AFTER `await` due to cancellation
        self.last_msg = Some(now);
    }
}

#[cfg(test)]
mod tests {
    use iox_time::MockProvider;
    use std::future::Future;

    use super::*;

    #[tokio::test]
    async fn new_always_works() {
        let mut limiter = RateLimiter::new(
            NonZeroU64::new(1).unwrap(),
            Arc::new(MockProvider::new(Time::MIN)),
        );
        limiter.send().await;
    }

    #[tokio::test]
    async fn u64_max_msgs_per_second() {
        let mut limiter = RateLimiter::new(
            NonZeroU64::new(u64::MAX).unwrap(),
            Arc::new(MockProvider::new(Time::MIN)),
        );
        limiter.send().await;
        limiter.send().await;
    }

    #[tokio::test]
    async fn throttle() {
        let time_provider = Arc::new(MockProvider::new(Time::MIN));
        let mut limiter =
            RateLimiter::new(NonZeroU64::new(1).unwrap(), Arc::clone(&time_provider) as _);
        limiter.send().await;

        {
            // do NOT advance time
            let fut = limiter.send();
            tokio::pin!(fut);
            assert_fut_pending(&mut fut).await;

            // tick
            time_provider.inc(Duration::from_secs(1));
            fut.await;

            // fut dropped here (important because it mut-borrows `limiter`)
        }

        // tick (but not enough)
        time_provider.inc(Duration::from_millis(500));
        let fut = limiter.send();
        tokio::pin!(fut);
        assert_fut_pending(&mut fut).await;

        // tick (enough)
        time_provider.inc(Duration::from_millis(500));
        fut.await;
    }

    #[tokio::test]
    async fn throttle_after_cancel() {
        let time_provider = Arc::new(MockProvider::new(Time::MIN));
        let mut limiter =
            RateLimiter::new(NonZeroU64::new(1).unwrap(), Arc::clone(&time_provider) as _);
        limiter.send().await;

        // do NOT advance time
        {
            let fut = limiter.send();
            tokio::pin!(fut);
            assert_fut_pending(&mut fut).await;

            // fut dropped here
        }

        // 2nd try should still be pending
        let fut = limiter.send();
        tokio::pin!(fut);
        assert_fut_pending(&mut fut).await;

        // tick
        time_provider.inc(Duration::from_secs(1));
        fut.await;
    }

    /// Assert that given future is pending.
    async fn assert_fut_pending<F>(fut: &mut F)
    where
        F: Future + Send + Unpin,
        F::Output: std::fmt::Debug,
    {
        tokio::select! {
            e = fut => panic!("future is not pending, yielded: {e:?}"),
            _ = tokio::time::sleep(Duration::from_millis(10)) => {},
        };
    }
}
