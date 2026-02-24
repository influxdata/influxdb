use std::{sync::Arc, time::Duration};

use futures::{FutureExt, StreamExt, stream::BoxStream};
use iox_time::{Time, TimeProvider};
use metric::U64Counter;
use tokio::time::MissedTickBehavior;
use tracing::info;

/// A trigger for a [reactor].
///
///
/// [reactor]: super::Reactor
pub type Trigger = BoxStream<'static, ()>;

/// Helper for [`TriggerExt::throttle`].
struct ThrottleState {
    time_provider: Arc<dyn TimeProvider>,
    trigger: Trigger,
    wait_until: Option<Time>,
    cache: &'static str,
    trigger_name: &'static str,
    counter: U64Counter,
}

/// Extension trait for [triggers](Trigger).
pub trait TriggerExt {
    /// Throttle given trigger.
    ///
    /// Trigger events that happen during the backoff are dropped.
    fn throttle(
        self,
        backoff: Duration,
        time_provider: Arc<dyn TimeProvider>,
        cache: &'static str,
        trigger: &'static str,
        metrics: &metric::Registry,
    ) -> Trigger;

    /// Observe trigger events.
    fn observe(
        self,
        cache: &'static str,
        trigger: &'static str,
        metrics: &metric::Registry,
    ) -> Trigger;
}

impl TriggerExt for Trigger {
    fn throttle(
        self,
        backoff: Duration,
        time_provider: Arc<dyn TimeProvider>,
        cache: &'static str,
        trigger: &'static str,
        metrics: &metric::Registry,
    ) -> Trigger {
        let counter = metrics
            .register_metric::<U64Counter>(
                "mem_cache_reactor_throttled",
                "Actions throttled for the in-mem cache reactor",
            )
            .recorder(&[("cache", cache), ("trigger", trigger)]);
        futures::stream::unfold(
            ThrottleState {
                time_provider,
                trigger: self,
                wait_until: None,
                cache,
                trigger_name: trigger,
                counter,
            },
            move |mut state| async move {
                if let Some(wait_until) = state.wait_until.take() {
                    let mut sleep_fut =
                        std::pin::pin!(state.time_provider.sleep_until(wait_until).fuse());

                    loop {
                        tokio::select! {
                            // make sure we try to finish sleeping before consuming any more trigger events
                            biased;

                            _ = &mut sleep_fut => {
                                break;
                            }

                            maybe = state.trigger.next() => {
                                match maybe {
                                    Some(()) => {
                                        info!(cache=state.cache, trigger=state.trigger_name, "reactor throttled");
                                        state.counter.inc(1);
                                        continue;
                                    }
                                    None => {
                                        return None;
                                    }
                                }
                            }
                        }
                    }
                }

                state.trigger.next().await;
                state.wait_until = Some(state.time_provider.now() + backoff);
                Some(((), state))
            },
        )
        .boxed()
    }

    fn observe(
        self,
        cache: &'static str,
        trigger: &'static str,
        metrics: &metric::Registry,
    ) -> Trigger {
        let counter = metrics
            .register_metric::<U64Counter>(
                "mem_cache_reactor_triggered",
                "Actions triggered for the in-mem cache reactor",
            )
            .recorder(&[("cache", cache), ("trigger", trigger)]);
        self.inspect(move |()| {
            info!(cache, trigger, "reactor triggered");
            counter.inc(1);
        })
        .boxed()
    }
}

/// Produce trigger events in regular intervals.
pub fn ticker(delta: Duration) -> Trigger {
    let mut interval = tokio::time::interval(delta);
    interval.set_missed_tick_behavior(MissedTickBehavior::Delay);
    interval.reset(); // otherwise first tick return immediately

    futures::stream::unfold(interval, async move |mut interval| {
        interval.tick().await;
        Some(((), interval))
    })
    .boxed()
}

#[cfg(test)]
mod tests {
    use futures_test_utils::AssertFutureExt;
    use iox_time::MockProvider;
    use metric::{Attributes, Metric};

    use super::*;

    #[tokio::test]
    async fn test_throttle() {
        let (tx, rx) = tokio::sync::mpsc::channel::<()>(1);
        let trigger =
            futures::stream::unfold(rx, async move |mut rx| rx.recv().await.map(|_| ((), rx)))
                .boxed();

        let time_provider = Arc::new(MockProvider::new(Time::MIN));
        let metrics = metric::Registry::default();
        let throttle_count = || {
            metrics
                .get_instrument::<Metric<U64Counter>>("mem_cache_reactor_throttled")
                .unwrap()
                .get_observer(&Attributes::from(&[
                    ("cache", "my_cache"),
                    ("trigger", "my_trigger"),
                ]))
                .unwrap()
                .fetch()
        };
        let mut trigger = trigger.throttle(
            Duration::from_secs(1),
            Arc::clone(&time_provider) as _,
            "my_cache",
            "my_trigger",
            &metrics,
        );

        let mut fut_next = trigger.next();

        // not triggered yet
        fut_next.assert_pending().await;
        assert_eq!(throttle_count(), 0);

        // trigger first time
        tx.send(()).boxed().poll_timeout().await.unwrap();
        fut_next.poll_timeout().await.unwrap();
        assert_eq!(throttle_count(), 0);

        // wait for next trigger and trigger again
        time_provider.inc(Duration::from_secs(1));
        tx.send(()).boxed().poll_timeout().await.unwrap();
        trigger.next().poll_timeout().await.unwrap();
        assert_eq!(throttle_count(), 0);

        // trigger too fast
        tx.send(()).boxed().poll_timeout().await.unwrap();
        let mut fut_next = trigger.next();
        fut_next.assert_pending().await;
        assert_eq!(throttle_count(), 1);

        // still too fast
        tx.send(()).boxed().poll_timeout().await.unwrap();
        fut_next.assert_pending().await;
        assert_eq!(throttle_count(), 2);

        // still too fast
        time_provider.inc(Duration::from_millis(10));
        tx.send(()).boxed().poll_timeout().await.unwrap();
        fut_next.assert_pending().await;
        assert_eq!(throttle_count(), 3);

        // unthrottled
        time_provider.inc(Duration::from_secs(1));
        tx.send(()).boxed().poll_timeout().await.unwrap();
        fut_next.poll_timeout().await.unwrap();
        assert_eq!(throttle_count(), 3);
    }
}
