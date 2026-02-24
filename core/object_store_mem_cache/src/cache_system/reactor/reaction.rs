use std::sync::{Arc, Weak};

use futures::future::BoxFuture;
use iox_time::TimeProvider;
use metric::DurationHistogram;
use tracing::{info, warn};

use crate::cache_system::DynError;

/// Reaction to a [trigger].
///
///
/// [trigger]: super::trigger::Trigger
pub trait Reaction: Send + Sync + 'static {
    fn exec(&self) -> BoxFuture<'_, Result<(), DynError>>;
}

impl<T> Reaction for Weak<T>
where
    T: Reaction + Sync,
{
    fn exec(&self) -> BoxFuture<'_, Result<(), DynError>> {
        Box::pin(async {
            let Some(inner) = self.upgrade() else {
                return Ok(());
            };
            inner.exec().await
        })
    }
}

/// Extension trait for [reactions](Reaction).
pub trait ReactionExt {
    /// Place reaction into a box and erase types.
    fn boxed(self) -> Box<dyn Reaction>;

    /// Observe reaction events.
    fn observe(
        self,
        cache: &'static str,
        metrics: &metric::Registry,
        time_provider: Arc<dyn TimeProvider>,
    ) -> impl Reaction;
}

impl<T> ReactionExt for T
where
    T: Reaction,
{
    fn boxed(self) -> Box<dyn Reaction> {
        Box::new(self)
    }

    fn observe(
        self,
        cache: &'static str,
        metrics: &metric::Registry,
        time_provider: Arc<dyn TimeProvider>,
    ) -> impl Reaction {
        let hist = metrics.register_metric::<DurationHistogram>(
            "mem_cache_reactor_duration",
            "Reaction duration for cache reactor",
        );
        Observer {
            cache,
            time_provider,
            hist_ok: hist.recorder(&[("cache", cache), ("status", "ok")]),
            hist_err: hist.recorder(&[("cache", cache), ("status", "err")]),
            inner: self.boxed(),
        }
    }
}

/// Helper for [`ReactionExt::observe`]
struct Observer {
    cache: &'static str,
    time_provider: Arc<dyn TimeProvider>,
    hist_ok: DurationHistogram,
    hist_err: DurationHistogram,
    inner: Box<dyn Reaction>,
}

impl Reaction for Observer {
    fn exec(&self) -> BoxFuture<'_, Result<(), DynError>> {
        Box::pin(async {
            let t_begin = self.time_provider.now();

            let res = self.inner.exec().await;

            let hist = match &res {
                Ok(()) => {
                    info!(cache = self.cache, "reaction executed");
                    &self.hist_ok
                }
                Err(e) => {
                    warn!(cache=self.cache, %e, "reaction failed to execute");
                    &self.hist_err
                }
            };
            if let Some(d) = self.time_provider.now().checked_duration_since(t_begin) {
                hist.record(d);
            }

            res
        })
    }
}
