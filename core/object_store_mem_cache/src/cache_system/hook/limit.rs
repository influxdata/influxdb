//! A memory limiter
use std::{
    num::NonZeroUsize,
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
};

use crate::cache_system::{DynError, hook::Hook};

use super::{
    EvictResult, HookDecision,
    notify::{Mailbox, Notifier},
};

/// Memory limiter.
#[derive(Debug)]
pub struct MemoryLimiter {
    current: AtomicUsize,
    limit: NonZeroUsize,
    oom: Arc<Mailbox>,
}

impl MemoryLimiter {
    /// Create a new [`MemoryLimiter`] limited to `limit` bytes
    pub fn new(limit: NonZeroUsize) -> Self {
        Self {
            current: AtomicUsize::new(0),
            limit,
            oom: Default::default(),
        }
    }

    /// Notifier for out-of-memory.
    pub fn oom(&self) -> Notifier {
        self.oom.notifier()
    }
}

impl<K> Hook<K> for MemoryLimiter {
    fn fetched(&self, _gen: u64, _k: &Arc<K>, res: Result<usize, &DynError>) -> HookDecision {
        let Ok(size) = res else {
            return HookDecision::Keep;
        };

        let limit = self.limit;
        let Some(max) = limit.get().checked_sub(size) else {
            // too large
            return HookDecision::Evict;
        };

        // We can use relaxed ordering as not relying on this to
        // synchronise memory accesses beyond itself
        match self
            .current
            .fetch_update(Ordering::Relaxed, Ordering::Relaxed, |current| {
                // This cannot overflow as current + size <= limit
                (current <= max).then_some(current + size)
            }) {
            Ok(_) => HookDecision::Keep,
            Err(_) => {
                self.oom.notify();
                HookDecision::Evict
            }
        }
    }

    fn evict(&self, _gen: u64, _k: &Arc<K>, res: EvictResult) {
        if let EvictResult::Fetched { size } = res {
            self.current.fetch_sub(size, Ordering::Relaxed);
        }
    }
}

#[cfg(test)]
mod tests {
    use crate::cache_system::{hook::notify::test_utils::NotificationCounter, utils::str_err};

    use super::*;

    #[tokio::test]
    async fn test_limiter() {
        let limiter = MemoryLimiter::new(NonZeroUsize::new(100).unwrap());
        let oom_counter = NotificationCounter::new(limiter.oom()).await;

        assert_eq!(
            limiter.fetched(1, &Arc::new(()), Ok(20)),
            HookDecision::Keep
        );
        assert_eq!(
            limiter.fetched(2, &Arc::new(()), Ok(70)),
            HookDecision::Keep
        );
        oom_counter.wait_for(0).await;

        assert_eq!(
            limiter.fetched(3, &Arc::new(()), Ok(20)),
            HookDecision::Evict
        );
        oom_counter.wait_for(1).await;

        assert_eq!(
            limiter.fetched(4, &Arc::new(()), Ok(10)),
            HookDecision::Keep
        );
        assert_eq!(limiter.fetched(5, &Arc::new(()), Ok(0)), HookDecision::Keep);

        assert_eq!(
            limiter.fetched(6, &Arc::new(()), Ok(1)),
            HookDecision::Evict
        );
        oom_counter.wait_for(2).await;

        limiter.evict(7, &Arc::new(()), EvictResult::Fetched { size: 10 });
        assert_eq!(
            limiter.fetched(8, &Arc::new(()), Ok(10)),
            HookDecision::Keep
        );

        limiter.evict(9, &Arc::new(()), EvictResult::Fetched { size: 100 });

        // Can add single value taking entire range
        assert_eq!(
            limiter.fetched(10, &Arc::new(()), Ok(100)),
            HookDecision::Keep
        );
        limiter.evict(11, &Arc::new(()), EvictResult::Fetched { size: 100 });

        // Protected against overflow
        assert_eq!(
            limiter.fetched(12, &Arc::new(()), Ok(usize::MAX)),
            HookDecision::Evict,
        );

        // errors are kept
        assert_eq!(
            limiter.fetched(13, &Arc::new(()), Err(&str_err("foo"))),
            HookDecision::Keep
        );
    }
}
