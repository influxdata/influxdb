use std::sync::{
    Arc,
    atomic::{AtomicUsize, Ordering},
};

use crate::cache_system::DynError;

use super::{
    EvictResult, Hook, HookDecision,
    notify::{Mailbox, Notifier},
};

/// A [hook](Hook) that triggers whenever data is ingested and we are above a given threshold.
#[derive(Debug)]
pub struct LevelTrigger {
    current_sum: AtomicUsize,
    level: usize,
    mailbox: Arc<Mailbox>,
}

impl LevelTrigger {
    /// Create new trigger at given level/threshold.
    pub fn new(level: usize) -> Self {
        Self {
            current_sum: AtomicUsize::new(0),
            level,
            mailbox: Default::default(),
        }
    }

    /// Notify when then given level is reached or overshot.
    ///
    /// This will trigger every time a new entry is [fetched](Hook::fetched) while the condition is met.
    pub fn level_reached(&self) -> Notifier {
        self.mailbox.notifier()
    }
}

impl<K> Hook<K> for LevelTrigger {
    fn fetched(&self, _gen: u64, _k: &Arc<K>, res: Result<usize, &DynError>) -> HookDecision {
        if let Ok(size) = res {
            let new_sum = self.current_sum.fetch_add(size, Ordering::Relaxed) + size;
            if new_sum >= self.level {
                self.mailbox.notify();
            }
        }

        HookDecision::default()
    }

    fn evict(&self, _gen: u64, _k: &Arc<K>, res: EvictResult) {
        if let EvictResult::Fetched { size } = res {
            self.current_sum.fetch_sub(size, Ordering::Relaxed);
        }
    }
}

#[cfg(test)]
mod tests {
    use crate::cache_system::hook::notify::test_utils::NotificationCounter;

    use super::*;

    #[tokio::test]
    async fn test() {
        let trigger = LevelTrigger::new(10);
        let counter = NotificationCounter::new(trigger.level_reached()).await;
        counter.wait_for(0).await;

        assert_eq!(
            trigger.fetched(0, &Arc::new(()), Ok(9)),
            HookDecision::default()
        );
        counter.wait_for(0).await;

        assert_eq!(
            trigger.fetched(0, &Arc::new(()), Ok(1)),
            HookDecision::default()
        );
        counter.wait_for(1).await;

        // trigger when already "high"
        assert_eq!(
            trigger.fetched(0, &Arc::new(()), Ok(1)),
            HookDecision::default()
        );
        counter.wait_for(2).await;

        // evictions don't trigger
        trigger.evict(0, &Arc::new(()), EvictResult::Fetched { size: 1 });
        counter.wait_for(2).await;
    }
}
