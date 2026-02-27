use std::{
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
    time::{Duration, Instant},
};

use futures::StreamExt;
use tokio::task::JoinSet;

use super::Notifier;

/// Counter helper for [`Notifier`].
pub(crate) struct NotificationCounter {
    _task: JoinSet<()>,
    count: Arc<AtomicUsize>,
}

impl NotificationCounter {
    /// Create new counter from given [`Notifier`].
    ///
    /// # Background Task
    /// This is marked as `async`` so we can spawn a tokio task.
    pub(crate) async fn new(notifier: Notifier) -> Self {
        let count = Arc::new(AtomicUsize::new(0));
        let count_captured = Arc::clone(&count);

        let mut task = JoinSet::new();
        task.spawn(async move {
            let mut notifier = notifier;

            loop {
                notifier.next().await;
                count_captured.fetch_add(1, Ordering::SeqCst);
            }
        });

        Self { _task: task, count }
    }

    /// Get current count.
    fn get(&self) -> usize {
        self.count.load(Ordering::SeqCst)
    }

    /// Generate panic message.
    #[track_caller]
    fn fail(current: usize, expected: usize) -> ! {
        panic!("timeout: got={current} expected={expected}")
    }

    /// Wait for count.
    ///
    /// # Panic
    /// Will panic after timeout.
    pub(crate) async fn wait_for(&self, expected: usize) {
        let start = Instant::now();
        loop {
            let current = self.get();
            if current == expected {
                // wait a bit longer and make sure this is still true
                tokio::time::sleep(Duration::from_millis(10)).await;
                let current2 = self.get();
                if current != current2 {
                    Self::fail(current2, expected)
                }

                return;
            } else if start.elapsed() >= Duration::from_millis(100) {
                Self::fail(current, expected)
            }

            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    }
}

mod tests {
    use crate::cache_system::hook::notify::Mailbox;

    use super::*;

    #[tokio::test]
    async fn test_inc() {
        let mailbox = Arc::new(Mailbox::default());
        let counter = NotificationCounter::new(mailbox.notifier()).await;
        assert_eq!(counter.get(), 0);

        mailbox.notify();
        counter.wait_for(1).await;

        mailbox.notify();
        counter.wait_for(2).await;
    }

    #[tokio::test]
    async fn test_event_compression() {
        let mailbox = Arc::new(Mailbox::default());
        let counter = NotificationCounter::new(mailbox.notifier()).await;

        mailbox.notify();
        mailbox.notify();
        counter.wait_for(1).await;
    }

    #[tokio::test]
    #[should_panic(expected = "timeout: got=0 expected=1")]
    async fn test_timeout() {
        let mailbox = Arc::new(Mailbox::default());
        let counter = NotificationCounter::new(mailbox.notifier()).await;
        counter.wait_for(1).await;
    }

    #[tokio::test]
    #[should_panic(expected = "timeout: got=1 expected=0")]
    async fn test_too_high() {
        let mailbox = Arc::new(Mailbox::default());
        let counter = NotificationCounter::new(mailbox.notifier()).await;

        let fut1 = counter.wait_for(0);
        let fut2 = async { mailbox.notify() };
        tokio::join!(fut1, fut2);
    }
}
