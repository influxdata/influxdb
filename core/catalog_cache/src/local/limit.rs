//! A memory limiter

use super::{Error, Result};
use std::{
    future::Future,
    sync::{
        Arc, Mutex,
        atomic::{AtomicUsize, Ordering},
    },
    task::Waker,
};

/// Memory limiter for [`CatalogCache`].
///
/// [`CatalogCache`]: crate::local::CatalogCache
#[derive(Debug)]
pub struct MemoryLimiter {
    current: AtomicUsize,
    limit: usize,
    oom: SharedOomMailbox,
}

impl MemoryLimiter {
    /// Create a new [`MemoryLimiter`] limited to `limit` bytes
    pub fn new(limit: usize) -> Self {
        Self {
            current: AtomicUsize::new(0),
            limit,
            oom: Default::default(),
        }
    }

    /// Reserve `size` bytes, returning an error if this would exceed the limit
    pub(crate) fn reserve(&self, size: usize) -> Result<()> {
        let limit = self.limit;
        let max = limit
            .checked_sub(size)
            .ok_or(Error::TooLarge { size, limit })?;

        // We can use relaxed ordering as not relying on this to
        // synchronise memory accesses beyond itself
        self.current
            .fetch_update(Ordering::Relaxed, Ordering::Relaxed, |current| {
                // This cannot overflow as current + size <= limit
                (current <= max).then_some(current + size)
            })
            .map_err(|current| {
                self.oom.notify();

                Error::OutOfMemory {
                    size,
                    current,
                    limit,
                }
            })?;
        Ok(())
    }

    /// Free `size` bytes
    pub(crate) fn free(&self, size: usize) {
        self.current.fetch_sub(size, Ordering::Relaxed);
    }

    /// Notifier for out-of-memory.
    pub fn oom(&self) -> OomNotify {
        OomNotify {
            mailbox: Arc::clone(&self.oom),
            counter: 0,
        }
    }
}

type SharedOomMailbox = Arc<OomMailbox>;

#[derive(Debug, Default)]
struct OomMailbox {
    counter: AtomicUsize,
    wakers: Mutex<Vec<Waker>>,
}

impl OomMailbox {
    fn notify(&self) {
        let mut guard = self.wakers.lock().expect("not poisoned");

        // bump counter AFTER acquiring lock but before notifying wakers
        self.counter.fetch_add(1, Ordering::SeqCst);

        for waker in guard.drain(..) {
            waker.wake();
        }
    }
}

/// Notification for out-of-memory situations.
#[derive(Debug, Clone)]
pub struct OomNotify {
    mailbox: SharedOomMailbox,
    counter: usize,
}

impl OomNotify {
    /// Wait for notification.
    ///
    /// If there was an OOM situation between this and the previous call, this will return immediately, so the caller
    /// will never miss an OOM situation.
    pub fn wait(&mut self) -> OomNotifyFut<'_> {
        OomNotifyFut { notify: self }
    }

    /// Clear notification status.
    pub fn clear(&mut self) {
        self.counter = self.mailbox.counter.load(Ordering::SeqCst);
    }
}

#[derive(Debug)]
pub struct OomNotifyFut<'a> {
    notify: &'a mut OomNotify,
}

impl Future for OomNotifyFut<'_> {
    type Output = ();

    fn poll(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Self::Output> {
        let this = &mut *self;

        let upstream = this.notify.mailbox.counter.load(Ordering::SeqCst);
        if upstream != this.notify.counter {
            this.notify.counter = upstream;
            std::task::Poll::Ready(())
        } else {
            let mut guard = this.notify.mailbox.wakers.lock().expect("not poisoned");

            // check upstream again because counter might have changed
            let upstream = this.notify.mailbox.counter.load(Ordering::SeqCst);
            if upstream != this.notify.counter {
                this.notify.counter = upstream;
                std::task::Poll::Ready(())
            } else {
                guard.push(cx.waker().clone());
                std::task::Poll::Pending
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use tokio::task::JoinSet;

    use super::*;

    #[tokio::test]
    async fn test_limiter() {
        let limiter = MemoryLimiter::new(100);
        let oom_counter = OomCounter::new(&limiter);

        limiter.reserve(20).unwrap();
        limiter.reserve(70).unwrap();
        assert_eq!(oom_counter.get(), 0);

        let err = limiter.reserve(20).unwrap_err().to_string();
        assert_eq!(
            err,
            "Cannot reserve additional 20 bytes for cache containing 90 bytes as would exceed limit of 100 bytes"
        );
        oom_counter.wait_for(1).await;

        limiter.reserve(10).unwrap();
        limiter.reserve(0).unwrap();

        let err = limiter.reserve(1).unwrap_err().to_string();
        assert_eq!(
            err,
            "Cannot reserve additional 1 bytes for cache containing 100 bytes as would exceed limit of 100 bytes"
        );
        oom_counter.wait_for(2).await;

        limiter.free(10);
        limiter.reserve(10).unwrap();

        limiter.free(100);

        // Can add single value taking entire range
        limiter.reserve(100).unwrap();
        limiter.free(100);

        // Protected against overflow
        let err = limiter.reserve(usize::MAX).unwrap_err();
        assert!(matches!(err, Error::TooLarge { .. }), "{err}");
    }

    struct OomCounter {
        _task: JoinSet<()>,
        count: Arc<AtomicUsize>,
    }

    impl OomCounter {
        fn new(limiter: &MemoryLimiter) -> Self {
            let count = Arc::new(AtomicUsize::new(0));
            let count_captured = Arc::clone(&count);

            let mut oom = limiter.oom();

            let mut task = JoinSet::new();
            task.spawn(async move {
                loop {
                    oom.wait().await;
                    count_captured.fetch_add(1, Ordering::SeqCst);
                }
            });

            Self { _task: task, count }
        }

        fn get(&self) -> usize {
            self.count.load(Ordering::SeqCst)
        }

        async fn wait_for(&self, expected: usize) {
            tokio::time::timeout(Duration::from_secs(1), async {
                loop {
                    if self.get() == expected {
                        return;
                    }

                    tokio::time::sleep(Duration::from_millis(10)).await;
                }
            })
            .await
            .unwrap();
        }
    }
}
