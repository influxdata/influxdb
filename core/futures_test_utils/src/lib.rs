use std::{
    pin::Pin,
    sync::{
        Arc,
        atomic::{AtomicBool, AtomicUsize, Ordering},
    },
    task::{Context, Poll, Waker},
    time::Duration,
};

use futures::{FutureExt, task::ArcWake};
use futures_concurrency::future::FutureExt as _;

// Workaround for "unused crate" lint false positives.
#[cfg(test)]
use clap as _;
#[cfg(test)]
use futures_concurrency as _;
#[cfg(test)]
use rand as _;
use workspace_hack as _;

/// Helper trait for asserting state of a future
pub trait AssertFutureExt {
    /// The output type
    type Output;

    /// Panic's if the future does not return `Poll::Pending`
    fn assert_pending(&mut self) -> impl Future<Output = ()>;

    /// Pols with a timeout
    fn poll_timeout(self) -> impl Future<Output = Self::Output>;
}

impl<F> AssertFutureExt for F
where
    F: Future + Send + Unpin,
{
    type Output = F::Output;

    async fn assert_pending(&mut self) {
        let this = async {
            self.await;
            true
        };
        let timeout = async {
            tokio::time::sleep(Duration::from_millis(10)).await;
            false
        };
        let was_this = this.race(timeout).await;
        if was_this {
            panic!("not pending")
        }
    }

    async fn poll_timeout(self) -> Self::Output {
        tokio::time::timeout(Duration::from_millis(10), self)
            .await
            .expect("timeout")
    }
}

/// Statistics about a [`Future`].
#[derive(Debug)]
pub struct FutureStats {
    polled: AtomicUsize,
    polled_after_ready: AtomicBool,
    ready: AtomicBool,
    woken: Arc<AtomicUsize>,
    name: Arc<str>,
}

impl FutureStats {
    /// Number of [polls](Future::poll).
    pub fn polled(&self) -> usize {
        self.polled.load(Ordering::SeqCst)
    }

    /// Future is [ready](Poll::Ready).
    pub fn ready(&self) -> bool {
        self.ready.load(Ordering::SeqCst)
    }

    /// How often the callers [`Waker::wake`](std::task::Waker::wake) was called.
    pub fn woken(&self) -> usize {
        self.woken.load(Ordering::SeqCst)
    }
}

/// Observer for a [`Future`]
#[derive(Debug)]
pub struct FutureObserver<F>
where
    F: Future,
{
    inner: Pin<Box<F>>,
    stats: Arc<FutureStats>,
}

impl<F> FutureObserver<F>
where
    F: Future,
{
    /// Create new observer.
    pub fn new(inner: F, name: impl Into<Arc<str>>) -> Self {
        Self {
            inner: Box::pin(inner),
            stats: Arc::new(FutureStats {
                polled: AtomicUsize::new(0),
                polled_after_ready: AtomicBool::new(false),
                ready: AtomicBool::new(false),
                woken: Arc::new(AtomicUsize::new(0)),
                name: name.into(),
            }),
        }
    }

    /// Get [statistics](FutureStats) for the contained future.
    pub fn stats(&self) -> Arc<FutureStats> {
        Arc::clone(&self.stats)
    }
}

impl<F> Future for FutureObserver<F>
where
    F: Future,
{
    type Output = F::Output;

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        self.stats.polled.fetch_add(1, Ordering::SeqCst);
        if self.stats.ready() {
            println!("{}: poll after ready", self.stats.name);
            self.stats.polled_after_ready.store(true, Ordering::SeqCst);
        } else {
            println!("{}: poll", self.stats.name);
        }

        let waker = futures::task::waker(Arc::new(WakeObserver {
            inner: cx.waker().clone(),
            woken: Arc::clone(&self.stats.woken),
            name: Arc::clone(&self.stats.name),
        }));
        let mut cx = Context::from_waker(&waker);

        match self.inner.poll_unpin(&mut cx) {
            Poll::Ready(res) => {
                println!("{}: ready", self.stats.name);
                self.stats.ready.store(true, Ordering::SeqCst);
                Poll::Ready(res)
            }
            Poll::Pending => Poll::Pending,
        }
    }
}

struct WakeObserver {
    inner: Waker,
    woken: Arc<AtomicUsize>,
    name: Arc<str>,
}

impl ArcWake for WakeObserver {
    fn wake_by_ref(arc_self: &Arc<Self>) {
        println!("{}: wake", arc_self.name);
        arc_self.woken.fetch_add(1, Ordering::SeqCst);
        arc_self.inner.wake_by_ref();
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_assert_pending_happy() {
        futures::future::pending::<()>().assert_pending().await;
    }

    #[tokio::test]
    #[should_panic(expected = "not pending")]
    async fn test_assert_pending_fail() {
        futures::future::ready(()).assert_pending().await;
    }

    #[tokio::test]
    async fn test_poll_timeout_happy() {
        futures::future::ready(()).poll_timeout().await;
    }

    #[tokio::test]
    #[should_panic(expected = "timeout")]
    async fn test_poll_timeout_fail() {
        futures::future::pending::<()>().poll_timeout().await;
    }

    #[tokio::test]
    async fn test_assert_pending_polls_once() {
        let mut fut = FutureObserver::new(futures::future::pending::<()>(), "pending");
        assert_eq!(fut.stats().polled(), 0);
        fut.assert_pending().await;
        assert_eq!(fut.stats().polled(), 1);
    }
}
