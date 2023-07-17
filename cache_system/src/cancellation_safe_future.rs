use std::{
    future::Future,
    pin::Pin,
    sync::Arc,
    task::{Context, Poll},
};

use futures::future::BoxFuture;
use parking_lot::Mutex;
use tokio::task::JoinHandle;

/// Receiver for [`CancellationSafeFuture`] join handles if the future was rescued from cancellation.
///
/// `T` is the [output type](Future::Output) of the wrapped future.
#[derive(Debug, Default, Clone)]
pub struct CancellationSafeFutureReceiver<T> {
    inner: Arc<ReceiverInner<T>>,
}

#[derive(Debug, Default)]
struct ReceiverInner<T> {
    slot: Mutex<Option<JoinHandle<T>>>,
}

impl<T> Drop for ReceiverInner<T> {
    fn drop(&mut self) {
        let handle = self.slot.lock();
        if let Some(handle) = handle.as_ref() {
            handle.abort();
        }
    }
}

/// Wrapper around a future that cannot be cancelled.
///
/// When the future is dropped/cancelled, we'll spawn a tokio task to _rescue_ it.
pub struct CancellationSafeFuture<F>
where
    F: Future + Send + 'static,
    F::Output: Send,
{
    /// Mark if the inner future finished. If not, we must spawn a helper task on drop.
    done: bool,

    /// Inner future.
    ///
    /// Wrapped in an `Option` so we can extract it during drop. Inside that option however we also need a pinned
    /// box because once this wrapper is polled, it will be pinned in memory -- even during drop. Now the inner
    /// future does not necessarily implement `Unpin`, so we need a heap allocation to pin it in memory even when we
    /// move it out of this option.
    inner: Option<BoxFuture<'static, F::Output>>,

    /// Where to store the join handle on drop.
    receiver: CancellationSafeFutureReceiver<F::Output>,
}

impl<F> Drop for CancellationSafeFuture<F>
where
    F: Future + Send + 'static,
    F::Output: Send,
{
    fn drop(&mut self) {
        if !self.done {
            // acquire lock BEFORE checking the Arc
            let mut receiver = self.receiver.inner.slot.lock();
            assert!(receiver.is_none());

            // The Mutex is owned by the Arc and cannot be moved out of it. So after we acquired the lock we can safely
            // check if any external party still has access to the receiver state. If not, we assume there is no
            // interest in this future at all (e.g. during shutdown) and will NOT spawn it.
            if Arc::strong_count(&self.receiver.inner) > 1 {
                let inner = self.inner.take().expect("Double-drop?");
                let handle = tokio::task::spawn(inner);
                *receiver = Some(handle);
            }
        }
    }
}

impl<F> CancellationSafeFuture<F>
where
    F: Future + Send,
    F::Output: Send,
{
    /// Create new future that is protected from cancellation.
    ///
    /// If [`CancellationSafeFuture`] is cancelled (i.e. dropped) and there is still some external receiver of the state
    /// left, than we will drive the payload (`f`) to completion. Otherwise `f` will be cancelled.
    pub fn new(fut: F, receiver: CancellationSafeFutureReceiver<F::Output>) -> Self {
        Self {
            done: false,
            inner: Some(Box::pin(fut)),
            receiver,
        }
    }
}

impl<F> Future for CancellationSafeFuture<F>
where
    F: Future + Send,
    F::Output: Send,
{
    type Output = F::Output;

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        assert!(!self.done, "Polling future that already returned");

        match self.inner.as_mut().expect("not dropped").as_mut().poll(cx) {
            Poll::Ready(res) => {
                self.done = true;
                Poll::Ready(res)
            }
            Poll::Pending => Poll::Pending,
        }
    }
}

#[cfg(test)]
mod tests {
    use std::{
        sync::atomic::{AtomicBool, Ordering},
        time::Duration,
    };

    use tokio::sync::Barrier;

    use super::*;

    #[tokio::test]
    async fn test_happy_path() {
        let done = Arc::new(AtomicBool::new(false));
        let done_captured = Arc::clone(&done);

        let receiver = Default::default();
        let fut = CancellationSafeFuture::new(
            async move {
                done_captured.store(true, Ordering::SeqCst);
            },
            receiver,
        );

        fut.await;

        assert!(done.load(Ordering::SeqCst));
    }

    #[tokio::test]
    async fn test_cancel_future() {
        let done = Arc::new(Barrier::new(2));
        let done_captured = Arc::clone(&done);

        let receiver = CancellationSafeFutureReceiver::default();
        let fut = CancellationSafeFuture::new(
            async move {
                done_captured.wait().await;
            },
            receiver.clone(),
        );

        drop(fut);

        tokio::time::timeout(Duration::from_secs(5), done.wait())
            .await
            .unwrap();
    }

    #[tokio::test]
    async fn test_receiver_gone() {
        let done = Arc::new(Barrier::new(2));
        let done_captured = Arc::clone(&done);

        let receiver = Default::default();
        let fut = CancellationSafeFuture::new(
            async move {
                done_captured.wait().await;
            },
            receiver,
        );

        drop(fut);

        assert_eq!(Arc::strong_count(&done), 1);
    }
}
