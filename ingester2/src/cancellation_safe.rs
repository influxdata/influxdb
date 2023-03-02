//! A helper [`Future`] decorator that provides cancellation safety.

use std::{
    pin::Pin,
    task::{Context, Poll},
};

use futures::Future;

/// A [`Future`] decorator that ensures the inner future is always run to
/// completion.
///
/// Normally when an incomplete future is dropped, it makes no further progress.
/// If a [`CancellationSafe`] instance is dropped and the inner future is not
/// complete, it is spawned as a detached task on the async runtime (with
/// [`tokio::spawn`]) and driven to completion.
///
/// If the inner future is polled to completion, [`CancellationSafe`] does
/// nothing.
///
/// # Example
///
/// ```ignore
/// // A future that should always complete
/// let fut = async { println!("always print me"); };
///
/// // Wrap the future that must always complete in the decorator
/// let wrapped = CancellationSafe::new(fut);
///
/// // Drop the now cancellation safe future
/// drop(wrapped);
///
/// // It'll print the message!
/// ```
#[derive(Debug)]
pub(crate) struct CancellationSafe<F>
where
    F: Future + Send + 'static,
    F::Output: Send + 'static,
{
    fut: Option<Pin<Box<F>>>,
}

impl<F> CancellationSafe<F>
where
    F: Future + Send,
    F::Output: Send + 'static,
{
    /// [`Box`] `F` and provide cancellation safety.
    pub(crate) fn new(fut: F) -> Self {
        Self {
            fut: Some(Box::pin(fut)),
        }
    }
}

impl<F> Future for CancellationSafe<F>
where
    F: Future + Send + 'static,
    F::Output: Send + 'static,
{
    type Output = F::Output;

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let res = self.fut.as_mut().unwrap().as_mut().poll(cx);

        if res.is_ready() {
            // There's no need to spawn an already complete future on drop.
            self.fut = None;
        }

        res
    }
}

impl<F> Drop for CancellationSafe<F>
where
    F: Future + Send + 'static,
    F::Output: Send + 'static,
{
    fn drop(&mut self) {
        if let Some(f) = self.fut.take() {
            tokio::task::spawn(f);
        }
    }
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use test_helpers::timeout::FutureTimeout;
    use tokio::sync::oneshot;

    use super::*;

    struct Sequencer {
        start: Option<oneshot::Sender<()>>,
        wait_for_complete: oneshot::Receiver<()>,
    }

    impl Sequencer {
        fn start(&mut self) {
            self.start
                .take()
                .unwrap()
                .send(())
                .expect("future not running");
        }

        async fn wait_for_completion(self) {
            self.wait_for_complete.await.expect("future not running")
        }
    }

    fn make_fut() -> (Sequencer, impl Future<Output = ()>) {
        let (start_tx, start_rx) = oneshot::channel();
        let (done_tx, done_rx) = oneshot::channel();

        let fut = async move {
            let _ = start_rx.await;
            let _ = done_tx.send(());
        };

        (
            Sequencer {
                start: Some(start_tx),
                wait_for_complete: done_rx,
            },
            fut,
        )
    }

    #[tokio::test]
    async fn test_no_cancel() {
        let (mut ctx, fut) = make_fut();

        let wrapped = CancellationSafe::new(fut);

        ctx.start();
        wrapped.await;
        ctx.wait_for_completion().await;
    }

    #[tokio::test]
    async fn test_cancelled() {
        let (mut ctx, fut) = make_fut();

        let wrapped = CancellationSafe::new(fut);

        drop(wrapped);
        ctx.start(); // inner send() does not error, because task is running

        // Doesn't deadlock because the task was spawned when "wrapped" dropped.
        ctx.wait_for_completion()
            .with_timeout_panic(Duration::from_secs(5))
            .await;
    }
}
