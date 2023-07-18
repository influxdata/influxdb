use std::{future::Future, sync::Arc, time::Duration};

use async_trait::async_trait;
use futures::FutureExt;
use tokio::{sync::Barrier, task::JoinHandle};

#[async_trait]
pub trait EnsurePendingExt {
    type Out;

    /// Ensure that the future is pending. In the pending case, try to pass the given barrier. Afterwards await the future again.
    ///
    /// This is helpful to ensure a future is in a pending state before continuing with the test setup.
    async fn ensure_pending(self, barrier: Arc<Barrier>) -> Self::Out;
}

#[async_trait]
impl<F> EnsurePendingExt for F
where
    F: Future + Send + Unpin,
{
    type Out = F::Output;

    async fn ensure_pending(self, barrier: Arc<Barrier>) -> Self::Out {
        let mut fut = self.fuse();
        futures::select_biased! {
            _ = fut => panic!("fut should be pending"),
            _ = barrier.wait().fuse() => (),
        }

        fut.await
    }
}

#[async_trait]
pub trait AbortAndWaitExt {
    /// Abort handle and wait for completion.
    ///
    /// Note that this is NOT just a "wait with timeout or panic". This extension is specific to [`JoinHandle`] and will:
    ///
    /// 1. Call [`JoinHandle::abort`].
    /// 2. Await the [`JoinHandle`] with a timeout (or panic if the timeout is reached).
    /// 3. Check that the handle returned a [`JoinError`] that signals that the tracked task was indeed cancelled and
    ///    didn't exit otherwise (either by finishing or by panicking).
    async fn abort_and_wait(self);
}

#[async_trait]
impl<T> AbortAndWaitExt for JoinHandle<T>
where
    T: std::fmt::Debug + Send,
{
    async fn abort_and_wait(mut self) {
        self.abort();

        let join_err = tokio::time::timeout(Duration::from_secs(1), self)
            .await
            .expect("no timeout")
            .expect_err("handle was aborted and therefore MUST fail");
        assert!(join_err.is_cancelled());
    }
}
