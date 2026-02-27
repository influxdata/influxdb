use std::time::Duration;

use async_trait::async_trait;
use std::future::Future;
use tokio::time::error::Elapsed;

/// An extension trait to add wall-clock execution timeouts to a future running
/// in a tokio runtime.
///
/// Uses [tokio::time::timeout] internally to apply the timeout.
#[async_trait]
pub trait FutureTimeout: Future + Sized {
    /// Wraps `self` returning the result, or panicking if the future hasn't
    /// completed after `d` length of time.
    ///
    /// # Safety
    ///
    /// This method panics if `d` elapses before the task rejoins.
    #[track_caller]
    async fn with_timeout_panic(mut self, d: Duration) -> <Self as Future>::Output {
        self.with_timeout(d)
            .await
            .expect("timeout waiting for task to join")
    }

    /// Wraps `self` returning the result, or an error if the future hasn't
    /// completed after `d` length of time.
    #[track_caller]
    async fn with_timeout(mut self, d: Duration) -> Result<<Self as Future>::Output, Elapsed> {
        tokio::time::timeout(d, self).await
    }
}

impl<F> FutureTimeout for F where F: Future {}

#[cfg(test)]
mod tests {
    use tokio::sync::oneshot;

    use super::*;

    #[tokio::test]
    #[should_panic(expected = "timeout")]
    async fn test_exceeded_panic() {
        let (_tx, rx) = oneshot::channel::<()>();
        let task = tokio::spawn(rx);

        let _ = task.with_timeout_panic(Duration::from_millis(1)).await;
    }

    #[tokio::test]
    async fn test_exceeded() {
        let (_tx, rx) = oneshot::channel::<()>();
        let task = tokio::spawn(rx);

        let _ = task
            .with_timeout(Duration::from_millis(1))
            .await
            .expect_err("should time out");
    }
}
