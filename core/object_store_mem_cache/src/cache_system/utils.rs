//! A few helpers to build cache-related code.
use std::{
    future::Future,
    panic::{AssertUnwindSafe, resume_unwind},
    pin::Pin,
    sync::Arc,
    task::{Context, Poll},
};

use futures::{FutureExt, Stream, StreamExt};
use tokio::task::JoinHandle;

use super::DynError;

/// Holds a tokio task that is aborted when the handle is dropped.
///
/// [`JoinError`]s are handled like:
/// - **panic:** wrapped via [`CatchUnwindDynErrorExt`]
/// - **runtime lost:** converted into [`DynError`]
///
///
/// [`JoinError`]: tokio::task::JoinError
pub struct TokioTask<T>(JoinHandle<Result<T, DynError>>)
where
    T: Send + 'static;

impl<T> TokioTask<T>
where
    T: Send + 'static,
{
    /// Span new task on current runtime.
    pub fn spawn<F>(future: F) -> Self
    where
        F: Future<Output = Result<T, DynError>> + Send + 'static,
    {
        Self(tokio::spawn(future.catch_unwind_dyn_error()))
    }
}

impl<T> std::fmt::Debug for TokioTask<T>
where
    T: Send + 'static,
{
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("TokioTask")
            .field("finished", &self.0.is_finished())
            .finish()
    }
}

impl<T> Drop for TokioTask<T>
where
    T: Send + 'static,
{
    fn drop(&mut self) {
        self.0.abort();
    }
}

impl<T> Future for TokioTask<T>
where
    T: Send + 'static,
{
    type Output = Result<T, DynError>;

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        Poll::Ready(match std::task::ready!(self.0.poll_unpin(cx)) {
            Ok(v) => v,
            Err(e) if e.is_cancelled() => Err(str_err("Runtime was shut down")),
            Err(e) => resume_unwind(e.into_panic()),
        })
    }
}

/// Extension trait to [catch unwind] -- during a panic -- and convert the message into a [`DynError`].
///
/// This automatically [assumes unwind safety](AssertUnwindSafe).
///
/// The [`DynError`] can be [cast down] to [`PanicError`].
///
///
// Note: Linking to `dyn Error` doesn't really work via intradoc links, see
// https://github.com/rust-lang/rust/issues/74563
/// [cast down]: https://doc.rust-lang.org/std/error/trait.Error.html#method.downcast_ref-2
/// [catch unwind]: std::panic::catch_unwind
pub trait CatchUnwindDynErrorExt {
    type Output;

    /// Catch unwind and convert panic message into a [`DynError`].
    fn catch_unwind_dyn_error(self) -> impl Future<Output = Result<Self::Output, DynError>> + Send;
}

impl<F, T> CatchUnwindDynErrorExt for F
where
    F: Future<Output = Result<T, DynError>> + Send,
{
    type Output = T;

    async fn catch_unwind_dyn_error(self) -> Result<Self::Output, DynError> {
        match AssertUnwindSafe(self).catch_unwind().await {
            Ok(res) => res,
            Err(e) => Err(PanicError::dyn_error_from_panic(e)),
        }
    }
}

/// Same as [`CatchUnwindDynErrorExt`] but for streams.
pub trait CatchUnwindDynErrorStreamExt {
    type Output;

    /// Catch unwind and convert panic message into a [`DynError`].
    fn catch_unwind_dyn_error(self) -> impl Stream<Item = Result<Self::Output, DynError>> + Send;
}

impl<S, T> CatchUnwindDynErrorStreamExt for S
where
    S: Stream<Item = Result<T, DynError>> + Send,
{
    type Output = T;

    fn catch_unwind_dyn_error(self) -> impl Stream<Item = Result<Self::Output, DynError>> + Send {
        AssertUnwindSafe(self).catch_unwind().map(|res| match res {
            Ok(res) => res,
            Err(e) => Err(PanicError::dyn_error_from_panic(e)),
        })
    }
}

/// Error produced by [`CatchUnwindDynErrorExt`].
#[derive(Debug)]
pub struct PanicError {
    message: StringError,
}

impl PanicError {
    /// Panic message.
    pub fn message(&self) -> &str {
        self.message.inner()
    }

    fn dyn_error_from_panic(e: Box<dyn std::any::Any + Send>) -> DynError {
        let msg = if let Some(s) = e.downcast_ref::<String>() {
            s.clone()
        } else if let Some(s) = e.downcast_ref::<&str>() {
            (*s).to_owned()
        } else {
            "<unknown>".to_owned()
        };

        Arc::new(Self {
            message: StringError(msg),
        })
    }
}

impl std::fmt::Display for PanicError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "panic: {}", self.message)
    }
}

impl std::error::Error for PanicError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        Some(&self.message)
    }
}

/// Create a [`DynError`] from a string.
///
/// The contained error can be [cast down] to [`StringError`].
///
///
// Note: Linking to `dyn Error` doesn't really work via intradoc links, see
// https://github.com/rust-lang/rust/issues/74563
/// [cast down]: https://doc.rust-lang.org/std/error/trait.Error.html#method.downcast_ref-2
pub fn str_err(s: &str) -> DynError {
    Arc::new(StringError(s.to_owned()))
}

/// Error produced by [`str_err`].
#[derive(Debug)]
pub struct StringError(String);

impl StringError {
    /// Get string.
    pub fn inner(&self) -> &str {
        &self.0
    }
}

impl std::fmt::Display for StringError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}

impl std::error::Error for StringError {}

#[cfg(test)]
mod tests {
    use std::{error::Error, time::Duration};

    use futures::FutureExt as _;
    use futures_concurrency::future::FutureExt as _;
    use futures_test_utils::{AssertFutureExt, FutureObserver};
    use tokio::sync::Barrier;

    use crate::cache_system::test_utils::assert_converge_eq;

    use super::*;

    #[tokio::test]
    async fn test_tokio_task_runs_in_background() {
        let barrier = Arc::new(Barrier::new(2));
        let barrier_captured = Arc::clone(&barrier);
        let _handle = TokioTask::spawn(async move {
            barrier_captured.wait().await;
            Ok(())
        });

        // this would timeout if the task is just an ordinary future
        barrier.wait().boxed().poll_timeout().await;
    }

    #[tokio::test]
    async fn test_tokio_task_dbg() {
        let barrier = Arc::new(Barrier::new(2));
        let barrier_captured = Arc::clone(&barrier);
        let mut handle = TokioTask::spawn(async move {
            barrier_captured.wait().await;
            Ok(())
        });

        assert_eq!(format!("{handle:?}"), "TokioTask { finished: false }",);

        let (_, res) = barrier.wait().join(&mut handle).await;
        res.unwrap();

        assert_eq!(format!("{handle:?}"), "TokioTask { finished: true }",);
    }

    #[tokio::test]
    async fn test_tokio_task_abort() {
        let barrier = Arc::new(Barrier::new(2));
        let barrier_captured = Arc::clone(&barrier);
        let handle = TokioTask::spawn(async move {
            barrier_captured.wait().await;
            Ok(())
        });
        drop(handle);
        assert_converge_eq(|| Arc::strong_count(&barrier), 1).await;
    }

    #[tokio::test]
    async fn test_tokio_task_panic() {
        let handle = TokioTask::<()>::spawn(async move { panic!("foo") });

        assert_eq!(handle.await.unwrap_err().to_string(), "panic: foo",);
    }

    #[test]
    #[expect(clippy::async_yields_async)]
    fn test_tokio_task_runtime_shutdown() {
        let rt_1 = tokio::runtime::Builder::new_current_thread()
            .enable_time()
            .build()
            .unwrap();
        let barrier = Arc::new(Barrier::new(2));
        let barrier_captured = Arc::clone(&barrier);
        let mut fut = rt_1
            .block_on(async move {
                TokioTask::<()>::spawn(async move {
                    barrier_captured.wait().await;
                    panic!("foo")
                })
            })
            .boxed();
        rt_1.block_on(async {
            fut.assert_pending().await;
        });

        rt_1.shutdown_timeout(Duration::from_secs(1));

        let rt_2 = tokio::runtime::Builder::new_current_thread()
            .build()
            .unwrap();
        let err = rt_2
            .block_on(async move {
                let (res, _) = tokio::join!(fut, barrier.wait());
                res
            })
            .unwrap_err();

        assert_eq!(err.to_string(), "Runtime was shut down");

        rt_2.shutdown_timeout(Duration::from_secs(1));
    }

    /// Ensure that we don't wake every single consumer for every single IO interaction.
    #[tokio::test]
    async fn test_tokio_task_perfect_waking() {
        const N_IO_STEPS: usize = 20;
        let barriers = Arc::new((0..N_IO_STEPS).map(|_| Barrier::new(2)).collect::<Vec<_>>());
        let barriers_captured = Arc::clone(&barriers);
        let mut fut = TokioTask::spawn(async move {
            for barrier in barriers_captured.iter() {
                barrier.wait().await;
            }
            Ok(())
        })
        .shared();
        fut.assert_pending().await;

        let fut_io = async {
            for barrier in barriers.iter() {
                barrier.wait().await;
            }
        }
        .boxed();

        let fut = FutureObserver::new(fut, "fut");
        let stats = fut.stats();
        let fut_io = FutureObserver::new(fut_io, "fut_io");

        // Don't use `tokio::select!` or `tokio::join!` because they poll too often. What the H?!
        // So we use this lovely crate instead: https://crates.io/crates/futures-concurrency
        let (res, ()) = fut.join(fut_io).await;
        res.unwrap();

        // polled once for to determine that all of them are pending, and then once when we finally got
        // the result
        assert_eq!(stats.polled(), 2);

        // it seems that we wake during the final poll (which is unnecessary, because we are about to return `Ready`).
        // Not perfect, but "good enough".
        assert_eq!(stats.woken(), 2);
    }

    #[tokio::test]
    async fn test_catch_unwind_dyn_error_payload_handling() {
        assert_eq!(
            infer_fut_type(async move { panic!("foo") })
                .catch_unwind_dyn_error()
                .await
                .unwrap_err()
                .to_string(),
            "panic: foo",
        );

        let s = String::from("foo");
        assert_eq!(
            infer_fut_type(async move { panic!("{s}") })
                .catch_unwind_dyn_error()
                .await
                .unwrap_err()
                .to_string(),
            "panic: foo",
        );

        assert_eq!(
            infer_fut_type(async move { std::panic::panic_any(1u8) })
                .catch_unwind_dyn_error()
                .await
                .unwrap_err()
                .to_string(),
            "panic: <unknown>",
        );
    }

    #[tokio::test]
    async fn test_catch_unwind_dyn_error_types() {
        let e = infer_fut_type(async move { panic!("foo") })
            .catch_unwind_dyn_error()
            .await
            .unwrap_err();
        let e = e.downcast_ref::<PanicError>().unwrap();

        let source = e.source().unwrap();
        let source = source.downcast_ref::<StringError>().unwrap();
        assert_eq!(source.to_string(), "foo");
        assert_eq!(source.inner(), "foo");
    }

    /// Helps the compiler to infer certain [`Future`] types, esp. when the future only contains a single panic
    /// statement.
    fn infer_fut_type<F>(fut: F) -> F
    where
        F: Future<Output = Result<(), DynError>>,
    {
        fut
    }
}
