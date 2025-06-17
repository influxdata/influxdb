//! Manage application shutdown
//!
//! This crate provides a set of types for managing graceful application shutdown.
//!
//! # Coordinate shutdown with the [`ShutdownManager`] type
//!
//! The [`ShutdownManager`] is used to coordinate shutdown of the process in an ordered fashion.
//! When a shutdown is signaled externally, e.g., `ctrl+c`, or internally by some error state, then
//! there may be processes running on the backend that need to be gracefully stopped before the
//! HTTP/gRPC frontend. For example, if the WAL has writes buffered, it needs to flush the buffer to
//! object store and respond to the write request before the HTTP/gRPC frontend is taken down.
//!
//! Components can [`register`][ShutdownManager::register] to receive a [`ShutdownToken`], which can
//! be used to [`wait_for_shutdown`][ShutdownToken::wait_for_shutdown] to trigger component-specific
//! cleanup logic before signaling back via [`complete`][ShutdownToken::complete] to indicate that
//! shutdown can proceed.

use std::{sync::Arc, time::Duration};

use iox_time::TimeProvider;
use observability_deps::tracing::{info, warn};
use parking_lot::Mutex;
use tokio::{sync::oneshot, task::JoinError};
pub use tokio_util::sync::CancellationToken;
use tokio_util::task::TaskTracker;

/// Wait for a `SIGTERM` or `SIGINT` to stop the process on UNIX systems
#[cfg(unix)]
pub async fn wait_for_signal() {
    use tokio::signal::unix::{SignalKind, signal};

    let mut term = signal(SignalKind::terminate()).expect("failed to register signal handler");
    let mut int = signal(SignalKind::interrupt()).expect("failed to register signal handler");

    tokio::select! {
        _ = term.recv() => info!("Received SIGTERM"),
        _ = int.recv() => info!("Received SIGINT"),
    }
}

/// Wait for a `ctrl+c` to stop the process on Windows systems
#[cfg(windows)]
pub async fn wait_for_signal() {
    let _ = tokio::signal::ctrl_c().await;
    info!("Received SIGINT");
}

/// Manage application shutdown
///
/// This deries `Clone`, as the underlying `tokio` types can be shared via clone.
#[derive(Debug, Clone)]
pub struct ShutdownManager {
    frontend_shutdown: CancellationToken,
    backend_shutdown: CancellationToken,
    tasks: TaskTracker,
}

impl ShutdownManager {
    /// Create a [`ShutdownManager`]
    ///
    /// Accepts a [`CancellationToken`] which the `ShutdownManager` will signal cancellation to
    /// after the backend has cleanly shutdown.
    pub fn new(frontend_shutdown: CancellationToken) -> Self {
        Self {
            frontend_shutdown,
            backend_shutdown: CancellationToken::new(),
            tasks: TaskTracker::new(),
        }
    }

    /// Create a [`ShutdownManager`] for testing purposes
    ///
    /// This handles creation of the frontend [`CancellationToken`] for tests where `tokio-util` is
    /// not a dependency, or handling of the frontend shutdown is not necessary.
    pub fn new_testing() -> Self {
        Self {
            frontend_shutdown: CancellationToken::new(),
            backend_shutdown: CancellationToken::new(),
            tasks: TaskTracker::new(),
        }
    }

    /// Register a task that needs to perform work before the process may exit
    ///
    /// Provides a [`ShutdownToken`] which the caller is responsible for handling. The caller must
    /// invoke [`complete`][ShutdownToken::complete] in order for process shutdown to succeed.
    pub fn register(&self) -> ShutdownToken {
        let (tx, rx) = oneshot::channel();
        self.tasks.spawn(rx);
        ShutdownToken::new(self.backend_shutdown.clone(), tx)
    }

    /// Waits for registered tasks to complete before signaling shutdown to frontend
    ///
    /// The future returned will complete when all registered tasks have signaled completion via
    /// [`complete`][ShutdownToken::complete]
    pub async fn join(&self) {
        self.tasks.close();
        self.tasks.wait().await;
        self.frontend_shutdown.cancel();
    }

    /// Invoke application shutdown
    ///
    /// This will signal backend shutdown and wake the
    /// [`wait_for_shutdown`][ShutdownToken::wait_for_shutdown] future so that registered tasks can
    /// clean up before indicating completion.
    pub fn shutdown(&self) {
        self.backend_shutdown.cancel();
    }
}

/// A token that a component can obtain via [`register`][ShutdownManager::register]
///
/// This does not implement `Clone` because there should only be a single instance of a given
/// `ShutdownToken`. If you just need a copy of the `CancellationToken` for invoking shutdown, use
/// [`ShutdownToken::clone_cancellation_token`].
#[derive(Debug)]
pub struct ShutdownToken {
    token: CancellationToken,
    complete_tx: Mutex<Option<oneshot::Sender<()>>>,
}

impl ShutdownToken {
    /// Create a new [`ShutdownToken`]
    fn new(token: CancellationToken, complete_tx: oneshot::Sender<()>) -> Self {
        Self {
            token,
            complete_tx: Mutex::new(Some(complete_tx)),
        }
    }

    /// Trigger application shutdown due to some unrecoverable state
    pub fn trigger_shutdown(&self) {
        self.token.cancel();
    }

    /// Get a clone of the cancellation token for triggering shutdown
    pub fn clone_cancellation_token(&self) -> CancellationToken {
        self.token.clone()
    }

    /// Future that completes when [`ShutdownManager`] that issued this token is shutdown
    pub async fn wait_for_shutdown(&self) {
        self.token.cancelled().await;
    }

    /// Signal back to the [`ShutdownManager`] that the component that owns this token is finished
    /// cleaning up and it is safe for the process to exit
    ///
    /// # Implementation Note
    ///
    /// It is not required that registered components invoke this method, as the `ShutdownToken`
    /// type invokes this method on `Drop`.
    pub fn complete(&self) {
        if let Some(s) = self.complete_tx.lock().take() {
            let _ = s.send(());
        }
    }
}

/// `ShutdownToken` implements `Drop` such that the completion signal is guaranteed to be sent
/// to the `ShutdownManager`. This will prevent application hang on shutdown in the event that a
/// registered component fails before signaling completion.
impl Drop for ShutdownToken {
    fn drop(&mut self) {
        self.complete();
    }
}

#[derive(Debug, thiserror::Error)]
pub enum AbortableTaskRunnerError {
    #[error("aborted the background task")]
    Aborted,

    #[error("background task panicked {0}")]
    Panicked(String),

    #[error("error when running background task {0}")]
    Other(JoinError),
}

/// This type aborts a long running future immediately (based on the `check_interval`) by
/// running it in the background and checking if token has received a cancellation signal.
#[derive(Debug)]
pub struct AbortableTaskRunner<F> {
    task: F,
    time_provider: Arc<dyn TimeProvider>,
    check_interval: Duration,
    cancellation_token: CancellationToken,
}

impl<F> AbortableTaskRunner<F>
where
    F: Future + Send + 'static,
    F::Output: Send + 'static,
{
    pub fn new(
        task: F,
        time_provider: Arc<dyn TimeProvider>,
        check_interval: Duration,
        token: CancellationToken,
    ) -> Self {
        Self {
            task,
            time_provider,
            check_interval,
            cancellation_token: token,
        }
    }

    /// The return conditions for this method are,
    ///   - the original task [`Self::task`] output if it exits cleanly
    ///   - the task itself runs into an error
    ///       - the task gets aborted, returns [`AbortableTaskRunnerError::Aborted`].
    ///         it can be argued that this is not an error but useful to distinguish this exit
    ///         condition for the call site if it chooses to handle it (for logging etc.)
    ///       - if it panics this method returns [`AbortableTaskRunnerError::Panicked`].
    ///       - all other failure paths (like runtime shutdown) is not really an error that needs
    ///         to be handled, but it is returned as `Other` [`AbortableTaskRunnerError::Other`]
    ///         variant
    pub async fn run(self) -> Result<F::Output, AbortableTaskRunnerError> {
        let worker = tokio::spawn(self.task);
        let checker = tokio::spawn({
            let handle = worker.abort_handle();
            async move {
                loop {
                    self.time_provider.sleep(self.check_interval).await;
                    // at this point we only care about cancelled branch, if the work itself is
                    // finished then the select! below will choose the other path
                    if self.cancellation_token.is_cancelled() {
                        warn!("aborting the running process");
                        handle.abort();
                        break;
                    }
                }
            }
        });
        let run_result = tokio::select! {
            _ = checker => {
                return Err(AbortableTaskRunnerError::Aborted);
            }
            result = worker => {
                result.map_err(|join_err| {
                    if join_err.is_panic() {
                        // bubbling up this panic here means it'll keep it's original panic behaviour
                        // i.e if this task were to panic (without `tokio::spawn`), this would have
                        //     resulted in bubbling up the panic through the stack
                        AbortableTaskRunnerError::Panicked(join_err.to_string())

                    } else if join_err.is_cancelled() {
                        // if it's cancelled already then before the "checker" branch got a chance
                        // to return, the worker is marked as "cancelled" because of "abort()",
                        // safe to return `Aborted`
                        AbortableTaskRunnerError::Aborted

                    } else {
                        AbortableTaskRunnerError::Other(join_err)
                    }
                })?
            }
        };
        Ok(run_result)
    }

    /// This method runs in background, but checks if the process is shutting down by checking the
    /// cancellation token and exiting. But this method more importantly does not return or
    /// indicate why it exited which is compatible with how usually `tokio::spawn` is used to run
    /// task in the background
    pub fn run_in_background(self) {
        let worker = tokio::spawn(self.task);
        let checker = tokio::spawn({
            let handle = worker.abort_handle();
            async move {
                loop {
                    self.time_provider.sleep(self.check_interval).await;
                    // at this point we only care about cancelled branch, if the work itself is
                    // finished then the select! below will choose the other path
                    if self.cancellation_token.is_cancelled() {
                        warn!("aborting the running background process");
                        handle.abort();
                        break;
                    }
                }
            }
        });

        tokio::spawn(async move {
            tokio::select! {
                _ = checker => {}
                _ = worker => {}
            };
        });
    }
}

#[cfg(test)]
mod tests {
    use std::{
        sync::{
            Arc,
            atomic::{AtomicBool, AtomicU64, Ordering},
        },
        time::Duration,
    };

    use futures::FutureExt;
    use iox_time::{MockProvider, Time, TimeProvider};
    use observability_deps::tracing::{error, info};
    use tokio_util::sync::CancellationToken;

    use crate::{AbortableTaskRunner, AbortableTaskRunnerError, ShutdownManager};

    #[tokio::test]
    async fn test_shutdown_order() {
        let frontend_token = CancellationToken::new();
        let shutdown_manager = ShutdownManager::new(frontend_token.clone());

        static CLEAN: AtomicBool = AtomicBool::new(false);

        let token = shutdown_manager.register();
        tokio::spawn(async move {
            loop {
                futures::select! {
                    _ = token.wait_for_shutdown().fuse() => {
                        CLEAN.store(true, Ordering::SeqCst);
                        break;
                    }
                    _ = tokio::time::sleep(Duration::from_millis(10)).fuse() => {
                        // sleeping... 😴
                    }
                }
            }
        });

        shutdown_manager.shutdown();
        shutdown_manager.join().await;
        assert!(
            CLEAN.load(Ordering::SeqCst),
            "backend shutdown did not compelte"
        );
        assert!(
            frontend_token.is_cancelled(),
            "frontend shutdown was not triggered"
        );
    }

    #[test_log::test(tokio::test)]
    async fn test_abortable_background_task_runner() {
        let timer = Arc::new(MockProvider::new(Time::from_timestamp_nanos(0)));
        let worker_timer = Arc::clone(&timer);
        let cancel_timer = Arc::clone(&timer);
        let fut = async move {
            let mut count = 0;
            loop {
                count += 1;
                if count > 1_000_000_000 {
                    break;
                }
                worker_timer.sleep(Duration::from_millis(10)).await;
            }
            count
        };

        let token = CancellationToken::new();
        let token_clone = token.clone();

        let runner = AbortableTaskRunner::new(
            fut,
            Arc::clone(&timer) as _,
            Duration::from_millis(1),
            token,
        );

        tokio::spawn(async move {
            // wait for a millisecond and cancel
            cancel_timer.sleep(Duration::from_millis(1)).await;
            token_clone.cancel();
        });

        tokio::spawn(async move {
            loop {
                timer.inc(Duration::from_millis(10));
                tokio::time::sleep(Duration::from_millis(20)).await;
            }
        });
        let res = runner.run().await.unwrap_err();
        error!(?res, "error after abort");
        assert!(matches!(res, AbortableTaskRunnerError::Aborted));
    }

    #[test_log::test(tokio::test)]
    async fn test_abortable_background_task_runner_runs_forever_should_abort() {
        let timer = Arc::new(MockProvider::new(Time::from_timestamp_nanos(0)));
        let worker_timer = Arc::clone(&timer);
        let cancel_timer = Arc::clone(&timer);
        let atomic_counter = Arc::new(AtomicU64::new(0));
        let cloned = Arc::clone(&atomic_counter);
        let fut = async move {
            loop {
                let current = atomic_counter.fetch_add(1, Ordering::SeqCst);
                if current > 1_000_000_000 {
                    break;
                }
                worker_timer.sleep(Duration::from_millis(10)).await;
            }
            atomic_counter.load(Ordering::SeqCst)
        };

        let token = CancellationToken::new();
        let token_clone = token.clone();

        let runner = AbortableTaskRunner::new(
            fut,
            Arc::clone(&timer) as _,
            Duration::from_millis(1),
            token,
        );

        tokio::spawn(async move {
            // wait for a millisecond and cancel
            cancel_timer.sleep(Duration::from_millis(1)).await;
            token_clone.cancel();
        });

        tokio::spawn(async move {
            loop {
                timer.inc(Duration::from_millis(10));
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        });

        // although it's set to run forever because of cancelling the token this should stop
        // running forever and break
        runner.run_in_background();

        tokio::time::sleep(Duration::from_millis(500)).await;
        let current_1 = cloned.load(Ordering::SeqCst);
        info!(current_1, "current val");
        assert_ne!(current_1, 1_000_000_000);

        tokio::time::sleep(Duration::from_millis(500)).await;
        let current_2 = cloned.load(Ordering::SeqCst);
        info!(current_2, "current val");
        assert_ne!(current_2, 1_000_000_000);
        assert_eq!(current_1, current_2);
    }
}
