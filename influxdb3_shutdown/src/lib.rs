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
use std::sync::Arc;

use observability_deps::tracing::info;
use parking_lot::Mutex;
use tokio::sync::oneshot;
use tokio_util::{sync::CancellationToken, task::TaskTracker};

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
#[derive(Debug)]
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
    /// not a dependency, or hanlding of the frontend shutdown is not necessary.
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
/// This implements [`Clone`] so that a component that obtains it can make copies as needed for
/// sub-components or tasks  that may be responsible for triggering a shutdown internally.
#[derive(Debug, Clone)]
pub struct ShutdownToken {
    token: CancellationToken,
    complete_tx: Arc<Mutex<Option<oneshot::Sender<()>>>>,
}

impl ShutdownToken {
    /// Create a new [`ShutdownToken`]
    fn new(token: CancellationToken, complete_tx: oneshot::Sender<()>) -> Self {
        Self {
            token,
            complete_tx: Arc::new(Mutex::new(Some(complete_tx))),
        }
    }

    /// Trigger application shutdown due to some unrecoverable state
    pub fn trigger_shutdown(&self) {
        self.token.cancel();
    }

    /// Future that completes when [`ShutdownManager`] that issued this token is shutdown
    pub async fn wait_for_shutdown(&self) {
        self.token.cancelled().await;
    }

    /// Signal back to the [`ShutdownManager`] that the component that owns this token is finished
    /// cleaning up and it is safe for the process to exit
    pub fn complete(&self) {
        if let Some(s) = self.complete_tx.lock().take() {
            let _ = s.send(());
        }
    }
}

#[cfg(test)]
mod tests {
    use std::{
        sync::atomic::{AtomicBool, Ordering},
        time::Duration,
    };

    use futures::FutureExt;
    use tokio_util::sync::CancellationToken;

    use crate::ShutdownManager;

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
                        token.complete();
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
}
