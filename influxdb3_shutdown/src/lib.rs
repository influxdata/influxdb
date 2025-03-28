use std::sync::Arc;

use observability_deps::tracing::{debug, info};
use parking_lot::Mutex;
use tokio::sync::oneshot;
use tokio_util::{sync::CancellationToken, task::TaskTracker};

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

#[cfg(windows)]
pub async fn wait_for_signal() {
    let _ = tokio::signal::ctrl_c().await;
    info!("Received SIGINT");
}

#[derive(Debug)]
pub struct ShutdownManager {
    frontend_shutdown: CancellationToken,
    backend_shutdown: CancellationToken,
    tasks: TaskTracker,
}

impl ShutdownManager {
    pub fn new(frontend_shutdown: CancellationToken) -> Self {
        Self {
            frontend_shutdown,
            backend_shutdown: CancellationToken::new(),
            tasks: TaskTracker::new(),
        }
    }

    pub fn new_testing() -> Self {
        Self {
            frontend_shutdown: CancellationToken::new(),
            backend_shutdown: CancellationToken::new(),
            tasks: TaskTracker::new(),
        }
    }

    /// Register a task that needs to perform work before the process may exit
    pub fn register(&self) -> ShutdownToken {
        let (tx, rx) = oneshot::channel();
        debug!("spawning task...");
        self.tasks.spawn(rx);
        debug!("spawning task... done.");
        ShutdownToken::new(self.backend_shutdown.clone(), tx)
    }

    /// Waits for registered tasks to complete before signaling shutdown to frontend
    pub async fn join(&self) {
        self.tasks.close();
        self.tasks.wait().await;
        self.frontend_shutdown.cancel();
    }

    pub fn shutdown(&self) {
        self.backend_shutdown.cancel();
    }
}

#[derive(Debug)]
pub struct ShutdownToken {
    token: CancellationToken,
    complete_tx: Arc<Mutex<Option<oneshot::Sender<()>>>>,
}

impl ShutdownToken {
    fn new(token: CancellationToken, complete_tx: oneshot::Sender<()>) -> Self {
        Self {
            token,
            complete_tx: Arc::new(Mutex::new(Some(complete_tx))),
        }
    }

    pub async fn wait_for_shutdown(&self) {
        self.token.cancelled().await;
    }

    pub fn complete(&self) {
        if let Some(s) = self.complete_tx.lock().take() {
            let _ = s.send(());
        }
    }
}
