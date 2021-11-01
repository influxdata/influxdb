use std::future::Future;
use std::sync::Arc;

use futures::future::{BoxFuture, Shared};
use futures::{FutureExt, TryFutureExt};
use parking_lot::Mutex;
use tokio::task::JoinError;

use crate::db::ArcDb;
use crate::Db;
use lifecycle::LifecyclePolicy;
use observability_deps::tracing::{info, warn};
use tokio_util::sync::CancellationToken;

/// A lifecycle worker manages a background task that drives a [`LifecyclePolicy`]
#[derive(Debug)]
pub struct LifecycleWorker {
    /// Future that resolves when the background worker exits
    join: Shared<BoxFuture<'static, Result<(), Arc<JoinError>>>>,

    /// Shared worker state
    state: Arc<WorkerState>,
}

#[derive(Debug)]
struct WorkerState {
    policy: Mutex<LifecyclePolicy<ArcDb>>,

    shutdown: CancellationToken,
}

impl LifecycleWorker {
    /// Creates a new `LifecycleWorker`
    ///
    /// The worker starts with persistence suppressed, persistence must be enabled
    /// by a call to [`LifecycleWorker::unsuppress_persistence`]
    pub fn new(db: Arc<Db>) -> Self {
        let db = ArcDb::new(db);
        let shutdown = CancellationToken::new();

        let policy = LifecyclePolicy::new_suppress_persistence(db);

        let state = Arc::new(WorkerState {
            policy: Mutex::new(policy),
            shutdown,
        });

        let join = tokio::spawn(background_worker(Arc::clone(&state)))
            .map_err(Arc::new)
            .boxed()
            .shared();

        Self { join, state }
    }

    /// Stop suppressing persistence and allow it if the database rules allow it.
    pub fn unsuppress_persistence(&self) {
        self.state.policy.lock().unsuppress_persistence()
    }

    /// Triggers shutdown of this `WriteBufferConsumer`
    pub fn shutdown(&self) {
        self.state.shutdown.cancel()
    }

    /// Waits for the background worker of this `Database` to exit
    pub fn join(&self) -> impl Future<Output = Result<(), Arc<JoinError>>> {
        self.join.clone()
    }
}

impl Drop for LifecycleWorker {
    fn drop(&mut self) {
        if !self.state.shutdown.is_cancelled() {
            warn!("lifecycle worker dropped without calling shutdown()");
            self.state.shutdown.cancel();
        }

        if self.join.clone().now_or_never().is_none() {
            warn!("lifecycle worker dropped without waiting for worker termination");
        }
    }
}

async fn background_worker(state: Arc<WorkerState>) {
    loop {
        let fut = state.policy.lock().check_for_work();
        tokio::select! {
            _ = fut => {},
            _ = state.shutdown.cancelled() => {
                info!("lifecycle worker shutting down");
                break
            }
        }
    }
}
