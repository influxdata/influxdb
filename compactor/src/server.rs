//! Compactor server entrypoint.

use std::sync::Arc;

use crate::handler::CompactorHandler;
use std::fmt::Debug;

/// The [`CompactorServer`] manages the lifecycle and contains all state for a
/// `compactor` server instance.
#[derive(Debug, Default)]
pub struct CompactorServer<C: CompactorHandler> {
    metrics: Arc<metric::Registry>,

    handler: Arc<C>,
}

impl<C: CompactorHandler> CompactorServer<C> {
    /// Initialise a new [`CompactorServer`] using the provided HTTP and gRPC
    /// handlers.
    pub fn new(metrics: Arc<metric::Registry>, handler: Arc<C>) -> Self {
        Self { metrics, handler }
    }

    /// Return the [`metric::Registry`] used by the router.
    pub fn metric_registry(&self) -> Arc<metric::Registry> {
        Arc::clone(&self.metrics)
    }

    /// Join shutdown worker.
    pub async fn join(&self) {
        self.handler.join().await;
    }

    /// Shutdown background worker.
    pub fn shutdown(&self) {
        self.handler.shutdown();
    }
}
