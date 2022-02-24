//! Querier server entrypoint.

use std::sync::Arc;

use crate::handler::QuerierHandler;
use std::fmt::Debug;

/// The [`QuerierServer`] manages the lifecycle and contains all state for a
/// `querier` server instance.
#[derive(Debug, Default)]
pub struct QuerierServer<C: QuerierHandler> {
    metrics: Arc<metric::Registry>,

    handler: Arc<C>,
}

impl<C: QuerierHandler> QuerierServer<C> {
    /// Initialise a new [`QuerierServer`] using the provided HTTP and gRPC
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
