//! Compactor server entrypoint.

use self::grpc::GrpcDelegate;
use crate::handler::CompactorHandler;
use std::fmt::Debug;
use std::sync::Arc;

pub mod grpc;

/// The [`CompactorServer`] manages the lifecycle and contains all state for a
/// `compactor` server instance.
#[derive(Debug, Default)]
pub struct CompactorServer<C: CompactorHandler> {
    metrics: Arc<metric::Registry>,

    grpc: GrpcDelegate<C>,

    handler: Arc<C>,
}

impl<C: CompactorHandler> CompactorServer<C> {
    /// Initialise a new [`CompactorServer`] using the provided HTTP and gRPC
    /// handlers.
    pub fn new(metrics: Arc<metric::Registry>, grpc: GrpcDelegate<C>, handler: Arc<C>) -> Self {
        Self {
            metrics,
            grpc,
            handler,
        }
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

impl<I: CompactorHandler + Debug> CompactorServer<I> {
    /// Get a reference to the grpc delegate.
    pub fn grpc(&self) -> &GrpcDelegate<I> {
        &self.grpc
    }
}
