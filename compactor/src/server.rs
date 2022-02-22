//! Compactor server entrypoint.

use std::sync::Arc;

use self::{grpc::GrpcDelegate, http::HttpDelegate};
use crate::handler::CompactorHandler;
use std::fmt::Debug;

pub mod grpc;
pub mod http;

/// The [`CompactorServer`] manages the lifecycle and contains all state for a
/// `compactor` server instance.
#[derive(Debug, Default)]
pub struct CompactorServer<C: CompactorHandler> {
    metrics: Arc<metric::Registry>,

    http: HttpDelegate<C>,
    grpc: GrpcDelegate<C>,

    handler: Arc<C>,
}

impl<C: CompactorHandler> CompactorServer<C> {
    /// Initialise a new [`CompactorServer`] using the provided HTTP and gRPC
    /// handlers.
    pub fn new(
        metrics: Arc<metric::Registry>,
        http: HttpDelegate<C>,
        grpc: GrpcDelegate<C>,
        handler: Arc<C>,
    ) -> Self {
        Self {
            metrics,
            http,
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

    /// Get a reference to the router http delegate.
    pub fn http(&self) -> &HttpDelegate<C> {
        &self.http
    }

    /// Get a reference to the router grpc delegate.
    pub fn grpc(&self) -> &GrpcDelegate<C> {
        &self.grpc
    }
}
