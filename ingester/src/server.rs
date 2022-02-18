//! Ingester server entrypoint.

use std::sync::Arc;

use self::{grpc::GrpcDelegate, http::HttpDelegate};
use crate::handler::IngestHandler;
use std::fmt::Debug;

pub mod grpc;
pub mod http;

/// The [`IngesterServer`] manages the lifecycle and contains all state for a
/// `ingester` server instance.
#[derive(Debug, Default)]
pub struct IngesterServer<I: IngestHandler> {
    metrics: Arc<metric::Registry>,

    http: HttpDelegate<I>,
    grpc: GrpcDelegate<I>,

    handler: Arc<I>,
}

impl<I: IngestHandler> IngesterServer<I> {
    /// Initialise a new [`IngesterServer`] using the provided HTTP and gRPC
    /// handlers.
    pub fn new(
        metrics: Arc<metric::Registry>,
        http: HttpDelegate<I>,
        grpc: GrpcDelegate<I>,
        handler: Arc<I>,
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
}

impl<I: IngestHandler + Debug> IngesterServer<I> {
    /// Get a reference to the router http delegate.
    pub fn http(&self) -> &HttpDelegate<I> {
        &self.http
    }

    /// Get a reference to the router grpc delegate.
    pub fn grpc(&self) -> &GrpcDelegate<I> {
        &self.grpc
    }
}
