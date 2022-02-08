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
}

impl<I: IngestHandler> IngesterServer<I> {
    /// Initialise a new [`IngesterServer`] using the provided HTTP and gRPC
    /// handlers.
    pub fn new(
        metrics: Arc<metric::Registry>,
        http: HttpDelegate<I>,
        grpc: GrpcDelegate<I>,
    ) -> Self {
        Self {
            metrics,
            http,
            grpc,
        }
    }

    /// Return the [`metric::Registry`] used by the router.
    pub fn metric_registry(&self) -> Arc<metric::Registry> {
        Arc::clone(&self.metrics)
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
