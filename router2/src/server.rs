//! Router server entrypoint.

use std::sync::Arc;

use self::{grpc::GrpcDelegate, http::HttpDelegate};

pub mod grpc;
pub mod http;

/// The [`RouterServer`] manages the lifecycle and contains all state for a
/// `router2` server instance.
#[derive(Debug, Default)]
pub struct RouterServer {
    metrics: Arc<metric::Registry>,

    http: HttpDelegate,
    grpc: GrpcDelegate,
}

impl RouterServer {
    /// Get a reference to the router http delegate.
    pub fn http(&self) -> &HttpDelegate {
        &self.http
    }

    /// Get a reference to the router grpc delegate.
    pub fn grpc(&self) -> &GrpcDelegate {
        &self.grpc
    }

    /// Return the [`metric::Registry`] used by the router.
    pub fn metric_registry(&self) -> Arc<metric::Registry> {
        Arc::clone(&self.metrics)
    }
}
