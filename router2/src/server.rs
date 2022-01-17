//! Router server entrypoint.

use std::sync::Arc;

use crate::dml_handler::DmlHandler;

use self::{grpc::GrpcDelegate, http::HttpDelegate};

pub mod grpc;
pub mod http;

/// The [`RouterServer`] manages the lifecycle and contains all state for a
/// `router2` server instance.
#[derive(Debug, Default)]
pub struct RouterServer<D> {
    metrics: Arc<metric::Registry>,

    http: HttpDelegate<D>,
    grpc: GrpcDelegate,
}

impl<D> RouterServer<D> {
    /// Initialise a new [`RouterServer`] using the provided HTTP and gRPC
    /// handlers.
    pub fn new(http: HttpDelegate<D>, grpc: GrpcDelegate) -> Self {
        Self {
            metrics: Default::default(),
            http,
            grpc,
        }
    }

    /// Return the [`metric::Registry`] used by the router.
    pub fn metric_registry(&self) -> Arc<metric::Registry> {
        Arc::clone(&self.metrics)
    }
}

impl<D> RouterServer<D>
where
    D: DmlHandler,
{
    /// Get a reference to the router http delegate.
    pub fn http(&self) -> &HttpDelegate<D> {
        &self.http
    }

    /// Get a reference to the router grpc delegate.
    pub fn grpc(&self) -> &GrpcDelegate {
        &self.grpc
    }
}
