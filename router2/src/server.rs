//! Router server entrypoint.

use std::sync::Arc;

use crate::dml_handlers::DmlHandler;
use hashbrown::HashMap;
use mutable_batch::MutableBatch;
use trace::TraceCollector;

use self::{grpc::GrpcDelegate, http::HttpDelegate};

pub mod grpc;
pub mod http;

/// The [`RouterServer`] manages the lifecycle and contains all state for a
/// `router2` server instance.
#[derive(Debug, Default)]
pub struct RouterServer<D> {
    metrics: Arc<metric::Registry>,
    trace_collector: Option<Arc<dyn TraceCollector>>,

    http: HttpDelegate<D>,
    grpc: GrpcDelegate,
}

impl<D> RouterServer<D> {
    /// Initialise a new [`RouterServer`] using the provided HTTP and gRPC
    /// handlers.
    pub fn new(
        http: HttpDelegate<D>,
        grpc: GrpcDelegate,
        metrics: Arc<metric::Registry>,
        trace_collector: Option<Arc<dyn TraceCollector>>,
    ) -> Self {
        Self {
            metrics,
            trace_collector,
            http,
            grpc,
        }
    }

    /// Return the [`metric::Registry`] used by the router.
    pub fn metric_registry(&self) -> Arc<metric::Registry> {
        Arc::clone(&self.metrics)
    }

    /// Trace collector associated with this server.
    pub fn trace_collector(&self) -> &Option<Arc<dyn TraceCollector>> {
        &self.trace_collector
    }
}

impl<D> RouterServer<D>
where
    D: DmlHandler<WriteInput = HashMap<String, MutableBatch>>,
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
