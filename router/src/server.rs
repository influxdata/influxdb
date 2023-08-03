//! Router server entrypoint.

use self::{grpc::RpcWriteGrpcDelegate, http::HttpDelegate};
use std::sync::Arc;
use trace::TraceCollector;

pub mod grpc;
pub mod http;

/// The [`RpcWriteRouterServer`] manages the lifecycle and contains all state for a
/// `router-rpc-write` server instance.
#[derive(Debug)]
pub struct RpcWriteRouterServer<D, N> {
    metrics: Arc<metric::Registry>,
    trace_collector: Option<Arc<dyn TraceCollector>>,

    http: HttpDelegate<D, N>,
    grpc: RpcWriteGrpcDelegate,
}

impl<D, N> RpcWriteRouterServer<D, N> {
    /// Initialise a new [`RpcWriteRouterServer`] using the provided HTTP and gRPC
    /// handlers.
    pub fn new(
        http: HttpDelegate<D, N>,
        grpc: RpcWriteGrpcDelegate,
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

    /// Get a reference to the router http delegate.
    pub fn http(&self) -> &HttpDelegate<D, N> {
        &self.http
    }

    /// Get a reference to the router grpc delegate.
    pub fn grpc(&self) -> &RpcWriteGrpcDelegate {
        &self.grpc
    }
}
