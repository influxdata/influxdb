//! Router server entrypoint.

use std::sync::Arc;

use hashbrown::HashMap;
use mutable_batch::MutableBatch;
use trace::TraceCollector;

use self::{
    grpc::{GrpcDelegate, RpcWriteGrpcDelegate},
    http::HttpDelegate,
};
use crate::dml_handlers::DmlHandler;

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

/// The [`RouterServer`] manages the lifecycle and contains all state for a
/// `router` server instance.
#[derive(Debug)]
pub struct RouterServer<D, N, S> {
    metrics: Arc<metric::Registry>,
    trace_collector: Option<Arc<dyn TraceCollector>>,

    http: HttpDelegate<D, N>,
    grpc: GrpcDelegate<S>,
}

impl<D, N, S> RouterServer<D, N, S> {
    /// Initialise a new [`RouterServer`] using the provided HTTP and gRPC
    /// handlers.
    pub fn new(
        http: HttpDelegate<D, N>,
        grpc: GrpcDelegate<S>,
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

impl<D, N, S> RouterServer<D, N, S>
where
    D: DmlHandler<WriteInput = HashMap<String, MutableBatch>>,
{
    /// Get a reference to the router http delegate.
    pub fn http(&self) -> &HttpDelegate<D, N> {
        &self.http
    }

    /// Get a reference to the router grpc delegate.
    pub fn grpc(&self) -> &GrpcDelegate<S> {
        &self.grpc
    }
}
