mod query;
mod replication;

use std::{fmt::Debug, sync::Arc};

use arrow_flight::flight_service_server::FlightServiceServer;
use generated_types::influxdata::iox::ingester::v1::replication_service_server::ReplicationServiceServer;

use crate::ReplicationBuffer;
use crate::{
    query::{response::QueryResponse, QueryExec},
    IngestReplicaRpcInterface,
};

use self::replication::ReplicationServer;

/// This type is responsible for injecting internal dependencies that SHOULD NOT
/// leak outside of the ingester crate into public gRPC handlers.
///
/// Configuration and external dependencies SHOULD be injected through the
/// respective gRPC handler constructor method.
#[derive(Debug)]
pub(crate) struct GrpcDelegate<B> {
    buffer: Arc<B>,
    metrics: Arc<metric::Registry>,
}

impl<B> GrpcDelegate<B>
where
    B: ReplicationBuffer + QueryExec<Response = QueryResponse> + 'static,
{
    /// Initialise a new [`GrpcDelegate`].
    pub(crate) fn new(buffer: Arc<B>, metrics: Arc<metric::Registry>) -> Self {
        Self { buffer, metrics }
    }
}

/// Implement the type-erasure trait to hide internal types from crate-external
/// callers.
impl<B> IngestReplicaRpcInterface for GrpcDelegate<B>
where
    B: ReplicationBuffer + QueryExec<Response = QueryResponse> + 'static,
{
    type ReplicationHandler = ReplicationServer<B>;
    type FlightHandler = query::FlightService<Arc<B>>;

    /// Return a [`ReplicationService`] gRPC implementation.
    ///
    /// [`ReplicationService`]: generated_types::influxdata::iox::catalog::v1::write_service_server::WriteService.
    fn replication_service(&self) -> ReplicationServiceServer<Self::ReplicationHandler> {
        ReplicationServiceServer::new(ReplicationServer::new(Arc::clone(&self.buffer)))
    }

    /// Return an Arrow [`FlightService`] gRPC implementation.
    ///
    /// [`FlightService`]: arrow_flight::flight_service_server::FlightService
    fn query_service(
        &self,
        max_simultaneous_requests: usize,
    ) -> FlightServiceServer<Self::FlightHandler> {
        FlightServiceServer::new(query::FlightService::new(
            Arc::clone(&self.buffer),
            max_simultaneous_requests,
            &self.metrics,
        ))
    }
}
