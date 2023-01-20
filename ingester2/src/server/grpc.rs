//! gRPC service implementations for `ingester`.

mod persist;
mod query;
mod rpc_write;

use std::{fmt::Debug, sync::Arc};

use arrow_flight::flight_service_server::FlightServiceServer;
use generated_types::influxdata::iox::{
    catalog::v1::catalog_service_server::CatalogServiceServer,
    ingester::v1::{
        persist_service_server::PersistServiceServer, write_service_server::WriteServiceServer,
    },
};
use iox_catalog::interface::Catalog;
use service_grpc_catalog::CatalogService;

use crate::{
    dml_sink::DmlSink,
    ingest_state::IngestState,
    ingester_id::IngesterId,
    init::IngesterRpcInterface,
    partition_iter::PartitionIter,
    persist::queue::PersistQueue,
    query::{response::QueryResponse, QueryExec},
    timestamp_oracle::TimestampOracle,
};

use self::{persist::PersistHandler, rpc_write::RpcWrite};

/// This type is responsible for injecting internal dependencies that SHOULD NOT
/// leak outside of the ingester crate into public gRPC handlers.
///
/// Configuration and external dependencies SHOULD be injected through the
/// respective gRPC handler constructor method.
#[derive(Debug)]
pub(crate) struct GrpcDelegate<D, Q, T, P> {
    dml_sink: Arc<D>,
    query_exec: Arc<Q>,
    timestamp: Arc<TimestampOracle>,
    ingest_state: Arc<IngestState>,
    ingester_id: IngesterId,
    catalog: Arc<dyn Catalog>,
    metrics: Arc<metric::Registry>,
    buffer: Arc<T>,
    persist_handle: Arc<P>,
}

impl<D, Q, T, P> GrpcDelegate<D, Q, T, P>
where
    D: DmlSink + 'static,
    Q: QueryExec<Response = QueryResponse> + 'static,
    T: PartitionIter + Sync + 'static,
    P: PersistQueue + Sync + 'static,
{
    /// Initialise a new [`GrpcDelegate`].
    #[allow(clippy::too_many_arguments)]
    pub(crate) fn new(
        dml_sink: Arc<D>,
        query_exec: Arc<Q>,
        timestamp: Arc<TimestampOracle>,
        ingest_state: Arc<IngestState>,
        ingester_id: IngesterId,
        catalog: Arc<dyn Catalog>,
        metrics: Arc<metric::Registry>,
        buffer: Arc<T>,
        persist_handle: Arc<P>,
    ) -> Self {
        Self {
            dml_sink,
            query_exec,
            timestamp,
            ingest_state,
            ingester_id,
            catalog,
            metrics,
            buffer,
            persist_handle,
        }
    }
}

/// Implement the type-erasure trait to hide internal types from crate-external
/// callers.
impl<D, Q, T, P> IngesterRpcInterface for GrpcDelegate<D, Q, T, P>
where
    D: DmlSink + 'static,
    Q: QueryExec<Response = QueryResponse> + 'static,
    T: PartitionIter + Sync + 'static,
    P: PersistQueue + Sync + 'static,
{
    type CatalogHandler = CatalogService;
    type WriteHandler = RpcWrite<Arc<D>>;
    type PersistHandler = PersistHandler<Arc<T>, Arc<P>>;
    type FlightHandler = query::FlightService<Arc<Q>>;

    /// Acquire a [`CatalogService`] gRPC service implementation.
    ///
    /// [`CatalogService`]: generated_types::influxdata::iox::catalog::v1::catalog_service_server::CatalogService.
    fn catalog_service(&self) -> CatalogServiceServer<Self::CatalogHandler> {
        CatalogServiceServer::new(CatalogService::new(Arc::clone(&self.catalog)))
    }

    /// Return a [`WriteService`] gRPC implementation.
    ///
    /// [`WriteService`]: generated_types::influxdata::iox::ingester::v1::write_service_server::WriteService.
    fn write_service(&self) -> WriteServiceServer<Self::WriteHandler> {
        WriteServiceServer::new(RpcWrite::new(
            Arc::clone(&self.dml_sink),
            Arc::clone(&self.timestamp),
            Arc::clone(&self.ingest_state),
        ))
    }

    /// Return a [`PersistService`] gRPC implementation.
    ///
    /// [`PersistService`]: generated_types::influxdata::iox::ingester::v1::persist_service_server::PersistService.
    fn persist_service(&self) -> PersistServiceServer<Self::PersistHandler> {
        PersistServiceServer::new(PersistHandler::new(
            Arc::clone(&self.buffer),
            Arc::clone(&self.persist_handle),
            Arc::clone(&self.catalog),
        ))
    }

    /// Return an Arrow [`FlightService`] gRPC implementation.
    ///
    /// [`FlightService`]: arrow_flight::flight_service_server::FlightService
    fn query_service(
        &self,
        max_simultaneous_requests: usize,
    ) -> FlightServiceServer<Self::FlightHandler> {
        FlightServiceServer::new(query::FlightService::new(
            Arc::clone(&self.query_exec),
            self.ingester_id,
            max_simultaneous_requests,
            &self.metrics,
        ))
    }
}
