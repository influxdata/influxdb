//! gRPC service implementations for `ingester`.

mod query;
mod rpc_write;
mod write_info;

use std::sync::{atomic::AtomicU64, Arc};

use arrow_flight::flight_service_server::{
    FlightService as Flight, FlightServiceServer as FlightServer,
};
use generated_types::influxdata::iox::{
    catalog::v1::*,
    ingester::v1::write_info_service_server::{WriteInfoService, WriteInfoServiceServer},
};
use iox_catalog::interface::Catalog;
use service_grpc_catalog::CatalogService;

use crate::handler::IngestHandler;

/// This type is responsible for managing all gRPC services exposed by `ingester`.
#[derive(Debug)]
pub struct GrpcDelegate<I: IngestHandler> {
    catalog: Arc<dyn Catalog>,
    ingest_handler: Arc<I>,

    /// How many `do_get` flight requests should panic for testing purposes.
    ///
    /// Every panic will decrease the counter until it reaches zero. At zero, no panics will occur.
    test_flight_do_get_panic: Arc<AtomicU64>,
}

impl<I: IngestHandler + Send + Sync + 'static> GrpcDelegate<I> {
    /// Initialise a new [`GrpcDelegate`] passing valid requests to the specified `ingest_handler`.
    pub fn new(
        catalog: Arc<dyn Catalog>,
        ingest_handler: Arc<I>,
        test_flight_do_get_panic: Arc<AtomicU64>,
    ) -> Self {
        Self {
            catalog,
            ingest_handler,
            test_flight_do_get_panic,
        }
    }

    /// Acquire an Arrow Flight gRPC service implementation.
    pub fn flight_service(&self) -> FlightServer<impl Flight> {
        FlightServer::new(query::FlightService::new(
            Arc::clone(&self.ingest_handler),
            Arc::clone(&self.test_flight_do_get_panic),
        ))
    }

    /// Acquire an WriteInfo gRPC service implementation.
    pub fn write_info_service(&self) -> WriteInfoServiceServer<impl WriteInfoService> {
        WriteInfoServiceServer::new(write_info::WriteInfoServiceImpl::new(Arc::clone(
            &self.ingest_handler,
        ) as _))
    }

    /// Acquire a [`CatalogService`] gRPC service implementation.
    ///
    /// [`CatalogService`]: generated_types::influxdata::iox::catalog::v1::catalog_service_server::CatalogService.
    pub fn catalog_service(
        &self,
    ) -> catalog_service_server::CatalogServiceServer<impl catalog_service_server::CatalogService>
    {
        catalog_service_server::CatalogServiceServer::new(CatalogService::new(Arc::clone(
            &self.catalog,
        )))
    }
}
