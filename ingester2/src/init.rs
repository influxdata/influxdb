use std::sync::Arc;

use arrow_flight::flight_service_server::{FlightService, FlightServiceServer};
use generated_types::influxdata::iox::{
    catalog::v1::catalog_service_server::{CatalogService, CatalogServiceServer},
    ingester::v1::write_service_server::{WriteService, WriteServiceServer},
};
use iox_catalog::interface::Catalog;

/// Acquire opaque handles to the Ingester RPC service implementations.
///
/// This trait serves as the public crate API boundary - callers external to the
/// Ingester crate utilise this abstraction to acquire type erased handles to
/// the RPC service implementations, hiding internal Ingester implementation
/// details & types.
///
/// Callers can mock out this trait or decorate the returned implementation in
/// order to simulate or modify the behaviour of an ingester in their own tests.
pub trait IngesterRpcInterface: Send + Sync + std::fmt::Debug {
    /// The type of the [`CatalogService`] implementation.
    type CatalogHandler: CatalogService;
    /// The type of the [`WriteService`] implementation.
    type WriteHandler: WriteService;
    /// The type of the [`FlightService`] implementation.
    type FlightHandler: FlightService;

    /// Acquire an opaque handle to the Ingester's [`CatalogService`] RPC
    /// handler implementation.
    fn catalog_service(
        &self,
        catalog: Arc<dyn Catalog>,
    ) -> CatalogServiceServer<Self::CatalogHandler>;

    /// Acquire an opaque handle to the Ingester's [`WriteService`] RPC
    /// handler implementation.
    fn write_service(&self) -> WriteServiceServer<Self::WriteHandler>;

    /// Acquire an opaque handle to the Ingester's Arrow Flight
    /// [`FlightService`] RPC handler implementation, allowing at most
    /// `max_simultaneous_requests` queries to be running at any one time.
    fn query_service(
        &self,
        max_simultaneous_requests: usize,
        metrics: &metric::Registry,
    ) -> FlightServiceServer<Self::FlightHandler>;
}
