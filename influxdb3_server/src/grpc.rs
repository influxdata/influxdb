use std::sync::Arc;

use arrow_flight::flight_service_server::{
    FlightService as Flight, FlightServiceServer as FlightServer,
};
use authz::Authorizer;
use iox_query::QueryNamespaceProvider;

#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("tonic server error: {0}")]
    Tonic(#[from] tonic::transport::Error),
}

pub(crate) fn make_flight_server<Q: QueryNamespaceProvider>(
    server: Arc<Q>,
    authz: Option<Arc<dyn Authorizer>>,
) -> FlightServer<impl Flight> {
    service_grpc_flight::make_server(server, authz)
}
