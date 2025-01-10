use std::sync::Arc;

use arrow_flight::flight_service_server::{
    FlightService as Flight, FlightServiceServer as FlightServer,
};
use authz::Authorizer;
use influxdb3_internal_api::query_executor::QueryExecutor;

pub(crate) fn make_flight_server(
    server: Arc<dyn QueryExecutor>,
    authz: Option<Arc<dyn Authorizer>>,
) -> FlightServer<impl Flight> {
    let query_db = server.upcast();
    service_grpc_flight::make_server(query_db, authz)
}
