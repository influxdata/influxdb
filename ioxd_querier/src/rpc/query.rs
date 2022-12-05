use std::sync::Arc;

use arrow_flight::flight_service_server::{
    FlightService as Flight, FlightServiceServer as FlightServer,
};
use generated_types::storage_server::{Storage, StorageServer};
use service_common::QueryNamespaceProvider;

pub fn make_flight_server<S: QueryNamespaceProvider>(server: Arc<S>) -> FlightServer<impl Flight> {
    service_grpc_flight::make_server(server)
}

pub fn make_storage_server<S: QueryNamespaceProvider>(
    server: Arc<S>,
) -> StorageServer<impl Storage> {
    service_grpc_influxrpc::make_server(server)
}
