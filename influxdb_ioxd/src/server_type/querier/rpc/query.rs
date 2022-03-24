use std::sync::Arc;

use arrow_flight::flight_service_server::{
    FlightService as Flight, FlightServiceServer as FlightServer,
};
use generated_types::storage_server::{Storage, StorageServer};
use querier::database::QuerierDatabase;

pub fn make_flight_server(server: Arc<QuerierDatabase>) -> FlightServer<impl Flight> {
    service_grpc_flight::make_server(server)
}

pub fn make_storage_server(server: Arc<QuerierDatabase>) -> StorageServer<impl Storage> {
    service_grpc_influxrpc::make_server(server)
}
