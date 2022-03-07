use std::sync::Arc;

use arrow_flight::flight_service_server::{
    FlightService as Flight, FlightServiceServer as FlightServer,
};
use data_types::DatabaseName;
use db::Db;
use generated_types::storage_server::{Storage, StorageServer};
use server::Server;

use crate::rpc::{
    common::QueryDatabaseProvider, flight::make_server as make_flight_server_inner,
    storage::make_server as make_storage_server_inner,
};

impl QueryDatabaseProvider for Server {
    type Db = Db;

    fn db(&self, name: &str) -> Option<Arc<Self::Db>> {
        DatabaseName::new(name)
            .ok()
            .and_then(|name| self.db(&name).ok())
    }
}

pub fn make_flight_server(server: Arc<Server>) -> FlightServer<impl Flight> {
    make_flight_server_inner(server)
}

pub fn make_storage_server(server: Arc<Server>) -> StorageServer<impl Storage> {
    make_storage_server_inner(server)
}
