use std::sync::Arc;

use arrow_flight::flight_service_server::{
    FlightService as Flight, FlightServiceServer as FlightServer,
};
use generated_types::storage_server::{Storage, StorageServer};
use querier::{database::QuerierDatabase, namespace::QuerierNamespace};

use crate::rpc::{
    common::QueryDatabaseProvider, flight::make_server as make_flight_server_inner,
    storage::make_server as make_storage_server_inner,
};

impl QueryDatabaseProvider for QuerierDatabase {
    type Db = QuerierNamespace;

    fn db(&self, name: &str) -> Option<Arc<Self::Db>> {
        self.namespace(name)
    }
}

pub fn make_flight_server(server: Arc<QuerierDatabase>) -> FlightServer<impl Flight> {
    make_flight_server_inner(server)
}

pub fn make_storage_server(server: Arc<QuerierDatabase>) -> StorageServer<impl Storage> {
    make_storage_server_inner(server)
}
