use std::sync::Arc;

use arrow_flight::flight_service_server::{
    FlightService as Flight, FlightServiceServer as FlightServer,
};
use data_types::DatabaseName;
use querier::{database::QuerierDatabase, namespace::QuerierNamespace};

use crate::influxdb_ioxd::rpc::flight::{make_server as make_server_inner, QueryDatabaseProvider};

impl QueryDatabaseProvider for QuerierDatabase {
    type Db = QuerierNamespace;

    fn db(&self, db_name: &DatabaseName<'_>) -> Result<Arc<Self::Db>, tonic::Status> {
        self.namespace(db_name)
            .ok_or_else(|| tonic::Status::not_found(format!("Unknown namespace: {db_name}")))
    }
}

pub fn make_server(server: Arc<QuerierDatabase>) -> FlightServer<impl Flight> {
    make_server_inner(server)
}
