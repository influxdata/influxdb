use std::sync::Arc;

use arrow_flight::flight_service_server::{
    FlightService as Flight, FlightServiceServer as FlightServer,
};
use data_types::DatabaseName;
use db::Db;
use server::Server;

use crate::rpc::flight::{make_server as make_server_inner, QueryDatabaseProvider};

use super::error::default_server_error_handler;

impl QueryDatabaseProvider for Server {
    type Db = Db;

    fn db(&self, db_name: &DatabaseName<'_>) -> Result<Arc<Self::Db>, tonic::Status> {
        self.db(db_name).map_err(default_server_error_handler)
    }
}

pub fn make_server(server: Arc<Server>) -> FlightServer<impl Flight> {
    make_server_inner(server)
}
