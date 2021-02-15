//! This module contains gRPC service implementatations

/// `[0x00]` is the magic value that that the storage gRPC layer uses to
/// encode a tag_key that means "measurement name"
pub(crate) const TAG_KEY_MEASUREMENT: &[u8] = &[0];

/// `[0xff]` is is the magic value that that the storage gRPC layer uses
/// to encode a tag_key that means "field name"
pub(crate) const TAG_KEY_FIELD: &[u8] = &[255];

pub mod data;
pub mod expr;
pub mod id;
pub mod input;
pub mod service;

use arrow_deps::arrow_flight::flight_service_server::FlightServiceServer;
use data_types::error::ErrorLogger;
use generated_types::{i_ox_testing_server::IOxTestingServer, storage_server::StorageServer};
use query::DatabaseStore;
use snafu::{ResultExt, Snafu};
use std::sync::Arc;
use tokio::net::TcpListener;
use tokio_stream::wrappers::TcpListenerStream;

#[derive(Debug, Snafu)]
pub enum Error {
    #[snafu(display("gRPC server error:  {}", source))]
    ServerError { source: tonic::transport::Error },
}

pub type Result<T, E = Error> = std::result::Result<T, E>;

/// Instantiate a server listening on the specified address
/// implementing the IOx, Storage, and Flight gRPC interfaces, the
/// underlying hyper server instance. Resolves when the server has
/// shutdown.
pub async fn make_server<T>(socket: TcpListener, storage: Arc<T>) -> Result<()>
where
    T: DatabaseStore + 'static,
{
    let stream = TcpListenerStream::new(socket);

    tonic::transport::Server::builder()
        .add_service(IOxTestingServer::new(GrpcService::new(Arc::clone(
            &storage,
        ))))
        .add_service(StorageServer::new(GrpcService::new(Arc::clone(&storage))))
        .add_service(FlightServiceServer::new(GrpcService::new(storage)))
        .serve_with_incoming(stream)
        .await
        .context(ServerError {})
        .log_if_error("Running Tonic Server")
}

#[derive(Debug)]
pub struct GrpcService<T: DatabaseStore> {
    pub db_store: Arc<T>,
}

impl<T> GrpcService<T>
where
    T: DatabaseStore + 'static,
{
    /// Create a new GrpcService connected to `db_store`
    pub fn new(db_store: Arc<T>) -> Self {
        Self { db_store }
    }
}
