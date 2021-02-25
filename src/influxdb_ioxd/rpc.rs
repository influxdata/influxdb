use std::fmt::Debug;
use std::sync::Arc;

use snafu::{ResultExt, Snafu};
use tokio::net::TcpListener;
use tokio_stream::wrappers::TcpListenerStream;

use data_types::error::ErrorLogger;
use server::{ConnectionManager, Server};

mod flight;
mod storage;
mod testing;

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
pub async fn make_server<M>(socket: TcpListener, server: Arc<Server<M>>) -> Result<()>
where
    M: ConnectionManager + Send + Sync + Debug + 'static,
{
    let stream = TcpListenerStream::new(socket);

    let (mut health_reporter, health_service) = tonic_health::server::health_reporter();

    let services = [
        generated_types::STORAGE_SERVICE,
        generated_types::IOX_TESTING_SERVICE,
        generated_types::ARROW_SERVICE,
    ];

    for service in &services {
        health_reporter
            .set_service_status(service, tonic_health::ServingStatus::Serving)
            .await;
    }

    tonic::transport::Server::builder()
        .add_service(health_service)
        .add_service(testing::make_server())
        .add_service(storage::make_server(Arc::clone(&server)))
        .add_service(flight::make_server(server))
        .serve_with_incoming(stream)
        .await
        .context(ServerError {})
        .log_if_error("Running Tonic Server")
}
