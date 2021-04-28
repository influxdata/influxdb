use std::fmt::Debug;
use std::sync::Arc;

use tokio::net::TcpListener;
use tokio_stream::wrappers::TcpListenerStream;

use server::{ConnectionManager, Server};
use tokio_util::sync::CancellationToken;

pub mod error;
mod flight;
mod management;
mod operations;
mod storage;
mod testing;
mod write;

/// Instantiate a server listening on the specified address
/// implementing the IOx, Storage, and Flight gRPC interfaces, the
/// underlying hyper server instance. Resolves when the server has
/// shutdown.
pub async fn serve<M>(
    socket: TcpListener,
    server: Arc<Server<M>>,
    shutdown: CancellationToken,
) -> Result<(), tonic::transport::Error>
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
        .add_service(storage::make_server(
            Arc::clone(&server),
            Arc::clone(&server.registry),
        ))
        .add_service(flight::make_server(Arc::clone(&server)))
        .add_service(write::make_server(Arc::clone(&server)))
        .add_service(management::make_server(Arc::clone(&server)))
        .add_service(operations::make_server(server))
        .serve_with_incoming_shutdown(stream, shutdown.cancelled())
        .await
}
