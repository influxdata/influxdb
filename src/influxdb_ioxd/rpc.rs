use std::fmt::Debug;
use std::sync::Arc;

use tokio::net::TcpListener;
use tokio_stream::wrappers::TcpListenerStream;

use server::{ConnectionManager, Server};
use snafu::{ResultExt, Snafu};
use tokio_util::sync::CancellationToken;

pub mod error;
mod flight;
mod management;
mod operations;
mod storage;
mod testing;
mod write;

#[derive(Debug, Snafu)]
pub enum Error {
    #[snafu(display("gRPC transport error: {}", source))]
    TransportError { source: tonic::transport::Error },

    #[snafu(display("gRPC reflection error: {}", source))]
    ReflectionError {
        source: tonic_reflection::server::Error,
    },
}

pub type Result<T, E = Error> = std::result::Result<T, E>;

/// Instantiate a server listening on the specified address
/// implementing the IOx, Storage, and Flight gRPC interfaces, the
/// underlying hyper server instance. Resolves when the server has
/// shutdown.
pub async fn serve<M>(
    socket: TcpListener,
    server: Arc<Server<M>>,
    shutdown: CancellationToken,
) -> Result<()>
where
    M: ConnectionManager + Send + Sync + Debug + 'static,
{
    let stream = TcpListenerStream::new(socket);

    let (mut health_reporter, health_service) = tonic_health::server::health_reporter();
    let reflection_service = tonic_reflection::server::Builder::configure()
        .register_encoded_file_descriptor_set(generated_types::FILE_DESCRIPTOR_SET)
        .build()
        .context(ReflectionError)?;

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
        .add_service(reflection_service)
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
        .context(TransportError)
}
