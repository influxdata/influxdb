use std::fmt::Debug;
use std::sync::Arc;

use tokio::net::TcpListener;
use tokio_stream::wrappers::TcpListenerStream;

use server::{ConnectionManager, Server};
use snafu::{ResultExt, Snafu};
use tokio_util::sync::CancellationToken;
use tonic::transport::NamedService;

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

fn service_name<S: NamedService>(_service: &S) -> &'static str {
    S::NAME
}

macro_rules! add_service {
    ($builder:ident, $health_reporter:ident, $status:expr, $($tail:tt)* ) => {{
        add_service!(@internal $ builder, $health_reporter, $status, $($tail)* );
        $builder
    }};
    (@internal $builder:ident, $health_reporter:ident, $status:expr, [ $($x:expr),* $(,)*] $(,)*) => {
        $(
          $health_reporter.set_service_status(service_name(&$x), $status).await;
          let $builder = $builder.add_service($x);
        )*
    };
}

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

    let mut builder = tonic::transport::Server::builder();

    let builder = add_service!(
        builder,
        health_reporter,
        tonic_health::ServingStatus::Serving,
        [
            health_service,
            reflection_service,
            testing::make_server(),
            storage::make_server(Arc::clone(&server), Arc::clone(&server.registry),),
            flight::make_server(Arc::clone(&server)),
            write::make_server(Arc::clone(&server)),
            management::make_server(Arc::clone(&server)),
            operations::make_server(Arc::clone(&server)),
        ],
    );

    builder
        .serve_with_incoming_shutdown(stream, shutdown.cancelled())
        .await
        .context(TransportError)
}
