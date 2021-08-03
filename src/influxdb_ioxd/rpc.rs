use std::fmt::Debug;
use std::sync::Arc;

use snafu::{ResultExt, Snafu};
use tokio::net::TcpListener;
use tokio_stream::wrappers::TcpListenerStream;
use tokio_util::sync::CancellationToken;
use tonic::transport::NamedService;

use crate::influxdb_ioxd::serving_readiness::{ServingReadiness, ServingReadinessState};
use server::{ApplicationState, ConnectionManager, Server};
use tonic::{Interceptor, Status};

pub mod error;
mod flight;
mod management;
mod operations;
mod storage;
mod testing;
mod write;
mod write_pb;

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

/// Returns the name of the gRPC service S.
fn service_name<S: NamedService>(_: &S) -> &'static str {
    S::NAME
}

/// Adds a gRPC service to ServiceBuilder `builder` while registering it to the HealthReporter `health_reporter`.
macro_rules! add_service {
    ($builder:ident, $health_reporter:ident, $status:expr, $($tail:tt)* ) => {{
        add_service!(@internal $ builder, $health_reporter, $status, $($tail)* );
        $builder
    }};
    (@internal $builder:ident, $health_reporter:ident, $status:expr, [ $($svc:expr),* $(,)*] $(,)*) => {
        $(
          let service = $svc;
          $health_reporter.set_service_status(service_name(&service), $status).await;
          let $builder = $builder.add_service(service);
        )*
    };
}

/// Implements the gRPC interceptor that returns SERVICE_UNAVAILABLE gRPC status
/// if the service is not ready.
#[derive(Debug, Clone)]
pub struct ServingReadinessInterceptor(ServingReadiness);

impl From<ServingReadinessInterceptor> for Interceptor {
    fn from(serving_readiness: ServingReadinessInterceptor) -> Self {
        let interceptor = move |req| match serving_readiness.0.get() {
            ServingReadinessState::Unavailable => {
                Err(Status::unavailable("service not ready to serve"))
            }
            ServingReadinessState::Serving => Ok(req),
        };
        interceptor.into()
    }
}

/// Instantiate a server listening on the specified address
/// implementing the IOx, Storage, and Flight gRPC interfaces, the
/// underlying hyper server instance. Resolves when the server has
/// shutdown.
pub async fn serve<M>(
    socket: TcpListener,
    application: Arc<ApplicationState>,
    server: Arc<Server<M>>,
    shutdown: CancellationToken,
    serving_readiness: ServingReadiness,
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

    let serving_gate = ServingReadinessInterceptor(serving_readiness.clone());

    let mut builder = tonic::transport::Server::builder();
    let builder = add_service!(
        builder,
        health_reporter,
        tonic_health::ServingStatus::Serving,
        [
            health_service,
            reflection_service,
            testing::make_server(),
            storage::make_server(
                Arc::clone(&server),
                Arc::clone(application.metric_registry()),
                serving_gate.clone(),
            ),
            flight::make_server(Arc::clone(&server), serving_gate.clone()),
            write::make_server(Arc::clone(&server), serving_gate.clone()),
            write_pb::make_server(Arc::clone(&server), serving_gate.clone()),
            management::make_server(
                Arc::clone(&application),
                Arc::clone(&server),
                serving_readiness.clone()
            ),
            operations::make_server(Arc::clone(application.job_registry())),
        ],
    );

    builder
        .serve_with_incoming_shutdown(stream, shutdown.cancelled())
        .await
        .context(TransportError)
}
