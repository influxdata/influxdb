use std::fmt::Debug;
use std::sync::Arc;

use snafu::{ResultExt, Snafu};
use tokio::net::TcpListener;
use tokio_stream::wrappers::TcpListenerStream;
use tokio_util::sync::CancellationToken;
use tonic::codegen::InterceptedService;
use tonic::transport::NamedService;

use crate::influxdb_ioxd::serving_readiness::ServingReadiness;
use server::{ApplicationState, ConnectionManager, Server};
use trace::TraceCollector;

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
    #[snafu(display("gRPC transport error: {}{}", source, details))]
    TransportError {
        source: tonic::transport::Error,
        details: String,
    },

    #[snafu(display("gRPC reflection error: {}", source))]
    ReflectionError {
        source: tonic_reflection::server::Error,
    },
}

pub type Result<T, E = Error> = std::result::Result<T, E>;

// Custom impl to include underlying source (not included in tonic
// transport error)
impl From<tonic::transport::Error> for Error {
    fn from(source: tonic::transport::Error) -> Self {
        use std::error::Error;
        let details = source
            .source()
            .map(|e| format!(" ({})", e))
            .unwrap_or_else(|| "".to_string());

        Self::TransportError { source, details }
    }
}

/// Returns the name of the gRPC service S.
fn service_name<S: NamedService>(_: &S) -> &'static str {
    S::NAME
}

/// Adds a gRPC service to the builder, and registers it with the
/// health reporter
macro_rules! add_service {
    ($builder:ident, $health_reporter:expr, $svc:expr) => {
        let service = $svc;
        let status = tonic_health::ServingStatus::Serving;
        $health_reporter
            .set_service_status(service_name(&service), status)
            .await;
        let $builder = $builder.add_service(service);
    };
}

/// Adds a gRPC service to the builder gated behind the serving
/// readiness check, and registers it with the health reporter
macro_rules! add_gated_service {
    ($builder:ident, $health_reporter:expr, $serving_readiness:expr, $svc:expr) => {
        let service = $svc;
        let interceptor = $serving_readiness.clone().into_interceptor();
        let service = InterceptedService::new(service, interceptor);
        add_service!($builder, $health_reporter, service);
    };
}

/// Instantiate a server listening on the specified address
/// implementing the IOx, Storage, and Flight gRPC interfaces, the
/// underlying hyper server instance. Resolves when the server has
/// shutdown.
pub async fn serve<M>(
    socket: TcpListener,
    application: Arc<ApplicationState>,
    server: Arc<Server<M>>,
    trace_collector: Option<Arc<dyn TraceCollector>>,
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

    let builder = tonic::transport::Server::builder();
    let mut builder = builder.layer(trace_http::tower::TraceLayer::new(
        Arc::clone(application.metric_registry()),
        trace_collector,
        true,
    ));

    // important that this one is NOT gated so that it can answer health requests
    add_service!(builder, health_reporter, health_service);
    add_service!(builder, health_reporter, reflection_service);
    add_service!(builder, health_reporter, testing::make_server());
    add_gated_service!(
        builder,
        health_reporter,
        serving_readiness,
        storage::make_server(Arc::clone(&server),)
    );
    add_gated_service!(
        builder,
        health_reporter,
        serving_readiness,
        flight::make_server(Arc::clone(&server))
    );
    add_gated_service!(
        builder,
        health_reporter,
        serving_readiness,
        write::make_server(Arc::clone(&server))
    );
    add_gated_service!(
        builder,
        health_reporter,
        serving_readiness,
        write_pb::make_server(Arc::clone(&server))
    );
    // Also important this is not behind a readiness check (as it is
    // used to change the check!)
    add_service!(
        builder,
        health_reporter,
        management::make_server(
            Arc::clone(&application),
            Arc::clone(&server),
            serving_readiness.clone()
        )
    );
    add_service!(
        builder,
        health_reporter,
        operations::make_server(Arc::clone(application.job_registry()))
    );

    builder
        .serve_with_incoming_shutdown(stream, shutdown.cancelled())
        .await?;

    Ok(())
}
