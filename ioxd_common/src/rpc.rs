use std::any::Any;
use std::sync::Arc;

use tokio::net::TcpListener;
use tokio_util::sync::CancellationToken;
use tonic::{body::BoxBody, transport::NamedService, Code};
use tonic_health::server::HealthReporter;
use trace_http::ctx::TraceHeaderParser;

use crate::server_type::{RpcError, ServerType};

/// Returns the name of the gRPC service S.
pub fn service_name<S: NamedService>(_: &S) -> &'static str {
    S::NAME
}

#[derive(Debug)]
pub struct RpcBuilderInput {
    pub socket: TcpListener,
    pub trace_header_parser: TraceHeaderParser,
    pub shutdown: CancellationToken,
}

#[derive(Debug)]
pub struct RpcBuilder<T> {
    pub inner: T,
    pub health_reporter: HealthReporter,
    pub shutdown: CancellationToken,
    pub socket: TcpListener,
}

/// Adds a gRPC service to the builder, and registers it with the
/// health reporter
#[macro_export]
macro_rules! add_service {
    ($builder:ident, $svc:expr) => {
        let $builder = {
            // `inner` might be required to be `mut` or not depending if we're acting on:
            // - a `Server`, no service added yet, no `mut` required
            // - a `Router`, some service was added already, `mut` required
            #[allow(unused_mut)]
            {
                use $crate::rpc::{service_name, RpcBuilder};

                let RpcBuilder {
                    mut inner,
                    mut health_reporter,
                    shutdown,
                    socket,
                } = $builder;
                let service = $svc;

                let status = $crate::reexport::tonic_health::ServingStatus::Serving;
                health_reporter
                    .set_service_status(service_name(&service), status)
                    .await;

                let inner = inner.add_service(service);

                RpcBuilder {
                    inner,
                    health_reporter,
                    shutdown,
                    socket,
                }
            }
        };
    };
}

/// Creates a [`RpcBuilder`] from [`RpcBuilderInput`].
///
/// The resulting builder can be used w/ [`add_service`]. After adding all services it should
/// be used w/ [`serve_builder!`](crate::serve_builder).
#[macro_export]
macro_rules! setup_builder {
    ($input:ident, $server_type:ident) => {{
        #[allow(unused_imports)]
        use $crate::{add_service, rpc::RpcBuilder, server_type::ServerType};

        let RpcBuilderInput {
            socket,
            trace_header_parser,
            shutdown,
        } = $input;

        let (health_reporter, health_service) =
            $crate::reexport::tonic_health::server::health_reporter();
        let reflection_service = $crate::reexport::tonic_reflection::server::Builder::configure()
            .register_encoded_file_descriptor_set(
                $crate::reexport::generated_types::FILE_DESCRIPTOR_SET,
            )
            .build()
            .expect("gRPC reflection data broken");

        let builder = $crate::reexport::tonic::transport::Server::builder();
        let builder = builder
            .layer($crate::reexport::trace_http::tower::TraceLayer::new(
                trace_header_parser,
                $server_type.metric_registry(),
                $server_type.trace_collector(),
                true,
                $server_type.name(),
            ))
            .layer(
                $crate::reexport::tower_http::catch_panic::CatchPanicLayer::custom(
                    $crate::rpc::handle_panic,
                ),
            );

        let builder = RpcBuilder {
            inner: builder,
            health_reporter,
            shutdown,
            socket,
        };

        add_service!(builder, health_service);
        add_service!(builder, reflection_service);
        add_service!(
            builder,
            $crate::reexport::service_grpc_testing::make_server()
        );

        builder
    }};
}

/// Serve a server constructed using [`RpcBuilder`].
#[macro_export]
macro_rules! serve_builder {
    ($builder:ident) => {{
        use $crate::rpc::RpcBuilder;

        let RpcBuilder {
            inner,
            shutdown,
            socket,
            ..
        } = $builder;

        let stream = $crate::reexport::tonic::transport::server::TcpIncoming::from_listener(
            socket, true, None,
        )
        .expect("failed to initialise tcp socket");
        inner
            .serve_with_incoming_shutdown(stream, shutdown.cancelled())
            .await?;
    }};
}

pub fn handle_panic(err: Box<dyn Any + Send + 'static>) -> http::Response<BoxBody> {
    let message = if let Some(s) = err.downcast_ref::<String>() {
        s.clone()
    } else if let Some(s) = err.downcast_ref::<&str>() {
        s.to_string()
    } else {
        "unknown internal error".to_string()
    };

    http::Response::builder()
        .status(http::StatusCode::OK)
        .header(http::header::CONTENT_TYPE, "application/grpc")
        .header("grpc-status", Code::Internal as u32)
        .header("grpc-message", message) // we don't want to leak the panic message
        .body(tonic::body::empty_body())
        .unwrap()
}

/// Instantiate a server listening on the specified address
/// implementing the IOx, Storage, and Flight gRPC interfaces, the
/// underlying hyper server instance. Resolves when the server has
/// shutdown.
pub async fn serve(
    socket: TcpListener,
    server_type: Arc<dyn ServerType>,
    trace_header_parser: TraceHeaderParser,
    shutdown: CancellationToken,
) -> Result<(), RpcError> {
    let builder_input = RpcBuilderInput {
        socket,
        trace_header_parser,
        shutdown,
    };

    server_type.server_grpc(builder_input).await
}
