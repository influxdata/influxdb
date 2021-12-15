use std::sync::Arc;

use tokio::net::TcpListener;
use tokio_util::sync::CancellationToken;
use tonic::transport::NamedService;
use tonic_health::server::HealthReporter;
use trace_http::ctx::TraceHeaderParser;

use crate::influxdb_ioxd::{
    server_type::{RpcError, ServerType},
    serving_readiness::ServingReadiness,
};

pub(crate) mod testing;

/// Returns the name of the gRPC service S.
pub fn service_name<S: NamedService>(_: &S) -> &'static str {
    S::NAME
}

#[derive(Debug)]
pub struct RpcBuilderInput {
    pub socket: TcpListener,
    pub trace_header_parser: TraceHeaderParser,
    pub shutdown: CancellationToken,
    pub serving_readiness: ServingReadiness,
}

#[derive(Debug)]
pub struct RpcBuilder<T> {
    pub inner: T,
    pub health_reporter: HealthReporter,
    pub shutdown: CancellationToken,
    pub socket: TcpListener,
    pub serving_readiness: ServingReadiness,
}

/// Adds a gRPC service to the builder, and registers it with the
/// health reporter
macro_rules! add_service {
    ($builder:ident, $svc:expr) => {
        let $builder = {
            // `inner` might be required to be `mut` or not depending if we're acting on:
            // - a `Server`, no service added yet, no `mut` required
            // - a `Router`, some service was added already, `mut` required
            #[allow(unused_mut)]
            {
                use $crate::influxdb_ioxd::rpc::{service_name, RpcBuilder};

                let RpcBuilder {
                    mut inner,
                    mut health_reporter,
                    shutdown,
                    socket,
                    serving_readiness,
                } = $builder;
                let service = $svc;

                let status = tonic_health::ServingStatus::Serving;
                health_reporter
                    .set_service_status(service_name(&service), status)
                    .await;

                let inner = inner.add_service(service);

                RpcBuilder {
                    inner,
                    health_reporter,
                    shutdown,
                    socket,
                    serving_readiness,
                }
            }
        };
    };
}

pub(crate) use add_service;

/// Adds a gRPC service to the builder gated behind the serving
/// readiness check, and registers it with the health reporter
macro_rules! add_gated_service {
    ($builder:ident, $svc:expr) => {
        let $builder = {
            let service = $svc;

            let interceptor = $builder.serving_readiness.clone().into_interceptor();
            let service = tonic::codegen::InterceptedService::new(service, interceptor);

            add_service!($builder, service);

            $builder
        };
    };
}

pub(crate) use add_gated_service;

/// Creates a [`RpcBuilder`] from [`RpcBuilderInput`].
///
/// The resulting builder can be used w/ [`add_service`] and [`add_gated_service`]. After adding all services it should
/// be used w/ [`serve_builder`].
macro_rules! setup_builder {
    ($input:ident, $server_type:ident) => {{
        #[allow(unused_imports)]
        use $crate::influxdb_ioxd::{
            rpc::{add_service, testing, RpcBuilder},
            server_type::ServerType,
        };

        let RpcBuilderInput {
            socket,
            trace_header_parser,
            shutdown,
            serving_readiness,
        } = $input;

        let (health_reporter, health_service) = tonic_health::server::health_reporter();
        let reflection_service = tonic_reflection::server::Builder::configure()
            .register_encoded_file_descriptor_set(generated_types::FILE_DESCRIPTOR_SET)
            .build()
            .expect("gRPC reflection data broken");

        let builder = tonic::transport::Server::builder();
        let builder = builder.layer(trace_http::tower::TraceLayer::new(
            trace_header_parser,
            $server_type.metric_registry(),
            $server_type.trace_collector(),
            true,
        ));

        let builder = RpcBuilder {
            inner: builder,
            health_reporter,
            shutdown,
            socket,
            serving_readiness,
        };

        // important that this one is NOT gated so that it can answer health requests
        add_service!(builder, health_service);
        add_service!(builder, reflection_service);
        add_service!(builder, testing::make_server());

        builder
    }};
}

pub(crate) use setup_builder;

/// Serve a server constructed using [`RpcBuilder`].
macro_rules! serve_builder {
    ($builder:ident) => {{
        use tokio_stream::wrappers::TcpListenerStream;
        use $crate::influxdb_ioxd::rpc::RpcBuilder;

        let RpcBuilder {
            inner,
            shutdown,
            socket,
            ..
        } = $builder;

        let stream = TcpListenerStream::new(socket);
        inner
            .serve_with_incoming_shutdown(stream, shutdown.cancelled())
            .await?;
    }};
}

pub(crate) use serve_builder;

/// Instantiate a server listening on the specified address
/// implementing the IOx, Storage, and Flight gRPC interfaces, the
/// underlying hyper server instance. Resolves when the server has
/// shutdown.
pub async fn serve<T>(
    socket: TcpListener,
    server_type: Arc<T>,
    trace_header_parser: TraceHeaderParser,
    shutdown: CancellationToken,
    serving_readiness: ServingReadiness,
) -> Result<(), RpcError>
where
    T: ServerType,
{
    let builder_input = RpcBuilderInput {
        socket,
        trace_header_parser,
        shutdown,
        serving_readiness,
    };

    server_type.server_grpc(builder_input).await
}
