use trace_exporters::TracingConfig;
use trogging::cli::LoggingConfig;

use crate::{
    clap_blocks::{
        object_store::ObjectStoreConfig, server_id::ServerIdConfig, socket_addr::SocketAddr,
    },
    influxdb_ioxd::serving_readiness::ServingReadinessState,
};

/// The default bind address for the HTTP API.
pub const DEFAULT_API_BIND_ADDR: &str = "127.0.0.1:8080";

/// The default bind address for the gRPC.
pub const DEFAULT_GRPC_BIND_ADDR: &str = "127.0.0.1:8082";

/// Common config for all `run` commands.
#[derive(Debug, Clone, clap::Parser)]
pub struct RunConfig {
    /// logging options
    #[clap(flatten)]
    pub(crate) logging_config: LoggingConfig,

    /// tracing options
    #[clap(flatten)]
    pub(crate) tracing_config: TracingConfig,

    /// object store config
    #[clap(flatten)]
    pub(crate) server_id_config: ServerIdConfig,

    /// The address on which IOx will serve HTTP API requests.
    #[clap(
    long = "--api-bind",
    env = "INFLUXDB_IOX_BIND_ADDR",
    default_value = DEFAULT_API_BIND_ADDR,
    )]
    pub http_bind_address: SocketAddr,

    /// The address on which IOx will serve Storage gRPC API requests.
    #[clap(
    long = "--grpc-bind",
    env = "INFLUXDB_IOX_GRPC_BIND_ADDR",
    default_value = DEFAULT_GRPC_BIND_ADDR,
    )]
    pub grpc_bind_address: SocketAddr,

    /// After startup the IOx server can either accept serving data plane traffic right away
    /// or require a SetServingReadiness call from the Management API to enable serving.
    #[clap(
        long = "--initial-serving-readiness-state",
        env = "INFLUXDB_IOX_INITIAL_SERVING_READINESS_STATE",
        default_value = "serving"
    )]
    pub initial_serving_state: ServingReadinessState,

    /// Maximum size of HTTP requests.
    #[clap(
        long = "--max-http-request-size",
        env = "INFLUXDB_IOX_MAX_HTTP_REQUEST_SIZE",
        default_value = "10485760" // 10 MiB
    )]
    pub max_http_request_size: usize,

    /// object store config
    #[clap(flatten)]
    pub(crate) object_store_config: ObjectStoreConfig,
}
