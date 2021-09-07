//! Implementation of command line option for running server

use crate::{
    influxdb_ioxd::{self, serving_readiness::ServingReadinessState},
    object_store::ObjectStoreConfig,
    server_id::ServerIdConfig,
};
use std::{net::SocketAddr, net::ToSocketAddrs};
use structopt::StructOpt;
use thiserror::Error;
use trace_exporters::TracingConfig;
use trogging::cli::LoggingConfig;

/// The default bind address for the HTTP API.
pub const DEFAULT_API_BIND_ADDR: &str = "127.0.0.1:8080";

/// The default bind address for the gRPC.
pub const DEFAULT_GRPC_BIND_ADDR: &str = "127.0.0.1:8082";

#[derive(Debug, Error)]
pub enum Error {
    #[error("Run: {0}")]
    ServerError(#[from] influxdb_ioxd::Error),
}

pub type Result<T, E = Error> = std::result::Result<T, E>;

/// Boolean flag that works with environment variables.
///
/// Workaround for <https://github.com/TeXitoi/structopt/issues/428>
#[derive(Debug, Clone, Copy)]
pub enum BooleanFlag {
    True,
    False,
}

impl std::str::FromStr for BooleanFlag {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s.to_ascii_lowercase().as_str() {
            "yes" | "y" | "true" | "t" | "1" => Ok(Self::True),
            "no" | "n" | "false" | "f" | "0" => Ok(Self::False),
            _ => Err(format!(
                "Invalid boolean flag '{}'. Valid options: yes, no, y, n, true, false, t, f, 1, 0",
                s
            )),
        }
    }
}

impl From<BooleanFlag> for bool {
    fn from(yes_no: BooleanFlag) -> Self {
        matches!(yes_no, BooleanFlag::True)
    }
}
#[derive(Debug, StructOpt)]
#[structopt(
    name = "run",
    about = "Runs in server mode",
    long_about = "Run the IOx server.\n\nThe configuration options below can be \
    set either with the command line flags or with the specified environment \
    variable. If there is a file named '.env' in the current working directory, \
    it is sourced before loading the configuration.

Configuration is loaded from the following sources (highest precedence first):
        - command line arguments
        - user set environment variables
        - .env file contents
        - pre-configured default values"
)]
pub struct Config {
    // logging options
    #[structopt(flatten)]
    pub(crate) logging_config: LoggingConfig,

    // tracing options
    #[structopt(flatten)]
    pub(crate) tracing_config: TracingConfig,

    // object store config
    #[structopt(flatten)]
    pub(crate) object_store_config: ObjectStoreConfig,

    // object store config
    #[structopt(flatten)]
    pub(crate) server_id_config: ServerIdConfig,

    /// The address on which IOx will serve HTTP API requests.
    #[structopt(
    long = "--api-bind",
    env = "INFLUXDB_IOX_BIND_ADDR",
    default_value = DEFAULT_API_BIND_ADDR,
    parse(try_from_str = parse_socket_addr),
    )]
    pub http_bind_address: SocketAddr,

    /// The address on which IOx will serve Storage gRPC API requests.
    #[structopt(
    long = "--grpc-bind",
    env = "INFLUXDB_IOX_GRPC_BIND_ADDR",
    default_value = DEFAULT_GRPC_BIND_ADDR,
    parse(try_from_str = parse_socket_addr),
    )]
    pub grpc_bind_address: SocketAddr,

    /// The number of threads to use for all worker pools.
    ///
    /// IOx uses a pool with `--num-threads` threads *each* for
    /// 1. Handling API requests
    /// 2. Running queries.
    /// 3. Reorganizing data (e.g. compacting chunks)
    ///
    /// If not specified, defaults to the number of cores on the system
    #[structopt(long = "--num-worker-threads", env = "INFLUXDB_IOX_NUM_WORKER_THREADS")]
    pub num_worker_threads: Option<usize>,

    /// When IOx nodes need to talk to remote peers they consult an internal remote address
    /// mapping. This mapping is populated via API calls. If the mapping doesn't produce
    /// a result, this config entry allows to generate a hostname from at template:
    /// occurrences of the "{id}" substring will be replaced with the remote Server ID.
    ///
    /// Example: http://node-{id}.ioxmydomain.com:8082
    #[structopt(long = "--remote-template", env = "INFLUXDB_IOX_REMOTE_TEMPLATE")]
    pub remote_template: Option<String>,

    /// After startup the IOx server can either accept serving data plane traffic right away
    /// or require a SetServingReadiness call from the Management API to enable serving.
    #[structopt(
        long = "--initial-serving-readiness-state",
        env = "INFLUXDB_IOX_INITIAL_SERVING_READINESS_STATE",
        default_value = "serving"
    )]
    pub initial_serving_state: ServingReadinessState,

    /// Maximum size of HTTP requests.
    #[structopt(
        long = "--max-http-request-size",
        env = "INFLUXDB_IOX_MAX_HTTP_REQUEST_SIZE",
        default_value = "10485760" // 10 MiB
    )]
    pub max_http_request_size: usize,

    /// Automatically wipe the preserved catalog on error
    #[structopt(
        long = "--wipe-catalog-on-error",
        env = "INFLUXDB_IOX_WIPE_CATALOG_ON_ERROR",
        // TODO: Don't automatically wipe on error (#1522)
        default_value = "yes"
    )]
    pub wipe_catalog_on_error: BooleanFlag,

    /// Skip replaying the write buffer and seek to high watermark instead.
    #[structopt(
        long = "--skip-replay",
        env = "INFLUXDB_IOX_SKIP_REPLAY",
        default_value = "no"
    )]
    pub skip_replay_and_seek_instead: BooleanFlag,
}

pub async fn command(config: Config) -> Result<()> {
    Ok(influxdb_ioxd::main(config).await?)
}

fn parse_socket_addr(s: &str) -> std::io::Result<SocketAddr> {
    let mut addrs = s.to_socket_addrs()?;
    // when name resolution fails, to_socket_address returns a validation error
    // so generally there is at least one result address, unless the resolver is
    // drunk.
    Ok(addrs
        .next()
        .expect("name resolution should return at least one address"))
}

#[cfg(test)]
mod tests {

    use super::*;

    use std::net::{Ipv4Addr, Ipv6Addr, SocketAddrV4, SocketAddrV6};

    fn to_vec(v: &[&str]) -> Vec<String> {
        v.iter().map(|s| s.to_string()).collect()
    }

    #[test]
    fn test_socketaddr() {
        let c =
            Config::from_iter_safe(to_vec(&["server", "--api-bind", "127.0.0.1:1234"]).into_iter())
                .unwrap();
        assert_eq!(
            c.http_bind_address,
            SocketAddr::from(([127, 0, 0, 1], 1234))
        );

        let c =
            Config::from_iter_safe(to_vec(&["server", "--api-bind", "localhost:1234"]).into_iter())
                .unwrap();
        // depending on where the test runs, localhost will either resolve to a ipv4 or
        // an ipv6 addr.
        match c.http_bind_address {
            SocketAddr::V4(so) => {
                assert_eq!(so, SocketAddrV4::new(Ipv4Addr::new(127, 0, 0, 1), 1234))
            }
            SocketAddr::V6(so) => assert_eq!(
                so,
                SocketAddrV6::new(Ipv6Addr::new(0, 0, 0, 0, 0, 0, 0, 1), 1234, 0, 0)
            ),
        };

        assert_eq!(
            Config::from_iter_safe(
                to_vec(&["server", "--api-bind", "!@INv_a1d(ad0/resp_!"]).into_iter(),
            )
            .map_err(|e| e.kind)
            .expect_err("must fail"),
            clap::ErrorKind::ValueValidation
        );
    }
}
