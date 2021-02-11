//! Implementation of command line option for manipulating and showing server
//! config

use std::{net::SocketAddr, net::ToSocketAddrs, path::PathBuf};

use lazy_static::lazy_static;
use structopt::StructOpt;

/// The default bind address for the HTTP API.
pub const DEFAULT_API_BIND_ADDR: &str = "127.0.0.1:8080";

/// The default bind address for the gRPC.
pub const DEFAULT_GRPC_BIND_ADDR: &str = "127.0.0.1:8082";

lazy_static! {
    static ref DEFAULT_DATA_DIR: String = dirs::home_dir()
        .map(|mut path| {
            path.push(".influxdb_iox");
            path
        })
        .and_then(|dir| dir.to_str().map(|s| s.to_string()))
        .unwrap();
}

#[derive(Debug, StructOpt)]
#[structopt(
    name = "server",
    about = "Runs in server mode (default)",
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
    /// This controls the IOx server logging level, as described in
    /// https://crates.io/crates/env_logger.
    ///
    /// Levels for different modules can be specified as well. For example
    /// `debug,hyper::proto::h1=info` specifies debug logging for all modules
    /// except for the `hyper::proto::h1' module which will only display info
    /// level logging.
    #[structopt(long = "--log", env = "RUST_LOG")]
    pub rust_log: Option<String>,

    /// Log message format. Can be one of:
    ///
    /// "rust" (default)
    /// "logfmt" (logfmt/Heroku style - https://brandur.org/logfmt)
    #[structopt(long = "--log_format", env = "INFLUXDB_IOX_LOG_FORMAT")]
    pub log_format: Option<LogFormat>,

    /// This sets logging up with a pre-configured set of convenient log levels.
    ///
    /// -v  means 'info' log levels
    /// -vv means 'verbose' log level (with the exception of some particularly
    /// low level libraries)
    ///
    /// This option is ignored if  --log / RUST_LOG are set
    #[structopt(
        short = "-v",
        long = "--verbose",
        multiple = true,
        takes_value = false,
        parse(from_occurrences)
    )]
    pub verbose_count: u64,

    /// The identifier for the server.
    ///
    /// Used for writing to object storage and as an identifier that is added to
    /// replicated writes, WAL segments and Chunks. Must be unique in a group of
    /// connected or semi-connected IOx servers. Must be a number that can be
    /// represented by a 32-bit unsigned integer.
    #[structopt(long = "--writer-id", env = "INFLUXDB_IOX_ID")]
    pub writer_id: Option<u32>,

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

    /// The location InfluxDB IOx will use to store files locally.
    #[structopt(long = "--data-dir", env = "INFLUXDB_IOX_DB_DIR")]
    pub database_directory: Option<PathBuf>,

    /// If using Google Cloud Storage for the object store, this item, as well
    /// as SERVICE_ACCOUNT must be set.
    #[structopt(long = "--gcp-bucket", env = "INFLUXDB_IOX_GCP_BUCKET")]
    pub gcp_bucket: Option<String>,

    /// If set, Jaeger traces are emitted to this host
    /// using the OpenTelemetry tracer.
    ///
    /// NOTE: The OpenTelemetry agent CAN ONLY be
    /// configured using environment variables. It CAN NOT be configured
    /// using the command line at this time. Some useful variables:
    ///
    /// * OTEL_SERVICE_NAME: emitter service name (iox by default)
    /// * OTEL_EXPORTER_JAEGER_AGENT_HOST: hostname/address of the collector
    /// * OTEL_EXPORTER_JAEGER_AGENT_PORT: listening port of the collector.
    ///
    /// The entire list of variables can be found in
    /// https://github.com/open-telemetry/opentelemetry-specification/blob/master/specification/sdk-environment-variables.md#jaeger-exporter
    #[structopt(
        long = "--oetl_exporter_jaeger_agent",
        env = "OTEL_EXPORTER_JAEGER_AGENT_HOST"
    )]
    pub jaeger_host: Option<String>,
}

/// Load the config if `server` was not specified on the command line
/// (from environment variables and default)
///
/// This pulls in config from the following sources, in order of precedence:
///
///     - user set environment variables
///     - .env file contents
///     - pre-configured default values
pub fn load_config() -> Config {
    // Load the Config struct - this pulls in any envs set by the user or
    // sourced above, and applies any defaults.
    //

    //let args = std::env::args().filter(|arg| arg != "server");
    Config::from_iter(strip_server(std::env::args()).iter())
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

/// Strip everything prior to the "server" portion of the args so the generated
/// Clap instance plays nicely with the subcommand bits in main.
fn strip_server(args: impl Iterator<Item = String>) -> Vec<String> {
    let mut seen_server = false;
    args.enumerate()
        .filter_map(|(i, arg)| {
            if i != 0 && !seen_server {
                if arg == "server" {
                    seen_server = true;
                }
                None
            } else {
                Some(arg)
            }
        })
        .collect::<Vec<_>>()
}

/// How to format output logging messages
#[derive(Debug, Clone, Copy)]
pub enum LogFormat {
    /// Default formatted logging
    ///
    /// Example:
    /// ```
    /// level=warn msg="NO PERSISTENCE: using memory for object storage" target="influxdb_iox::influxdb_ioxd"
    /// ```
    Rust,

    /// Use the (somwhat pretentiously named) Heroku / logfmt formatted output
    /// format
    ///
    /// Example:
    /// ```
    /// Jan 31 13:19:39.059  WARN influxdb_iox::influxdb_ioxd: NO PERSISTENCE: using memory for object storage
    /// ```
    LogFmt,
}

impl Default for LogFormat {
    fn default() -> Self {
        Self::Rust
    }
}

impl std::str::FromStr for LogFormat {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s.to_ascii_lowercase().as_str() {
            "rust" => Ok(Self::Rust),
            "logfmt" => Ok(Self::LogFmt),
            _ => Err(format!(
                "Invalid log format '{}'. Valid options: rust, logfmt",
                s
            )),
        }
    }
}

#[cfg(test)]
mod tests {

    use super::*;

    use std::net::{Ipv4Addr, Ipv6Addr, SocketAddrV4, SocketAddrV6};

    #[test]
    fn test_strip_server() {
        assert_eq!(
            strip_server(to_vec(&["cmd",]).into_iter()),
            to_vec(&["cmd"])
        );
        assert_eq!(
            strip_server(to_vec(&["cmd", "-v"]).into_iter()),
            to_vec(&["cmd"])
        );
        assert_eq!(
            strip_server(to_vec(&["cmd", "-v", "server"]).into_iter()),
            to_vec(&["cmd"])
        );
        assert_eq!(
            strip_server(to_vec(&["cmd", "-v", "server", "-v"]).into_iter()),
            to_vec(&["cmd", "-v"])
        );
        assert_eq!(
            strip_server(to_vec(&["cmd", "-v", "server", "-vv"]).into_iter()),
            to_vec(&["cmd", "-vv"])
        );

        // and it doesn't strip repeated instances of server
        assert_eq!(
            strip_server(to_vec(&["cmd", "-v", "server", "--gcp_path"]).into_iter()),
            to_vec(&["cmd", "--gcp_path"])
        );
        assert_eq!(
            strip_server(to_vec(&["cmd", "-v", "server", "--gcp_path", "server"]).into_iter()),
            to_vec(&["cmd", "--gcp_path", "server"])
        );

        assert_eq!(
            strip_server(to_vec(&["cmd", "-vv"]).into_iter()),
            to_vec(&["cmd"])
        );
        assert_eq!(
            strip_server(to_vec(&["cmd", "-vv", "server"]).into_iter()),
            to_vec(&["cmd"])
        );
        assert_eq!(
            strip_server(to_vec(&["cmd", "-vv", "server", "-vv"]).into_iter()),
            to_vec(&["cmd", "-vv"])
        );
    }

    fn to_vec(v: &[&str]) -> Vec<String> {
        v.iter().map(|s| s.to_string()).collect()
    }

    #[test]
    fn test_socketaddr() -> Result<(), clap::Error> {
        let c = Config::from_iter_safe(strip_server(
            to_vec(&["cmd", "server", "--api-bind", "127.0.0.1:1234"]).into_iter(),
        ))?;
        assert_eq!(
            c.http_bind_address,
            SocketAddr::from(([127, 0, 0, 1], 1234))
        );

        let c = Config::from_iter_safe(strip_server(
            to_vec(&["cmd", "server", "--api-bind", "localhost:1234"]).into_iter(),
        ))?;
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
            Config::from_iter_safe(strip_server(
                to_vec(&["cmd", "server", "--api-bind", "!@INv_a1d(ad0/resp_!"]).into_iter(),
            ))
            .map_err(|e| e.kind)
            .expect_err("must fail"),
            clap::ErrorKind::ValueValidation
        );

        assert_eq!(
            Config::from_iter_safe(strip_server(
                to_vec(&["cmd", "server", "--api-bind", "badhost.badtld:1234"]).into_iter(),
            ))
            .map_err(|e| e.kind)
            .expect_err("must fail"),
            clap::ErrorKind::ValueValidation
        );

        Ok(())
    }
}
