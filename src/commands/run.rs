//! Implementation of command line option for running server

use crate::commands::logging::LoggingLevel;
use crate::influxdb_ioxd;
use clap::arg_enum;
use std::num::NonZeroU32;
use std::{net::SocketAddr, net::ToSocketAddrs, path::PathBuf};
use structopt::StructOpt;
use thiserror::Error;

/// The default bind address for the HTTP API.
pub const DEFAULT_API_BIND_ADDR: &str = "127.0.0.1:8080";

/// The default bind address for the gRPC.
pub const DEFAULT_GRPC_BIND_ADDR: &str = "127.0.0.1:8082";

/// The AWS region to use for Amazon S3 based object storage if none is
/// specified.
pub const FALLBACK_AWS_REGION: &str = "us-east-1";

#[derive(Debug, Error)]
pub enum Error {
    #[error("Run: {0}")]
    ServerError(#[from] influxdb_ioxd::Error),
}

pub type Result<T, E = Error> = std::result::Result<T, E>;

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
    pub writer_id: Option<NonZeroU32>,

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

    #[structopt(
    long = "--object-store",
    env = "INFLUXDB_IOX_OBJECT_STORE",
    possible_values = &ObjectStore::variants(),
    case_insensitive = true,
    long_help = r#"Which object storage to use. If not specified, defaults to memory.

Possible values (case insensitive):

* memory (default): Effectively no object persistence.
* file: Stores objects in the local filesystem. Must also set `--data-dir`.
* s3: Amazon S3. Must also set `--bucket`, `--aws-access-key-id`, `--aws-secret-access-key`, and
   possibly `--aws-default-region`.
* google: Google Cloud Storage. Must also set `--bucket` and `--google-service-account`.
* azure: Microsoft Azure blob storage. Must also set `--bucket`, `--azure-storage-account`,
   and `--azure-storage-access-key`.
        "#,
    )]
    pub object_store: Option<ObjectStore>,

    /// Name of the bucket to use for the object store. Must also set
    /// `--object-store` to a cloud object storage to have any effect.
    ///
    /// If using Google Cloud Storage for the object store, this item as well
    /// as `--google-service-account` must be set.
    ///
    /// If using S3 for the object store, must set this item as well
    /// as `--aws-access-key-id` and `--aws-secret-access-key`. Can also set
    /// `--aws-default-region` if not using the fallback region.
    ///
    /// If using Azure for the object store, set this item to the name of a
    /// container you've created in the associated storage account, under
    /// Blob Service > Containers. Must also set `--azure-storage-account` and
    /// `--azure-storage-access-key`.
    #[structopt(long = "--bucket", env = "INFLUXDB_IOX_BUCKET")]
    pub bucket: Option<String>,

    /// When using Amazon S3 as the object store, set this to an access key that
    /// has permission to read from and write to the specified S3 bucket.
    ///
    /// Must also set `--object-store=s3`, `--bucket`, and
    /// `--aws-secret-access-key`. Can also set `--aws-default-region` if not
    /// using the fallback region.
    ///
    /// Prefer the environment variable over the command line flag in shared
    /// environments.
    #[structopt(long = "--aws-access-key-id", env = "AWS_ACCESS_KEY_ID")]
    pub aws_access_key_id: Option<String>,

    /// When using Amazon S3 as the object store, set this to the secret access
    /// key that goes with the specified access key ID.
    ///
    /// Must also set `--object-store=s3`, `--bucket`, `--aws-access-key-id`.
    /// Can also set `--aws-default-region` if not using the fallback region.
    ///
    /// Prefer the environment variable over the command line flag in shared
    /// environments.
    #[structopt(long = "--aws-secret-access-key", env = "AWS_SECRET_ACCESS_KEY")]
    pub aws_secret_access_key: Option<String>,

    /// When using Amazon S3 as the object store, set this to the region
    /// that goes with the specified bucket if different from the fallback
    /// value.
    ///
    /// Must also set `--object-store=s3`, `--bucket`, `--aws-access-key-id`,
    /// and `--aws-secret-access-key`.
    #[structopt(
    long = "--aws-default-region",
    env = "AWS_DEFAULT_REGION",
    default_value = FALLBACK_AWS_REGION,
    )]
    pub aws_default_region: String,

    /// When using Google Cloud Storage as the object store, set this to the
    /// path to the JSON file that contains the Google credentials.
    ///
    /// Must also set `--object-store=google` and `--bucket`.
    #[structopt(long = "--google-service-account", env = "GOOGLE_SERVICE_ACCOUNT")]
    pub google_service_account: Option<String>,

    /// When using Microsoft Azure as the object store, set this to the
    /// name you see when going to All Services > Storage accounts > [name].
    ///
    /// Must also set `--object-store=azure`, `--bucket`, and
    /// `--azure-storage-access-key`.
    #[structopt(long = "--azure-storage-account", env = "AZURE_STORAGE_ACCOUNT")]
    pub azure_storage_account: Option<String>,

    /// When using Microsoft Azure as the object store, set this to one of the
    /// Key values in the Storage account's Settings > Access keys.
    ///
    /// Must also set `--object-store=azure`, `--bucket`, and
    /// `--azure-storage-account`.
    ///
    /// Prefer the environment variable over the command line flag in shared
    /// environments.
    #[structopt(long = "--azure-storage-access-key", env = "AZURE_STORAGE_ACCESS_KEY")]
    pub azure_storage_access_key: Option<String>,

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

pub async fn command(logging_level: LoggingLevel, config: Config) -> Result<()> {
    Ok(influxdb_ioxd::main(logging_level, config).await?)
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

arg_enum! {
    #[derive(Debug, Copy, Clone, PartialEq)]
    pub enum ObjectStore {
        Memory,
        File,
        S3,
        Google,
        Azure,
    }
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
