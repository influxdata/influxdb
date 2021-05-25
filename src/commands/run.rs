//! Implementation of command line option for running server

use crate::commands::tracing;
use crate::influxdb_ioxd::{self, serving_readiness::ServingReadinessState};
use clap::arg_enum;
use core::num::NonZeroU16;
use data_types::server_id::ServerId;
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
    /// Logs: filter directive
    ///
    /// Configures log severity level filter, by target.
    ///
    /// Simplest options: error, warn, info, debug, trace
    ///
    /// Levels for different modules can be specified. For example
    /// `debug,hyper::proto::h1=info` specifies debug logging for all modules
    /// except for the `hyper::proto::h1' module which will only display info
    /// level logging.
    ///
    /// Extended syntax provided by `tracing-subscriber` includes span/field
    /// filters. See <https://docs.rs/tracing-subscriber/0.2.17/tracing_subscriber/filter/struct.EnvFilter.html> for more details.
    #[structopt(long = "--log-filter", env = "LOG_FILTER", default_value = "warn")]
    pub log_filter: String,

    /// Logs: filter short-hand
    ///
    /// Convenient way to set log severity level filter.
    /// Overrides `--log-filter`.
    ///
    /// -v   'info'
    ///
    /// -vv  'debug,hyper::proto::h1=info,h2=info'
    ///
    /// -vvv 'trace,hyper::proto::h1=info,h2=info'
    #[structopt(
        short = "-v",
        long = "--verbose",
        multiple = true,
        takes_value = false,
        parse(from_occurrences)
    )]
    pub log_verbose_count: u8,

    #[rustfmt::skip]
    /// Logs: message format
    ///
    /// Can be one of:
    ///
    /// full: human-readable, single line
    ///
    ///   Oct 24 12:55:47.815 ERROR shaving_yaks{yaks=3}: fmt::yak_shave: failed to shave yak yak=3 error=missing yak
    ///   Oct 24 12:55:47.815 TRACE shaving_yaks{yaks=3}: fmt::yak_shave: yaks_shaved=2
    ///   Oct 24 12:55:47.815  INFO fmt: yak shaving completed all_yaks_shaved=false
    ///
    /// pretty: human-readable, multi line
    ///
    ///   Oct 24 12:57:29.387 fmt_pretty::yak_shave: failed to shave yak, yak: 3, error: missing yak
    ///     at examples/examples/fmt/yak_shave.rs:48 on main
    ///     in fmt_pretty::yak_shave::shaving_yaks with yaks: 3
    ///
    ///   Oct 24 12:57:29.387 fmt_pretty::yak_shave: yaks_shaved: 2
    ///     at examples/examples/fmt/yak_shave.rs:52 on main
    ///     in fmt_pretty::yak_shave::shaving_yaks with yaks: 3
    ///
    ///   Oct 24 12:57:29.387 fmt_pretty: yak shaving completed, all_yaks_shaved: false
    ///     at examples/examples/fmt-pretty.rs:19 on main
    ///
    /// json: machine-parseable
    ///
    ///   {"timestamp":"Oct 24 13:00:00.875","level":"ERROR","fields":{"message":"failed to shave yak","yak":3,"error":"missing yak"},"target":"fmt_json::yak_shave","spans":[{"yaks":3,"name":"shaving_yaks"}]}
    ///   {"timestamp":"Oct 24 13:00:00.875","level":"TRACE","fields":{"yaks_shaved":2},"target":"fmt_json::yak_shave","spans":[{"yaks":3,"name":"shaving_yaks"}]}
    ///   {"timestamp":"Oct 24 13:00:00.875","level":"INFO","fields":{"message":"yak shaving completed","all_yaks_shaved":false},"target":"fmt_json"}
    ///
    /// logfmt: human-readable and machine-parseable
    ///
    ///   level=info msg="This is an info message" target="logging" location="logfmt/tests/logging.rs:36" time=1612181556329599000
    ///   level=debug msg="This is a debug message" target="logging" location="logfmt/tests/logging.rs:37" time=1612181556329618000
    ///   level=trace msg="This is a trace message" target="logging" location="logfmt/tests/logging.rs:38" time=1612181556329634000
    #[structopt(long = "--log-format", env = "LOG_FORMAT", default_value = "full", verbatim_doc_comment)]
    pub log_format: tracing::LogFormat,

    /// Logs: destination
    ///
    /// Can be one of: stdout, stderr
    //
    // TODO(jacobmarble): consider adding file path, file rotation, syslog, ?
    #[structopt(
        long = "--log-destination",
        env = "LOG_DESTINATION",
        default_value = "stdout",
        verbatim_doc_comment
    )]
    pub log_destination: tracing::LogDestination,

    /// Tracing: exporter type
    ///
    /// Can be one of: none, jaeger, otlp
    ///
    /// When enabled, additional flags are considered (see flags related to OTLP
    /// and Jaeger), and log output is disabled.
    //
    // TODO(jacobmarble): allow logs and traces simultaneously
    #[structopt(
        long = "--traces-exporter",
        env = "TRACES_EXPORTER",
        default_value = "none"
    )]
    pub traces_exporter: tracing::TracesExporter,

    /// Tracing: filter directive
    ///
    /// Configures traces severity level filter, by target.
    ///
    /// Simplest options: error, warn, info, debug, trace
    ///
    /// Levels for different modules can be specified. For example
    /// `debug,hyper::proto::h1=info` specifies debug tracing for all modules
    /// except for the `hyper::proto::h1` module which will only display info
    /// level tracing.
    ///
    /// Extended syntax provided by `tracing-subscriber` includes span/field
    /// filters. See <https://docs.rs/tracing-subscriber/0.2.17/tracing_subscriber/filter/struct.EnvFilter.html> for more details.
    ///
    /// No filter by default.
    #[structopt(long = "--traces-filter", env = "TRACES_FILTER")]
    pub traces_filter: Option<String>,

    /// Tracing: sampler type
    ///
    /// Can be one of:
    /// always_on, always_off, traceidratio,
    /// parentbased_always_on, parentbased_always_off, parentbased_traceidratio
    ///
    /// These alternatives are described in detail at <https://github.com/open-telemetry/opentelemetry-specification/blob/v1.1.0/specification/sdk-environment-variables.md#general-sdk-configuration>.
    #[structopt(
        long = "--traces-sampler",
        env = "TRACES_SAMPLER",
        default_value = "parentbased_traceidratio"
    )]
    pub traces_sampler: tracing::TracesSampler,

    #[rustfmt::skip]
    /// Tracing: sampler argument
    ///
    /// Valid range: [0.0, 1.0].
    ///
    /// Only used if `--traces-sampler` is set to
    /// parentbased_traceidratio (default) or traceidratio.
    ///
    /// With sample parentbased_traceidratio, the following rules apply:
    /// - if parent is sampled, then all of its children are sampled
    /// - else sample this portion of traces (0.5 = 50%)
    ///
    /// More details about this sampling argument at <https://github.com/open-telemetry/opentelemetry-specification/blob/v1.1.0/specification/sdk-environment-variables.md#general-sdk-configuration>.
    #[structopt(
        long = "--traces-sampler-arg",
        env = "TRACES_SAMPLER_ARG",
        default_value = "1.0",
        verbatim_doc_comment
    )]
    pub traces_sampler_arg: f64,

    /// Tracing: OTLP (eg OpenTelemetry collector) network hostname
    ///
    /// Only used if `--traces-exporter` is "otlp".
    ///
    /// Protocol is gRPC. HTTP is not supported.
    #[structopt(
        long = "--traces-exporter-otlp-host",
        env = "TRACES_EXPORTER_OTLP_HOST",
        default_value = "localhost"
    )]
    pub traces_exporter_otlp_host: String,

    /// Tracing: OTLP (eg OpenTelemetry collector) network port
    ///
    /// Only used if `--traces-exporter` is "otlp".
    ///
    /// Protocol is gRPC. HTTP is not supported.
    #[structopt(
        long = "--traces-exporter-otlp-port",
        env = "TRACES_EXPORTER_OTLP_PORT",
        default_value = "4317"
    )]
    pub traces_exporter_otlp_port: NonZeroU16,

    /// Tracing: Jaeger agent network hostname
    ///
    /// Protocol is Thrift/Compact over UDP.
    ///
    /// Only used if `--traces-exporter` is "jaeger".
    #[structopt(
        long = "--traces-exporter-jaeger-agent-host",
        env = "TRACES_EXPORTER_JAEGER_AGENT_HOST",
        default_value = "0.0.0.0"
    )]
    pub traces_exporter_jaeger_agent_host: String,

    /// Tracing: Jaeger agent network port
    ///
    /// Protocol is Thrift/Compact over UDP.
    ///
    /// Only used if `--traces-exporter` is "jaeger".
    #[structopt(
        long = "--traces-exporter-jaeger-agent-port",
        env = "TRACES_EXPORTER_JAEGER_AGENT_PORT",
        default_value = "6831"
    )]
    pub traces_exporter_jaeger_agent_port: NonZeroU16,

    /// The identifier for the server.
    ///
    /// Used for writing to object storage and as an identifier that is added to
    /// replicated writes, write buffer segments, and Chunks. Must be unique in
    /// a group of connected or semi-connected IOx servers. Must be a nonzero
    /// number that can be represented by a 32-bit unsigned integer.
    #[structopt(long = "--server-id", env = "INFLUXDB_IOX_ID")]
    pub server_id: Option<ServerId>,

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

    /// The number of threads to use for the query worker pool.
    ///
    /// IOx uses `--num-threads` threads for handling API requests and
    /// will use a dedicated thread pool with `--num-worker-threads`
    /// for running queries.
    ///
    /// If not specified, defaults to the number of cores on the system
    #[structopt(long = "--num-worker-threads", env = "INFLUXDB_IOX_NUM_WORKER_THREADS")]
    pub num_worker_threads: Option<usize>,

    #[structopt(
    long = "--object-store",
    env = "INFLUXDB_IOX_OBJECT_STORE",
    possible_values = &ObjectStore::variants(),
    case_insensitive = true,
    long_help = r#"Which object storage to use. If not specified, defaults to memory.

Possible values (case insensitive):

* memory (default): Effectively no object persistence.
* memorythrottled: Like `memory` but with latency and throughput that somewhat resamble a cloud
   object store. Useful for testing and benchmarking.
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

    /// When using Amazon s3 compatibility storage service, set this to the
    /// endpoint.
    ///
    /// Must also set `--object-store=s3`, `--bucket`. Can also set `--aws-default-region`
    /// if not using the fallback region.
    ///
    /// Prefer the environment variable over the command line flag in shared
    /// environments.
    #[structopt(long = "--aws-endpoint", env = "AWS_ENDPOINT")]
    pub aws_endpoint: Option<String>,

    /// When using Google Cloud Storage as the object store, set this to the
    /// path to the JSON file that contains the Google credentials.
    ///
    /// Must also set `--object-store=google` and `--bucket`.
    #[structopt(long = "--google-service-account", env = "GOOGLE_SERVICE_ACCOUNT")]
    pub google_service_account: Option<String>,

    /// When using Microsoft Azure as the object store, set this to the
    /// name you see when going to All Services > Storage accounts > `[name]`.
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

arg_enum! {
    #[derive(Debug, Copy, Clone, PartialEq)]
    pub enum ObjectStore {
        Memory,
        MemoryThrottled,
        File,
        S3,
        Google,
        Azure,
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
