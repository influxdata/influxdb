//! CLI config for the router using the RPC write path

use crate::ingester_address::IngesterAddress;
use std::{
    num::{NonZeroUsize, ParseIntError},
    time::Duration,
};

/// CLI config for the router using the RPC write path
#[derive(Debug, Clone, clap::Parser)]
#[allow(missing_copy_implementations)]
pub struct Router2Config {
    /// Differential handling based upon deployment to CST vs MT.
    ///
    /// At minimum, differs in supports of v1 endpoint. But also includes
    /// differences in namespace handling, etc.
    #[clap(
        long = "single-tenancy",
        env = "INFLUXDB_IOX_SINGLE_TENANCY",
        default_value = "false"
    )]
    pub single_tenant_deployment: bool,

    /// The maximum number of simultaneous requests the HTTP server is
    /// configured to accept.
    ///
    /// This number of requests, multiplied by the maximum request body size the
    /// HTTP server is configured with gives the rough amount of memory a HTTP
    /// server will use to buffer request bodies in memory.
    ///
    /// A default maximum of 200 requests, multiplied by the default 10MiB
    /// maximum for HTTP request bodies == ~2GiB.
    #[clap(
        long = "max-http-requests",
        env = "INFLUXDB_IOX_MAX_HTTP_REQUESTS",
        default_value = "200",
        action
    )]
    pub http_request_limit: usize,

    /// gRPC address for the router to talk with the ingesters. For
    /// example:
    ///
    /// "http://127.0.0.1:8083"
    ///
    /// or
    ///
    /// "http://10.10.10.1:8083,http://10.10.10.2:8083"
    ///
    /// for multiple addresses.
    #[clap(
        long = "ingester-addresses",
        env = "INFLUXDB_IOX_INGESTER_ADDRESSES",
        required = true,
        num_args=1..,
        value_delimiter = ','
    )]
    pub ingester_addresses: Vec<IngesterAddress>,

    /// Write buffer topic/database that should be used.
    // This isn't really relevant to the RPC write path and will be removed eventually.
    #[clap(
        long = "write-buffer-topic",
        env = "INFLUXDB_IOX_WRITE_BUFFER_TOPIC",
        default_value = "iox-shared",
        action
    )]
    pub topic: String,

    /// Query pool name to dispatch writes to.
    // This isn't really relevant to the RPC write path and will be removed eventually.
    #[clap(
        long = "query-pool",
        env = "INFLUXDB_IOX_QUERY_POOL_NAME",
        default_value = "iox-shared",
        action
    )]
    pub query_pool_name: String,

    /// Retention period to use when auto-creating namespaces.
    /// For infinite retention, leave this unset and it will default to `None`.
    /// Setting it to zero will not make it infinite.
    /// Ignored if namespace-autocreation-enabled is set to false.
    #[clap(
        long = "new-namespace-retention-hours",
        env = "INFLUXDB_IOX_NEW_NAMESPACE_RETENTION_HOURS",
        action
    )]
    pub new_namespace_retention_hours: Option<u64>,

    /// When writing data to a non-existent namespace, should the router auto-create the namespace
    /// or reject the write? Set to false to disable namespace autocreation.
    #[clap(
        long = "namespace-autocreation-enabled",
        env = "INFLUXDB_IOX_NAMESPACE_AUTOCREATION_ENABLED",
        default_value = "true",
        action
    )]
    pub namespace_autocreation_enabled: bool,

    /// A "strftime" format string used to derive the partition key from the row
    /// timestamps.
    ///
    /// Changing this from the default value is experimental.
    #[clap(
        long = "partition-key-pattern",
        env = "INFLUXDB_IOX_PARTITION_KEY_PATTERN",
        default_value = "%Y-%m-%d",
        action
    )]
    pub partition_key_pattern: String,

    /// Specify the timeout in seconds for a single RPC write request to an
    /// ingester.
    #[clap(
        long = "rpc-write-timeout-seconds",
        env = "INFLUXDB_IOX_RPC_WRITE_TIMEOUT_SECONDS",
        default_value = "3",
        value_parser = parse_duration
    )]
    pub rpc_write_timeout_seconds: Duration,

    /// Specify the optional replication factor for each RPC write.
    ///
    /// The total number of copies of data after replication will be this value,
    /// plus 1.
    ///
    /// If the desired replication level is not achieved, a partial write error
    /// will be returned to the user. The write MAY be queryable after a partial
    /// write failure.
    #[clap(long = "rpc-write-replicas", env = "INFLUXDB_IOX_RPC_WRITE_REPLICAS")]
    pub rpc_write_replicas: Option<NonZeroUsize>,
}

/// Map a string containing an integer number of seconds into a [`Duration`].
fn parse_duration(input: &str) -> Result<Duration, ParseIntError> {
    input.parse().map(Duration::from_secs)
}
