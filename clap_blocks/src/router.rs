//! CLI config for the router using the RPC write path

use crate::{
    gossip::GossipConfig,
    ingester_address::IngesterAddress,
    single_tenant::{
        CONFIG_AUTHZ_ENV_NAME, CONFIG_AUTHZ_FLAG, CONFIG_CST_ENV_NAME, CONFIG_CST_FLAG,
    },
};
use std::{
    num::{NonZeroUsize, ParseIntError},
    time::Duration,
};

/// CLI config for the router using the RPC write path
#[derive(Debug, Clone, clap::Parser)]
#[allow(missing_copy_implementations)]
pub struct RouterConfig {
    /// Gossip config.
    #[clap(flatten)]
    pub gossip_config: GossipConfig,

    /// Addr for connection to authz
    #[clap(
        long = CONFIG_AUTHZ_FLAG,
        env = CONFIG_AUTHZ_ENV_NAME,
        requires("single_tenant_deployment"),
    )]
    pub authz_address: Option<String>,

    /// Differential handling based upon deployment to CST vs MT.
    ///
    /// At minimum, differs in supports of v1 endpoint. But also includes
    /// differences in namespace handling, etc.
    #[clap(
        long = CONFIG_CST_FLAG,
        env = CONFIG_CST_ENV_NAME,
        default_value = "false",
        requires_if("true", "authz_address")
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

    /// Specify the timeout in seconds for a single RPC write request to an
    /// ingester.
    #[clap(
        long = "rpc-write-timeout-seconds",
        env = "INFLUXDB_IOX_RPC_WRITE_TIMEOUT_SECONDS",
        default_value = "3",
        value_parser = parse_duration
    )]
    pub rpc_write_timeout_seconds: Duration,

    /// Specify the maximum allowed outgoing RPC write message size when
    /// communicating with the Ingester.
    #[clap(
        long = "rpc-write-max-outgoing-bytes",
        env = "INFLUXDB_IOX_RPC_WRITE_MAX_OUTGOING_BYTES",
        default_value = "104857600", // 100MiB
    )]
    pub rpc_write_max_outgoing_bytes: usize,

    /// Enable optional replication for each RPC write.
    ///
    /// This value specifies the total number of copies of data after
    /// replication, defaulting to 1.
    ///
    /// If the desired replication level is not achieved, a partial write error
    /// will be returned to the user. The write MAY be queryable after a partial
    /// write failure.
    #[clap(
        long = "rpc-write-replicas",
        env = "INFLUXDB_IOX_RPC_WRITE_REPLICAS",
        default_value = "1"
    )]
    pub rpc_write_replicas: NonZeroUsize,

    /// Specify the (discrete) slices of time in which the router's write
    /// request failures must exceed the write client's maximum error ratio of
    /// 80% for a downstream RPC write handler to be considered in the unhealthy
    /// state.
    #[clap(
        long = "rpc-write-health-error-window-seconds",
        env = "INFLUXDB_IOX_RPC_WRITE_HEALTH_ERROR_WINDOW_SECONDS",
        default_value = "5",
        value_parser = parse_duration
    )]
    pub rpc_write_health_error_window_seconds: Duration,

    /// Specify the maximum number of probe requests to be sent per second.
    ///
    /// At least 20% of these requests must succeed within a second for the
    /// endpoint to be considered healthy.
    #[clap(
        long = "rpc-write-health-num-probes",
        env = "INFLUXDB_IOX_RPC_WRITE_HEALTH_NUM_PROBES",
        default_value = "10"
    )]
    pub rpc_write_health_num_probes: u64,
}

/// Map a string containing an integer number of seconds into a [`Duration`].
fn parse_duration(input: &str) -> Result<Duration, ParseIntError> {
    input.parse().map(Duration::from_secs)
}
