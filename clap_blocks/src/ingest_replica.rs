//! CLI config for the ingest_replica

use crate::ingester_address::IngesterAddress;

/// CLI config for the ingest_replica
#[derive(Debug, Clone, clap::Parser)]
#[allow(missing_copy_implementations)]
pub struct IngestReplicaConfig {
    /// gRPC address for the replica to talk with the ingesters. For
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

    /// Sets how many queries the replica will handle simultaneously before
    /// rejecting further incoming requests.
    #[clap(
        long = "concurrent-query-limit",
        env = "INFLUXDB_IOX_CONCURRENT_QUERY_LIMIT",
        default_value = "200",
        action
    )]
    pub concurrent_query_limit: usize,
}
