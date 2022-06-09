use snafu::Snafu;
use std::collections::BTreeSet;

#[derive(Debug, Snafu)]
pub enum Error {
    #[snafu(display("ingester address '{}' was repeated", ingester_address))]
    RepeatedAddress { ingester_address: String },
}

/// CLI config for querier configuration
#[derive(Debug, Clone, PartialEq, clap::Parser)]
pub struct QuerierConfig {
    /// The number of threads to use for queries.
    ///
    /// If not specified, defaults to the number of cores on the system
    #[clap(
        long = "--num-query-threads",
        env = "INFLUXDB_IOX_NUM_QUERY_THREADS",
        action
    )]
    pub num_query_threads: Option<usize>,

    /// gRPC address for the querier to talk with the ingester. For
    /// example:
    ///
    /// "http://127.0.0.1:8083"
    ///
    /// or
    ///
    /// "http://10.10.10.1:8083,http://10.10.10.2:8083"
    ///
    /// for multiple addresses.
    ///
    /// Note we plan to improve this interface in
    /// <https://github.com/influxdata/influxdb_iox/issues/3996>
    #[clap(
        long = "--ingester-address",
        env = "INFLUXDB_IOX_INGESTER_ADDRESSES",
        multiple_values = true,
        use_value_delimiter = true,
        action
    )]
    pub ingester_addresses: Vec<String>,

    /// Size of the RAM cache pool in bytes.
    #[clap(
        long = "--ram-pool-bytes",
        env = "INFLUXDB_IOX_RAM_POOL_BYTES",
        default_value = "1073741824",
        action
    )]
    pub ram_pool_bytes: usize,

    /// Limit the number of concurrent queries.
    #[clap(
        long = "--max-concurrent-queries",
        env = "INFLUXDB_IOX_MAX_CONCURRENT_QUERIES",
        default_value = "10",
        action
    )]
    pub max_concurrent_queries: usize,
}

impl QuerierConfig {
    /// Get the querier config's num query threads.
    #[must_use]
    pub fn num_query_threads(&self) -> Option<usize> {
        self.num_query_threads
    }

    /// Return the querier config's ingester addresses. Returns `Err`
    /// if any of the addresses is repeated
    pub fn ingester_addresses(&self) -> Result<Vec<String>, Error> {
        let mut current_addresses = BTreeSet::new();
        self.ingester_addresses
            .iter()
            .map(|ingester_address| {
                if current_addresses.contains(ingester_address) {
                    RepeatedAddressSnafu { ingester_address }.fail()
                } else {
                    current_addresses.insert(ingester_address);
                    Ok(ingester_address.clone())
                }
            })
            .collect()
    }

    /// Size of the RAM cache pool in bytes.
    pub fn ram_pool_bytes(&self) -> usize {
        self.ram_pool_bytes
    }

    /// Number of queries allowed to run concurrently
    pub fn max_concurrent_queries(&self) -> usize {
        self.max_concurrent_queries
    }
}

#[cfg(test)]
mod tests {
    use clap::StructOpt;

    use super::*;

    #[test]
    fn test_default() {
        let actual = QuerierConfig::try_parse_from(["my_binary"]).unwrap();

        assert_eq!(actual.num_query_threads(), None);
        assert!(actual.ingester_addresses().unwrap().is_empty());
    }

    #[test]
    fn test_num_threads() {
        let actual =
            QuerierConfig::try_parse_from(["my_binary", "--num-query-threads", "42"]).unwrap();

        assert_eq!(actual.num_query_threads(), Some(42));
        assert!(actual.ingester_addresses().unwrap().is_empty());
    }

    #[test]
    fn test_one_ingester_address() {
        let actual = QuerierConfig::try_parse_from([
            "my_binary",
            "--ingester-address",
            "http://127.0.0.1:9090",
        ])
        .unwrap();

        assert_eq!(actual.num_query_threads(), None);
        assert_eq!(
            actual.ingester_addresses().unwrap(),
            &["http://127.0.0.1:9090".to_string()]
        );
    }

    #[test]
    fn test_multiple_ingester_addresses() {
        let actual = QuerierConfig::try_parse_from([
            "my_binary",
            "--ingester-address",
            "http://127.0.0.1:9090,http://10.10.2.11:8080",
        ])
        .unwrap();

        assert_eq!(actual.num_query_threads(), None);
        assert_eq!(
            actual.ingester_addresses().unwrap(),
            &[
                "http://127.0.0.1:9090".to_string(),
                "http://10.10.2.11:8080".to_string()
            ]
        );
    }

    #[test]
    fn test_multiple_ingester_addresses_repeated() {
        let actual = QuerierConfig::try_parse_from([
            "my_binary",
            "--ingester-address",
            "http://127.0.0.1:9090,http://10.10.2.11:8080,http://127.0.0.1:9090",
        ])
        .unwrap();

        assert_eq!(actual.num_query_threads(), None);
        assert_eq!(
            actual.ingester_addresses().unwrap_err().to_string(),
            "ingester address 'http://127.0.0.1:9090' was repeated"
        );
    }
}
