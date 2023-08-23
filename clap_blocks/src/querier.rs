//! Querier-related configs.

use crate::{
    ingester_address::IngesterAddress,
    memory_size::MemorySize,
    single_tenant::{CONFIG_AUTHZ_ENV_NAME, CONFIG_AUTHZ_FLAG},
};
use std::{collections::HashMap, num::NonZeroUsize};

/// CLI config for querier configuration
#[derive(Debug, Clone, PartialEq, Eq, clap::Parser)]
pub struct QuerierConfig {
    /// Addr for connection to authz
    #[clap(long = CONFIG_AUTHZ_FLAG, env = CONFIG_AUTHZ_ENV_NAME)]
    pub authz_address: Option<String>,

    /// The number of threads to use for queries.
    ///
    /// If not specified, defaults to the number of cores on the system
    #[clap(
        long = "num-query-threads",
        env = "INFLUXDB_IOX_NUM_QUERY_THREADS",
        action
    )]
    pub num_query_threads: Option<NonZeroUsize>,

    /// Size of memory pool used during query exec, in bytes.
    ///
    /// If queries attempt to allocate more than this many bytes
    /// during execution, they will error with "ResourcesExhausted".
    ///
    /// Can be given as absolute value or in percentage of the total available memory (e.g. `10%`).
    #[clap(
        long = "exec-mem-pool-bytes",
        env = "INFLUXDB_IOX_EXEC_MEM_POOL_BYTES",
        default_value = "8589934592",  // 8GB
        action
    )]
    pub exec_mem_pool_bytes: MemorySize,

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
        required = false,
        num_args = 0..,
        value_delimiter = ','
    )]
    pub ingester_addresses: Vec<IngesterAddress>,

    /// Size of the RAM cache used to store catalog metadata information in bytes.
    ///
    /// Can be given as absolute value or in percentage of the total available memory (e.g. `10%`).
    #[clap(
        long = "ram-pool-metadata-bytes",
        env = "INFLUXDB_IOX_RAM_POOL_METADATA_BYTES",
        default_value = "134217728",  // 128MB
        action
    )]
    pub ram_pool_metadata_bytes: MemorySize,

    /// Size of the RAM cache used to store data in bytes.
    ///
    /// Can be given as absolute value or in percentage of the total available memory (e.g. `10%`).
    #[clap(
        long = "ram-pool-data-bytes",
        env = "INFLUXDB_IOX_RAM_POOL_DATA_BYTES",
        default_value = "1073741824",  // 1GB
        action
    )]
    pub ram_pool_data_bytes: MemorySize,

    /// Limit the number of concurrent queries.
    #[clap(
        long = "max-concurrent-queries",
        env = "INFLUXDB_IOX_MAX_CONCURRENT_QUERIES",
        default_value = "10",
        action
    )]
    pub max_concurrent_queries: usize,

    /// After how many ingester query errors should the querier enter circuit breaker mode?
    ///
    /// The querier normally contacts the ingester for any unpersisted data during query planning.
    /// However, when the ingester can not be contacted for some reason, the querier will begin
    /// returning results that do not include unpersisted data and enter "circuit breaker mode"
    /// to avoid continually retrying the failing connection on subsequent queries.
    ///
    /// If circuits are open, the querier will NOT contact the ingester and no unpersisted data
    /// will be presented to the user.
    ///
    /// Circuits will switch to "half open" after some jittered timeout and the querier will try to
    /// use the ingester in question again. If this succeeds, we are back to normal, otherwise it
    /// will back off exponentially before trying again (and again ...).
    ///
    /// In a production environment the `ingester_circuit_state` metric should be monitored.
    #[clap(
        long = "ingester-circuit-breaker-threshold",
        env = "INFLUXDB_IOX_INGESTER_CIRCUIT_BREAKER_THRESHOLD",
        default_value = "10",
        action
    )]
    pub ingester_circuit_breaker_threshold: u64,

    /// DataFusion config.
    #[clap(
        long = "datafusion-config",
        env = "INFLUXDB_IOX_DATAFUSION_CONFIG",
        default_value = "",
        value_parser = parse_datafusion_config,
        action
    )]
    pub datafusion_config: HashMap<String, String>,
}

fn parse_datafusion_config(
    s: &str,
) -> Result<HashMap<String, String>, Box<dyn std::error::Error + Send + Sync + 'static>> {
    let s = s.trim();
    if s.is_empty() {
        return Ok(HashMap::with_capacity(0));
    }

    let mut out = HashMap::new();
    for part in s.split(',') {
        let kv = part.trim().splitn(2, ':').collect::<Vec<_>>();
        match kv.as_slice() {
            [key, value] => {
                let key_owned = key.trim().to_owned();
                let value_owned = value.trim().to_owned();
                let existed = out.insert(key_owned, value_owned).is_some();
                if existed {
                    return Err(format!("key '{key}' passed multiple times").into());
                }
            }
            _ => {
                return Err(
                    format!("Invalid key value pair - expected 'KEY:VALUE' got '{s}'").into(),
                );
            }
        }
    }

    Ok(out)
}

#[cfg(test)]
mod tests {
    use super::*;
    use clap::Parser;
    use test_helpers::assert_contains;

    #[test]
    fn test_default() {
        let actual = QuerierConfig::try_parse_from(["my_binary"]).unwrap();

        assert_eq!(actual.num_query_threads, None);
        assert!(actual.ingester_addresses.is_empty());
        assert!(actual.datafusion_config.is_empty());
    }

    #[test]
    fn test_num_threads() {
        let actual =
            QuerierConfig::try_parse_from(["my_binary", "--num-query-threads", "42"]).unwrap();

        assert_eq!(
            actual.num_query_threads,
            Some(NonZeroUsize::new(42).unwrap())
        );
    }

    #[test]
    fn test_ingester_addresses_list() {
        let querier = QuerierConfig::try_parse_from([
            "my_binary",
            "--ingester-addresses",
            "http://ingester-0:8082,http://ingester-1:8082",
        ])
        .unwrap();

        let actual: Vec<_> = querier
            .ingester_addresses
            .iter()
            .map(ToString::to_string)
            .collect();

        let expected = vec!["http://ingester-0:8082/", "http://ingester-1:8082/"];
        assert_eq!(actual, expected);
    }

    #[test]
    fn bad_ingester_addresses_list() {
        let actual = QuerierConfig::try_parse_from([
            "my_binary",
            "--ingester-addresses",
            "\\ingester-0:8082",
        ])
        .unwrap_err()
        .to_string();

        assert_contains!(
            actual,
            "error: \
            invalid value '\\ingester-0:8082' \
            for '--ingester-addresses [<INGESTER_ADDRESSES>...]': \
            Invalid: invalid uri character"
        );
    }

    #[test]
    fn test_datafusion_config() {
        let actual = QuerierConfig::try_parse_from([
            "my_binary",
            "--datafusion-config= foo : bar , x:y:z  ",
        ])
        .unwrap();

        assert_eq!(
            actual.datafusion_config,
            HashMap::from([
                (String::from("foo"), String::from("bar")),
                (String::from("x"), String::from("y:z")),
            ]),
        );
    }

    #[test]
    fn bad_datafusion_config() {
        let actual = QuerierConfig::try_parse_from(["my_binary", "--datafusion-config=foo"])
            .unwrap_err()
            .to_string();
        assert_contains!(
            actual,
            "error: invalid value 'foo' for '--datafusion-config <DATAFUSION_CONFIG>': Invalid key value pair - expected 'KEY:VALUE' got 'foo'"
        );

        let actual =
            QuerierConfig::try_parse_from(["my_binary", "--datafusion-config=foo:bar,baz:1,foo:2"])
                .unwrap_err()
                .to_string();
        assert_contains!(
            actual,
            "error: invalid value 'foo:bar,baz:1,foo:2' for '--datafusion-config <DATAFUSION_CONFIG>': key 'foo' passed multiple times"
        );
    }
}
