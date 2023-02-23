//! Querier-related configs.
use crate::ingester_address::IngesterAddress;
use data_types::{IngesterMapping, ShardIndex};
use serde::Deserialize;
use snafu::{ResultExt, Snafu};
use std::{
    collections::HashMap, fs, io, num::NonZeroUsize, path::PathBuf, str::FromStr, sync::Arc,
};

#[derive(Debug, Snafu)]
#[allow(missing_docs)]
pub enum Error {
    #[snafu(display("Could not read shard to ingester file `{}`: {source}", file.display()))]
    ShardToIngesterFileReading { source: io::Error, file: PathBuf },

    #[snafu(display("Could not deserialize JSON from ingester config: {source}"))]
    ShardToIngesterDeserializing { source: serde_json::Error },

    #[snafu(display(
        "Specifying `\"ignoreAll\": true` requires that both the `ingesters` and \
        `shards` configurations are empty. `ingesters`: `{:#?}`,  `shards`: `{:#?}`",
        ingesters,
        shards,
    ))]
    IgnoreAllRequiresEmptyConfig {
        ingesters: HashMap<Arc<str>, Arc<IngesterConfig>>,
        shards: HashMap<ShardIndex, ShardConfig>,
    },

    #[snafu(display(
        "Ingester `{name}` must either set the `addr` to a non-empty value or set `ignore` to true"
    ))]
    IngesterAddrRequired { name: Arc<str> },

    #[snafu(display(
        "Could not find ingester `{name}` specified for shard index `{shard_index}`"
    ))]
    IngesterNotFound {
        shard_index: ShardIndex,
        name: Arc<str>,
    },

    #[snafu(context(false))]
    IngesterAddress {
        source: crate::ingester_address::Error,
    },
}

/// CLI config for querier configuration
#[derive(Debug, Clone, PartialEq, Eq, clap::Parser)]
pub struct QuerierConfig {
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
    #[clap(
        long = "exec-mem-pool-bytes",
        env = "INFLUXDB_IOX_EXEC_MEM_POOL_BYTES",
        default_value = "8589934592",  // 8GB
        action
    )]
    pub exec_mem_pool_bytes: usize,

    /// Path to a JSON file containing a Shard index to ingesters gRPC mapping. For example:
    ///
    /// ```json
    /// {
    ///   // Flag to ignore all ingesters and only query persisted data. Useful for development
    ///   // or creating "cold data only" clusters.
    ///   //
    ///   // If this is set to `true`, having non-empty `ingesters` or `shards` is a startup
    ///   // error.
    ///   //
    ///   // default: false
    ///   "ignoreAll": false,
    ///
    ///   // Mapping of ingester name to config.
    ///   //
    ///   // default: {}
    ///   "ingesters": {
    ///     "i1": {
    ///       // Ingester address as URL.
    ///       //
    ///       // If this is `null` but `ignore` is false, it is an error.
    ///       //
    ///       // default: null
    ///       "addr": "http://ingester-1:1234"
    ///     },
    ///     "i2": {
    ///       // Flag to ignore this ingester at query time and not contact it.
    ///       //
    ///       // default: false
    ///       "ignore": true
    ///     }
    ///   },
    ///
    ///   // Mapping of shard indexes (as strings) to ingester names. Queries to shards that do
    ///   // not appear in this mapping will return an error. Using an ingester name in the
    ///   // `shards` mapping that does not appear in the `ingesters` mapping is a startup error.
    ///   //
    ///   // default: {}
    ///   "shards": {
    ///     "1": {
    ///       // Name of an ingester from the `ingester` mapping.
    ///       //
    ///       // If this is `null`, queries to this shard will error.
    ///       //
    ///       // default: null
    ///       "ingester": "i1"
    ///     },
    ///     "2": {
    ///       "ingester": "i1"
    ///     },
    ///     "3": {
    ///       "ingester": "i2"
    ///     },
    ///     "5": {
    ///       // Flag to not fetch data from any ingester for queries to this shard.
    ///       //
    ///       // default: false
    ///       "ignore": true
    ///     }
    ///   }
    /// }
    /// ```
    #[clap(
        long = "shard-to-ingesters-file",
        env = "INFLUXDB_IOX_SHARD_TO_INGESTERS_FILE",
        action
    )]
    pub shard_to_ingesters_file: Option<PathBuf>,

    /// JSON containing a Shard index to ingesters gRPC mapping. For example:
    ///
    /// ```json
    /// {
    ///   // Flag to ignore all ingesters and only query persisted data. Useful for development
    ///   // or creating "cold data only" clusters.
    ///   //
    ///   // If this is set to `true`, having non-empty `ingesters` or `shards` is a startup
    ///   // error.
    ///   //
    ///   // default: false
    ///   "ignoreAll": false,
    ///
    ///   // Mapping of ingester name to config.
    ///   //
    ///   // default: {}
    ///   "ingesters": {
    ///     "i1": {
    ///       // Ingester address as URL.
    ///       //
    ///       // If this is `null` but `ignore` is false, it is an error.
    ///       //
    ///       // default: null
    ///       "addr": "http://ingester-1:1234"
    ///     },
    ///     "i2": {
    ///       // Flag to ignore this ingester at query time and not contact it.
    ///       //
    ///       // default: false
    ///       "ignore": true
    ///     }
    ///   },
    ///
    ///   // Mapping of shard indexes (as strings) to ingester names. Queries to shards that do
    ///   // not appear in this mapping will return an error. Using an ingester name in the
    ///   // `shards` mapping that does not appear in the `ingesters` mapping is a startup error.
    ///   //
    ///   // default: {}
    ///   "shards": {
    ///     "1": {
    ///       // Name of an ingester from the `ingester` mapping.
    ///       //
    ///       // If this is `null`, queries to this shard will error.
    ///       //
    ///       // default: null
    ///       "ingester": "i1"
    ///     },
    ///     "2": {
    ///       "ingester": "i1"
    ///     },
    ///     "3": {
    ///       "ingester": "i2"
    ///     },
    ///     "5": {
    ///       // Flag to not fetch data from any ingester for queries to this shard.
    ///       //
    ///       // default: false
    ///       "ignore": true
    ///     }
    ///   }
    /// }
    /// ```
    #[clap(
        long = "shard-to-ingesters",
        env = "INFLUXDB_IOX_SHARD_TO_INGESTERS",
        action
    )]
    pub shard_to_ingesters: Option<String>,

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
    #[clap(long = "ingester-addresses", env = "INFLUXDB_IOX_INGESTER_ADDRESSES", num_args=1.., value_delimiter = ',')]
    pub ingester_addresses: Vec<String>,

    /// Size of the RAM cache used to store catalog metadata information in bytes.
    #[clap(
        long = "ram-pool-metadata-bytes",
        env = "INFLUXDB_IOX_RAM_POOL_METADATA_BYTES",
        default_value = "134217728",  // 128MB
        action
    )]
    pub ram_pool_metadata_bytes: usize,

    /// Size of the RAM cache used to store data in bytes.
    #[clap(
        long = "ram-pool-data-bytes",
        env = "INFLUXDB_IOX_RAM_POOL_DATA_BYTES",
        default_value = "1073741824",  // 1GB
        action
    )]
    pub ram_pool_data_bytes: usize,

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
    /// If circuits are open, the querier will NOT contact the ingester and no unpersisted data will be presented to the user.
    ///
    /// Circuits will switch to "half open" after some jittered timeout and the querier will try to use the ingester in
    /// question again. If this succeeds, we are back to normal, otherwise it will back off exponentially before trying
    /// again (and again ...).
    ///
    /// In a production environment the `ingester_circuit_state` metric should be monitored.
    #[clap(
        long = "ingester-circuit-breaker-threshold",
        env = "INFLUXDB_IOX_INGESTER_CIRCUIT_BREAKER_THRESHOLD",
        default_value = "10",
        action
    )]
    pub ingester_circuit_breaker_threshold: u64,
}

impl QuerierConfig {
    /// Get the querier config's num query threads.
    #[must_use]
    pub fn num_query_threads(&self) -> Option<NonZeroUsize> {
        self.num_query_threads
    }

    /// Return the querier config's ingester addresses. If `--shard-to-ingesters-file` is used to
    /// specify a JSON file containing shard to ingester address mappings, this returns `Err` if
    /// there are any problems reading, deserializing, or interpreting the file.

    // When we have switched to using the RPC write path only, this method can be changed to be
    // infallible as clap will handle failure to parse the list of strings.
    //
    // Switching into the RPC write path mode requires *both* the `INFLUXDB_IOX_RPC_MODE`
    // environment variable to be specified *and* `--ingester-addresses` to be set in order to
    // switch. Setting `INFLUXDB_IOX_RPC_MODE` and shard-to-ingesters mapping, or not setting
    // `INFLUXDB_IOX_RPC_MODE` and setting ingester addresses, will panic.
    pub fn ingester_addresses(&self) -> Result<IngesterAddresses, Error> {
        if let Some(file) = &self.shard_to_ingesters_file {
            let contents =
                fs::read_to_string(file).context(ShardToIngesterFileReadingSnafu { file })?;
            let map = deserialize_shard_ingester_map(&contents)?;
            if map.is_empty() {
                Ok(IngesterAddresses::None)
            } else {
                Ok(IngesterAddresses::ByShardIndex(map))
            }
        } else if let Some(contents) = &self.shard_to_ingesters {
            let map = deserialize_shard_ingester_map(contents)?;
            if map.is_empty() {
                Ok(IngesterAddresses::None)
            } else {
                Ok(IngesterAddresses::ByShardIndex(map))
            }
        } else if !self.ingester_addresses.is_empty() {
            Ok(IngesterAddresses::List(
                self.ingester_addresses
                    .iter()
                    .map(|addr| IngesterAddress::from_str(addr))
                    .collect::<Result<Vec<_>, _>>()?,
            ))
        } else {
            Ok(IngesterAddresses::None)
        }
    }

    /// Size of the RAM cache pool for metadata in bytes.
    pub fn ram_pool_metadata_bytes(&self) -> usize {
        self.ram_pool_metadata_bytes
    }

    /// Size of the RAM cache pool for payload in bytes.
    pub fn ram_pool_data_bytes(&self) -> usize {
        self.ram_pool_data_bytes
    }

    /// Number of queries allowed to run concurrently
    pub fn max_concurrent_queries(&self) -> usize {
        self.max_concurrent_queries
    }
}

fn deserialize_shard_ingester_map(
    contents: &str,
) -> Result<HashMap<ShardIndex, IngesterMapping>, Error> {
    let ingesters_config: IngestersConfig =
        serde_json::from_str(contents).context(ShardToIngesterDeserializingSnafu)?;

    if ingesters_config.ignore_all
        && (!ingesters_config.ingesters.is_empty() || !ingesters_config.shards.is_empty())
    {
        return IgnoreAllRequiresEmptyConfigSnafu {
            ingesters: ingesters_config.ingesters,
            shards: ingesters_config.shards,
        }
        .fail();
    }

    let mut ingester_mapping_by_name = HashMap::new();

    for (name, config) in &ingesters_config.ingesters {
        match (config.ignore, config.addr.as_ref()) {
            (true, _) => {
                ingester_mapping_by_name.insert(name, IngesterMapping::Ignore);
            }
            (false, None) => {
                return IngesterAddrRequiredSnafu {
                    name: Arc::clone(name),
                }
                .fail();
            }
            (false, Some(addr)) if addr.is_empty() => {
                return IngesterAddrRequiredSnafu {
                    name: Arc::clone(name),
                }
                .fail();
            }
            (false, Some(addr)) => {
                ingester_mapping_by_name.insert(name, IngesterMapping::Addr(Arc::clone(addr)));
            }
        }
    }

    let mut map = HashMap::new();

    for (shard_index, shard_config) in ingesters_config.shards {
        if shard_config.ignore {
            map.insert(shard_index, IngesterMapping::Ignore);
            continue;
        }
        match shard_config.ingester {
            Some(ingester) => match ingester_mapping_by_name.get(&ingester) {
                Some(ingester_mapping) => {
                    map.insert(shard_index, ingester_mapping.clone());
                }
                None => {
                    return IngesterNotFoundSnafu {
                        name: Arc::clone(&ingester),
                        shard_index,
                    }
                    .fail();
                }
            },
            None => {
                map.insert(shard_index, IngesterMapping::NotMapped);
            }
        }
    }

    Ok(map)
}

/// Ingester addresses.
#[derive(Debug, PartialEq, Eq)]
pub enum IngesterAddresses {
    /// A mapping from shard index to ingesters.
    ByShardIndex(HashMap<ShardIndex, IngesterMapping>),

    /// A list of ingester2 addresses.
    List(Vec<IngesterAddress>),

    /// No connections, meaning only persisted data should be used.
    None,
}

#[derive(Debug, Deserialize, Default)]
#[serde(rename_all = "camelCase")]
struct IngestersConfig {
    #[serde(default)]
    ignore_all: bool,
    #[serde(default)]
    ingesters: HashMap<Arc<str>, Arc<IngesterConfig>>,
    #[serde(default)]
    shards: HashMap<ShardIndex, ShardConfig>,
}

/// Ingester config.
#[derive(Debug, Deserialize)]
pub struct IngesterConfig {
    addr: Option<Arc<str>>,
    #[serde(default)]
    ignore: bool,
}

/// Shard config.
#[derive(Debug, Deserialize)]
pub struct ShardConfig {
    ingester: Option<Arc<str>>,
    #[serde(default)]
    ignore: bool,
}

#[cfg(test)]
mod tests {
    use super::*;
    use clap::Parser;
    use test_helpers::assert_error;

    #[test]
    fn test_default() {
        let actual = QuerierConfig::try_parse_from(["my_binary"]).unwrap();

        assert_eq!(actual.num_query_threads(), None);
        assert!(matches!(
            actual.ingester_addresses().unwrap(),
            IngesterAddresses::None,
        ));
    }

    #[test]
    fn test_num_threads() {
        let actual =
            QuerierConfig::try_parse_from(["my_binary", "--num-query-threads", "42"]).unwrap();

        assert_eq!(
            actual.num_query_threads(),
            Some(NonZeroUsize::new(42).unwrap())
        );
        assert!(matches!(
            actual.ingester_addresses().unwrap(),
            IngesterAddresses::None,
        ));
    }

    #[test]
    fn test_ingester_addresses_list() {
        let actual = QuerierConfig::try_parse_from([
            "my_binary",
            "--ingester-addresses",
            "http://ingester-0:8082,http://ingester-1:8082",
        ])
        .unwrap();

        let expected = IngesterAddresses::List(vec![
            IngesterAddress::from_str("http://ingester-0:8082").unwrap(),
            IngesterAddress::from_str("http://ingester-1:8082").unwrap(),
        ]);
        assert_eq!(actual.ingester_addresses().unwrap(), expected);
    }

    #[test]
    fn bad_ingester_addresses_list() {
        let actual = QuerierConfig::try_parse_from([
            "my_binary",
            "--ingester-addresses",
            "\\ingester-0:8082",
        ])
        .unwrap()
        .ingester_addresses();
        assert_error!(actual, Error::IngesterAddress { .. });
    }

    #[test]
    fn supply_json_value() {
        let actual = QuerierConfig::try_parse_from([
            "my_binary",
            "--shard-to-ingesters",
            r#"{
              "ignoreAll": false,
              "ingesters": {
                "i1": {
                  "addr": "http://ingester-1:1234"
                },
                "i2": {
                  "ignore": true
                },
                "i3": {
                  "ignore": true,
                  "addr": "http://ingester-2:2345"
                }
              },
              "shards": {
                "1": {
                  "ingester": "i1"
                },
                "2": {
                  "ingester": "i2"
                },
                "5": {
                  "ignore": true
                }
              }
            }"#,
        ])
        .unwrap();

        let expected = IngesterAddresses::ByShardIndex(
            [
                (
                    ShardIndex::new(1),
                    IngesterMapping::Addr("http://ingester-1:1234".into()),
                ),
                (ShardIndex::new(2), IngesterMapping::Ignore),
                (ShardIndex::new(5), IngesterMapping::Ignore),
            ]
            .into_iter()
            .collect(),
        );

        assert_eq!(actual.ingester_addresses().unwrap(), expected);
    }

    #[test]
    fn successful_deserialization() {
        let contents = r#"{
          "ignoreAll": false,
          "ingesters": {
            "i1": {
              "addr": "http://ingester-1:1234"
            },
            "i2": {
              "ignore": true
            },
            "i3": {
              "ignore": true,
              "addr": "http://ingester-2:2345"
            }
          },
          "shards": {
            "1": {
              "ingester": "i1"
            },
            "2": {
              "ingester": "i2"
            },
            "3": {
              "ingester": "i1",
              "ignore": true
            },
            "5": {
              "ignore": true
            }
          }
        }"#;

        let map = deserialize_shard_ingester_map(contents).unwrap();

        let expected = [
            (
                ShardIndex::new(1),
                IngesterMapping::Addr("http://ingester-1:1234".into()),
            ),
            (ShardIndex::new(2), IngesterMapping::Ignore),
            (ShardIndex::new(3), IngesterMapping::Ignore),
            (ShardIndex::new(5), IngesterMapping::Ignore),
        ]
        .into_iter()
        .collect();

        assert_eq!(map, expected);
    }

    #[test]
    fn unsuccessful_deserialization() {
        let map = deserialize_shard_ingester_map("");
        assert_error!(map, Error::ShardToIngesterDeserializing { .. });
    }

    #[test]
    fn ignore_all_requires_empty_maps() {
        let expected = HashMap::new();

        let map = deserialize_shard_ingester_map(
            r#"{
            "ignoreAll": true
        }"#,
        );
        assert_eq!(map.unwrap(), expected);

        let map = deserialize_shard_ingester_map(
            r#"{
            "ignoreAll": true,
            "ingesters": {},
            "shards": {}
        }"#,
        );
        assert_eq!(map.unwrap(), expected);

        let map = deserialize_shard_ingester_map(
            r#"{
            "ignoreAll": true,
            "ingesters": {
                "i1": {
                  "addr": "http://ingester-1:1234"
                }
            },
            "shards": {}
        }"#,
        );
        assert_error!(map, Error::IgnoreAllRequiresEmptyConfig { .. });

        let map = deserialize_shard_ingester_map(
            r#"{
            "ignoreAll": true,
            "ingesters": {},
            "shards": {
                "1": {
                  "ingester": "i1"
                }
            }
        }"#,
        );
        assert_error!(map, Error::IgnoreAllRequiresEmptyConfig { .. });

        let map = deserialize_shard_ingester_map(
            r#"{
            "ignoreAll": true,
            "ingesters": {
                "i1": {
                  "addr": "http://ingester-1:1234"
                }
            },
            "shards": {
                "1": {
                  "ingester": "i1"
                }
            }
        }"#,
        );
        assert_error!(map, Error::IgnoreAllRequiresEmptyConfig { .. });
    }

    #[test]
    fn ingester_addr_must_be_specified_if_not_ignored() {
        let map = deserialize_shard_ingester_map(
            r#"{
              "ingesters": {
                  "i1": {}
              }
            }"#,
        );
        assert_error!(map, Error::IngesterAddrRequired { ref name } if name.as_ref() == "i1");

        let map = deserialize_shard_ingester_map(
            r#"{
              "ingesters": {
                  "i1": {
                      "addr": ""
                  }
              }
            }"#,
        );
        assert_error!(map, Error::IngesterAddrRequired { ref name } if name.as_ref() == "i1");
    }

    #[test]
    fn ingester_must_be_found() {
        let map = deserialize_shard_ingester_map(
            r#"{
            "ingesters": {},
            "shards": {
                "1": {
                  "ingester": "i1"
                }
            }
        }"#,
        );
        assert_error!(
            map,
            Error::IngesterNotFound { shard_index, ref name }
              if shard_index.get() == 1 && name.as_ref() == "i1"
        );

        let map = deserialize_shard_ingester_map(
            r#"{
            "ingesters": {},
            "shards": {
                "1": {
                  "ingester": ""
                }
            }
        }"#,
        );
        assert_error!(
            map,
            Error::IngesterNotFound { shard_index, ref name }
              if shard_index.get() == 1 && name.as_ref() == ""
        );
    }

    #[test]
    fn shard_to_ingester_varieties() {
        let map = deserialize_shard_ingester_map(
            r#"{
            "ingesters": {
                "i1": {
                  "addr": "http://ingester-1:1234"
                }
            },
            "shards": {
                "1": {
                  "ingester": "i1"
                },
                "2": {},
                "3": {
                    "ingester": null
                },
                "4": {
                    "ignore": true
                },
                "5": {
                    "ignore": true,
                    "ingester": "i1"
                },
                "6": {
                    "ignore": true,
                    "ingester": null
                }
            }
        }"#,
        );

        let expected = [
            (
                ShardIndex::new(1),
                IngesterMapping::Addr("http://ingester-1:1234".into()),
            ),
            (ShardIndex::new(2), IngesterMapping::NotMapped),
            (ShardIndex::new(3), IngesterMapping::NotMapped),
            (ShardIndex::new(4), IngesterMapping::Ignore),
            (ShardIndex::new(5), IngesterMapping::Ignore),
            (ShardIndex::new(6), IngesterMapping::Ignore),
        ]
        .into_iter()
        .collect();

        assert_eq!(map.unwrap(), expected);
    }
}
