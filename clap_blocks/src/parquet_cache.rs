//! CLI handling for parquet data cache config (via CLI arguments and environment variables).

/// Config for cache client.
#[derive(Debug, Clone, Default, clap::Parser)]
pub struct ParquetCacheClientConfig {
    /// The address for the service namespace (not a given instance).
    ///
    /// When the client comes online, it discovers the keyspace
    /// by issue requests to this address.
    #[clap(
        long = "parquet-cache-namespace-addr",
        env = "INFLUXDB_IOX_PARQUET_CACHE_NAMESPACE_ADDR",
        required = false
    )]
    pub namespace_addr: String,
}

/// Config for cache instance.
#[derive(Debug, Clone, Default, clap::Parser)]
pub struct ParquetCacheInstanceConfig {
    /// The path to the config file for the keyspace.
    #[clap(
        long = "parquet-cache-keyspace-config-path",
        env = "INFLUXDB_IOX_PARQUET_CACHE_KEYSPACE_CONFIG_PATH",
        required = true
    )]
    pub keyspace_config_path: String,

    /// The hostname of the cache instance (k8s pod) running this process.
    ///
    /// Cache controller should be setting this env var.
    #[clap(
        long = "parquet-cache-instance-hostname",
        env = "HOSTNAME",
        required = true
    )]
    pub instance_hostname: String,

    /// The local directory to store data.
    #[clap(
        long = "parquet-cache-local-dir",
        env = "INFLUXDB_IOX_PARQUET_CACHE_LOCAL_DIR",
        required = true
    )]
    pub local_dir: String,
}

impl From<ParquetCacheInstanceConfig> for parquet_cache::ParquetCacheServerConfig {
    fn from(instance_config: ParquetCacheInstanceConfig) -> Self {
        Self {
            keyspace_config_path: instance_config.keyspace_config_path,
            hostname: instance_config.instance_hostname,
            local_dir: instance_config.local_dir,
            policy_config: Default::default(),
        }
    }
}
