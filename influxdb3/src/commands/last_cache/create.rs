use std::error::Error;

use secrecy::ExposeSecret;

use crate::commands::common::InfluxDb3Config;

#[derive(Debug, clap::Parser)]
pub struct Config {
    #[clap(flatten)]
    influxdb3_config: InfluxDb3Config,

    /// The table name for which the cache is being created
    #[clap(short = 't', long = "table")]
    table: String,

    /// Specify a name for the cache
    #[clap(long = "cache-name")]
    cache_name: Option<String>,

    /// Specify which columns in the table to use as keys in the cache
    #[clap(long = "key-columns")]
    key_columns: Option<Vec<String>>,

    /// Specify which columns in the table to store as values in the cache
    #[clap(long = "value-columns")]
    value_columns: Option<Vec<String>>,

    /// Specify the number of entries per unique key column combination the cache will
    /// store
    #[clap(long = "count")]
    count: Option<usize>,

    /// Specify the time-to-live (TTL) for entries in a cache in seconds
    #[clap(long = "ttl")]
    ttl: Option<u64>,
}

pub(super) async fn command(config: Config) -> Result<(), Box<dyn Error>> {
    let InfluxDb3Config {
        host_url,
        database_name,
        auth_token,
    } = config.influxdb3_config;
    let mut client = influxdb3_client::Client::new(host_url)?;
    if let Some(t) = auth_token {
        client = client.with_auth_token(t.expose_secret());
    }
    let mut b = client.api_v3_configure_last_cache_create(database_name, config.table);

    // Add optional parameters:
    if let Some(name) = config.cache_name {
        b = b.name(name);
    }
    if let Some(keys) = config.key_columns {
        b = b.key_columns(keys);
    }
    if let Some(vals) = config.value_columns {
        b = b.value_columns(vals);
    }
    if let Some(count) = config.count {
        b = b.count(count);
    }
    if let Some(ttl) = config.ttl {
        b = b.ttl(ttl);
    }

    // Make the request:
    match b.send().await? {
        Some(cache_name) => println!("new cache created: {cache_name}"),
        None => println!("a cache already exists for the provided parameters"),
    }

    Ok(())
}
