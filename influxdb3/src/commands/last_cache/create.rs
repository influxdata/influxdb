use std::error::Error;

use secrecy::ExposeSecret;

use crate::commands::common::{InfluxDb3Config, SeparatedList};

#[derive(Debug, clap::Parser)]
pub struct Config {
    #[clap(flatten)]
    influxdb3_config: InfluxDb3Config,

    #[clap(flatten)]
    last_cache_config: LastCacheConfig,
}

#[derive(Debug, clap::Parser)]
pub struct LastCacheConfig {
    /// The table name for which the cache is being created
    #[clap(short = 't', long = "table")]
    table: String,

    /// Give a name for the cache.
    #[clap(long = "cache-name")]
    cache_name: Option<String>,

    /// Which columns in the table to use as keys in the cache
    #[clap(long = "key-columns")]
    key_columns: Option<SeparatedList<String>>,

    /// Which columns in the table to store as values in the cache
    #[clap(long = "value-columns")]
    value_columns: Option<SeparatedList<String>>,

    /// The number of entries per unique key column combination the cache will store
    #[clap(long = "count")]
    count: Option<usize>,

    /// The time-to-live (TTL) for entries in a cache in seconds
    #[clap(long = "ttl")]
    ttl: Option<u64>,
}

pub(super) async fn command(config: Config) -> Result<(), Box<dyn Error>> {
    let InfluxDb3Config {
        host_url,
        database_name,
        auth_token,
    } = config.influxdb3_config;
    let LastCacheConfig {
        table,
        cache_name,
        key_columns,
        value_columns,
        count,
        ttl,
        ..
    } = config.last_cache_config;
    let mut client = influxdb3_client::Client::new(host_url)?;
    if let Some(t) = auth_token {
        client = client.with_auth_token(t.expose_secret());
    }
    let mut b = client.api_v3_configure_last_cache_create(database_name, table);

    // Add optional parameters:
    if let Some(name) = cache_name {
        b = b.name(name);
    }
    if let Some(keys) = key_columns {
        b = b.key_columns(keys);
    }
    if let Some(vals) = value_columns {
        b = b.value_columns(vals);
    }
    if let Some(count) = count {
        b = b.count(count);
    }
    if let Some(ttl) = ttl {
        b = b.ttl(ttl);
    }

    // Make the request:
    match b.send().await? {
        Some(def) => println!(
            "new cache created: {}",
            serde_json::to_string_pretty(&def).expect("serialize last cache definition as JSON")
        ),
        None => println!("a cache already exists for the provided parameters"),
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use clap::Parser;

    use crate::commands::last_cache::create::LastCacheConfig;

    #[test]
    fn parse_args() {
        let args = LastCacheConfig::parse_from([
            "last_cache_create",
            "--table",
            "foo",
            "--cache-name",
            "bar",
            "--key-columns",
            "tag1,tag2,tag3",
            "--value-columns",
            "field1,field2,field3",
            "--ttl",
            "3600",
            "--count",
            "5",
        ]);
        assert_eq!("foo", args.table);
        assert!(args.cache_name.is_some_and(|n| n == "bar"));
        assert!(args
            .key_columns
            .is_some_and(|keys| keys.0 == ["tag1", "tag2", "tag3"]));
        assert!(args
            .value_columns
            .is_some_and(|vals| vals.0 == ["field1", "field2", "field3"]));
        assert!(args.count.is_some_and(|c| c == 5));
        assert!(args.ttl.is_some_and(|t| t == 3600));
    }
}
