use std::{error::Error, num::NonZeroUsize};

use secrecy::ExposeSecret;

use crate::commands::common::{InfluxDb3Config, SeparatedList};

#[derive(Debug, clap::Parser)]
pub struct Config {
    #[clap(flatten)]
    influxdb3_config: InfluxDb3Config,

    #[clap(flatten)]
    meta_cache_config: MetaCacheConfig,
}

#[derive(Debug, clap::Parser)]
pub struct MetaCacheConfig {
    /// The table name for which the cache is being created
    #[clap(short = 't', long = "table")]
    table: String,

    /// Give the name of the cache.
    ///
    /// This will be automatically generated if not provided
    #[clap(long = "cache-name")]
    cache_name: Option<String>,

    /// Which columns in the table to cache distinct values for, as a comma-separated list of the
    /// column names.
    ///
    /// The cache is a hieararchical structure, with a level for each column specified; the order
    /// specified here will determine the order of the levels from top-to-bottom of the cache
    /// hierarchy.
    #[clap(long = "columns")]
    columns: SeparatedList<String>,

    /// The maximum number of distinct value combinations to hold in the cache
    #[clap(long = "max-cardinality")]
    max_cardinality: Option<NonZeroUsize>,

    /// The maximum age of an entry in the cache entered as a human-readable duration, e.g., "30d", "24h"
    #[clap(long = "max-age")]
    max_age: Option<humantime::Duration>,
}

pub(super) async fn command(config: Config) -> Result<(), Box<dyn Error>> {
    let InfluxDb3Config {
        host_url,
        database_name,
        auth_token,
    } = config.influxdb3_config;
    let MetaCacheConfig {
        table,
        cache_name,
        columns,
        max_cardinality,
        max_age,
    } = config.meta_cache_config;

    let mut client = influxdb3_client::Client::new(host_url)?;
    if let Some(t) = auth_token {
        client = client.with_auth_token(t.expose_secret());
    }
    let mut b = client.api_v3_configure_meta_cache_create(database_name, table, columns);

    // Add the optional stuff:
    if let Some(name) = cache_name {
        b = b.name(name);
    }
    if let Some(max_cardinality) = max_cardinality {
        b = b.max_cardinality(max_cardinality);
    }
    if let Some(max_age) = max_age {
        b = b.max_age(max_age.into());
    }

    match b.send().await? {
        Some(def) => println!(
            "new cache created: {}",
            serde_json::to_string_pretty(&def).expect("serialize meta cache definition as JSON")
        ),
        None => println!("a cache already exists for the provided parameters"),
    }

    Ok(())
}
