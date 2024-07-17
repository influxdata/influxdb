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

    /// The cache name for which the cache is being created
    #[clap(short = 'n', long = "cache-name")]
    cache_name: String,
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
    client
        .api_v3_configure_last_cache_delete(database_name, config.table, config.cache_name)
        .await?;

    println!("last cache deleted successfully");

    Ok(())
}
