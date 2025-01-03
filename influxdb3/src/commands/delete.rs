use super::common::InfluxDb3Config;
use secrecy::ExposeSecret;
use secrecy::Secret;
use std::error::Error;
use std::io;
use url::Url;

#[derive(Debug, clap::Parser)]
pub struct Config {
    #[clap(subcommand)]
    cmd: SubCommand,
}

#[derive(Debug, clap::Subcommand)]
pub enum SubCommand {
    /// Create a new database
    Database(DatabaseConfig),
    /// Create a new table in a database
    Table(TableConfig),
    /// Create a new last cache
    LastCache(LastCacheConfig),
    /// Create a new metacache
    MetaCache(MetaCacheConfig),
}

#[derive(Debug, clap::Args)]
pub struct DatabaseConfig {
    /// The host URL of the running InfluxDB 3 Core server
    #[clap(
        short = 'H',
        long = "host",
        env = "INFLUXDB3_HOST_URL",
        default_value = "http://127.0.0.1:8181"
    )]
    pub host_url: Url,

    /// The token for authentication with the InfluxDB 3 Core server
    #[clap(long = "token", env = "INFLUXDB3_AUTH_TOKEN")]
    pub auth_token: Option<Secret<String>>,

    /// The database name to run the query against
    #[clap(env = "INFLUXDB3_DATABASE_NAME", required = true)]
    pub database_name: String,
}

#[derive(Debug, clap::Args)]
pub struct TableConfig {
    #[clap(flatten)]
    influxdb3_config: InfluxDb3Config,
    #[clap(required = true)]
    table_name: String,
}

#[derive(Debug, clap::Args)]
pub struct LastCacheConfig {
    #[clap(flatten)]
    influxdb3_config: InfluxDb3Config,

    /// The table under which the cache is being deleted
    #[clap(short = 't', long = "table")]
    table: String,

    /// The name of the cache being deleted
    #[clap(short = 'n', long = "cache-name")]
    cache_name: String,
}

#[derive(Debug, clap::Args)]
pub struct MetaCacheConfig {
    #[clap(flatten)]
    influxdb3_config: InfluxDb3Config,

    /// The table under which the cache is being deleted
    #[clap(short = 't', long = "table")]
    table: String,

    /// The name of the cache being deleted
    #[clap(short = 'n', long = "cache-name")]
    cache_name: String,
}

pub async fn command(config: Config) -> Result<(), Box<dyn Error>> {
    match config.cmd {
        SubCommand::Database(DatabaseConfig {
            host_url,
            database_name,
            auth_token,
        }) => {
            println!(
                "Are you sure you want to delete {:?}? Enter 'yes' to confirm",
                database_name
            );
            let mut confirmation = String::new();
            let _ = io::stdin().read_line(&mut confirmation);
            if confirmation.trim() != "yes" {
                println!("Cannot delete database without confirmation");
            } else {
                let mut client = influxdb3_client::Client::new(host_url)?;
                if let Some(t) = auth_token {
                    client = client.with_auth_token(t.expose_secret());
                }
                client.api_v3_configure_db_delete(&database_name).await?;

                println!("Database {:?} deleted successfully", &database_name);
            }
        }
        SubCommand::Table(TableConfig {
            influxdb3_config:
                InfluxDb3Config {
                    host_url,
                    database_name,
                    auth_token,
                },
            table_name,
        }) => {
            println!(
                "Are you sure you want to delete {:?}.{:?}? Enter 'yes' to confirm",
                database_name, &table_name,
            );
            let mut confirmation = String::new();
            let _ = io::stdin().read_line(&mut confirmation);
            if confirmation.trim() != "yes" {
                println!("Cannot delete table without confirmation");
            } else {
                let mut client = influxdb3_client::Client::new(host_url)?;
                if let Some(t) = auth_token {
                    client = client.with_auth_token(t.expose_secret());
                }
                client
                    .api_v3_configure_table_delete(&database_name, &table_name)
                    .await?;

                println!(
                    "Table {:?}.{:?} deleted successfully",
                    &database_name, &table_name
                );
            }
        }
        SubCommand::LastCache(LastCacheConfig {
            influxdb3_config:
                InfluxDb3Config {
                    host_url,
                    database_name,
                    auth_token,
                },
            table,
            cache_name,
        }) => {
            let mut client = influxdb3_client::Client::new(host_url)?;
            if let Some(t) = auth_token {
                client = client.with_auth_token(t.expose_secret());
            }
            client
                .api_v3_configure_last_cache_delete(database_name, table, cache_name)
                .await?;

            println!("last cache deleted successfully");
        }
        SubCommand::MetaCache(MetaCacheConfig {
            influxdb3_config:
                InfluxDb3Config {
                    host_url,
                    database_name,
                    auth_token,
                },
            table,
            cache_name,
        }) => {
            let mut client = influxdb3_client::Client::new(host_url)?;
            if let Some(t) = auth_token {
                client = client.with_auth_token(t.expose_secret());
            }
            client
                .api_v3_configure_meta_cache_delete(database_name, table, cache_name)
                .await?;

            println!("meta cache deleted successfully");
        }
    }
    Ok(())
}
