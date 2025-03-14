use super::common::InfluxDb3Config;
use influxdb3_client::Client;
use influxdb3_types::http::NodeSpec;
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

impl Config {
    fn get_client(&self) -> Result<Client, Box<dyn Error>> {
        match &self.cmd {
            SubCommand::Database(DatabaseConfig {
                host_url,
                auth_token,
                ..
            })
            | SubCommand::FileIndex(FileIndexConfig {
                influxdb3_config:
                    InfluxDb3Config {
                        host_url,
                        auth_token,
                        ..
                    },
                ..
            })
            | SubCommand::LastCache(LastCacheConfig {
                influxdb3_config:
                    InfluxDb3Config {
                        host_url,
                        auth_token,
                        ..
                    },
                ..
            })
            | SubCommand::DistinctCache(DistinctCacheConfig {
                influxdb3_config:
                    InfluxDb3Config {
                        host_url,
                        auth_token,
                        ..
                    },
                ..
            })
            | SubCommand::Table(TableConfig {
                influxdb3_config:
                    InfluxDb3Config {
                        host_url,
                        auth_token,
                        ..
                    },
                ..
            })
            | SubCommand::Trigger(TriggerConfig {
                influxdb3_config:
                    InfluxDb3Config {
                        host_url,
                        auth_token,
                        ..
                    },
                ..
            }) => {
                let mut client = Client::new(host_url.clone())?;
                if let Some(token) = &auth_token {
                    client = client.with_auth_token(token.expose_secret());
                }
                Ok(client)
            }
        }
    }
}

#[derive(Debug, clap::Subcommand)]
pub enum SubCommand {
    /// Delete a database
    Database(DatabaseConfig),
    /// Delete a file index
    #[clap(name = "file_index")]
    FileIndex(FileIndexConfig),
    /// Delete a last value cache
    #[clap(name = "last_cache")]
    LastCache(LastCacheConfig),
    /// Delete a distinct value cache
    #[clap(name = "distinct_cache")]
    DistinctCache(DistinctCacheConfig),
    /// Delete a table in a database
    Table(TableConfig),
    /// Delete a trigger
    Trigger(TriggerConfig),
}

#[derive(Debug, clap::Args)]
pub struct DatabaseConfig {
    /// The host URL of the running InfluxDB 3 Enterprise server
    #[clap(
        short = 'H',
        long = "host",
        env = "INFLUXDB3_HOST_URL",
        default_value = "http://127.0.0.1:8181"
    )]
    pub host_url: Url,

    /// The token for authentication with the InfluxDB 3 Enterprise server
    #[clap(long = "token", env = "INFLUXDB3_AUTH_TOKEN")]
    pub auth_token: Option<Secret<String>>,

    /// The name of the database to be deleted
    #[clap(env = "INFLUXDB3_DATABASE_NAME", required = true)]
    pub database_name: String,
}

#[derive(Debug, clap::Args)]
pub struct FileIndexConfig {
    /// Common InfluxDB 3.0 config
    #[clap(flatten)]
    influxdb3_config: InfluxDb3Config,
    #[arg(short, long)]
    /// The table to delete the file index from
    table: Option<String>,
}

#[derive(Debug, clap::Args)]
pub struct LastCacheConfig {
    #[clap(flatten)]
    influxdb3_config: InfluxDb3Config,

    /// The table under which the cache is being deleted
    #[clap(short = 't', long = "table")]
    table: String,

    /// The name of the nodes for which the cache should be deleted. Two value formats are supported:
    ///
    /// # `all` (default)
    ///
    /// The cache is applied to `query` and `process` nodes. This is the default behavior when the
    /// flag is not specified.
    ///
    /// Example 1: --node-spec "all"
    ///
    /// # `nodes:<node-id>[,<node-id>..]`
    ///
    /// The cache is applied only to the specified comma-separated list of nodes. Only applies to
    /// `query` and `process` nodes.
    ///
    /// Example 2: --node-spec "node1,node2,node3"
    #[clap(short = 'n', long = "node-spec")]
    node_spec: Option<NodeSpec>,

    /// The name of the cache being deleted
    #[clap(required = true)]
    cache_name: String,
}

#[derive(Debug, clap::Args)]
pub struct DistinctCacheConfig {
    #[clap(flatten)]
    influxdb3_config: InfluxDb3Config,

    /// The table under which the cache is being deleted
    #[clap(short = 't', long = "table")]
    table: String,

    /// The name of the cache being deleted
    #[clap(required = true)]
    cache_name: String,
}

#[derive(Debug, clap::Args)]
pub struct TableConfig {
    #[clap(flatten)]
    influxdb3_config: InfluxDb3Config,
    #[clap(required = true)]
    /// The name of the table to be deleted
    table_name: String,
}

#[derive(Debug, clap::Parser)]
pub struct TriggerConfig {
    #[clap(flatten)]
    influxdb3_config: InfluxDb3Config,

    /// Force deletion even if trigger is active
    #[clap(long)]
    force: bool,

    /// Name of trigger to delete
    #[clap(required = true)]
    trigger_name: String,
}

pub async fn command(config: Config) -> Result<(), Box<dyn Error>> {
    let client = config.get_client()?;
    match config.cmd {
        SubCommand::Database(DatabaseConfig { database_name, .. }) => {
            println!(
                "Are you sure you want to delete {:?}? Enter 'yes' to confirm",
                database_name
            );
            let mut confirmation = String::new();
            let _ = io::stdin().read_line(&mut confirmation);
            if confirmation.trim() != "yes" {
                println!("Cannot delete database without confirmation");
            } else {
                client.api_v3_configure_db_delete(&database_name).await?;

                println!("Database {:?} deleted successfully", &database_name);
            }
        }
        SubCommand::FileIndex(FileIndexConfig {
            influxdb3_config: InfluxDb3Config { database_name, .. },
            table,
        }) => {
            client
                .api_v3_configure_file_index_delete(database_name, table)
                .await?
        }
        SubCommand::LastCache(LastCacheConfig {
            influxdb3_config: InfluxDb3Config { database_name, .. },
            node_spec,
            table,
            cache_name,
        }) => {
            client
                .api_v3_configure_last_cache_delete(
                    database_name,
                    table,
                    node_spec.unwrap_or_default(),
                    cache_name,
                )
                .await?;

            println!("last cache deleted successfully");
        }
        SubCommand::DistinctCache(DistinctCacheConfig {
            influxdb3_config: InfluxDb3Config { database_name, .. },
            table,
            cache_name,
        }) => {
            client
                .api_v3_configure_distinct_cache_delete(database_name, table, cache_name)
                .await?;

            println!("distinct cache deleted successfully");
        }
        SubCommand::Table(TableConfig {
            influxdb3_config: InfluxDb3Config { database_name, .. },
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
                client
                    .api_v3_configure_table_delete(&database_name, &table_name)
                    .await?;

                println!(
                    "Table {:?}.{:?} deleted successfully",
                    &database_name, &table_name
                );
            }
        }
        SubCommand::Trigger(TriggerConfig {
            influxdb3_config: InfluxDb3Config { database_name, .. },
            trigger_name,
            force,
        }) => {
            client
                .api_v3_configure_processing_engine_trigger_delete(
                    database_name,
                    &trigger_name,
                    force,
                )
                .await?;
            println!("Trigger {} deleted successfully", trigger_name);
        }
    }
    Ok(())
}
