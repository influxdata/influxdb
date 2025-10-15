use super::common::InfluxDb3Config;
use humantime::Duration;
use influxdb3_client::Client;
use secrecy::ExposeSecret;
use std::error::Error;
use std::path::PathBuf;
use tokio::fs;

#[derive(Debug, clap::Parser)]
pub struct Config {
    #[clap(subcommand)]
    cmd: SubCommand,
}

#[derive(Debug, clap::Subcommand)]
pub enum SubCommand {
    /// Update a database
    Database(UpdateDatabase),
    /// Update a trigger's plugin file
    Trigger(UpdateTrigger),
}

#[derive(Debug, clap::Args)]
pub struct UpdateDatabase {
    #[clap(flatten)]
    influxdb3_config: InfluxDb3Config,

    /// The retention period as a human-readable duration (e.g., "30d", "24h") or "none" to clear
    #[clap(long, short = 'r')]
    retention_period: Option<String>,

    /// An optional arg to use a custom ca for useful for testing with self signed certs
    #[clap(long = "tls-ca", env = "INFLUXDB3_TLS_CA")]
    ca_cert: Option<PathBuf>,
}

#[derive(Debug, clap::Args)]
pub struct UpdateTrigger {
    #[clap(flatten)]
    influxdb3_config: InfluxDb3Config,

    /// The name of the trigger to update
    #[clap(long = "trigger-name", short = 't')]
    trigger_name: String,

    /// Path to file containing plugin code to update
    #[clap(long, short = 'p')]
    path: PathBuf,

    /// An optional arg to use a custom ca for useful for testing with self signed certs
    #[clap(long = "tls-ca", env = "INFLUXDB3_TLS_CA")]
    ca_cert: Option<PathBuf>,
}

pub async fn command(config: Config) -> Result<(), Box<dyn Error>> {
    match config.cmd {
        SubCommand::Database(UpdateDatabase {
            influxdb3_config:
                InfluxDb3Config {
                    host_url,
                    auth_token,
                    database_name,
                    ..
                },
            retention_period,
            ca_cert,
        }) => {
            let mut client = Client::new(host_url, ca_cert)?;
            if let Some(token) = &auth_token {
                client = client.with_auth_token(token.expose_secret());
            }

            if let Some(retention_str) = retention_period {
                let retention = if retention_str.to_lowercase() == "none" {
                    None
                } else {
                    Some(retention_str.parse::<Duration>()?.into())
                };
                client
                    .api_v3_configure_db_update(&database_name, retention)
                    .await?;

                println!("Database \"{database_name}\" updated successfully");
            } else {
                return Err("--retention-period is required for update database".into());
            }
        }
        SubCommand::Trigger(UpdateTrigger {
            influxdb3_config:
                InfluxDb3Config {
                    host_url,
                    auth_token,
                    database_name,
                    ..
                },
            trigger_name,
            path,
            ca_cert,
        }) => {
            let mut client = Client::new(host_url, ca_cert)?;
            if let Some(token) = &auth_token {
                client = client.with_auth_token(token.expose_secret());
            }

            if !path.exists() {
                return Err(format!("Path does not exist: {}", path.display()).into());
            }

            if !path.is_file() {
                return Err("Path must be a file".into());
            }

            let content = fs::read_to_string(&path).await?;

            client
                .api_v3_update_plugin_file(&database_name, &trigger_name, &content)
                .await?;

            println!("Trigger '{}' updated successfully", trigger_name);
        }
    }
    Ok(())
}
