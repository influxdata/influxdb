use super::common::InfluxDb3Config;
use humantime::Duration;
use influxdb3_client::Client;
use secrecy::ExposeSecret;
use std::error::Error;
use std::path::PathBuf;

#[derive(Debug, clap::Parser)]
pub struct Config {
    #[clap(subcommand)]
    cmd: SubCommand,
}

#[derive(Debug, clap::Subcommand)]
pub enum SubCommand {
    /// Update a database
    Database(UpdateDatabase),
    /// Update a trigger's plugin files
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

    /// Path to file or directory containing plugin files to update.
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

            // Check if path exists
            if !path.exists() {
                return Err(format!("Path does not exist: {}", path.display()).into());
            }

            // Determine what to update based on the path
            if path.is_file() {
                let file_name = path
                    .file_name()
                    .and_then(|n| n.to_str())
                    .ok_or("Invalid file name")?;
                let content = std::fs::read_to_string(&path)?;

                client
                    .api_v3_update_plugin_file(&database_name, &trigger_name, file_name, &content)
                    .await?;

                println!("Plugin '{}' updated successfully", trigger_name);
            } else if path.is_dir() {
                // Directory update - update all files
                println!(
                    "Updating plugin '{}' from directory '{}'",
                    trigger_name,
                    path.display()
                );

                // Read all Python files from the directory
                for entry in std::fs::read_dir(&path)? {
                    let entry = entry?;
                    let file_path = entry.path();
                    let file_name = file_path
                        .file_name()
                        .and_then(|n| n.to_str())
                        .ok_or("Invalid file name")?;
                    let content = std::fs::read_to_string(&file_path)?;
                    client
                        .api_v3_update_plugin_file(
                            &database_name,
                            &trigger_name,
                            file_name,
                            &content,
                        )
                        .await?;
                    println!("  Updated: {}", file_name);
                }

                println!("Plugin '{}' updated successfully", trigger_name);
            } else {
                return Err("Path must be a file or directory".into());
            }
        }
    }
    Ok(())
}
