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
    }
    Ok(())
}
