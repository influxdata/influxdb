use crate::commands::common::InfluxDb3Config;
use influxdb3_client::Client;
use secrecy::ExposeSecret;
use std::{error::Error, path::PathBuf};

#[derive(Debug, clap::Parser)]
pub struct Config {
    #[clap(subcommand)]
    cmd: SubCommand,
}

impl Config {
    fn get_client(&self) -> Result<Client, Box<dyn Error>> {
        let (host_url, auth_token, ca_cert) = match &self.cmd {
            SubCommand::Trigger(TriggerConfig {
                ca_cert,
                influxdb3_config:
                    InfluxDb3Config {
                        host_url,
                        auth_token,
                        ..
                    },
                ..
            }) => (host_url, auth_token, ca_cert),
        };
        let mut client = Client::new(host_url.clone(), ca_cert.clone())?;
        if let Some(token) = &auth_token {
            client = client.with_auth_token(token.expose_secret());
        }
        Ok(client)
    }
}

#[derive(Debug, clap::Subcommand)]
enum SubCommand {
    /// Disable a plugin trigger
    Trigger(TriggerConfig),
}

#[derive(Debug, clap::Parser)]
struct TriggerConfig {
    #[clap(flatten)]
    influxdb3_config: InfluxDb3Config,

    /// Name of trigger to disable
    #[clap(required = true)]
    trigger_name: String,

    /// An optional arg to use a custom ca for useful for testing with self signed certs
    #[clap(long = "tls-ca")]
    pub ca_cert: Option<PathBuf>,
}

pub async fn command(config: Config) -> Result<(), Box<dyn Error>> {
    let client = config.get_client()?;
    match config.cmd {
        SubCommand::Trigger(TriggerConfig {
            influxdb3_config: InfluxDb3Config { database_name, .. },
            trigger_name,
            ..
        }) => {
            client
                .api_v3_configure_processing_engine_trigger_disable(database_name, &trigger_name)
                .await?;
            println!("Trigger {} disabled successfully", trigger_name);
        }
    }
    Ok(())
}
