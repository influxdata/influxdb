use crate::commands::common::InfluxDb3Config;
use influxdb3_client::Client;
use secrecy::ExposeSecret;
use std::error::Error;

#[derive(Debug, clap::Parser)]
pub struct Config {
    #[clap(subcommand)]
    cmd: SubCommand,
}

impl Config {
    fn get_client(&self) -> Result<Client, Box<dyn Error>> {
        let (host_url, auth_token) = match &self.cmd {
            SubCommand::Trigger(TriggerConfig {
                influxdb3_config:
                    InfluxDb3Config {
                        host_url,
                        auth_token,
                        ..
                    },
                ..
            }) => (host_url, auth_token),
        };
        let mut client = Client::new(host_url.clone())?;
        if let Some(token) = &auth_token {
            client = client.with_auth_token(token.expose_secret());
        }
        Ok(client)
    }
}

#[derive(Debug, clap::Subcommand)]
enum SubCommand {
    /// Deactivate a plugin trigger
    Trigger(TriggerConfig),
}

#[derive(Debug, clap::Parser)]
struct TriggerConfig {
    #[clap(flatten)]
    influxdb3_config: InfluxDb3Config,

    /// Name of trigger to deactivate
    #[clap(required = true)]
    trigger_name: String,
}

pub async fn command(config: Config) -> Result<(), Box<dyn Error>> {
    let client = config.get_client()?;
    match config.cmd {
        SubCommand::Trigger(TriggerConfig {
            influxdb3_config: InfluxDb3Config { database_name, .. },
            trigger_name,
        }) => {
            client
                .api_v3_configure_processing_engine_trigger_deactivate(database_name, &trigger_name)
                .await?;
            println!("Trigger {} deactivated successfully", trigger_name);
        }
    }
    Ok(())
}
