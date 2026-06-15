use crate::common::InfluxDb3Config;
use influxdb3_client::Client;
use std::{error::Error, path::PathBuf};

#[derive(Debug, clap::Parser)]
pub struct Config {
    #[clap(subcommand)]
    cmd: SubCommand,
}

impl Config {
    async fn get_client(&self) -> Result<Client, Box<dyn Error>> {
        let (host_url, auth_token, ca_cert, tls_no_verify) = match &self.cmd {
            SubCommand::Trigger(TriggerConfig {
                ca_cert,
                tls_no_verify,
                influxdb3_config:
                    InfluxDb3Config {
                        host_url,
                        auth_token,
                        ..
                    },
                ..
            }) => (host_url, auth_token, ca_cert, tls_no_verify),
        };
        let client = Client::new(host_url.clone(), ca_cert.clone(), *tls_no_verify)?
            .with_resolved_auth_token(auth_token.as_ref())
            .await?;
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

    /// An optional arg to use a custom CA, useful for testing with self-signed certs
    #[clap(long = "tls-ca", env = "INFLUXDB3_TLS_CA")]
    pub ca_cert: Option<PathBuf>,

    /// Disable TLS certificate verification
    #[clap(long = "tls-no-verify", env = "INFLUXDB3_TLS_NO_VERIFY")]
    pub tls_no_verify: bool,
}

pub async fn command(config: Config) -> Result<(), Box<dyn Error>> {
    let client = config.get_client().await?;
    match config.cmd {
        SubCommand::Trigger(TriggerConfig {
            influxdb3_config: InfluxDb3Config { database_name, .. },
            trigger_name,
            ..
        }) => {
            client
                .api_v3_configure_processing_engine_trigger_disable(database_name, &trigger_name)
                .await?;
            println!("Trigger {trigger_name} disabled successfully");
        }
    }
    Ok(())
}
