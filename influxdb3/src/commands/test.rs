use crate::commands::common::{InfluxDb3Config, SeparatedKeyValue, SeparatedList};
use influxdb3_client::plugin_development::WalPluginTestRequest;
use influxdb3_client::Client;
use secrecy::ExposeSecret;
use std::collections::HashMap;
use std::error::Error;

#[derive(Debug, clap::Parser)]
pub struct Config {
    #[clap(subcommand)]
    cmd: SubCommand,
}

impl Config {
    fn get_client(&self) -> Result<Client, Box<dyn Error>> {
        match &self.cmd {
            SubCommand::WalPlugin(WalPluginConfig {
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
    /// Test a WAL Plugin
    #[clap(name = "wal_plugin")]
    WalPlugin(WalPluginConfig),
}

#[derive(Debug, clap::Parser)]
pub struct WalPluginConfig {
    #[clap(flatten)]
    influxdb3_config: InfluxDb3Config,
    /// If given, pass this line protocol as input
    #[clap(long = "lp")]
    pub input_lp: Option<String>,
    /// If given, pass this file of LP as input from on the server `<plugin-dir>/<name>_test/<input-file>`
    #[clap(long = "file")]
    pub input_file: Option<String>,
    /// If given pass this map of string key/value pairs as input arguments
    #[clap(long = "input-arguments")]
    pub input_arguments: Option<SeparatedList<SeparatedKeyValue<String, String>>>,
    /// The name of the plugin, which should match its file name on the server `<plugin-dir>/<name>.py`
    #[clap(required = true)]
    pub name: String,
}

impl From<WalPluginConfig> for WalPluginTestRequest {
    fn from(val: WalPluginConfig) -> Self {
        let input_arguments = val.input_arguments.map(|a| {
            a.into_iter()
                .map(|SeparatedKeyValue((k, v))| (k, v))
                .collect::<HashMap<String, String>>()
        });

        Self {
            name: val.name,
            input_lp: val.input_lp,
            input_file: val.input_file,
            input_arguments,
        }
    }
}

pub async fn command(config: Config) -> Result<(), Box<dyn Error>> {
    let client = config.get_client()?;

    match config.cmd {
        SubCommand::WalPlugin(plugin_config) => {
            let wal_plugin_test_request: WalPluginTestRequest = plugin_config.into();

            let response = client.wal_plugin_test(wal_plugin_test_request).await?;

            println!(
                "{}",
                serde_json::to_string_pretty(&response)
                    .expect("serialize wal plugin test response as JSON")
            );
        }
    }

    Ok(())
}
