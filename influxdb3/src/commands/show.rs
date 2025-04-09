use clap::Parser;
use secrecy::{ExposeSecret, Secret};
use std::error::Error;
use url::Url;

use crate::commands::common::Format;

mod system;
use system::SystemConfig;

#[derive(Debug, Parser)]
pub struct Config {
    #[clap(subcommand)]
    cmd: SubCommand,
}

#[derive(Debug, Parser)]
pub enum SubCommand {
    /// List databases
    Databases(DatabaseConfig),

    /// List tokens
    Tokens(ShowTokensConfig),

    /// Display system table data.
    System(SystemConfig),
}

#[derive(Debug, Parser)]
pub struct ShowTokensConfig {
    /// The host URL of the running InfluxDB 3 Enterprise server
    #[clap(
        short = 'H',
        long = "host",
        env = "INFLUXDB3_HOST_URL",
        default_value = "http://127.0.0.1:8181"
    )]
    host_url: Url,

    /// The token for authentication with the InfluxDB 3 Enterprise server
    #[clap(long = "token", env = "INFLUXDB3_AUTH_TOKEN")]
    auth_token: Option<Secret<String>>,

    /// The format in which to output the list of databases
    #[clap(value_enum, long = "format", default_value = "pretty")]
    output_format: Format,
}

#[derive(Debug, Parser)]
pub struct DatabaseConfig {
    /// The host URL of the running InfluxDB 3 Core server
    #[clap(
        short = 'H',
        long = "host",
        env = "INFLUXDB3_HOST_URL",
        default_value = "http://127.0.0.1:8181"
    )]
    host_url: Url,

    /// The token for authentication with the InfluxDB 3 Core server
    #[clap(long = "token", env = "INFLUXDB3_AUTH_TOKEN")]
    auth_token: Option<Secret<String>>,

    /// Include databases that were marked as deleted in the output
    #[clap(long = "show-deleted", default_value = "false")]
    show_deleted: bool,

    /// The format in which to output the list of databases
    #[clap(value_enum, long = "format", default_value = "pretty")]
    output_format: Format,
}

pub(crate) async fn command(config: Config) -> Result<(), Box<dyn Error>> {
    match config.cmd {
        SubCommand::Databases(DatabaseConfig {
            host_url,
            auth_token,
            show_deleted,
            output_format,
        }) => {
            let mut client = influxdb3_client::Client::new(host_url)?;

            if let Some(t) = auth_token {
                client = client.with_auth_token(t.expose_secret());
            }

            let resp_bytes = client
                .api_v3_configure_db_show()
                .with_format(output_format.into())
                .with_show_deleted(show_deleted)
                .send()
                .await?;

            println!("{}", std::str::from_utf8(&resp_bytes)?);
        }
        SubCommand::System(cfg) => system::command(cfg).await?,
        SubCommand::Tokens(show_tokens_config) => {
            let mut client = influxdb3_client::Client::new(show_tokens_config.host_url.clone())?;

            if let Some(t) = show_tokens_config.auth_token {
                client = client.with_auth_token(t.expose_secret());
            }
            let resp_bytes = client
                .api_v3_query_sql("_internal", "select * from system.tokens")
                .format(show_tokens_config.output_format.into())
                .send()
                .await?;
            println!("{}", std::str::from_utf8(&resp_bytes)?);
        }
    }

    Ok(())
}
