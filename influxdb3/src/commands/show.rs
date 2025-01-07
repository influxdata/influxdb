use clap::{Parser, ValueEnum};
use secrecy::{ExposeSecret, Secret};
use std::error::Error;
use url::Url;

#[derive(Debug, Parser)]
pub struct Config {
    #[clap(subcommand)]
    cmd: SubCommand,
}

#[derive(Debug, Parser)]
pub enum SubCommand {
    /// List databases
    Databases(DatabaseConfig),
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

#[derive(Debug, ValueEnum, Clone)]
#[clap(rename_all = "snake_case")]
enum Format {
    Pretty,
    Json,
    JsonLines,
    Csv,
}

impl From<Format> for influxdb3_client::Format {
    fn from(this: Format) -> Self {
        match this {
            Format::Pretty => Self::Pretty,
            Format::Json => Self::Json,
            Format::JsonLines => Self::JsonLines,
            Format::Csv => Self::Csv,
        }
    }
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
    }

    Ok(())
}
