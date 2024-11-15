use clap::Parser;
use secrecy::ExposeSecret;

use super::common::InfluxDb3Config;

#[derive(Debug, thiserror::Error)]
pub(crate) enum Error {
    #[error(transparent)]
    Client(#[from] influxdb3_client::Error),
}

pub type Result<T> = std::result::Result<T, Error>;

#[derive(Debug, Parser)]
#[clap(visible_alias = "f", trailing_var_arg = true)]
pub struct Config {
    #[clap(subcommand)]
    /// The command
    command: Command,
}

#[derive(Debug, clap::Parser)]
pub enum Command {
    Create {
        /// Common InfluxDB 3.0 config
        #[clap(flatten)]
        influxdb3_config: InfluxDb3Config,

        #[arg(short, long)]
        table: Option<String>,
        #[arg(required = true)]
        columns: Vec<String>,
    },
    Delete {
        /// Common InfluxDB 3.0 config
        #[clap(flatten)]
        influxdb3_config: InfluxDb3Config,
        #[arg(short, long)]
        table: Option<String>,
    },
}

pub(crate) async fn command(config: Config) -> Result<()> {
    match config.command {
        Command::Create {
            influxdb3_config,
            table,
            columns,
        } => {
            let mut client = influxdb3_client::Client::new(influxdb3_config.host_url)?;
            if let Some(t) = influxdb3_config.auth_token {
                client = client.with_auth_token(t.expose_secret());
            }
            client
                .api_v3_configure_file_index_create_or_update(
                    influxdb3_config.database_name,
                    table,
                    columns,
                )
                .await?
        }
        Command::Delete {
            influxdb3_config,
            table,
        } => {
            let mut client = influxdb3_client::Client::new(influxdb3_config.host_url)?;
            if let Some(t) = influxdb3_config.auth_token {
                client = client.with_auth_token(t.expose_secret());
            }
            client
                .api_v3_configure_file_index_delete(influxdb3_config.database_name, table)
                .await?
        }
    }

    Ok(())
}
