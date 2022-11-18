//! This module implements the `namespace` CLI command

use influxdb_iox_client::{connection::Connection, namespace};
use thiserror::Error;

mod create;
mod retention;

#[allow(clippy::enum_variant_names)]
#[derive(Debug, Error)]
pub enum Error {
    #[error("JSON Serialization error: {0}")]
    Serde(#[from] serde_json::Error),

    #[error("Client error: {0}")]
    ClientError(#[from] influxdb_iox_client::error::Error),
}

/// Various commands for namespace inspection
#[derive(Debug, clap::Parser)]
pub struct Config {
    #[clap(subcommand)]
    command: Command,
}

/// All possible subcommands for namespace
#[derive(Debug, clap::Parser)]
enum Command {
    /// Create a new namespace
    Create(create::Config),

    /// Fetch namespaces
    List,

    /// Update retention of an existing namespace
    Retention(retention::Config),
}

pub async fn command(connection: Connection, config: Config) -> Result<(), Error> {
    match config.command {
        Command::Create(config) => {
            create::command(connection, config).await?;
        }
        Command::List => {
            let mut client = namespace::Client::new(connection);
            let namespaces = client.get_namespaces().await?;
            println!("{}", serde_json::to_string_pretty(&namespaces)?);
        }
        Command::Retention(config) => {
            retention::command(connection, config).await?;
        } // Deliberately not adding _ => so the compiler will direct people here to impl new
          // commands
    }
    Ok(())
}
