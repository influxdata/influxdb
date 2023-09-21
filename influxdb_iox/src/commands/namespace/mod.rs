//! This module implements the `namespace` CLI command

use influxdb_iox_client::{connection::Connection, namespace};
use thiserror::Error;

mod create;
mod delete;
mod retention;
mod update_limit;

#[allow(clippy::enum_variant_names)]
#[derive(Debug, Error)]
pub enum Error {
    #[error("JSON Serialization error: {0}")]
    Serde(#[from] serde_json::Error),

    #[error("Client error: {0}")]
    ClientError(#[from] influxdb_iox_client::error::Error),
}

pub type Result<T, E = Error> = std::result::Result<T, E>;

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

    /// Update one of the service protection limits for an existing namespace
    UpdateLimit(update_limit::Config),

    /// Delete a namespace
    Delete(delete::Config),
}

pub async fn command(connection: Connection, config: Config) -> Result<()> {
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
        }
        Command::UpdateLimit(config) => {
            update_limit::command(connection, config).await?;
        }
        Command::Delete(config) => {
            delete::command(connection, config).await?;
        } // Deliberately not adding _ => so the compiler will direct people here to impl new
          // commands
    }
    Ok(())
}
