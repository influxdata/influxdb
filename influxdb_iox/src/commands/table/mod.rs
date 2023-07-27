//! This module implements the `table` CLI command

use influxdb_iox_client::connection::Connection;
use observability_deps::tracing::info;
use thiserror::Error;

mod create;

#[allow(clippy::enum_variant_names)]
#[derive(Debug, Error)]
pub enum Error {
    #[error("JSON Serialization error: {0}")]
    Serde(#[from] serde_json::Error),

    #[error("Client error: {0}")]
    ClientError(#[from] influxdb_iox_client::error::Error),
}

pub type Result<T, E = Error> = std::result::Result<T, E>;

/// Various commands for table inspection
#[derive(Debug, clap::Parser)]
pub struct Config {
    #[clap(subcommand)]
    command: Command,
}

/// All possible subcommands for table
#[derive(Debug, clap::Parser)]
enum Command {
    /// Create a new table
    Create(create::Config),
}

pub async fn command(connection: Connection, config: Config) -> Result<()> {
    match config.command {
        Command::Create(config) => {
            info!("Creating table with config: {:?}", config);
            create::command(connection, config).await?;
        } // Deliberately not adding _ => so the compiler will direct people here to impl new
          // commands
    }
    Ok(())
}
