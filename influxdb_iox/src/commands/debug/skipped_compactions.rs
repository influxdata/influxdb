//! This module implements the `skipped-compactions` CLI command

use influxdb_iox_client::{compactor, connection::Connection};
use thiserror::Error;

#[derive(Debug, Error)]
pub enum Error {
    #[error("JSON Serialization error: {0}")]
    Serde(#[from] serde_json::Error),

    #[error("Client error: {0}")]
    Client(#[from] influxdb_iox_client::error::Error),
}

/// Various commands for skipped compaction inspection
#[derive(Debug, clap::Parser)]
pub struct Config {
    #[clap(subcommand)]
    command: Command,
}

/// All possible subcommands for skipped compaction
#[derive(Debug, clap::Parser)]
enum Command {
    /// List all skipped compactions
    List,
}

pub async fn command(connection: Connection, config: Config) -> Result<(), Error> {
    let mut client = compactor::Client::new(connection);
    match config.command {
        Command::List => {
            let skipped_compactions = client.skipped_compactions().await?;
            println!("{}", serde_json::to_string_pretty(&skipped_compactions)?);
        } // Deliberately not adding _ => so the compiler will direct people here to impl new
          // commands
    }

    Ok(())
}
