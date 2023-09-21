//! This module implements the `schema` CLI command

use influxdb_iox_client::{connection::Connection, schema};
use thiserror::Error;

#[allow(clippy::enum_variant_names)]
#[derive(Debug, Error)]
pub enum Error {
    #[error("JSON Serialization error: {0}")]
    Serde(#[from] serde_json::Error),

    #[error("Client error: {0}")]
    ClientError(#[from] influxdb_iox_client::error::Error),
}

/// Various commands for catalog schema inspection
#[derive(Debug, clap::Parser)]
pub struct Config {
    #[clap(subcommand)]
    command: Command,
}

/// Get the schema of a namespace
#[derive(Debug, clap::Parser)]
struct Get {
    /// The name of the namespace for which you want to fetch the schema
    #[clap(action)]
    namespace: String,
}

/// All possible subcommands for catalog
#[derive(Debug, clap::Parser)]
enum Command {
    /// Fetch schema for a namespace
    Get(Get),
}

pub async fn command(connection: Connection, config: Config) -> Result<(), Error> {
    match config.command {
        Command::Get(command) => {
            let mut client = schema::Client::new(connection);
            let schema = client.get_schema(&command.namespace, None).await?;
            println!("{}", serde_json::to_string_pretty(&schema)?);
        } // Deliberately not adding _ => so the compiler will direct people here to impl new
          // commands
    }

    Ok(())
}
