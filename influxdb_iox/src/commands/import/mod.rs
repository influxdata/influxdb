use influxdb_iox_client::connection::Connection;
use thiserror::Error;

mod schema;

#[derive(Debug, Error)]
pub enum ImportError {
    #[error("Error in schema command: {0}")]
    SchemaError(#[from] schema::SchemaCommandError),
}

#[derive(Debug, clap::Parser)]
pub struct Config {
    #[clap(subcommand)]
    command: Command,
}

#[derive(clap::Parser, Debug)]
pub enum Command {
    /// Operations related to schema analysis.
    #[clap(subcommand)]
    Schema(Box<schema::Config>),
}

/// Handle variants of the schema command.
pub async fn command(connection: Connection, config: Config) -> Result<(), ImportError> {
    match config.command {
        Command::Schema(schema_config) => schema::command(connection, *schema_config)
            .await
            .map_err(ImportError::SchemaError),
    }
}
