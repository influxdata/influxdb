//! This module implements the `catalog` CLI command

use thiserror::Error;

mod topic;

#[allow(clippy::enum_variant_names)]
#[derive(Debug, Error)]
pub enum Error {
    #[error("Error formatting: {0}")]
    FormattingError(#[from] influxdb_iox_client::format::Error),

    #[error("Error in topic subcommand: {0}")]
    Topic(#[from] topic::Error),

    #[error("Client error: {0}")]
    ClientError(#[from] influxdb_iox_client::error::Error),
}

/// Various commands for catalog manipulation
#[derive(Debug, clap::Parser)]
pub struct Config {
    #[clap(subcommand)]
    command: Command,
}

/// Run database migrations
#[derive(Debug, clap::Parser)]
struct Setup {
    /// Postgres connection string
    #[clap(long = "--catalog-dsn", env = "INFLUXDB_IOX_CATALOG_DSN")]
    catalog_dsn: String,
}

/// All possible subcommands for catalog
#[derive(Debug, clap::Parser)]
enum Command {
    /// Run database migrations
    Setup(Setup),

    /// Manage kafka topic
    Topic(topic::Config),
}

pub async fn command(config: Config) -> Result<(), Error> {
    match config.command {
        Command::Setup(_command) => {
            unimplemented!();
        }
        Command::Topic(config) => {
            topic::command(config).await?;
        }
    }

    Ok(())
}
