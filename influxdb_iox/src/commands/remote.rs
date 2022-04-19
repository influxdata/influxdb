//! This module implements the `remote` CLI command (NG)

use influxdb_iox_client::connection::Connection;
use thiserror::Error;

mod partition;
mod store;

#[allow(clippy::enum_variant_names)]
#[derive(Debug, Error)]
pub enum Error {
    #[error("Error in partition subcommand: {0}")]
    Partition(#[from] partition::Error),

    #[error("Error in the store subcommand: {0}")]
    Store(#[from] store::Error),

    #[error("Catalog error: {0}")]
    Catalog(#[from] iox_catalog::interface::Error),

    #[error("Catalog DSN error: {0}")]
    CatalogDsn(#[from] clap_blocks::catalog_dsn::Error),
}

/// Various commands against a remote IOx API
#[derive(Debug, clap::Parser)]
pub struct Config {
    #[clap(subcommand)]
    command: Command,
}

/// All possible subcommands for catalog
#[derive(Debug, clap::Parser)]
enum Command {
    /// Get partition data
    Partition(partition::Config),
    /// Get parquet files from the object store
    Store(store::Config),
}

pub async fn command(connection: Connection, config: Config) -> Result<(), Error> {
    match config.command {
        Command::Partition(config) => {
            partition::command(connection, config).await?;
        }
        Command::Store(config) => {
            store::command(connection, config).await?;
        }
    }

    Ok(())
}
