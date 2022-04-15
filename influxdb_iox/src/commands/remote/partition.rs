//! This module implements the `remote partition` CLI subcommand

use clap_blocks::catalog_dsn::CatalogDsnConfig;
use influxdb_iox_client::{catalog, connection::Connection};
use thiserror::Error;

#[allow(clippy::enum_variant_names)]
#[derive(Debug, Error)]
pub enum Error {
    #[error("JSON Serialization error: {0}")]
    Serde(#[from] serde_json::Error),

    #[error("Client error: {0}")]
    ClientError(#[from] influxdb_iox_client::error::Error),

    #[error("Catalog DSN error: {0}")]
    CatalogDsn(#[from] clap_blocks::catalog_dsn::Error),
}

/// Manage IOx chunks
#[derive(Debug, clap::Parser)]
pub struct Config {
    #[clap(subcommand)]
    command: Command,
}

/// Show the parqet_files of a partiton
#[derive(Debug, clap::Parser)]
struct Show {
    #[clap(flatten)]
    catalog_dsn: CatalogDsnConfig,

    /// The id of the partition
    id: i64,
}

/// All possible subcommands for partition
#[derive(Debug, clap::Parser)]
enum Command {
    Show(Show),
}

pub async fn command(connection: Connection, config: Config) -> Result<(), Error> {
    match config.command {
        Command::Show(show) => {
            let mut client = catalog::Client::new(connection);
            let files = client.get_parquet_files_by_partition_id(show.id).await?;
            println!("{}", serde_json::to_string_pretty(&files)?);

            Ok(())
        }
    }
}
