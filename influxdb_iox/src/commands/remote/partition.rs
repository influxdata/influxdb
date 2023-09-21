//! This module implements the `remote partition` CLI subcommand

use influxdb_iox_client::{
    catalog::{self},
    connection::Connection,
};
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

    #[error("Cannot parse object store config: {0}")]
    ObjectStoreParsing(#[from] clap_blocks::object_store::ParseError),

    #[error("Catalog error: {0}")]
    Catalog(#[from] iox_catalog::interface::Error),

    #[error("Object store error: {0}")]
    ObjectStore(#[from] object_store::Error),
}

/// Manage IOx chunks
#[derive(Debug, clap::Parser)]
pub struct Config {
    #[clap(subcommand)]
    command: Command,
}

/// Show the parqet_files of a partition
#[derive(Debug, clap::Parser)]
struct Show {
    /// The id of the partition. If not specified, all parquet files are shown
    #[clap(action)]
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
