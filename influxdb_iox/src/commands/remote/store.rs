//! This module implements the `remote store` CLI subcommand

use futures::StreamExt;
use influxdb_iox_client::{connection::Connection, store};
use thiserror::Error;
use tokio::{
    fs::{self, File},
    io::AsyncWriteExt,
};

#[allow(clippy::enum_variant_names)]
#[derive(Debug, Error)]
pub enum Error {
    #[error("JSON Serialization error: {0}")]
    Serde(#[from] serde_json::Error),

    #[error("Client error: {0}")]
    ClientError(#[from] influxdb_iox_client::error::Error),

    #[error("Writing file: {0}")]
    FileError(#[from] std::io::Error),
}

/// Object store commands
#[derive(Debug, clap::Parser)]
pub struct Config {
    #[clap(subcommand)]
    command: Command,
}

/// Get a Parquet file by its object store uuid
#[derive(Debug, clap::Parser)]
struct Get {
    /// The object store uuid of the Parquet file
    #[clap(action)]
    uuid: String,

    /// The filename to write the data to
    #[clap(action)]
    file_name: String,
}

/// Get all the Parquet files for a particular database's table
#[derive(Debug, clap::Parser)]
struct GetTable {
    /// The database (namespace) to get the Parquet files for
    #[clap(action)]
    database: String,

    /// The table to get the Parquet files for
    #[clap(action)]
    table: String,
}

/// All possible subcommands for store
#[derive(Debug, clap::Parser)]
enum Command {
    Get(Get),

    GetTable(GetTable),
}

pub async fn command(connection: Connection, config: Config) -> Result<(), Error> {
    match config.command {
        Command::Get(get) => {
            let mut client = store::Client::new(connection);
            let mut response = client.get_parquet_file_by_object_store_id(get.uuid).await?;
            let mut file = File::create(&get.file_name).await?;
            while let Some(res) = response.next().await {
                let res = res.unwrap();

                file.write_all(&res.data).await?;
            }
            println!("wrote data to {}", get.file_name);

            Ok(())
        }
        Command::GetTable(get_table) => {
            fs::create_dir_all(&get_table.table).await?;
            Ok(())
        }
    }
}
