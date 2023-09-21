//! This module implements the `remote store` CLI subcommand

use futures::StreamExt;
use import_export::file::RemoteExporter;
use influxdb_iox_client::{connection::Connection, store};
use std::path::PathBuf;
use thiserror::Error;
use tokio::{fs::File, io::AsyncWriteExt};

#[allow(clippy::enum_variant_names)]
#[derive(Debug, Error)]
pub enum Error {
    #[error("IOx request failed: {0}")]
    ClientError(#[from] influxdb_iox_client::error::Error),

    #[error("Writing file: {0}")]
    FileError(#[from] std::io::Error),

    #[error("Exporting: {0}")]
    ExportError(#[from] import_export::file::ExportError),
}

type Result<T, E = Error> = std::result::Result<T, E>;

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

/// Get data for a particular namespace's table into a local directory.
///
/// See `influxdb_iox debug build-catalog` to create a local catalog
/// from these files.
#[derive(Debug, clap::Parser)]
struct GetTable {
    /// The namespace to get the Parquet files for
    #[clap(action)]
    namespace: String,

    /// The name of the table to get the Parquet files for
    #[clap(action)]
    table: String,

    /// The output directory to use. If not specified, files will be placed in a directory named
    /// after the table in the current working directory.
    #[clap(action, short)]
    output_directory: Option<PathBuf>,
}

/// All possible subcommands for store
#[derive(Debug, clap::Parser)]
enum Command {
    Get(Get),

    GetTable(GetTable),
}

pub async fn command(connection: Connection, config: Config) -> Result<()> {
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
        Command::GetTable(GetTable {
            namespace,
            table,
            output_directory,
        }) => {
            let mut exporter = RemoteExporter::new(connection);
            Ok(exporter
                .export_table(output_directory, namespace, table)
                .await?)
        }
    }
}
