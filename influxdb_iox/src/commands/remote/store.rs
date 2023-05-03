//! This module implements the `remote store` CLI subcommand

use futures::StreamExt;
use futures_util::TryStreamExt;
use influxdb_iox_client::{
    catalog::{self, generated_types::ParquetFile},
    connection::Connection,
    store,
};
use std::path::PathBuf;
use thiserror::Error;
use tokio::{
    fs::{self, File},
    io::{self, AsyncWriteExt},
};
use tokio_util::compat::FuturesAsyncReadCompatExt;

#[allow(clippy::enum_variant_names)]
#[derive(Debug, Error)]
pub enum Error {
    #[error("JSON Serialization error: {0}")]
    Serde(#[from] serde_json::Error),

    #[error("IOx request failed: {0}")]
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

/// Get all the Parquet files for a particular namespace's table into a local directory
#[derive(Debug, clap::Parser)]
struct GetTable {
    /// The namespace to get the Parquet files for
    #[clap(action)]
    namespace: String,

    /// The name of the table to get the Parquet files for
    #[clap(action)]
    table: String,

    /// If specified, only files from the specified partitions are downloaded
    #[clap(action, short, long)]
    partition_id: Option<i64>,

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
        Command::GetTable(GetTable {
            namespace,
            table,
            partition_id,
            output_directory,
        }) => {
            let directory = output_directory.unwrap_or_else(|| PathBuf::from(&table));
            fs::create_dir_all(&directory).await?;
            let mut catalog_client = catalog::Client::new(connection.clone());
            let mut store_client = store::Client::new(connection);

            let parquet_files = catalog_client
                .get_parquet_files_by_namespace_table(namespace.clone(), table.clone())
                .await?;

            let num_parquet_files = parquet_files.len();
            println!("found {num_parquet_files} Parquet files, downloading...");
            let indexed_parquet_file_metadata = parquet_files.into_iter().enumerate();

            if let Some(partition_id) = partition_id {
                println!("Filtering by partition {partition_id}");
            }

            for (index, parquet_file) in indexed_parquet_file_metadata {
                let uuid = &parquet_file.object_store_id;
                let file_partition_id = parquet_file.partition_id;
                let file_size_bytes = parquet_file.file_size_bytes as u64;

                let index = index + 1;
                let filename = format!("{uuid}.{file_partition_id}.parquet");
                let file_path = directory.join(&filename);

                if fs::metadata(&file_path)
                    .await
                    .map_or(false, |metadata| metadata.len() == file_size_bytes)
                {
                    println!(
                        "skipping file {index} of {num_parquet_files} ({filename} already exists)"
                    );
                } else if !download_partition(&parquet_file, partition_id) {
                    println!(
                        "skipping file {index} of {num_parquet_files} ({file_partition_id} does not match request)"
                    );
                } else {
                    println!("downloading file {index} of {num_parquet_files} ({filename})...");
                    let mut response = store_client
                        .get_parquet_file_by_object_store_id(uuid.clone())
                        .await?
                        .map_ok(|res| res.data)
                        .map_err(|err| io::Error::new(io::ErrorKind::Other, err))
                        .into_async_read()
                        .compat();
                    let mut file = File::create(file_path).await?;

                    io::copy(&mut response, &mut file).await?;
                }
            }
            println!("Done.");

            Ok(())
        }
    }
}

/// evaluate the partition_filter on this file
fn download_partition(parquet_file: &ParquetFile, partition_id: Option<i64>) -> bool {
    partition_id
        .map(|partition_id| {
            // if a partition_id was specified, only download the file if
            // the partition matches
            parquet_file.partition_id == partition_id
        })
        // download files if there is no partition
        .unwrap_or(true)
}
