//! This module implements the `chunk` CLI command
use std::str::FromStr;

use data_types::chunk_metadata::{ChunkId, ChunkIdConversionError};
use generated_types::google::FieldViolation;
use influxdb_iox_client::{
    connection::Connection,
    management::{self, generated_types::Chunk},
};
use thiserror::Error;

#[allow(clippy::enum_variant_names)]
#[derive(Debug, Error)]
pub enum Error {
    #[error("Error interpreting server response: {0}")]
    ConvertingResponse(#[from] FieldViolation),

    #[error("Error rendering response as JSON: {0}")]
    WritingJson(#[from] serde_json::Error),

    #[error("Error connecting to IOx: {0}")]
    ConnectionError(#[from] influxdb_iox_client::connection::Error),

    #[error("Chunk {value:?} not found")]
    ChunkNotFound { value: String },

    #[error("Invalid chunk ID: {0}")]
    InvalidChunkIDError(#[from] ChunkIdConversionError),

    #[error("Client error: {0}")]
    ClientError(#[from] influxdb_iox_client::error::Error),
}

pub type Result<T, E = Error> = std::result::Result<T, E>;

/// Manage IOx chunks
#[derive(Debug, clap::Parser)]
pub struct Config {
    #[clap(subcommand)]
    command: Command,
}

/// List the chunks for the specified database in JSON format
#[derive(Debug, clap::Parser)]
struct List {
    /// The name of the database
    db_name: String,
}

/// Loads the specified chunk in the specified database from the Object Store to the Read Buffer.
#[derive(Debug, clap::Parser)]
struct Load {
    /// The name of the database
    db_name: String,

    /// The ID of the chunk
    chunk_id: String,
}

/// All possible subcommands for chunk
#[derive(Debug, clap::Parser)]
enum Command {
    List(List),
    Load(Load),
}

pub async fn command(connection: Connection, config: Config) -> Result<()> {
    match config.command {
        Command::List(get) => {
            let List { db_name } = get;

            let mut client = management::Client::new(connection);

            let chunks = client.list_chunks(db_name).await?;

            serde_json::to_writer_pretty(std::io::stdout(), &chunks)?;
        }
        Command::Load(load) => {
            let Load { db_name, chunk_id } = load;

            let mut client = management::Client::new(connection);
            let chunks = client.list_chunks(&db_name).await?;

            let load_chunk_id = ChunkId::from_str(&chunk_id)?;
            for chunk in chunks {
                let id: ChunkId = chunk
                    .id
                    .clone()
                    .try_into()
                    .expect("catalog chunk IDs to be valid");

                if id == load_chunk_id {
                    return load_chunk_to_read_buffer(&mut client, &db_name, chunk).await;
                }
            }

            return Err(Error::ChunkNotFound {
                value: load_chunk_id.to_string(),
            });
        }
    }

    Ok(())
}

async fn load_chunk_to_read_buffer(
    client: &mut management::Client,
    db_name: &str,
    chunk: Chunk,
) -> Result<()> {
    let operation = client
        .load_partition_chunk(db_name, chunk.table_name, chunk.partition_key, chunk.id)
        .await?;

    serde_json::to_writer_pretty(std::io::stdout(), &operation)?;
    Ok(())
}
