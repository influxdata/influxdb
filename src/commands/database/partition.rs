//! This module implements the `partition` CLI command
use data_types::chunk_metadata::ChunkSummary;
use generated_types::google::FieldViolation;
use influxdb_iox_client::{
    connection::Connection,
    management::{
        self, ClosePartitionChunkError, DropPartitionError, GetPartitionError,
        ListPartitionChunksError, ListPartitionsError, NewPartitionChunkError,
        PersistPartitionError, UnloadPartitionChunkError,
    },
};
use std::convert::TryFrom;
use structopt::StructOpt;
use thiserror::Error;

#[allow(clippy::enum_variant_names)]
#[derive(Debug, Error)]
pub enum Error {
    #[error("Error listing partitions: {0}")]
    ListPartitionsError(#[from] ListPartitionsError),

    #[error("Error getting partition: {0}")]
    GetPartitionsError(#[from] GetPartitionError),

    #[error("Error persisting partition: {0}")]
    PersistPartitionError(#[from] PersistPartitionError),

    #[error("Error dropping partition: {0}")]
    DropPartitionError(#[from] DropPartitionError),

    #[error("Error listing partition chunks: {0}")]
    ListPartitionChunksError(#[from] ListPartitionChunksError),

    #[error("Error creating new partition chunk: {0}")]
    NewPartitionChunkError(#[from] NewPartitionChunkError),

    #[error("Error closing chunk: {0}")]
    ClosePartitionChunkError(#[from] ClosePartitionChunkError),

    #[error("Error unloading chunk: {0}")]
    UnloadPartitionChunkError(#[from] UnloadPartitionChunkError),

    #[error("Error rendering response as JSON: {0}")]
    WritingJson(#[from] serde_json::Error),

    #[error("Received invalid response: {0}")]
    InvalidResponse(#[from] FieldViolation),
}

pub type Result<T, E = Error> = std::result::Result<T, E>;

/// Manage IOx partitions
#[derive(Debug, StructOpt)]
pub struct Config {
    #[structopt(subcommand)]
    command: Command,
}

/// List all known partition keys for a database
#[derive(Debug, StructOpt)]
struct List {
    /// The name of the database
    db_name: String,
}

/// Get details of a specific partition in JSON format (TODO)
#[derive(Debug, StructOpt)]
struct Get {
    /// The name of the database
    db_name: String,

    /// The partition key
    partition_key: String,
}

/// Persist partition.
///
/// Errors if there is nothing to persist at the moment as per the lifecycle rules. If successful it returns the
/// chunk that contains the persisted data.
#[derive(Debug, StructOpt)]
struct Persist {
    /// The name of the database
    db_name: String,

    /// The partition key
    partition_key: String,

    /// The table name
    table_name: String,
}

/// lists all chunks in this partition
#[derive(Debug, StructOpt)]
struct ListChunks {
    /// The name of the database
    db_name: String,

    /// The partition key
    partition_key: String,
}

/// Create a new, open chunk in the partiton's Mutable Buffer which will receive
/// new writes.
#[derive(Debug, StructOpt)]
struct NewChunk {
    /// The name of the database
    db_name: String,

    /// The partition key
    partition_key: String,

    /// The table name
    table_name: String,
}

/// Closes a chunk in the mutable buffer for writing and starts its migration to
/// the read buffer
#[derive(Debug, StructOpt)]
struct CloseChunk {
    /// The name of the database
    db_name: String,

    /// The partition key
    partition_key: String,

    /// The table name
    table_name: String,

    /// The chunk id
    chunk_id: u32,
}

/// Unload chunk from read buffer but keep it in object store.
#[derive(Debug, StructOpt)]
struct UnloadChunk {
    /// The name of the database
    db_name: String,

    /// The partition key
    partition_key: String,

    /// The table name
    table_name: String,

    /// The chunk id
    chunk_id: u32,
}

/// Drop partition from memory and (if persisted) from object store.
#[derive(Debug, StructOpt)]
struct DropPartition {
    /// The name of the database
    db_name: String,

    /// The partition key
    partition_key: String,

    /// The table name
    table_name: String,
}

/// All possible subcommands for partition
#[derive(Debug, StructOpt)]
enum Command {
    /// List partitions
    List(List),

    /// Get details about a particular partition
    Get(Get),

    /// Persist partition.
    ///
    /// Errors if there is nothing to persist at the moment as per the lifecycle rules. If successful it returns the
    /// chunk that contains the persisted data.
    Persist(Persist),

    /// Drop partition from memory and (if persisted) from object store.
    Drop(DropPartition),

    /// List chunks in a partition
    ListChunks(ListChunks),

    /// Create a new chunk in the partition
    NewChunk(NewChunk),

    /// Close the chunk and move to read buffer
    CloseChunk(CloseChunk),

    /// Unload chunk from read buffer but keep it in object store.
    UnloadChunk(UnloadChunk),
}

pub async fn command(connection: Connection, config: Config) -> Result<()> {
    let mut client = management::Client::new(connection);

    match config.command {
        Command::List(list) => {
            let List { db_name } = list;
            let partitions = client.list_partitions(db_name).await?;
            let partition_keys = partitions.into_iter().map(|p| p.key).collect::<Vec<_>>();

            serde_json::to_writer_pretty(std::io::stdout(), &partition_keys)?;
        }
        Command::Get(get) => {
            let Get {
                db_name,
                partition_key,
            } = get;

            let management::generated_types::Partition { key } =
                client.get_partition(db_name, partition_key).await?;

            // TODO: get more details from the partition, andprint it
            // out better (i.e. move to using Partition summary that
            // is already in data_types)
            #[derive(serde::Serialize)]
            struct PartitionDetail {
                key: String,
            }

            let partition_detail = PartitionDetail { key };

            serde_json::to_writer_pretty(std::io::stdout(), &partition_detail)?;
        }
        Command::Persist(persist) => {
            let Persist {
                db_name,
                partition_key,
                table_name,
            } = persist;

            client
                .persist_partition(db_name, table_name, partition_key)
                .await?;
            println!("Ok");
        }
        Command::Drop(drop_partition) => {
            let DropPartition {
                db_name,
                partition_key,
                table_name,
            } = drop_partition;

            client
                .drop_partition(db_name, table_name, partition_key)
                .await?;
            println!("Ok");
        }
        Command::ListChunks(list_chunks) => {
            let ListChunks {
                db_name,
                partition_key,
            } = list_chunks;

            let chunks = client.list_partition_chunks(db_name, partition_key).await?;

            let chunks = chunks
                .into_iter()
                .map(ChunkSummary::try_from)
                .collect::<Result<Vec<_>, FieldViolation>>()?;

            serde_json::to_writer_pretty(std::io::stdout(), &chunks)?;
        }
        Command::NewChunk(new_chunk) => {
            let NewChunk {
                db_name,
                partition_key,
                table_name,
            } = new_chunk;

            // Ignore response for now
            client
                .new_partition_chunk(db_name, table_name, partition_key)
                .await?;
            println!("Ok");
        }
        Command::CloseChunk(close_chunk) => {
            let CloseChunk {
                db_name,
                partition_key,
                table_name,
                chunk_id,
            } = close_chunk;

            let operation = client
                .close_partition_chunk(db_name, table_name, partition_key, chunk_id)
                .await?;

            serde_json::to_writer_pretty(std::io::stdout(), &operation)?;
        }
        Command::UnloadChunk(close_chunk) => {
            let UnloadChunk {
                db_name,
                partition_key,
                table_name,
                chunk_id,
            } = close_chunk;

            client
                .unload_partition_chunk(db_name, table_name, partition_key, chunk_id)
                .await?;
            println!("Ok");
        }
    }

    Ok(())
}
