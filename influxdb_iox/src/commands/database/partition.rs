//! This module implements the `partition` CLI command
use data_types::chunk_metadata::ChunkStorage;
use generated_types::google::FieldViolation;
use influxdb_iox_client::{
    connection::Connection,
    management::{self},
};
use std::collections::BTreeSet;
use thiserror::Error;
use uuid::Uuid;

#[allow(clippy::enum_variant_names)]
#[derive(Debug, Error)]
pub enum Error {
    #[error("Client error: {0}")]
    ClientError(#[from] influxdb_iox_client::error::Error),

    #[error("Error rendering response as JSON: {0}")]
    WritingJson(#[from] serde_json::Error),

    #[error("Received invalid response: {0}")]
    InvalidResponse(#[from] FieldViolation),

    #[error("Must either specify --table-name or --all-tables")]
    MissingTableName,

    #[error("Must either specify a --partition-key or --all-partitions")]
    MissingPartitionKey,

    #[error("Some operations returned an error, but --continue-on-error passed")]
    ContinuedOnError,
}

pub type Result<T, E = Error> = std::result::Result<T, E>;

/// Manage IOx partitions
#[derive(Debug, clap::Parser)]
pub struct Config {
    #[clap(subcommand)]
    command: Command,
}

/// List all known partition keys for a database
#[derive(Debug, clap::Parser)]
struct List {
    /// The name of the database
    db_name: String,
}

/// Get details of a specific partition in JSON format (TODO)
#[derive(Debug, clap::Parser)]
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
#[derive(Debug, clap::Parser)]
struct Persist {
    /// The name of the database
    db_name: String,

    /// The partition key
    #[clap(long)]
    partition_key: Option<String>,

    /// The table name
    #[clap(long)]
    table_name: Option<String>,

    /// Persist all data irrespective of arrival time
    #[clap(long)]
    force: bool,

    /// Persist all tables that have data
    #[clap(long)]
    all_tables: bool,

    /// Persist all partitions that have data
    #[clap(long)]
    all_partitions: bool,

    /// Continue on error
    #[clap(long)]
    continue_on_error: bool,
}

/// Compact Object Store Chunks
///
/// Errors if the chunks are not yet compacted and not contiguous.
#[derive(Debug, clap::Parser)]
struct CompactObjectStoreChunks {
    /// The name of the database
    db_name: String,

    /// The partition key
    partition_key: String,

    /// The table name
    table_name: String,

    /// The chunk ids
    chunk_ids: Vec<Uuid>,
}

/// Compact all Object Store Chunks of a partition
#[derive(Debug, clap::Parser)]
struct CompactObjectStorePartition {
    /// The name of the database
    db_name: String,

    /// The partition key
    partition_key: String,

    /// The table name
    table_name: String,
}

/// lists all chunks in this partition
#[derive(Debug, clap::Parser)]
struct ListChunks {
    /// The name of the database
    db_name: String,

    /// The partition key
    partition_key: String,
}

/// Create a new, open chunk in the partiton's Mutable Buffer which will receive
/// new writes.
#[derive(Debug, clap::Parser)]
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
#[derive(Debug, clap::Parser)]
struct CloseChunk {
    /// The name of the database
    db_name: String,

    /// The partition key
    partition_key: String,

    /// The table name
    table_name: String,

    /// The chunk id
    chunk_id: Uuid,
}

/// Unload chunk from read buffer but keep it in object store.
#[derive(Debug, clap::Parser)]
struct UnloadChunk {
    /// The name of the database
    db_name: String,

    /// The partition key
    partition_key: String,

    /// The table name
    table_name: String,

    /// The chunk id
    chunk_id: Uuid,
}

/// Drop partition from memory and (if persisted) from object store.
#[derive(Debug, clap::Parser)]
struct DropPartition {
    /// The name of the database
    db_name: String,

    /// The partition key
    partition_key: String,

    /// The table name
    table_name: String,
}

/// All possible subcommands for partition
#[derive(Debug, clap::Parser)]
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

    /// Compact Object Store Chunks
    ///
    /// Errors if the chunks are not yet compacted and not contiguous.
    CompactObjectStoreChunks(CompactObjectStoreChunks),

    /// Compact all object store chunks of a given partition
    CompactObjectStorePartition(CompactObjectStorePartition),

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

            let management::generated_types::Partition { key, table_name } =
                client.get_partition(db_name, partition_key).await?;

            // TODO: get more details from the partition, and print it
            // out better (i.e. move to using Partition summary that
            // is already in data_types)
            #[derive(serde::Serialize)]
            struct PartitionDetail {
                key: String,
                table_name: String,
            }

            let partition_detail = PartitionDetail { key, table_name };

            serde_json::to_writer_pretty(std::io::stdout(), &partition_detail)?;
        }
        Command::Persist(persist) => {
            let Persist {
                db_name,
                partition_key,
                table_name,
                force,
                all_tables,
                all_partitions,
                continue_on_error,
            } = persist;

            let mut has_error = false;

            let partition_filter = match (partition_key, all_partitions) {
                (Some(partition_key), false) => Some(partition_key),
                (None, true) => None,
                _ => return Err(Error::MissingPartitionKey),
            };

            let table_filter = match (table_name, all_tables) {
                (Some(table_name), false) => Some(table_name),
                (None, true) => None,
                _ => return Err(Error::MissingTableName),
            };

            let mut partition_tables = BTreeSet::new();
            let chunks = client.list_chunks(&db_name).await?;
            for chunk in chunks {
                let partition_mismatch =
                    matches!(&partition_filter, Some(x) if &chunk.partition_key != x);

                let table_mismatch = matches!(&table_filter, Some(x) if &chunk.table_name != x);
                let already_persisted = ChunkStorage::try_from(chunk.storage())?.has_object_store();
                if !partition_mismatch && !table_mismatch && !already_persisted {
                    partition_tables.insert((chunk.partition_key, chunk.table_name));
                }
            }

            for (partition, table_name) in partition_tables {
                println!(
                    "Persisting partition: \"{}\", table: \"{}\"",
                    partition, table_name
                );

                let result = client
                    .persist_partition(&db_name, &table_name, &partition, force)
                    .await;

                if let Err(e) = result {
                    if !continue_on_error {
                        return Err(e.into());
                    }

                    has_error = true;
                    eprintln!(
                        "Error persisting partition: \"{}\", table: \"{}\": {}",
                        partition, table_name, e
                    )
                }
            }

            match has_error {
                true => return Err(Error::ContinuedOnError),
                false => println!("Ok"),
            }
        }
        Command::CompactObjectStoreChunks(compact) => {
            let CompactObjectStoreChunks {
                db_name,
                partition_key,
                table_name,
                chunk_ids,
            } = compact;

            let chunk_ids = chunk_ids
                .iter()
                .map(|chunk_id| chunk_id.as_bytes().to_vec().into())
                .collect();

            let operation = client
                .compact_object_store_chunks(db_name, table_name, partition_key, chunk_ids)
                .await?;

            serde_json::to_writer_pretty(std::io::stdout(), &operation)?;
        }
        Command::CompactObjectStorePartition(compact) => {
            let CompactObjectStorePartition {
                db_name,
                partition_key,
                table_name,
            } = compact;

            let operation = client
                .compact_object_store_partition(db_name, table_name, partition_key)
                .await?;

            serde_json::to_writer_pretty(std::io::stdout(), &operation)?;
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
                .close_partition_chunk(
                    db_name,
                    table_name,
                    partition_key,
                    chunk_id.as_bytes().to_vec().into(),
                )
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
                .unload_partition_chunk(
                    db_name,
                    table_name,
                    partition_key,
                    chunk_id.as_bytes().to_vec().into(),
                )
                .await?;
            println!("Ok");
        }
    }

    Ok(())
}
