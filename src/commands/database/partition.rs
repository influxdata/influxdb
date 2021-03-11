//! This module implements the `partition` CLI command
use influxdb_iox_client::{
    connection::Builder,
    management::{self, GetPartitionError, ListPartitionsError},
};
use structopt::StructOpt;
use thiserror::Error;

#[derive(Debug, Error)]
pub enum Error {
    #[error("Error listing partitions: {0}")]
    ListPartitionsError(#[from] ListPartitionsError),

    #[error("Error getting partition: {0}")]
    GetPartitionsError(#[from] GetPartitionError),

    #[error("Error rendering response as JSON: {0}")]
    WritingJson(#[from] serde_json::Error),

    // #[error("Error rendering response as JSON: {0}")]
    // WritingJson(#[from] serde_json::Error),
    #[error("Error connecting to IOx: {0}")]
    ConnectionError(#[from] influxdb_iox_client::connection::Error),
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

/// All possible subcommands for partition
#[derive(Debug, StructOpt)]
enum Command {
    // List partitions
    List(List),
    // Get details about a particular partition
    Get(Get),
}

pub async fn command(url: String, config: Config) -> Result<()> {
    let connection = Builder::default().build(url).await?;
    let mut client = management::Client::new(connection);

    match config.command {
        Command::List(list) => {
            let List { db_name } = list;
            let partition_keys = client.list_partitions(db_name).await?;
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
    }

    Ok(())
}
