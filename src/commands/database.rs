//! This module implements the `database` CLI command
use std::{fs::File, io::Read, path::PathBuf, str::FromStr};

use influxdb_iox_client::{
    connection::Builder,
    flight,
    format::QueryOutputFormat,
    management::{
        self, generated_types::*, CreateDatabaseError, GetDatabaseError, ListDatabaseError,
    },
    write::{self, WriteError},
};
use structopt::StructOpt;
use thiserror::Error;

mod chunk;
mod partition;

#[derive(Debug, Error)]
pub enum Error {
    #[error("Error creating database: {0}")]
    CreateDatabaseError(#[from] CreateDatabaseError),

    #[error("Error getting database: {0}")]
    GetDatabaseError(#[from] GetDatabaseError),

    #[error("Error listing databases: {0}")]
    ListDatabaseError(#[from] ListDatabaseError),

    #[error("Error connecting to IOx: {0}")]
    ConnectionError(#[from] influxdb_iox_client::connection::Error),

    #[error("Error reading file {:?}: {}", file_name, source)]
    ReadingFile {
        file_name: PathBuf,
        source: std::io::Error,
    },

    #[error("Error writing: {0}")]
    WriteError(#[from] WriteError),

    #[error("Error formatting: {0}")]
    FormattingError(#[from] influxdb_iox_client::format::Error),

    #[error("Error querying: {0}")]
    Query(#[from] influxdb_iox_client::flight::Error),

    #[error("Error in chunk subcommand: {0}")]
    Chunk(#[from] chunk::Error),

    #[error("Error in partition subcommand: {0}")]
    Partition(#[from] partition::Error),
}

pub type Result<T, E = Error> = std::result::Result<T, E>;

/// Manage IOx databases
#[derive(Debug, StructOpt)]
pub struct Config {
    #[structopt(subcommand)]
    command: Command,
}

/// Create a new database
#[derive(Debug, StructOpt)]
struct Create {
    /// The name of the database
    name: String,

    /// Create a mutable buffer of the specified size in bytes.  If
    /// size is 0, no mutable buffer is created.
    #[structopt(short, long, default_value = "104857600")] // 104857600 = 100*1024*1024
    mutable_buffer: u64,
}

/// Get list of databases
#[derive(Debug, StructOpt)]
struct List {}

/// Return configuration of specific database
#[derive(Debug, StructOpt)]
struct Get {
    /// The name of the database
    name: String,
}

/// Write data into the specified database
#[derive(Debug, StructOpt)]
struct Write {
    /// The name of the database
    name: String,

    /// File with data to load. Currently supported formats are .lp
    file_name: PathBuf,
}

/// Query the data with SQL
#[derive(Debug, StructOpt)]
struct Query {
    /// The name of the database
    name: String,

    /// The query to run, in SQL format
    query: String,

    /// Optional format ('pretty', 'json', or 'csv')
    #[structopt(short, long, default_value = "pretty")]
    format: String,
}

/// All possible subcommands for database
#[derive(Debug, StructOpt)]
enum Command {
    Create(Create),
    List(List),
    Get(Get),
    Write(Write),
    Query(Query),
    Chunk(chunk::Config),
    Partition(partition::Config),
}

pub async fn command(url: String, config: Config) -> Result<()> {
    let connection = Builder::default().build(url.clone()).await?;

    match config.command {
        Command::Create(command) => {
            let mut client = management::Client::new(connection);

            // Configure a mutable buffer if requested
            let buffer_size = command.mutable_buffer;
            let mutable_buffer_config = if buffer_size > 0 {
                Some(MutableBufferConfig {
                    buffer_size,
                    ..Default::default()
                })
            } else {
                None
            };

            let rules = DatabaseRules {
                name: command.name,

                mutable_buffer_config,

                // Default to hourly partitions
                partition_template: Some(PartitionTemplate {
                    parts: vec![partition_template::Part {
                        part: Some(partition_template::part::Part::Time(
                            "%Y-%m-%d %H:00:00".into(),
                        )),
                    }],
                }),

                // Note no wal buffer config
                ..Default::default()
            };

            client.create_database(rules).await?;

            println!("Ok");
        }
        Command::List(_) => {
            let mut client = management::Client::new(connection);
            let databases = client.list_databases().await?;
            println!("{}", databases.join(", "))
        }
        Command::Get(get) => {
            let mut client = management::Client::new(connection);
            let database = client.get_database(get.name).await?;
            // TOOD: Do something better than this
            println!("{:#?}", database);
        }
        Command::Write(write) => {
            let mut client = write::Client::new(connection);

            let mut file = File::open(&write.file_name).map_err(|e| Error::ReadingFile {
                file_name: write.file_name.clone(),
                source: e,
            })?;

            let mut lp_data = String::new();
            file.read_to_string(&mut lp_data)
                .map_err(|e| Error::ReadingFile {
                    file_name: write.file_name.clone(),
                    source: e,
                })?;

            let lines_written = client.write(write.name, lp_data).await?;

            println!("{} Lines OK", lines_written);
        }
        Command::Query(query) => {
            let mut client = flight::Client::new(connection);
            let Query {
                name,
                format,
                query,
            } = query;

            let format = QueryOutputFormat::from_str(&format)?;

            let mut query_results = client.perform_query(&name, query).await?;

            // It might be nice to do some sort of streaming write
            // rather than buffering the whole thing.
            let mut batches = vec![];
            while let Some(data) = query_results.next().await? {
                batches.push(data);
            }

            let formatted_result = format.format(&batches)?;

            println!("{}", formatted_result);
        }
        Command::Chunk(config) => {
            chunk::command(url, config).await?;
        }
        Command::Partition(config) => {
            partition::command(url, config).await?;
        }
    }

    Ok(())
}
