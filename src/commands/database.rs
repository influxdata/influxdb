//! This module implements the `database` CLI command
use std::{fs::File, io::Read, path::PathBuf};

use influxdb_iox_client::{
    connection::Builder,
    management::{
        self, generated_types::*, CreateDatabaseError, GetDatabaseError, ListDatabaseError,
    },
    write::{self, WriteError},
};
use structopt::StructOpt;
use thiserror::Error;

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

    /// Create a mutable buffer of the specified size in bytes
    #[structopt(short, long)]
    mutable_buffer: Option<u64>,
}

/// Get list of databases, or return configuration of specific database
#[derive(Debug, StructOpt)]
struct Get {
    /// If specified returns configuration of database
    name: Option<String>,
}

/// Write data into the specified database
#[derive(Debug, StructOpt)]
struct Write {
    /// The name of the database
    name: String,

    /// File with data to load. Currently supported formats are .lp
    file_name: PathBuf,
}

/// All possible subcommands for database
#[derive(Debug, StructOpt)]
enum Command {
    Create(Create),
    Get(Get),
    Write(Write),
}

pub async fn command(url: String, config: Config) -> Result<()> {
    let connection = Builder::default().build(url).await?;

    match config.command {
        Command::Create(command) => {
            let mut client = management::Client::new(connection);
            client
                .create_database(DatabaseRules {
                    name: command.name,
                    mutable_buffer_config: command.mutable_buffer.map(|buffer_size| {
                        MutableBufferConfig {
                            buffer_size,
                            ..Default::default()
                        }
                    }),
                    ..Default::default()
                })
                .await?;
            println!("Ok");
        }
        Command::Get(get) => {
            let mut client = management::Client::new(connection);
            if let Some(name) = get.name {
                let database = client.get_database(name).await?;
                // TOOD: Do something better than this
                println!("{:#?}", database);
            } else {
                let databases = client.list_databases().await?;
                println!("{}", databases.join(", "))
            }
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
    }

    Ok(())
}
