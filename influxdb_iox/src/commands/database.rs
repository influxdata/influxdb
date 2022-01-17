//! This module implements the `database` CLI command

use crate::TABLE_STYLE_SINGLE_LINE_BORDERS;
use comfy_table::{Cell, Table};
use influxdb_iox_client::{
    connection::Connection,
    flight,
    format::QueryOutputFormat,
    management::{self, generated_types::database_status::DatabaseState, generated_types::*},
    write,
};
use std::{fs::File, io::Read, num::NonZeroU64, path::PathBuf, str::FromStr, time::Duration};
use thiserror::Error;
use time::TimeProvider;
use uuid::Uuid;

mod chunk;
mod partition;
mod recover;

#[allow(clippy::enum_variant_names)]
#[derive(Debug, Error)]
pub enum Error {
    #[error("Error reading file {:?}: {}", file_name, source)]
    ReadingFile {
        file_name: PathBuf,
        source: std::io::Error,
    },

    #[error("Error formatting: {0}")]
    FormattingError(#[from] influxdb_iox_client::format::Error),

    #[error("Error querying: {0}")]
    Query(#[from] influxdb_iox_client::flight::Error),

    #[error("Error in chunk subcommand: {0}")]
    Chunk(#[from] chunk::Error),

    #[error("Error in partition subcommand: {0}")]
    Partition(#[from] partition::Error),

    #[error("JSON Serialization error: {0}")]
    Serde(#[from] serde_json::Error),

    #[error("Error in partition subcommand: {0}")]
    Catalog(#[from] recover::Error),

    #[error("Client error: {0}")]
    ClientError(#[from] influxdb_iox_client::error::Error),
}

pub type Result<T, E = Error> = std::result::Result<T, E>;

/// Manage IOx databases
#[derive(Debug, clap::Parser)]
pub struct Config {
    #[clap(subcommand)]
    command: Command,
}

/// Create a new database
#[derive(Debug, clap::Parser)]
struct Create {
    /// The name of the database
    name: String,
    /// Once the total amount of buffered data in memory reaches this size start
    /// dropping data from memory based on the drop_order
    #[clap(long, default_value = "52428800")] // 52428800 = 50*1024*1024
    buffer_size_soft: usize,

    /// Once the amount of data in memory reaches this size start
    /// rejecting writes
    #[clap(long, default_value = "104857600")] // 104857600 = 100*1024*1024
    buffer_size_hard: usize,

    /// Persists chunks to object storage.
    #[clap(long = "skip-persist", parse(from_flag = std::ops::Not::not))]
    persist: bool,

    /// Do not allow writing new data to this database
    #[clap(long)]
    immutable: bool,

    /// After how many transactions should IOx write a new checkpoint?
    #[clap(long, default_value = "100", parse(try_from_str))]
    catalog_transactions_until_checkpoint: NonZeroU64,

    /// Prune catalog transactions older than the given age.
    ///
    /// Keeping old transaction can be useful for debugging.
    #[clap(long, default_value = "1d", parse(try_from_str = humantime::parse_duration))]
    catalog_transaction_prune_age: Duration,

    /// Once a partition hasn't received a write for this period of time,
    /// it will be compacted and, if set, persisted. Writers will generally
    /// have this amount of time to send late arriving writes or this could
    /// be their clock skew.
    #[clap(long, default_value = "300")]
    late_arrive_window_seconds: u32,

    /// Maximum number of rows before triggering persistence
    #[clap(long, default_value = "100000")]
    persist_row_threshold: u64,

    /// Maximum age of a write before triggering persistence
    #[clap(long, default_value = "1800")]
    persist_age_threshold_seconds: u32,

    /// Maximum number of rows to buffer in a MUB chunk before compacting it
    #[clap(long, default_value = "100000")]
    mub_row_threshold: u64,

    /// Use up to this amount of space in bytes for caching Parquet files. A
    /// value of zero disables Parquet file caching.
    #[clap(long, default_value = "0")]
    parquet_cache_limit: u64,
}

/// Get list of databases
#[derive(Debug, clap::Parser)]
struct List {
    /// Whether to list detailed information about the databases along with their names.
    #[clap(long)]
    detailed: bool,
}

/// Return configuration of specific database
#[derive(Debug, clap::Parser)]
struct Get {
    /// The name of the database
    name: String,

    /// If false, returns values for all fields, with defaults filled
    /// in. If true, only returns values which were explicitly set on
    /// database creation or update
    #[clap(long)]
    omit_defaults: bool,
}

/// Write data into the specified database
#[derive(Debug, clap::Parser)]
struct Write {
    /// The name of the database
    name: String,

    /// File with data to load. Currently supported formats are .lp
    file_name: PathBuf,
}

/// Query the data with SQL
#[derive(Debug, clap::Parser)]
struct Query {
    /// The name of the database
    name: String,

    /// The query to run, in SQL format
    query: String,

    /// Optional format ('pretty', 'json', or 'csv')
    #[clap(short, long, default_value = "pretty")]
    format: String,
}

/// Release a database from its current server owner
#[derive(Debug, clap::Parser)]
struct Release {
    /// The name of the database to release
    name: String,

    /// Optionally, the UUID of the database to delete. This must match the UUID of the current
    /// database with the given name, or the release operation will result in an error.
    #[clap(short, long)]
    uuid: Option<Uuid>,
}

/// Claim an unowned database
#[derive(Debug, clap::Parser)]
struct Claim {
    /// The UUID of the database to claim
    uuid: Uuid,

    /// Force this server to claim this database, even if it is
    /// ostensibly owned by another server.
    ///
    /// WARNING: ONLY do this if you are sure that no other servers
    /// are writing to this database (for example, the data files have
    /// been copied somewhere). If another server is currently writing
    /// to this database, corruption will very likely occur
    #[clap(long)]
    force: bool,
}

/// All possible subcommands for database
#[derive(Debug, clap::Parser)]
enum Command {
    /// Create a new database
    Create(Create),

    /// Get list of databases
    List(List),

    /// Return configuration of specific database
    Get(Get),

    /// Write data into the specified database
    Write(Write),

    /// Query the data with SQL
    Query(Query),

    /// Manage database chunks
    Chunk(chunk::Config),

    /// Manage database partitions
    Partition(partition::Config),

    /// Recover broken databases
    Recover(recover::Config),

    /// Release a database from its current server owner
    Release(Release),

    /// Claim an unowned database
    Claim(Claim),
}

pub async fn command(connection: Connection, config: Config) -> Result<()> {
    match config.command {
        Command::Create(command) => {
            let mut client = management::Client::new(connection);
            #[allow(deprecated)]
            let rules = DatabaseRules {
                name: command.name.clone(),
                lifecycle_rules: Some(LifecycleRules {
                    buffer_size_soft: command.buffer_size_soft as _,
                    buffer_size_hard: command.buffer_size_hard as _,
                    persist: command.persist,
                    immutable: command.immutable,
                    worker_backoff_millis: Default::default(),
                    max_active_compactions_cfg: Default::default(),
                    catalog_transactions_until_checkpoint: command
                        .catalog_transactions_until_checkpoint
                        .get(),
                    catalog_transaction_prune_age: Some(
                        command.catalog_transaction_prune_age.into(),
                    ),
                    late_arrive_window_seconds: command.late_arrive_window_seconds,
                    persist_row_threshold: command.persist_row_threshold,
                    persist_age_threshold_seconds: command.persist_age_threshold_seconds,
                    mub_row_threshold: command.mub_row_threshold,
                    parquet_cache_limit: command.parquet_cache_limit,
                }),

                // Default to hourly partitions
                partition_template: Some(PartitionTemplate {
                    parts: vec![partition_template::Part {
                        part: Some(partition_template::part::Part::Time(
                            "%Y-%m-%d %H:00:00".into(),
                        )),
                    }],
                }),

                // Note no write buffer config
                ..Default::default()
            };

            let uuid = client.create_database(rules).await?;

            println!("Created database {}", command.name);
            println!("{}", uuid);
        }
        Command::List(list) => {
            let mut client = management::Client::new(connection);
            if list.detailed {
                let ServerStatus {
                    initialized,
                    error,
                    database_statuses,
                } = client.get_server_status().await?;
                if !initialized {
                    eprintln!("Can not list databases. Server is not yet initialized");
                    if let Some(err) = error {
                        println!("WARNING: Server is in error state: {}", err.message);
                    }
                    return Ok(());
                }

                if !database_statuses.is_empty() {
                    let mut table = Table::new();
                    table.load_preset(TABLE_STYLE_SINGLE_LINE_BORDERS);
                    table.set_header(vec![
                        Cell::new("Name"),
                        Cell::new("UUID"),
                        Cell::new("State"),
                        Cell::new("Error"),
                    ]);

                    for database in database_statuses {
                        let uuid = if !database.uuid.is_empty() {
                            Uuid::from_slice(&database.uuid)
                                .map(|uuid| uuid.to_string())
                                .unwrap_or_else(|_| String::from("<UUID parsing failed>"))
                        } else {
                            String::from("<UUID not yet known>")
                        };

                        let state = DatabaseState::from_i32(database.state)
                            .map(|state| state.description())
                            .unwrap_or("UNKNOWN STATE");

                        let error = database
                            .error
                            .map(|e| e.message)
                            .unwrap_or_else(|| String::from("<none>"));

                        table.add_row(vec![
                            Cell::new(&database.db_name),
                            Cell::new(&uuid),
                            Cell::new(&state),
                            Cell::new(&error),
                        ]);
                    }

                    println!("{}", table);
                }
            } else {
                let names = client.list_database_names().await?;
                if !names.is_empty() {
                    println!("{}", names.join("\n"))
                }
            }
        }
        Command::Get(get) => {
            let Get {
                name,
                omit_defaults,
            } = get;
            let mut client = management::Client::new(connection);
            let database = client.get_database(name, omit_defaults).await?;
            println!("{}", serde_json::to_string_pretty(&database)?);
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

            let default_time = time::SystemProvider::new().now().timestamp_nanos();
            let lines_written = client.write_lp(write.name, lp_data, default_time).await?;

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
            chunk::command(connection, config).await?;
        }
        Command::Partition(config) => {
            partition::command(connection, config).await?;
        }
        Command::Recover(config) => {
            recover::command(connection, config).await?;
        }
        Command::Release(command) => {
            let mut client = management::Client::new(connection);
            let uuid = client.release_database(&command.name, command.uuid).await?;
            println!("Released database {}", command.name);
            println!("{}", uuid);
        }
        Command::Claim(command) => {
            let mut client = management::Client::new(connection);
            let db_name = client.claim_database(command.uuid, command.force).await?;
            println!("Claimed database {}", db_name);
        }
    }

    Ok(())
}
