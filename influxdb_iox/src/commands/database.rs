//! This module implements the `database` CLI command

use influxdb_iox_client::{
    connection::Connection,
    flight::{self, generated_types::ReadInfo},
    format::QueryOutputFormat,
};
use std::{num::NonZeroU64, path::PathBuf, str::FromStr, time::Duration};
use thiserror::Error;
use uuid::Uuid;

#[allow(clippy::enum_variant_names)]
#[derive(Debug, Error)]
pub enum Error {
    #[error("Error formatting: {0}")]
    FormattingError(#[from] influxdb_iox_client::format::Error),

    #[error("Error querying: {0}")]
    Query(#[from] influxdb_iox_client::flight::Error),

    #[error("JSON Serialization error: {0}")]
    Serde(#[from] serde_json::Error),

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

/// Shutdown database
#[derive(Debug, clap::Parser)]
struct Shutdown {
    /// The name of the database
    name: String,
}

/// Restart database
#[derive(Debug, clap::Parser)]
struct Restart {
    /// The name of the database
    name: String,

    /// Skip replay
    #[clap(long)]
    skip_replay: bool,
}

/// All possible subcommands for database
#[derive(Debug, clap::Parser)]
enum Command {
    /// Query the data with SQL
    Query(Query),
}

pub async fn command(connection: Connection, config: Config) -> Result<()> {
    match config.command {
        Command::Query(query) => {
            let mut client = flight::Client::new(connection);
            let Query {
                name,
                format,
                query,
            } = query;

            let format = QueryOutputFormat::from_str(&format)?;

            let mut query_results = client
                .perform_query(ReadInfo {
                    namespace_name: name,
                    sql_query: query,
                })
                .await?;

            // It might be nice to do some sort of streaming write
            // rather than buffering the whole thing.
            let mut batches = vec![];
            while let Some(data) = query_results.next().await? {
                batches.push(data);
            }

            let formatted_result = format.format(&batches)?;

            println!("{}", formatted_result);
        }
    }

    Ok(())
}
