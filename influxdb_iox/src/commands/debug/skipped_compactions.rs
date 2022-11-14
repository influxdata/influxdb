//! This module implements the `skipped-compactions` CLI command

use comfy_table::{Cell, Table};
use influxdb_iox_client::{
    compactor::{self, generated_types::SkippedCompaction},
    connection::Connection,
};
use iox_time::Time;
use thiserror::Error;

#[derive(Debug, Error)]
pub enum Error {
    #[error("JSON Serialization error: {0}")]
    Serde(#[from] serde_json::Error),

    #[error("Client error: {0}")]
    Client(#[from] influxdb_iox_client::error::Error),

    #[error("Invalid `skipped_at` timestamp: {0}s")]
    InvalidTimestamp(i64),
}

/// Various commands for skipped compaction inspection
#[derive(Debug, clap::Parser)]
pub struct Config {
    #[clap(subcommand)]
    command: Command,
}

/// All possible subcommands for skipped compaction
#[derive(Debug, clap::Parser)]
enum Command {
    /// List all skipped compactions
    List,

    /// Delete the requested skipped compaction
    Delete { partition_id: i64 },
}

pub async fn command(connection: Connection, config: Config) -> Result<(), Error> {
    let mut client = compactor::Client::new(connection);
    match config.command {
        Command::List => {
            let skipped_compactions = client.skipped_compactions().await?;
            println!("{}", create_table(&skipped_compactions)?);
        }

        Command::Delete { partition_id } => {
            let deleted_skipped_compactions =
                client.delete_skipped_compactions(partition_id).await?;

            let deleted_skipped_compactions = deleted_skipped_compactions
                .as_ref()
                .map(std::slice::from_ref)
                .unwrap_or_default();

            println!("{}", create_table(deleted_skipped_compactions)?);
        } // Deliberately not adding _ => so the compiler will direct people here to impl new
          // commands
    }

    Ok(())
}

/// Turn skipped compaction records into a table
#[allow(clippy::result_large_err)]
fn create_table(skipped_compactions: &[SkippedCompaction]) -> Result<Table, Error> {
    let mut table = Table::new();
    table.load_preset("||--+-++|    ++++++");

    let headers: Vec<_> = [
        "partition_id",
        "reason",
        "skipped_at",
        "estimated_bytes",
        "limit_bytes",
        "num_files",
        "limit_num_files",
    ]
    .into_iter()
    .map(Cell::new)
    .collect();
    table.set_header(headers);

    for skipped_compaction in skipped_compactions {
        let timestamp = Time::from_timestamp(skipped_compaction.skipped_at, 0)
            .ok_or(Error::InvalidTimestamp(skipped_compaction.skipped_at))?;

        table.add_row(vec![
            Cell::new(skipped_compaction.partition_id.to_string()),
            Cell::new(&skipped_compaction.reason),
            Cell::new(timestamp.to_rfc3339()),
            Cell::new(skipped_compaction.estimated_bytes.to_string()),
            Cell::new(skipped_compaction.limit_bytes.to_string()),
            Cell::new(skipped_compaction.num_files.to_string()),
            Cell::new(skipped_compaction.limit_num_files.to_string()),
        ]);
    }

    Ok(table)
}
