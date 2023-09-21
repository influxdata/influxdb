//! This module implements CLI commands for debugging the ingester WAL.

use futures::Future;

use influxdb_iox_client::connection::Connection;
use thiserror::Error;

mod inspect;
mod regenerate_lp;

/// A command level error type to decorate WAL errors with some extra
/// "human" context for the user
#[derive(Debug, Error)]
pub enum Error {
    #[error("could not read WAL file: {0}")]
    UnableToReadWalFile(#[from] wal::Error),

    #[error("failed to decode write entries from the WAL file: {0}")]
    FailedToDecodeWriteOpEntry(#[from] wal::DecodeError),

    #[error(
        "failures encountered while regenerating line protocol writes from WAL file: {sources:?}"
    )]
    UnableToFullyRegenerateLineProtocol { sources: Vec<RegenerateError> },

    #[error("failed to initialise table name index fetcher: {0}")]
    UnableToInitTableNameFetcher(regenerate_lp::TableIndexLookupError),

    #[error("i/o failure: {0}")]
    IoFailure(#[from] std::io::Error),

    #[error("errors occurred during inspection of the WAL file: {sources:?}")]
    IncompleteInspection { sources: Vec<wal::Error> },
}

/// A set of non-fatal errors which can occur during the regeneration of write
/// operations from WAL entries
#[derive(Debug, Error)]
pub enum RegenerateError {
    #[error("failed to rediscover namespace schema: {0}")]
    NamespaceSchemaDiscoveryFailed(#[from] regenerate_lp::TableIndexLookupError),
    #[error("failed to rewrite a table batch: {0}")]
    TableBatchWriteFailure(#[from] wal_inspect::WriteError),
}

#[derive(Debug, clap::Parser)]
pub struct Config {
    #[clap(subcommand)]
    command: Command,
}

/// Subcommands for debugging the ingester WAL
#[derive(Debug, clap::Parser)]
enum Command {
    /// Inspect the encoded contents of a WAL file in a human readable manner
    Inspect(inspect::Config),
    /// Regenerate line protocol writes from the contents of a WAL file. When
    /// looking up measurement names from IOx, the target host must implement
    /// the namespace and schema APIs
    RegenerateLp(regenerate_lp::Config),
}

/// Executes a WAL debugging subcommand as directed by the config
pub async fn command<C, CFut>(connection: C, config: Config) -> Result<(), Error>
where
    C: Send + FnOnce() -> CFut,
    CFut: Send + Future<Output = Connection>,
{
    match config.command {
        Command::Inspect(config) => inspect::command(config),
        Command::RegenerateLp(config) => regenerate_lp::command(connection, config).await,
    }
}
