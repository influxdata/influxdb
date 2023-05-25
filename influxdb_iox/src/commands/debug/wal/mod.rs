//! This module implements CLI commands for debugging the ingester WAL.

use thiserror::Error;

mod regenerate_lp;

/// A command level error type to decorate WAL errors with some extra
/// "human" context for the user
#[derive(Debug, Error)]
pub enum Error {
    #[error("could not open WAL file: {source}")]
    UnableToOpenWalFile { source: wal::Error },

    #[error("failed to decode write entries from the WAL file: {0}")]
    FailedToDecodeWriteOpEntry(#[from] wal::DecodeError),

    #[error(
        "failures encountered while regenerating line protocol writes from WAL file: {sources:?}"
    )]
    UnableToFullyRegenerateLineProtocol {
        sources: Vec<wal_inspect::WriteError>,
    },

    #[error("i/o failure: {0}")]
    IoFailure(#[from] std::io::Error),
}

#[derive(Debug, clap::Parser)]
pub struct Config {
    #[clap(subcommand)]
    command: Command,
}

/// Subcommands for debugging the ingester WAL
#[derive(Debug, clap::Parser)]
enum Command {
    /// Regenerate line protocol writes from the contents of a WAL file
    RegenerateLp(regenerate_lp::Config),
}

/// Executes a WAL debugging subcommand as directed by the config
pub async fn command(config: Config) -> Result<(), Error> {
    match config.command {
        Command::RegenerateLp(config) => regenerate_lp::command(config).await,
    }
}
