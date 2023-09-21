//! Entrypoint for interactive SQL repl loop

use observability_deps::tracing::debug;
use snafu::{ResultExt, Snafu};

use influxdb_iox_client::{connection::Connection, health};

mod repl;
mod repl_command;

/// Start IOx interactive SQL REPL loop
///
/// Supports command history and interactive editing. History is
/// stored in $HOME/.iox_sql_history.
#[derive(Debug, clap::Parser)]
pub struct Config {
    // TODO add an option to avoid saving history

    // TODO add an option to specify the default database (rather than having to set it via USE DATABASE)
    /// Format to use for output. Can be overridden using
    /// `SET FORMAT` command
    ///
    /// Optional format ('pretty', 'json', or 'csv')
    #[clap(short, long, default_value = "pretty", action)]
    format: String,
}

#[derive(Debug, Snafu)]
pub enum Error {
    #[snafu(display("Health check request failed: {}", source))]
    Client {
        source: influxdb_iox_client::error::Error,
    },

    #[snafu(display("Storage service not running"))]
    StorageNotRunning,

    #[snafu(display("Repl Error: {}", source))]
    Repl { source: repl::Error },
}

pub type Result<T, E = Error> = std::result::Result<T, E>;

/// Fire up the interactive REPL
pub async fn command(connection: Connection, config: Config) -> Result<()> {
    debug!("Starting interactive SQL prompt with {:?}", config);

    if let Err(e) = check_health(connection.clone()).await {
        eprintln!("warning: flight healthcheck failed: {}", e);
    }

    println!("Connected to IOx Server");

    let mut repl = repl::Repl::new(connection).context(ReplSnafu)?;

    repl.set_output_format(config.format).context(ReplSnafu)?;

    repl.run().await.context(ReplSnafu)
}

async fn check_health(connection: Connection) -> Result<()> {
    let response = health::Client::new(connection)
        .check_arrow()
        .await
        .context(ClientSnafu)?;

    match response {
        true => Ok(()),
        false => Err(Error::StorageNotRunning),
    }
}
