//! Entrypoint for interactive SQL repl loop

use observability_deps::tracing::debug;
use snafu::{ResultExt, Snafu};
use structopt::StructOpt;

use influxdb_iox_client::connection::{Builder, Connection};

mod observer;
mod repl;
mod repl_command;

/// Start IOx interactive SQL REPL loop
///
/// Supports command history and interactive editing. History is
/// stored in $HOME/.iox_sql_history.
#[derive(Debug, StructOpt)]
pub struct Config {
    // TODO add an option to avoid saving history

    // TODO add an option to specify the default database (rather than having to set it via USE DATABASE)
    /// Format to use for output. Can be overridden using
    /// `SET FORMAT` command
    ///
    /// Optional format ('pretty', 'json', or 'csv')
    #[structopt(short, long, default_value = "pretty")]
    format: String,
}

#[derive(Debug, Snafu)]
pub enum Error {
    #[snafu(display("Error connecting to {}: {}", url, source))]
    Connecting {
        url: String,
        source: influxdb_iox_client::connection::Error,
    },

    #[snafu(display("Storage health check failed: {}", source))]
    HealthCheck {
        source: influxdb_iox_client::health::Error,
    },

    #[snafu(display("Repl Error: {}", source))]
    Repl { source: repl::Error },
}

pub type Result<T, E = Error> = std::result::Result<T, E>;

/// Fire up the interactive REPL
pub async fn command(url: String, config: Config) -> Result<()> {
    debug!("Starting interactive SQL prompt with {:?}", config);

    let connection = Builder::default()
        .build(&url)
        .await
        .context(Connecting { url: &url })?;

    println!("Connected to IOx Server at {}", url);
    check_health(connection.clone()).await?;

    let mut repl = repl::Repl::new(connection);

    repl.set_output_format(config.format).context(Repl)?;

    repl.run().await.context(Repl)
}

async fn check_health(connection: Connection) -> Result<()> {
    influxdb_iox_client::health::Client::new(connection)
        .check_storage()
        .await
        .context(HealthCheck)
}
