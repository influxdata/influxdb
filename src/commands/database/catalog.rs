use std::convert::TryInto;

use data_types::job::Operation;
use generated_types::google::FieldViolation;
use influxdb_iox_client::{
    connection::Builder,
    management::{self, WipePersistedCatalogError},
};
use snafu::{ResultExt, Snafu};
use structopt::StructOpt;

#[allow(clippy::enum_variant_names)]
#[derive(Debug, Snafu)]
pub enum Error {
    #[snafu(display("Error connection to IOx: {}", source))]
    ConnectionError {
        source: influxdb_iox_client::connection::Error,
    },

    #[snafu(display("Need to pass `--force`"))]
    NeedsTheForceError,

    #[snafu(display("Error wiping persisted catalog: {}", source))]
    WipeError { source: WipePersistedCatalogError },

    #[snafu(display("Received invalid response: {}", source))]
    InvalidResponse { source: FieldViolation },

    #[snafu(display("Error rendering response as JSON: {}", source))]
    WritingJson { source: serde_json::Error },
}
pub type Result<T, E = Error> = std::result::Result<T, E>;

/// Manage IOx persisted catalog
#[derive(Debug, StructOpt)]
pub struct Config {
    #[structopt(subcommand)]
    command: Command,
}

/// All possible subcommands for catalog
#[derive(Debug, StructOpt)]
enum Command {
    /// Wipe persisted catalog
    Wipe(Wipe),
}

/// Wipe persisted catalog.
#[derive(Debug, StructOpt)]
struct Wipe {
    /// Force wipe. Required option to prevent accidental erasure
    #[structopt(long)]
    force: bool,

    /// The name of the database
    db_name: String,
}

pub async fn command(url: String, config: Config) -> Result<()> {
    let connection = Builder::default()
        .build(url)
        .await
        .context(ConnectionError)?;
    let mut client = management::Client::new(connection);

    match config.command {
        Command::Wipe(wipe) => {
            let Wipe { force, db_name } = wipe;

            if !force {
                return Err(Error::NeedsTheForceError);
            }

            let operation: Operation = client
                .wipe_persisted_catalog(db_name)
                .await
                .context(WipeError)?
                .try_into()
                .context(InvalidResponse)?;

            serde_json::to_writer_pretty(std::io::stdout(), &operation).context(WritingJson)?;
        }
    }

    Ok(())
}
