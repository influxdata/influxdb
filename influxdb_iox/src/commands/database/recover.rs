use generated_types::google::FieldViolation;
use influxdb_iox_client::{connection::Connection, management};
use snafu::{ResultExt, Snafu};
use structopt::StructOpt;

#[allow(clippy::enum_variant_names)]
#[derive(Debug, Snafu)]
pub enum Error {
    #[snafu(display("Need to pass `--force`"))]
    NeedsTheForceError,

    #[snafu(display("Error wiping preserved catalog: {}", source))]
    WipeError {
        source: influxdb_iox_client::error::Error,
    },

    #[snafu(display("Error skipping replay: {}", source))]
    SkipReplayError {
        source: influxdb_iox_client::error::Error,
    },

    #[snafu(display("Error rebuilding preserved catalog: {}", source))]
    RebuildCatalog {
        source: influxdb_iox_client::error::Error,
    },

    #[snafu(display("Received invalid response: {}", source))]
    InvalidResponse { source: FieldViolation },

    #[snafu(display("Error rendering response as JSON: {}", source))]
    WritingJson { source: serde_json::Error },
}
pub type Result<T, E = Error> = std::result::Result<T, E>;

/// Recover broken databases.
#[derive(Debug, StructOpt)]
pub struct Config {
    #[structopt(subcommand)]
    command: Command,
}

/// All possible subcommands for recovering broken databases
#[derive(Debug, StructOpt)]
enum Command {
    /// Wipe preserved catalog
    Wipe(Wipe),

    /// Skip replay
    SkipReplay(SkipReplay),

    /// Rebuild catalog from parquet fles
    Rebuild(Rebuild),
}

/// Wipe preserved catalog.
#[derive(Debug, StructOpt)]
struct Wipe {
    /// Force wipe. Required option to prevent accidental erasure
    #[structopt(long)]
    force: bool,

    /// The name of the database
    db_name: String,
}

/// Rebuild catalog from parquet files
#[derive(Debug, StructOpt)]
struct Rebuild {
    /// Force rebuild, even if the database has already successfully started
    #[structopt(long)]
    force: bool,

    /// The name of the database
    db_name: String,
}

/// Skip replay
#[derive(Debug, StructOpt)]
struct SkipReplay {
    /// The name of the database
    db_name: String,
}

pub async fn command(connection: Connection, config: Config) -> Result<()> {
    let mut client = management::Client::new(connection);

    match config.command {
        Command::Wipe(wipe) => {
            let Wipe { force, db_name } = wipe;

            if !force {
                return Err(Error::NeedsTheForceError);
            }

            let operation = client
                .wipe_preserved_catalog(db_name)
                .await
                .context(WipeSnafu)?;

            serde_json::to_writer_pretty(std::io::stdout(), &operation)
                .context(WritingJsonSnafu)?;
        }
        Command::SkipReplay(skip_replay) => {
            let SkipReplay { db_name } = skip_replay;

            client.skip_replay(db_name).await.context(SkipReplaySnafu)?;

            println!("Ok");
        }
        Command::Rebuild(rebuild) => {
            let operation = client
                .rebuild_preserved_catalog(rebuild.db_name, rebuild.force)
                .await
                .context(RebuildCatalogSnafu)?;

            serde_json::to_writer_pretty(std::io::stdout(), &operation)
                .context(WritingJsonSnafu)?;
        }
    }

    Ok(())
}
