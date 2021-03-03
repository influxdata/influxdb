use influxdb_iox_client::{connection::Builder, management::*};
use std::num::NonZeroU32;
use structopt::StructOpt;
use thiserror::Error;

#[derive(Debug, Error)]
pub enum Error {
    #[error("Error getting writer ID: {0}")]
    GetWriterIdError(#[from] GetWriterIdError),

    #[error("Error updating writer ID: {0}")]
    UpdateWriterIdError(#[from] UpdateWriterIdError),

    #[error("Error connecting to IOx: {0}")]
    ConnectionError(#[from] influxdb_iox_client::connection::Error),
}

pub type Result<T, E = Error> = std::result::Result<T, E>;

/// Manage IOx writer ID
#[derive(Debug, StructOpt)]
pub struct Config {
    #[structopt(subcommand)]
    command: Command,
}

/// Set writer ID
#[derive(Debug, StructOpt)]
struct Set {
    /// The writer ID to set
    id: NonZeroU32,
}

#[derive(Debug, StructOpt)]
enum Command {
    Set(Set),

    /// Get writer ID
    Get,
}

pub async fn command(url: String, config: Config) -> Result<()> {
    let connection = Builder::default().build(url).await?;
    let mut client = Client::new(connection);

    match config.command {
        Command::Set(command) => {
            client.update_writer_id(command.id).await?;
            println!("Ok");
        }
        Command::Get => {
            let id = client.get_writer_id().await?;
            println!("{}", id);
        }
    }

    Ok(())
}
