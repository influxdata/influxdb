//! Implementation of command line option for manipulating and showing server
//! config

use std::{
    num::NonZeroU32,
    time::{Duration, Instant},
};

use crate::commands::server_remote;
use generated_types::database_state::DatabaseState;
use influxdb_iox_client::{connection::Connection, deployment, management};
use thiserror::Error;

#[allow(clippy::enum_variant_names)]
#[derive(Debug, Error)]
pub enum Error {
    #[error("Remote: {0}")]
    RemoteError(#[from] server_remote::Error),

    #[error("Request error: {0}")]
    Request(#[from] influxdb_iox_client::error::Error),

    #[error("Timeout waiting for databases to be loaded")]
    TimeoutDatabasesLoaded,

    #[error("Server startup error: {0}")]
    Server(String),

    #[error("Database startup error for \"{0}\": {1}")]
    Database(String, String),
}

pub type Result<T, E = Error> = std::result::Result<T, E>;

#[derive(Debug, clap::Parser)]
#[clap(name = "server", about = "IOx server commands")]
pub struct Config {
    #[clap(subcommand)]
    command: Command,
}

#[derive(Debug, clap::Parser)]
enum Command {
    /// Set server ID
    Set(Set),

    /// Get server ID
    Get,

    /// Wait until server is initialized.
    WaitServerInitialized(WaitSeverInitialized),

    Remote(Remote),
}

/// Set server ID
#[derive(Debug, clap::Parser)]
struct Set {
    /// The server ID to set
    id: NonZeroU32,
}

/// Wait until server and all databases are initialized
#[derive(Debug, clap::Parser)]
struct WaitSeverInitialized {
    /// Timeout in seconds.
    #[clap(short, default_value = "10")]
    timeout: u64,
}

#[derive(Debug, clap::Parser)]
struct Remote {
    #[clap(subcommand)]
    config: server_remote::Config,
}

pub async fn command(connection: Connection, config: Config) -> Result<()> {
    match config.command {
        Command::Set(command) => {
            let mut client = deployment::Client::new(connection);
            client.update_server_id(command.id).await?;
            println!("Ok");
            Ok(())
        }
        Command::Get => {
            let mut client = deployment::Client::new(connection);
            match client.get_server_id().await? {
                Some(id) => println!("{}", id.get()),
                None => println!("NONE"),
            }
            Ok(())
        }
        Command::WaitServerInitialized(command) => {
            let mut client = management::Client::new(connection);
            let end = Instant::now() + Duration::from_secs(command.timeout);
            loop {
                let server_status = client.get_server_status().await?;
                if let Some(err) = server_status.error {
                    return Err(Error::Server(err.message));
                }

                if server_status.initialized {
                    let mut initialized = true;
                    for db_status in server_status.database_statuses {
                        if let Some(err) = db_status.error {
                            return Err(Error::Database(db_status.db_name, err.message));
                        }

                        if db_status.state() != DatabaseState::Initialized {
                            initialized = false;
                            break;
                        }
                    }

                    if initialized {
                        println!("Server initialized.");
                        return Ok(());
                    }
                }

                if Instant::now() >= end {
                    eprintln!("timeout");
                    return Err(Error::TimeoutDatabasesLoaded);
                }
                tokio::time::sleep(Duration::from_millis(100)).await;
            }
        }
        Command::Remote(config) => Ok(server_remote::command(connection, config.config).await?),
    }
}
