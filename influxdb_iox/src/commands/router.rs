//! This module implements the `router` CLI command

use influxdb_iox_client::{
    connection::Connection,
    router::{self, generated_types::Router as RouterConfig},
};
use thiserror::Error;

#[allow(clippy::enum_variant_names)]
#[derive(Debug, Error)]
pub enum Error {
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

/// Create a new router or update an existing one.
#[derive(Debug, clap::Parser)]
struct CreateOrUpdate {
    /// The name of the router
    name: String,
}

/// Return configuration of specific router
#[derive(Debug, clap::Parser)]
struct Get {
    /// The name of the router
    name: String,
}

/// Delete specific router
#[derive(Debug, clap::Parser)]
struct Delete {
    /// The name of the router
    name: String,
}

/// All possible subcommands for router
#[derive(Debug, clap::Parser)]
enum Command {
    CreateOrUpdate(CreateOrUpdate),

    /// List routers
    List,

    Get(Get),

    Delete(Delete),
}

pub async fn command(connection: Connection, config: Config) -> Result<()> {
    match config.command {
        Command::CreateOrUpdate(command) => {
            let mut client = router::Client::new(connection);
            let config = RouterConfig {
                name: command.name.clone(),
                ..Default::default()
            };

            client.update_router(config).await?;

            println!("Created/Updated router {}", command.name);
        }
        Command::List => {
            let mut client = router::Client::new(connection);
            let routers = client.list_routers().await?;
            for router in routers {
                println!("{}", router.name);
            }
        }
        Command::Get(get) => {
            let Get { name } = get;
            let mut client = router::Client::new(connection);
            let router = client.get_router(&name).await?;
            println!("{}", serde_json::to_string_pretty(&router)?);
        }
        Command::Delete(delete) => {
            let Delete { name } = delete;
            let mut client = router::Client::new(connection);
            client.delete_router(&name).await?;

            println!("Deleted router {}", name);
        }
    }

    Ok(())
}
