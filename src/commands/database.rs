use influxdb_iox_client::{
    connection::Builder,
    management::{generated_types::*, *},
};
use structopt::StructOpt;
use thiserror::Error;

#[derive(Debug, Error)]
pub enum Error {
    #[error("Error creating database: {0}")]
    CreateDatabaseError(#[from] CreateDatabaseError),

    #[error("Error getting database: {0}")]
    GetDatabaseError(#[from] GetDatabaseError),

    #[error("Error listing databases: {0}")]
    ListDatabaseError(#[from] ListDatabaseError),

    #[error("Error connecting to IOx: {0}")]
    ConnectionError(#[from] influxdb_iox_client::connection::Error),
}

pub type Result<T, E = Error> = std::result::Result<T, E>;

/// Manage IOx databases
#[derive(Debug, StructOpt)]
pub struct Config {
    #[structopt(subcommand)]
    command: Command,
}

/// Create a new database
#[derive(Debug, StructOpt)]
struct Create {
    /// The name of the database
    name: String,

    /// Create a mutable buffer of the specified size in bytes
    #[structopt(short, long)]
    mutable_buffer: Option<u64>,
}

/// Get list of databases, or return configuration of specific database
#[derive(Debug, StructOpt)]
struct Get {
    /// If specified returns configuration of database
    name: Option<String>,
}

#[derive(Debug, StructOpt)]
enum Command {
    Create(Create),
    Get(Get),
}

pub async fn command(url: String, config: Config) -> Result<()> {
    let connection = Builder::default().build(url).await?;
    let mut client = Client::new(connection);

    match config.command {
        Command::Create(command) => {
            client
                .create_database(DatabaseRules {
                    name: command.name,
                    mutable_buffer_config: command.mutable_buffer.map(|buffer_size| {
                        MutableBufferConfig {
                            buffer_size,
                            ..Default::default()
                        }
                    }),
                    ..Default::default()
                })
                .await?;
            println!("Ok");
        }
        Command::Get(get) => {
            if let Some(name) = get.name {
                let database = client.get_database(name).await?;
                // TOOD: Do something better than this
                println!("{:#?}", database);
            } else {
                let databases = client.list_databases().await?;
                println!("{}", databases.join(", "))
            }
        }
    }

    Ok(())
}
