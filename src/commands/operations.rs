use data_types::job::Operation;
use generated_types::google::FieldViolation;
use influxdb_iox_client::{
    connection::Builder,
    management,
    operations::{self, Client},
};
use std::convert::TryInto;
use structopt::StructOpt;
use thiserror::Error;

#[allow(clippy::enum_variant_names)]
#[derive(Debug, Error)]
pub enum Error {
    #[error("Error connecting to IOx: {0}")]
    ConnectionError(#[from] influxdb_iox_client::connection::Error),

    #[error("Client error: {0}")]
    ClientError(#[from] operations::Error),

    #[error("Received invalid response: {0}")]
    InvalidResponse(#[from] FieldViolation),

    #[error("Failed to create dummy job: {0}")]
    CreateDummyJobError(#[from] management::CreateDummyJobError),

    #[error("Output serialization error: {0}")]
    SerializationError(#[from] serde_json::Error),
}

pub type Result<T, E = Error> = std::result::Result<T, E>;

/// Manage long-running IOx operations
#[derive(Debug, StructOpt)]
pub struct Config {
    #[structopt(subcommand)]
    command: Command,
}

#[derive(Debug, StructOpt)]
enum Command {
    /// Get list of running operations
    List,

    /// Get a specific operation
    Get {
        /// The id of the operation
        id: usize,
    },

    /// Wait for a specific operation to complete
    Wait {
        /// The id of the operation
        id: usize,

        /// Maximum number of nanoseconds to wait before returning current
        /// status
        nanos: Option<u64>,
    },

    /// Cancel a specific operation
    Cancel {
        /// The id of the operation
        id: usize,
    },

    /// Spawns a dummy test operation
    Test { nanos: Vec<u64> },
}

pub async fn command(url: String, config: Config) -> Result<()> {
    let connection = Builder::default().build(url).await?;

    match config.command {
        Command::List => {
            let result: Result<Vec<Operation>, _> = Client::new(connection)
                .list_operations()
                .await?
                .into_iter()
                .map(|c| c.operation())
                .map(TryInto::try_into)
                .collect();
            let operations = result?;
            serde_json::to_writer_pretty(std::io::stdout(), &operations)?;
        }
        Command::Get { id } => {
            let operation: Operation = Client::new(connection)
                .get_operation(id)
                .await?
                .try_into()?;
            serde_json::to_writer_pretty(std::io::stdout(), &operation)?;
        }
        Command::Wait { id, nanos } => {
            let timeout = nanos.map(std::time::Duration::from_nanos);
            let operation: Operation = Client::new(connection)
                .wait_operation(id, timeout)
                .await?
                .try_into()?;
            serde_json::to_writer_pretty(std::io::stdout(), &operation)?;
        }
        Command::Cancel { id } => {
            Client::new(connection).cancel_operation(id).await?;
            println!("Ok");
        }
        Command::Test { nanos } => {
            let operation: Operation = management::Client::new(connection)
                .create_dummy_job(nanos)
                .await?
                .try_into()?;
            serde_json::to_writer_pretty(std::io::stdout(), &operation)?;
        }
    }

    Ok(())
}
