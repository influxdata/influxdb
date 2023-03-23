use influxdb_iox_client::connection::Connection;
use thiserror::Error;

#[allow(clippy::enum_variant_names)]
#[derive(Debug, Error)]
pub enum Error {
    #[error("JSON Serialization error: {0}")]
    Serde(#[from] serde_json::Error),

    #[error("Client error: {0}")]
    ClientError(#[from] influxdb_iox_client::error::Error),
}

#[derive(Debug, clap::Parser)]
pub struct Config {
    /// The namespace to be deleted
    #[clap(action)]
    namespace: String,
}

pub async fn command(
    connection: Connection,
    config: Config,
) -> Result<(), crate::commands::namespace::Error> {
    let Config { namespace } = config;

    let mut client = influxdb_iox_client::namespace::Client::new(connection);

    client.delete_namespace(&namespace).await?;
    println!("Deleted namespace {namespace:?}");

    Ok(())
}
