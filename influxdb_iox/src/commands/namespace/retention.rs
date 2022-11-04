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

/// Write data into the specified database
#[derive(Debug, clap::Parser)]
pub struct Config {
    /// The namespace to update the retention period for
    #[clap(action)]
    namespace: String,

    /// Num of hours of the retention period of this namespace. Default is 0 representing infinite retention
    #[clap(action, long, short = 'c', default_value = "0")]
    retention_hours: u32,
}

pub async fn command(
    connection: Connection,
    config: Config,
) -> Result<(), crate::commands::namespace::Error> {
    let Config {
        namespace,
        retention_hours,
    } = config;

    let mut client = influxdb_iox_client::schema::Client::new(connection);
    let namespace = client
        .update_namespace_retention(&namespace, retention_hours.try_into().unwrap())
        .await?;
    println!("{}", serde_json::to_string_pretty(&namespace)?);

    Ok(())
}
