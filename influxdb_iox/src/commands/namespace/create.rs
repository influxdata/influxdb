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
    /// The namespace to to be created
    #[clap(action)]
    namespace: String,

    /// Num of hours of the retention period of this namespace.
    /// If not specified, an infinite retention period will be used.
    #[clap(
        action,
        long = "retention-hours",
        short = 'r',
        env = "INFLUXDB_IOX_NAMESPACE_RETENTION_HOURS",
        default_value = "0"
    )]
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

    let mut client = influxdb_iox_client::namespace::Client::new(connection);

    // retention_hours = 0 means infinite retention. Make it None/Null in the request.
    let retention: Option<i64> = if retention_hours == 0 {
        None
    } else {
        // we take retention from the user in hours, for ease of use, but it's stored as nanoseconds
        // internally
        Some(retention_hours as i64 * 60 * 60 * 1_000_000_000)
    };
    let namespace = client.create_namespace(&namespace, retention).await?;
    println!("{}", serde_json::to_string_pretty(&namespace)?);

    Ok(())
}
