use influxdb_iox_client::connection::Connection;

use crate::commands::namespace::Result;

/// Update the specified namespace's data retention period
#[derive(Debug, clap::Parser)]
pub struct Config {
    /// The namespace to update the retention period for
    #[clap(action)]
    namespace: String,

    /// Num of hours of the retention period of this namespace. Default is 0 representing
    /// infinite retention
    #[clap(action, long = "retention-hours", short = 'r', default_value = "0")]
    retention_hours: u32,
}

pub async fn command(connection: Connection, config: Config) -> Result<()> {
    let Config {
        namespace,
        retention_hours,
    } = config;

    // retention_hours = 0 means infinite retention. Make it None/Null in the request.
    let retention: Option<i64> = if retention_hours == 0 {
        None
    } else {
        // we take retention from the user in hours, for ease of use, but it's stored as nanoseconds
        // internally
        Some(retention_hours as i64 * 60 * 60 * 1_000_000_000)
    };
    let mut client = influxdb_iox_client::namespace::Client::new(connection);
    let namespace = client
        .update_namespace_retention(&namespace, retention)
        .await?;
    println!("{}", serde_json::to_string_pretty(&namespace)?);

    Ok(())
}
