use influxdb_iox_client::connection::Connection;

use crate::commands::namespace::Result;

#[derive(Debug, clap::Parser)]
pub struct Config {
    /// The namespace to be deleted
    #[clap(action)]
    namespace: String,
}

pub async fn command(connection: Connection, config: Config) -> Result<()> {
    let Config { namespace } = config;

    let mut client = influxdb_iox_client::namespace::Client::new(connection);

    client.delete_namespace(&namespace).await?;
    println!("Deleted namespace {namespace:?}");

    Ok(())
}
