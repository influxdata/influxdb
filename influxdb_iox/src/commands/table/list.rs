use crate::commands::table::Error;
use influxdb_iox_client::connection::Connection;

/// List tables within the specified database
#[derive(Debug, clap::Parser, Default, Clone)]
pub struct Config {
    /// The database to display the list of tables for
    #[clap(action)]
    database: String,
}

pub async fn command(connection: Connection, config: Config) -> Result<(), Error> {
    let mut client = influxdb_iox_client::table::Client::new(connection);

    let tables = client.get_tables(&config.database).await?;
    println!("{}", serde_json::to_string_pretty(&tables)?);

    Ok(())
}
