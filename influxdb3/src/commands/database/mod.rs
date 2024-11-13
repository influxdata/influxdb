use std::error::Error;

use secrecy::ExposeSecret;

use crate::commands::common::InfluxDb3Config;

pub async fn delete_database(db_config: InfluxDb3Config) -> Result<(), Box<dyn Error>> {
    let InfluxDb3Config {
        host_url,
        database_name,
        auth_token,
    } = db_config;
    let mut client = influxdb3_client::Client::new(host_url)?;
    if let Some(t) = auth_token {
        client = client.with_auth_token(t.expose_secret());
    }
    client.api_v3_configure_db_delete(&database_name).await?;

    println!("Database {:?} deleted successfully", &database_name);

    Ok(())
}
