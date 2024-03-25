use std::str::Utf8Error;

use clap::Parser;
use influxdb3_client::Format;
use secrecy::ExposeSecret;
use tokio::io;

use super::common::InfluxDb3Config;

#[derive(Debug, thiserror::Error)]
pub(crate) enum Error {
    #[error(transparent)]
    Client(#[from] influxdb3_client::Error),

    #[error("invlid UTF8 received from server: {0}")]
    Utf8(#[from] Utf8Error),

    #[error("io error: {0}")]
    Io(#[from] io::Error),
}

pub(crate) type Result<T> = std::result::Result<T, Error>;

#[derive(Debug, Parser)]
#[clap(visible_alias = "q", trailing_var_arg = true)]
pub(crate) struct Config {
    /// Common InfluxDB 3.0 config
    #[clap(flatten)]
    influxdb3_config: InfluxDb3Config,
}

pub(crate) async fn command(config: Config) -> Result<()> {
    let InfluxDb3Config {
        host_url,
        database_name,
        auth_token,
    } = config.influxdb3_config;
    let mut client = influxdb3_client::Client::new(host_url)?;
    if let Some(t) = auth_token {
        client = client.with_auth_token(t.expose_secret());
    }

    println!("hello from query!");

    let resp_bytes = client
        .api_v3_query_sql(database_name, "select * from foo limit 10;")
        .format(Format::Json)
        .send()
        .await?;

    println!("{}", std::str::from_utf8(&resp_bytes)?);

    Ok(())
}
