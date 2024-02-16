use clap::Parser;
use secrecy::ExposeSecret;
use tokio::{
    fs::File,
    io::{self, AsyncReadExt},
};

use super::common::InfluxDb3Config;

#[derive(Debug, thiserror::Error)]
pub(crate) enum Error {
    #[error(transparent)]
    Client(#[from] influxdb3_client::Error),

    #[error("error reading file: {0}")]
    Io(#[from] io::Error),
}

pub(crate) type Result<T> = std::result::Result<T, Error>;

#[derive(Debug, Parser)]
#[clap(visible_alias = "w", trailing_var_arg = true)]
pub struct Config {
    /// Common InfluxDB 3.0 config
    #[clap(flatten)]
    influxdb3_config: InfluxDb3Config,

    /// File path to load the write data from
    ///
    /// Currently, only files containing line protocol are supported.
    #[clap(short = 'f', long = "file")]
    file_path: String,
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

    let mut f = File::open(config.file_path).await?;
    let mut writes = Vec::new();
    f.read_to_end(&mut writes).await?;

    client
        .api_v3_write_lp(database_name)
        .body(writes)
        .send()
        .await?;

    println!("success");

    Ok(())
}
