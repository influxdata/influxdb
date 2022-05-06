use influxdb_iox_client::{connection::Connection, write};
use iox_time::TimeProvider;
use std::{fs::File, io::Read, path::PathBuf};
use thiserror::Error;

#[allow(clippy::enum_variant_names)]
#[derive(Debug, Error)]
pub enum Error {
    #[error("Error reading file {:?}: {}", file_name, source)]
    ReadingFile {
        file_name: PathBuf,
        source: std::io::Error,
    },

    #[error("Client error: {0}")]
    ClientError(#[from] influxdb_iox_client::error::Error),
}

pub type Result<T, E = Error> = std::result::Result<T, E>;

/// Write data into the specified database
#[derive(Debug, clap::Parser)]
pub struct Config {
    /// The name of the database
    name: String,

    /// File with data to load. Currently supported formats are .lp
    file_name: PathBuf,
}

pub async fn command(connection: Connection, config: Config) -> Result<()> {
    let mut client = write::Client::new(connection);

    let mut file = File::open(&config.file_name).map_err(|e| Error::ReadingFile {
        file_name: config.file_name.clone(),
        source: e,
    })?;

    let mut lp_data = String::new();
    file.read_to_string(&mut lp_data)
        .map_err(|e| Error::ReadingFile {
            file_name: config.file_name.clone(),
            source: e,
        })?;

    let default_time = iox_time::SystemProvider::new().now().timestamp_nanos();
    let lines_written = client.write_lp(config.name, lp_data, default_time).await?;

    println!("{} Lines OK", lines_written);

    Ok(())
}
