use influxdb_iox_client::{connection::Connection, write};
use snafu::{ResultExt, Snafu};
use std::{fs::File, io::Read, path::PathBuf};

#[allow(clippy::enum_variant_names)]
#[derive(Debug, Snafu)]
pub enum Error {
    #[snafu(display("Error reading file {:?}: {}", file_name, source))]
    ReadingFile {
        file_name: PathBuf,
        source: std::io::Error,
    },

    #[snafu(display("Client error: {source}"))]
    ClientError {
        source: influxdb_iox_client::error::Error,
    },
}

pub type Result<T, E = Error> = std::result::Result<T, E>;

/// Write data into the specified database
#[derive(Debug, clap::Parser)]
pub struct Config {
    /// The namespace into which to write
    #[clap(action)]
    namespace: String,

    /// File with data to load. Currently supported formats are .lp
    #[clap(action)]
    file_name: PathBuf,
}

pub async fn command(connection: Connection, config: Config) -> Result<()> {
    let Config {
        namespace,
        file_name,
    } = config;
    let file_name = &file_name;

    let mut file = File::open(file_name).context(ReadingFileSnafu { file_name })?;

    let mut lp_data = String::new();
    file.read_to_string(&mut lp_data)
        .context(ReadingFileSnafu { file_name })?;

    let mut client = write::Client::new(connection);

    let total_bytes = client
        .write_lp(namespace, lp_data)
        .await
        .context(ClientSnafu)?;

    println!("{} Bytes OK", total_bytes);

    Ok(())
}
