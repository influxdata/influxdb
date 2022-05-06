use influxdb_iox_client::{
    connection::Connection,
    flight::{self, generated_types::ReadInfo},
    format::QueryOutputFormat,
};
use std::str::FromStr;
use thiserror::Error;

#[allow(clippy::enum_variant_names)]
#[derive(Debug, Error)]
pub enum Error {
    // #[error("Error reading file {:?}: {}", file_name, source)]
    // ReadingFile {
    //     file_name: PathBuf,
    //     source: std::io::Error,
    // },
    #[error("Error formatting: {0}")]
    FormattingError(#[from] influxdb_iox_client::format::Error),

    #[error("Error querying: {0}")]
    Query(#[from] influxdb_iox_client::flight::Error),
    // #[error("Error in chunk subcommand: {0}")]
    // Chunk(#[from] chunk::Error),

    // #[error("Error in partition subcommand: {0}")]
    // Partition(#[from] partition::Error),

    // #[error("JSON Serialization error: {0}")]
    // Serde(#[from] serde_json::Error),

    // #[error("Error in partition subcommand: {0}")]
    // Catalog(#[from] recover::Error),

    // #[error("Client error: {0}")]
    // ClientError(#[from] influxdb_iox_client::error::Error),
}

pub type Result<T, E = Error> = std::result::Result<T, E>;

/// Query the data with SQL
#[derive(Debug, clap::Parser)]
pub struct Config {
    /// The name of the database
    name: String,

    /// The query to run, in SQL format
    query: String,

    /// Optional format ('pretty', 'json', or 'csv')
    #[clap(short, long, default_value = "pretty")]
    format: String,
}

pub async fn command(connection: Connection, config: Config) -> Result<()> {
    let mut client = flight::Client::new(connection);
    let Config {
        name,
        format,
        query,
    } = config;

    let format = QueryOutputFormat::from_str(&format)?;

    let mut query_results = client
        .perform_query(ReadInfo {
            namespace_name: name,
            sql_query: query,
        })
        .await?;

    // It might be nice to do some sort of streaming write
    // rather than buffering the whole thing.
    let mut batches = vec![];
    while let Some(data) = query_results.next().await? {
        batches.push(data);
    }

    let formatted_result = format.format(&batches)?;

    println!("{}", formatted_result);

    Ok(())
}
