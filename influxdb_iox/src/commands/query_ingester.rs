use influxdb_iox_client::{connection::Connection, flight, format::QueryOutputFormat};
use std::str::FromStr;
use thiserror::Error;

#[derive(Debug, Error)]
pub enum Error {
    #[error("Error formatting: {0}")]
    Formatting(#[from] influxdb_iox_client::format::Error),

    #[error("Error querying: {0}")]
    Query(#[from] influxdb_iox_client::flight::Error),
}

pub type Result<T, E = Error> = std::result::Result<T, E>;

/// Query the data held by a particular ingester for columns from a
/// particular table.
///
/// Hint: if you don't know the available tables and columns, use the
/// `debug schema` commands to see them
#[derive(Debug, clap::Parser)]
pub struct Config {
    /// The IOx namespace to query
    namespace: String,

    /// The table for which to retrieve data
    table: String,

    /// The columns to request
    columns: Vec<String>,

    /// Optional format ('pretty', 'json', or 'csv')
    #[clap(short, long, default_value = "pretty")]
    format: String,
}

pub async fn command(connection: Connection, config: Config) -> Result<()> {
    let mut client = flight::Client::new(connection);
    let Config {
        namespace,
        format,
        table,
        columns,
    } = config;

    let format = QueryOutputFormat::from_str(&format)?;

    // TODO it mightbe cool to parse / provide a predicate too
    let predicate = None;

    let request = flight::generated_types::IngesterQueryRequest {
        table,
        columns,
        predicate,
        namespace,
    };

    let mut query_results = client.perform_query(request).await?;

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
