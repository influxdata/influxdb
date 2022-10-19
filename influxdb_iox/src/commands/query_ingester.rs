use generated_types::ingester::{
    decode_proto_predicate_from_base64, DecodeProtoPredicateFromBase64Error,
};
use influxdb_iox_client::{
    connection::Connection,
    flight::{self, low_level::LowLevelMessage},
    format::QueryOutputFormat,
};
use std::str::FromStr;
use thiserror::Error;

#[derive(Debug, Error)]
pub enum Error {
    #[error("Error formatting: {0}")]
    Formatting(#[from] influxdb_iox_client::format::Error),

    #[error("Error querying: {0}")]
    Query(#[from] influxdb_iox_client::flight::Error),

    #[error("Error decoding base64-encoded predicate from argument: {0}")]
    PredicateFromBase64(#[from] DecodeProtoPredicateFromBase64Error),
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
    #[clap(action)]
    namespace: String,

    /// The table for which to retrieve data
    #[clap(action)]
    table: String,

    /// The columns to request
    #[clap(long = "columns", use_value_delimiter = true, action)]
    columns: Vec<String>,

    /// Predicate in base64 protobuf encoded form.
    /// (logged on error)
    #[clap(long = "predicate-base64", action)]
    predicate_base64: Option<String>,

    /// Optional format ('pretty', 'json', or 'csv')
    #[clap(short, long, default_value = "pretty", action)]
    format: String,
}

pub async fn command(connection: Connection, config: Config) -> Result<()> {
    let mut client = flight::low_level::Client::new(connection, None);
    let Config {
        namespace,
        format,
        table,
        columns,
        predicate_base64,
    } = config;

    let format = QueryOutputFormat::from_str(&format)?;

    let predicate = if let Some(predicate_base64) = predicate_base64 {
        Some(decode_proto_predicate_from_base64(&predicate_base64)?)
    } else {
        None
    };

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
    while let Some((msg, _md)) = query_results.next().await? {
        if let LowLevelMessage::RecordBatch(batch) = msg {
            batches.push(batch);
        }
    }

    let formatted_result = format.format(&batches)?;

    println!("{}", formatted_result);

    Ok(())
}
