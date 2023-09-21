use arrow_flight::Ticket;
use futures::TryStreamExt;
use influxdb_iox_client::{connection::Connection, format::QueryOutputFormat};
use ingester_query_grpc::{
    decode_proto_predicate_from_base64, influxdata::iox::ingester::v1::IngesterQueryRequest,
    DecodeProtoPredicateFromBase64Error,
};
use prost::Message;
use std::str::FromStr;
use thiserror::Error;

#[derive(Debug, Error)]
pub enum Error {
    #[error("Error formatting: {0}")]
    Formatting(#[from] influxdb_iox_client::format::Error),

    #[error("Error querying: {0}")]
    Query(#[from] arrow_flight::error::FlightError),

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
    namespace_id: i64,

    /// The table for which to retrieve data
    #[clap(action)]
    table_id: i64,

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
    let client = influxdb_iox_client::flight::Client::new(connection);
    let Config {
        namespace_id,
        format,
        table_id,
        columns,
        predicate_base64,
    } = config;

    let format = QueryOutputFormat::from_str(&format)?;

    let predicate = if let Some(predicate_base64) = predicate_base64 {
        Some(decode_proto_predicate_from_base64(&predicate_base64)?)
    } else {
        None
    };

    let request = IngesterQueryRequest {
        table_id,
        columns,
        predicate,
        namespace_id,
    };

    // send the message directly encoded as bytes to the ingester.
    let request = request.encode_to_vec();
    let ticket = Ticket {
        ticket: request.into(),
    };
    let query_results = client.into_inner().do_get(ticket).await?;

    // It might be nice to do some sort of streaming write
    // rather than buffering the whole thing.
    let batches: Vec<_> = query_results.try_collect().await?;

    let formatted_result = format.format(&batches)?;

    println!("{formatted_result}");

    Ok(())
}
