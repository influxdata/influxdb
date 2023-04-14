use arrow::record_batch::RecordBatch;
use clap::ValueEnum;
use futures::TryStreamExt;
use influxdb_iox_client::format::influxql::{write_columnar, Options};
use influxdb_iox_client::{connection::Connection, flight, format::QueryOutputFormat};
use thiserror::Error;

#[derive(Debug, Error)]
pub enum Error {
    #[error("Error formatting: {0}")]
    Formatting(#[from] influxdb_iox_client::format::Error),

    #[error("Error querying: {0}")]
    Query(#[from] influxdb_iox_client::flight::Error),

    #[error("Error formatting InfluxQL: {0}")]
    InfluxQlFormatting(#[from] influxdb_iox_client::format::influxql::Error),
}

pub type Result<T, E = Error> = std::result::Result<T, E>;

#[derive(clap::ValueEnum, Clone, Debug, PartialEq)]
#[clap(rename_all = "lower")]
enum QueryLanguage {
    /// Interpret the query as DataFusion SQL
    Sql,
    /// Interpret the query as InfluxQL
    InfluxQL,
}

/// Query the data with SQL
#[derive(Debug, clap::Parser)]
pub struct Config {
    /// The IOx namespace to query
    #[clap(action)]
    namespace: String,

    /// The query to run, in SQL format
    #[clap(action)]
    query: String,

    /// Output format of the query results
    #[clap(short, long, action)]
    #[clap(value_enum, default_value_t = OutputFormat::Pretty)]
    format: OutputFormat,

    /// Query type used
    #[clap(short = 'l', long = "lang", default_value = "sql")]
    query_lang: QueryLanguage,
}

#[derive(Debug, Clone, ValueEnum)]
enum OutputFormat {
    /// Output the most appropriate format for the query language
    Pretty,

    /// Output the query results using the Arrow JSON formatter
    Json,

    /// Output the query results using the Arrow CSV formatter
    Csv,

    /// Output the query results using the Arrow pretty formatter
    Table,
}

impl From<OutputFormat> for QueryOutputFormat {
    fn from(value: OutputFormat) -> Self {
        match value {
            OutputFormat::Pretty | OutputFormat::Table => Self::Pretty,
            OutputFormat::Json => Self::Json,
            OutputFormat::Csv => Self::Csv,
        }
    }
}

pub async fn command(connection: Connection, config: Config) -> Result<()> {
    let mut client = flight::Client::new(connection);

    let Config {
        namespace,
        format,
        query,
        query_lang,
    } = config;

    let mut query_results = match query_lang {
        QueryLanguage::Sql => client.sql(namespace, query).await,
        QueryLanguage::InfluxQL => client.influxql(namespace, query).await,
    }?;

    // It might be nice to do some sort of streaming write
    // rather than buffering the whole thing.
    let mut batches: Vec<_> = (&mut query_results).try_collect().await?;

    // read schema AFTER collection, otherwise the stream does not have the schema data yet
    let schema = query_results
        .inner()
        .schema()
        .cloned()
        .ok_or(influxdb_iox_client::flight::Error::NoSchema)?;

    // preserve schema so we print table headers even for empty results
    batches.push(RecordBatch::new_empty(schema));

    match (query_lang, &format) {
        (QueryLanguage::InfluxQL, OutputFormat::Pretty) => {
            write_columnar(std::io::stdout(), &batches, Options::default())?
        }
        _ => {
            let format: QueryOutputFormat = format.into();
            let formatted_result = format.format(&batches)?;
            println!("{formatted_result}");
        }
    }

    Ok(())
}
