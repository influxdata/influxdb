use influxdb_iox_client::{
    connection::Connection,
    flight::{
        self,
        generated_types::{read_info, ReadInfo},
    },
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
}

pub type Result<T, E = Error> = std::result::Result<T, E>;

#[derive(clap::ValueEnum, Clone, Debug, PartialEq)]
#[clap(rename_all = "lower")]
enum QueryLanguage {
    Sql,
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

    /// Optional format ('pretty', 'json', or 'csv')
    #[clap(short, long, default_value = "pretty", action)]
    format: String,

    /// Query type used
    #[clap(short = 'l', long = "lang", default_value = "sql")]
    query_lang: QueryLanguage,
}

pub async fn command(connection: Connection, config: Config) -> Result<()> {
    let mut client = flight::Client::new(connection);

    let Config {
        namespace,
        format,
        query,
        query_lang,
    } = config;

    let format = QueryOutputFormat::from_str(&format)?;

    let mut query_results = client
        .perform_query(ReadInfo {
            namespace_name: namespace,
            sql_query: query,
            query_type: match query_lang {
                QueryLanguage::Sql => read_info::QueryType::Sql,
                QueryLanguage::InfluxQL => read_info::QueryType::InfluxQl,
            }
            .into(),
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
