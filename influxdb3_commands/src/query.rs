use std::path::PathBuf;
use std::str::Utf8Error;

use clap::{Parser, ValueEnum};
use secrecy::ExposeSecret;
use std::fs;
use std::io::{BufReader, IsTerminal, Read, stdin};
use tokio::{
    fs::OpenOptions,
    io::{self, AsyncWriteExt},
};

use crate::common::Format;

use super::common::InfluxDb3Config;

#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error(transparent)]
    Client(#[from] influxdb3_client::Error),

    #[error("invlid UTF8 received from server: {0}")]
    Utf8(#[from] Utf8Error),

    #[error("io error: {0}")]
    Io(#[from] io::Error),

    #[error("cannot write parquet to a terminal, use `--output <file>` or pipe the output")]
    NoOutputFileForParquet,
    #[error(
        "No input from stdin detected, no string was passed in,  and no file \
        path was given"
    )]
    NoInput,
}

pub type Result<T> = std::result::Result<T, Error>;

#[derive(Debug, Parser)]
#[clap(visible_alias = "q")]
pub struct Config {
    /// Common InfluxDB 3 server config
    #[clap(flatten)]
    influxdb3_config: InfluxDb3Config,

    /// The query language used to format the provided query string
    #[clap(
        value_enum,
        long = "language",
        short = 'l',
        default_value_t = QueryLanguage::Sql,
    )]
    language: QueryLanguage,

    /// The format in which to output the query
    ///
    /// If `--format` is set to `parquet`, write to `--output` or pipe stdout to
    /// a file or another process.
    #[clap(value_enum, long = "format", default_value = "pretty")]
    output_format: Format,

    /// Put all query output into `output`
    #[clap(short = 'o', long = "output")]
    output_file_path: Option<String>,

    /// A file containing sql statements to execute
    #[clap(short = 'f', long = "file")]
    file_path: Option<String>,

    /// The query string to execute
    query: Option<String>,

    /// An optional arg to use a custom ca for useful for testing with self signed certs
    #[clap(long = "tls-ca", env = "INFLUXDB3_TLS_CA")]
    ca_cert: Option<PathBuf>,

    /// Disable TLS certificate verification
    #[clap(long = "tls-no-verify", env = "INFLUXDB3_TLS_NO_VERIFY")]
    tls_no_verify: bool,
}

#[derive(Debug, ValueEnum, Clone)]
enum QueryLanguage {
    Sql,
    Influxql,
}

pub async fn command(config: Config) -> Result<()> {
    let InfluxDb3Config {
        host_url,
        database_name,
        auth_token,
    } = config.influxdb3_config;
    let mut client = influxdb3_client::Client::new(host_url, config.ca_cert, config.tls_no_verify)?;
    if let Some(t) = auth_token {
        client = client.with_auth_token(t.expose_secret());
    }

    let query = if let Some(query) = config.query {
        query
    } else if let Some(file_path) = config.file_path {
        fs::read_to_string(file_path)?
    } else {
        let stdin = stdin();
        // Checks if stdin has had data passed to it via a pipe
        if stdin.is_terminal() {
            return Err(Error::NoInput);
        }
        let mut reader = BufReader::new(stdin);
        let mut buffer = String::new();
        reader.read_to_string(&mut buffer)?;
        buffer
    };

    // make the query using the client
    let mut resp_bytes = match config.language {
        QueryLanguage::Sql => {
            client
                .api_v3_query_sql(database_name, query)
                .format(config.output_format.into())
                .send()
                .await?
        }
        QueryLanguage::Influxql => {
            client
                .api_v3_query_influxql(database_name, query)
                .format(config.output_format.into())
                .send()
                .await?
        }
    };

    // write to file if output path specified
    if let Some(path) = &config.output_file_path {
        let mut f = OpenOptions::new()
            .write(true)
            .create(true)
            .truncate(true)
            .open(path)
            .await?;
        f.write_all_buf(&mut resp_bytes).await?;
    } else {
        if config.output_format.is_parquet() {
            if std::io::stdout().is_terminal() {
                return Err(Error::NoOutputFileForParquet);
            }
            // Write to stdout as binary.
            io::stdout().write_all_buf(&mut resp_bytes).await?;
            return Ok(());
        }
        println!("{}", std::str::from_utf8(&resp_bytes)?);
    }

    Ok(())
}
