use std::{error::Error, fmt::Display, io, str::FromStr};

use secrecy::ExposeSecret;

use crate::commands::common::InfluxDb3Config;

#[derive(Debug, clap::Parser)]
pub(crate) struct Config {
    #[clap(subcommand)]
    command: Command,
}

#[derive(Debug, clap::Parser)]
enum Command {
    Create(CreateTableConfig),
    Delete(DeleteTableConfig),
}

#[derive(Debug, clap::Parser)]
pub struct DeleteTableConfig {
    #[clap(short = 't', long = "table", required = true)]
    table_name: String,

    #[clap(flatten)]
    influxdb3_config: InfluxDb3Config,
}

#[derive(Debug, clap::Parser)]
pub struct CreateTableConfig {
    #[clap(short = 't', long = "table", required = true)]
    table_name: String,

    #[clap(long = "tags", required = true, num_args=0..)]
    tags: Vec<String>,

    #[clap(short = 'f', long = "fields", value_parser = parse_key_val::<String, DataType>, num_args=0..)]
    fields: Vec<(String, DataType)>,

    #[clap(flatten)]
    influxdb3_config: InfluxDb3Config,
}

pub async fn command(config: Config) -> Result<(), Box<dyn Error>> {
    match config.command {
        Command::Create(CreateTableConfig {
            table_name,
            tags,
            fields,
            influxdb3_config:
                InfluxDb3Config {
                    host_url,
                    database_name,
                    auth_token,
                },
        }) => {
            let mut client = influxdb3_client::Client::new(host_url)?;
            if let Some(t) = auth_token {
                client = client.with_auth_token(t.expose_secret());
            }
            client
                .api_v3_configure_table_create(&database_name, &table_name, tags, fields)
                .await?;

            println!(
                "Table {:?}.{:?} created successfully",
                &database_name, &table_name
            );
        }
        Command::Delete(config) => {
            let InfluxDb3Config {
                host_url,
                database_name,
                auth_token,
            } = config.influxdb3_config;
            println!(
                "Are you sure you want to delete {:?}.{:?}? Enter 'yes' to confirm",
                database_name, &config.table_name,
            );
            let mut confirmation = String::new();
            let _ = io::stdin().read_line(&mut confirmation);
            if confirmation.trim() != "yes" {
                println!("Cannot delete table without confirmation");
            } else {
                let mut client = influxdb3_client::Client::new(host_url)?;
                if let Some(t) = auth_token {
                    client = client.with_auth_token(t.expose_secret());
                }
                client
                    .api_v3_configure_table_delete(&database_name, &config.table_name)
                    .await?;

                println!(
                    "Table {:?}.{:?} deleted successfully",
                    &database_name, &config.table_name
                );
            }
        }
    }
    Ok(())
}

#[derive(Debug, Copy, Clone, PartialEq, Eq)]
pub enum DataType {
    Int64,
    Uint64,
    Float64,
    Utf8,
    Bool,
}

#[derive(Debug, PartialEq, Eq, thiserror::Error)]
#[error("{0} is not a valid data type, values are int64, uint64, float64, utf8, and bool")]
pub struct ParseDataTypeError(String);

impl FromStr for DataType {
    type Err = ParseDataTypeError;
    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            "int64" => Ok(Self::Int64),
            "uint64" => Ok(Self::Uint64),
            "float64" => Ok(Self::Float64),
            "utf8" => Ok(Self::Utf8),
            "bool" => Ok(Self::Bool),
            _ => Err(ParseDataTypeError(s.into())),
        }
    }
}

impl Display for DataType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Int64 => write!(f, "int64"),
            Self::Uint64 => write!(f, "uint64"),
            Self::Float64 => write!(f, "float64"),
            Self::Utf8 => write!(f, "utf8"),
            Self::Bool => write!(f, "bool"),
        }
    }
}

impl From<DataType> for String {
    fn from(data: DataType) -> Self {
        data.to_string()
    }
}

/// Parse a single key-value pair
fn parse_key_val<T, U>(s: &str) -> Result<(T, U), Box<dyn Error + Send + Sync + 'static>>
where
    T: std::str::FromStr,
    T::Err: Error + Send + Sync + 'static,
    U: std::str::FromStr,
    U::Err: Error + Send + Sync + 'static,
{
    let pos = s
        .find(':')
        .ok_or_else(|| format!("invalid FIELD:VALUE. No `:` found in `{s}`"))?;
    Ok((s[..pos].parse()?, s[pos + 1..].parse()?))
}
