use influxdb_iox_client::connection::Connection;
use influxdb_iox_client::namespace::generated_types::LimitUpdate;

use crate::commands::namespace::{Error, Result};

#[derive(Debug, clap::Parser)]
pub struct Config {
    /// The namespace to update a service protection limit for
    #[clap(action)]
    namespace: String,

    #[command(flatten)]
    args: Args,
}

#[derive(Debug, clap::Args)]
#[clap(group(
            clap::ArgGroup::new("limit")
                .required(true)
                .args(&["max_tables", "max_columns_per_table"])
        ))]
struct Args {
    /// The maximum number of tables to allow for this namespace
    #[clap(action, long = "max-tables", short = 't', group = "limit")]
    max_tables: Option<i32>,

    /// The maximum number of columns to allow per table for this namespace
    #[clap(action, long = "max-columns-per-table", short = 'c', group = "limit")]
    max_columns_per_table: Option<i32>,
}

impl TryFrom<Args> for LimitUpdate {
    type Error = Error;
    fn try_from(args: Args) -> Result<Self> {
        let Args {
            max_tables,
            max_columns_per_table,
        } = args;

        if let Some(n) = max_tables {
            return Ok(Self::MaxTables(n));
        }
        if let Some(n) = max_columns_per_table {
            return Ok(Self::MaxColumnsPerTable(n));
        }

        Err(Error::InvalidLimit)
    }
}

pub async fn command(connection: Connection, config: Config) -> Result<()> {
    let mut client = influxdb_iox_client::namespace::Client::new(connection);

    let namespace = client
        .update_namespace_service_protection_limit(
            &config.namespace,
            LimitUpdate::try_from(config.args)?,
        )
        .await?;
    println!("{}", serde_json::to_string_pretty(&namespace)?);

    println!(
        r"
NOTE: This change will NOT take effect until all router instances have been restarted!"
    );
    Ok(())
}
