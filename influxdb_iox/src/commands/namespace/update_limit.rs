use influxdb_iox_client::connection::Connection;
use influxdb_iox_client::namespace::generated_types::LimitUpdate;

use crate::commands::namespace::Result;

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
            // This arg group "limit" links the members of the below struct 
            // named "max_tables" and "max_columns_per_table" together as 
            // mutually exclusive flags. As we specify all flags & commands
            // using clap-derive rather than the imperative builder, v3 only
            // properly supports this kind of behaviour in a macro code block.
            // NOTE: It takes the variable names and not the flag long names.
            clap::ArgGroup::new("limit")
                .required(true)
                .args(&["max_tables", "max_columns_per_table"])
        ))]
pub struct Args {
    /// The maximum number of tables to allow for this namespace
    #[clap(action, long = "max-tables", short = 't', group = "limit")]
    max_tables: Option<i32>,

    /// The maximum number of columns to allow per table for this namespace
    #[clap(action, long = "max-columns-per-table", short = 'c', group = "limit")]
    max_columns_per_table: Option<i32>,
}

impl From<Args> for LimitUpdate {
    fn from(args: Args) -> Self {
        let Args {
            max_tables,
            max_columns_per_table,
        } = args;

        if let Some(n) = max_tables {
            return Self::MaxTables(n);
        }
        if let Some(n) = max_columns_per_table {
            return Self::MaxColumnsPerTable(n);
        }
        unreachable!();
    }
}

pub async fn command(connection: Connection, config: Config) -> Result<()> {
    let mut client = influxdb_iox_client::namespace::Client::new(connection);

    let namespace = client
        .update_namespace_service_protection_limit(
            &config.namespace,
            LimitUpdate::from(config.args),
        )
        .await?;
    println!("{}", serde_json::to_string_pretty(&namespace)?);

    println!(
        r"
NOTE: This change will NOT take effect until all router instances have been restarted!"
    );
    Ok(())
}
