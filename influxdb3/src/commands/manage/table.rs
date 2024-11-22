use std::{error::Error, io};

use secrecy::ExposeSecret;

use crate::commands::common::InfluxDb3Config;

#[derive(Debug, clap::Parser)]
pub(crate) struct ManageTableConfig {
    #[clap(subcommand)]
    command: Command,
}

#[derive(Debug, clap::Parser)]
enum Command {
    Delete(TableConfig),
}

#[derive(Debug, clap::Parser)]
pub struct TableConfig {
    #[clap(short = 't', long = "table")]
    table: String,

    #[clap(flatten)]
    influxdb3_config: InfluxDb3Config,
}

pub async fn delete_table(config: ManageTableConfig) -> Result<(), Box<dyn Error>> {
    match config.command {
        Command::Delete(config) => {
            let InfluxDb3Config {
                host_url,
                database_name,
                auth_token,
            } = config.influxdb3_config;
            println!(
                "Are you sure you want to delete {:?}.{:?}? Enter 'yes' to confirm",
                database_name, &config.table,
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
                    .api_v3_configure_table_delete(&database_name, &config.table)
                    .await?;

                println!(
                    "Table {:?}.{:?} deleted successfully",
                    &database_name, &config.table
                );
            }
        }
    }
    Ok(())
}
