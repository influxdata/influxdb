use crate::TABLE_STYLE_SINGLE_LINE_BORDERS;
use comfy_table::{Cell, Table};
use influxdb_iox_client::{connection::Connection, remote};
use thiserror::Error;

#[allow(clippy::enum_variant_names)]
#[derive(Debug, Error)]
pub enum Error {
    #[error("Client error: {0}")]
    ClientError(#[from] influxdb_iox_client::error::Error),
}

pub type Result<T, E = Error> = std::result::Result<T, E>;

#[derive(Debug, clap::Parser)]
#[clap(
    name = "remote",
    about = "Manage configuration about other IOx servers"
)]
pub enum Config {
    /// Set connection parameters for a remote IOx server.
    Set { id: u32, connection_string: String },

    /// Remove a reference to a remote IOx server.
    Remove { id: u32 },

    /// List configured remote IOx server.
    List,
}

pub async fn command(connection: Connection, config: Config) -> Result<()> {
    match config {
        Config::Set {
            id,
            connection_string,
        } => {
            let mut client = remote::Client::new(connection);
            client.update_remote(id, connection_string).await?;
        }
        Config::Remove { id } => {
            let mut client = remote::Client::new(connection);
            client.delete_remote(id).await?;
        }
        Config::List => {
            let mut client = remote::Client::new(connection);

            let remotes = client.list_remotes().await?;
            if remotes.is_empty() {
                println!("no remotes configured");
            } else {
                let mut table = Table::new();
                table.load_preset(TABLE_STYLE_SINGLE_LINE_BORDERS);
                table.set_header(vec![Cell::new("ID"), Cell::new("Connection string")]);

                for i in remotes {
                    table.add_row(vec![
                        Cell::new(&format!("{}", i.id)),
                        Cell::new(&i.connection_string),
                    ]);
                }
                print!("{}", table);
            }
        }
    };

    Ok(())
}
