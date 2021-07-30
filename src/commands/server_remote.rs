use influxdb_iox_client::{connection::Builder, management};
use structopt::StructOpt;
use thiserror::Error;

use prettytable::{format, Cell, Row, Table};

#[allow(clippy::enum_variant_names)]
#[derive(Debug, Error)]
pub enum Error {
    #[error("Error connecting to IOx: {0}")]
    ConnectionError(#[from] influxdb_iox_client::connection::Error),

    #[error("Update remote error: {0}")]
    UpdateError(#[from] management::UpdateRemoteError),

    #[error("List remote error: {0}")]
    ListError(#[from] management::ListRemotesError),
}

pub type Result<T, E = Error> = std::result::Result<T, E>;

#[derive(Debug, StructOpt)]
#[structopt(
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

pub async fn command(url: String, config: Config) -> Result<()> {
    let connection = Builder::default().build(url).await?;

    match config {
        Config::Set {
            id,
            connection_string,
        } => {
            let mut client = management::Client::new(connection);
            client.update_remote(id, connection_string).await?;
        }
        Config::Remove { id } => {
            let mut client = management::Client::new(connection);
            client.delete_remote(id).await?;
        }
        Config::List => {
            let mut client = management::Client::new(connection);

            let remotes = client.list_remotes().await?;
            if remotes.is_empty() {
                println!("no remotes configured");
            } else {
                let mut table = Table::new();
                table.set_format(*format::consts::FORMAT_NO_LINESEP_WITH_TITLE);
                table.set_titles(Row::new(vec![
                    Cell::new("ID"),
                    Cell::new("Connection string"),
                ]));

                for i in remotes {
                    table.add_row(Row::new(vec![
                        Cell::new(&format!("{}", i.id)),
                        Cell::new(&i.connection_string),
                    ]));
                }
                print!("{}", table);
            }
        }
    };

    Ok(())
}
