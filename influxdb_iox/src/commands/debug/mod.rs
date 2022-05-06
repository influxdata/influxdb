use futures::Future;
use influxdb_iox_client::connection::Connection;
use snafu::prelude::*;

mod dump_catalog;
mod print_cpu;
mod schema;

#[derive(Debug, Snafu)]
pub enum Error {
    #[snafu(context(false))]
    #[snafu(display("Error in dump-catalog subcommand: {}", source))]
    DumpCatalogError { source: dump_catalog::Error },

    #[snafu(context(false))]
    #[snafu(display("Error in schema subcommand: {}", source))]
    SchemaError { source: schema::Error },
}

pub type Result<T, E = Error> = std::result::Result<T, E>;

/// Interrogate internal database data
#[derive(Debug, clap::Parser)]
pub struct Config {
    #[clap(subcommand)]
    command: Command,
}

#[derive(Debug, clap::Parser)]
enum Command {
    /// Dump preserved catalog.
    DumpCatalog(Box<dump_catalog::Config>),

    /// Prints what CPU features are used by the compiler by default.
    PrintCpu,

    Schema(schema::Config),
}

pub async fn command<C, CFut>(connection: C, config: Config) -> Result<()>
where
    C: Send + FnOnce() -> CFut,
    CFut: Send + Future<Output = Connection>,
{
    match config.command {
        Command::DumpCatalog(dump_catalog) => dump_catalog::command(*dump_catalog).await?,
        Command::PrintCpu => print_cpu::main(),
        Command::Schema(config) => {
            let connection = connection().await;
            schema::command(connection, config).await?
        }
    }

    Ok(())
}
