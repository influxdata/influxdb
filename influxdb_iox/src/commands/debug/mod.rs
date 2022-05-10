use futures::Future;
use influxdb_iox_client::connection::Connection;
use snafu::prelude::*;

mod namespace;
mod print_cpu;
mod schema;

#[derive(Debug, Snafu)]
pub enum Error {
    #[snafu(context(false))]
    #[snafu(display("Error in schema subcommand: {}", source))]
    SchemaError { source: schema::Error },

    #[snafu(context(false))]
    #[snafu(display("Error in namespace subcommand: {}", source))]
    NamespaceError { source: namespace::Error },
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
    /// Prints what CPU features are used by the compiler by default.
    PrintCpu,

    /// Interrogate IOx namespaces
    Namespace(namespace::Config),

    /// Interrogate the schema of a namespace
    Schema(schema::Config),
}

pub async fn command<C, CFut>(connection: C, config: Config) -> Result<()>
where
    C: Send + FnOnce() -> CFut,
    CFut: Send + Future<Output = Connection>,
{
    match config.command {
        Command::PrintCpu => print_cpu::main(),
        Command::Namespace(config) => {
            let connection = connection().await;
            namespace::command(connection, config).await?
        }
        Command::Schema(config) => {
            let connection = connection().await;
            schema::command(connection, config).await?
        }
    }

    Ok(())
}
