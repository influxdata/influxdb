use snafu::{ResultExt, Snafu};
use structopt::StructOpt;

mod dump_catalog;
mod print_cpu;

#[derive(Debug, Snafu)]
pub enum Error {
    #[snafu(display("Error in dump-catalog subcommand: {}", source))]
    DumpCatalogError { source: dump_catalog::Error },
}

pub type Result<T, E = Error> = std::result::Result<T, E>;

/// Interrogate internal database data
#[derive(Debug, StructOpt)]
pub struct Config {
    #[structopt(subcommand)]
    command: Command,
}

#[derive(Debug, StructOpt)]
enum Command {
    /// Dump preserved catalog.
    DumpCatalog(Box<dump_catalog::Config>),

    /// Prints what CPU features are used by the compiler by default.
    PrintCpu,
}

pub async fn command(config: Config) -> Result<()> {
    match config.command {
        Command::DumpCatalog(dump_catalog) => dump_catalog::command(*dump_catalog)
            .await
            .context(DumpCatalogSnafu),
        Command::PrintCpu => {
            print_cpu::main();
            Ok(())
        }
    }
}
