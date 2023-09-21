use futures::Future;
use influxdb_iox_client::connection::Connection;
use snafu::prelude::*;

mod build_catalog;
mod parquet_to_lp;
mod print_cpu;
mod schema;
mod skipped_compactions;
mod wal;

#[derive(Debug, Snafu)]
pub enum Error {
    #[snafu(context(false))]
    #[snafu(display("Error in schema subcommand: {}", source))]
    Schema { source: schema::Error },

    #[snafu(context(false))]
    #[snafu(display("Error in build_catalog subcommand: {}", source))]
    BuildCatalog { source: build_catalog::Error },

    #[snafu(context(false))]
    #[snafu(display("Error in parquet_to_lp subcommand: {}", source))]
    ParquetToLp { source: parquet_to_lp::Error },

    #[snafu(context(false))]
    #[snafu(display("Error in skipped-compactions subcommand: {}", source))]
    SkippedCompactions { source: skipped_compactions::Error },

    #[snafu(context(false))]
    #[snafu(display("Error in wal subcommand: {}", source))]
    Wal { source: wal::Error },
}

pub type Result<T, E = Error> = std::result::Result<T, E>;

/// Debugging commands
#[derive(Debug, clap::Parser)]
pub struct Config {
    #[clap(subcommand)]
    command: Command,
}

#[derive(Debug, clap::Parser)]
enum Command {
    /// Prints what CPU features are used by the compiler by default.
    PrintCpu,

    /// Interrogate the schema of a namespace
    Schema(schema::Config),

    // NB: The example formatting below is weird so Clap make a nice help text
    /// Build a local catalog from the output of `remote store get-table`.
    ///
    /// For example:
    /// ```text
    ///  # download contents of table_name into a directory named 'table_name'
    ///  influxdb_iox remote store get-table <namespace> <table_name>
    ///
    ///  # Create a catalog and object_store in /tmp/data_dir
    ///  influxdb_iox debug build-catalog <table_dir> /tmp/data_dir
    ///
    ///  # Start iox using this data directory (you can now query `table_name` locally):
    ///  influxdb_iox --data-dir /tmp/data_dir
    /// ```
    #[clap(verbatim_doc_comment)]
    BuildCatalog(build_catalog::Config),

    /// Convert IOx Parquet files back into line protocol format
    ParquetToLp(parquet_to_lp::Config),

    /// Interrogate skipped compactions
    SkippedCompactions(skipped_compactions::Config),

    /// Subcommands for debugging the WAL
    Wal(wal::Config),
}

pub async fn command<C, CFut>(connection: C, config: Config) -> Result<()>
where
    C: Send + FnOnce() -> CFut,
    CFut: Send + Future<Output = Connection>,
{
    match config.command {
        Command::PrintCpu => print_cpu::main(),
        Command::Schema(config) => {
            let connection = connection().await;
            schema::command(connection, config).await?
        }
        Command::BuildCatalog(config) => build_catalog::command(config).await?,
        Command::ParquetToLp(config) => parquet_to_lp::command(config).await?,
        Command::SkippedCompactions(config) => {
            let connection = connection().await;
            skipped_compactions::command(connection, config).await?
        }
        Command::Wal(config) => wal::command(connection, config).await?,
    }

    Ok(())
}
