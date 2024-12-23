use std::error::Error;

pub mod wal;

#[derive(Debug, clap::Parser)]
pub(crate) struct Config {
    #[clap(subcommand)]
    command: Command,
}

#[derive(Debug, clap::Parser)]
enum Command {
    /// Test a plugin triggered by WAL writes
    Wal(wal::Config),
}

pub(crate) async fn command(config: Config) -> Result<(), Box<dyn Error>> {
    match config.command {
        Command::Wal(config) => wal::command(config).await,
    }
}
