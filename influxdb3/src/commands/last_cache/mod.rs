use std::error::Error;

pub mod create;
pub mod delete;

#[derive(Debug, clap::Parser)]
pub(crate) struct Config {
    #[clap(subcommand)]
    command: Command,
}

#[derive(Debug, clap::Parser)]
enum Command {
    /// Create a new last-n-value cache
    Create(create::Config),
    /// Delete an existing last-n-value cache
    Delete(delete::Config),
}

pub(crate) async fn command(config: Config) -> Result<(), Box<dyn Error>> {
    match config.command {
        Command::Create(config) => create::command(config).await,
        Command::Delete(config) => delete::command(config).await,
    }
}
