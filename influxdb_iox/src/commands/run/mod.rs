use snafu::{ResultExt, Snafu};
use structopt::StructOpt;

use crate::structopt_blocks::run_config::RunConfig;

pub mod database;
pub mod router;
pub mod test;

#[derive(Debug, Snafu)]
#[allow(clippy::enum_variant_names)]
pub enum Error {
    #[snafu(display("Error in database subcommand: {}", source))]
    DatabaseError { source: database::Error },

    #[snafu(display("Error in router subcommand: {}", source))]
    RouterError { source: router::Error },

    #[snafu(display("Error in test subcommand: {}", source))]
    TestError { source: test::Error },
}

pub type Result<T, E = Error> = std::result::Result<T, E>;

#[derive(Debug, StructOpt)]
pub struct Config {
    // TODO(marco) remove this
    /// Config for database mode, for backwards compatibility reasons.
    #[structopt(flatten)]
    database_config: database::Config,

    #[structopt(subcommand)]
    command: Option<Command>,
}

impl Config {
    pub fn run_config(&self) -> &RunConfig {
        match &self.command {
            None => &self.database_config.run_config,
            Some(Command::Database(config)) => &config.run_config,
            Some(Command::Router(config)) => &config.run_config,
            Some(Command::Test(config)) => &config.run_config,
        }
    }
}

#[derive(Debug, StructOpt)]
enum Command {
    /// Run the server in database mode
    Database(database::Config),

    /// Run the server in routing mode
    Router(router::Config),

    /// Run the server in test mode
    Test(test::Config),
}

pub async fn command(config: Config) -> Result<()> {
    match config.command {
        None => {
            println!(
                "WARNING: Not specifying the run-mode is deprecated. Defaulting to 'database'."
            );
            database::command(config.database_config)
                .await
                .context(DatabaseError)
        }
        Some(Command::Database(config)) => database::command(config).await.context(DatabaseError),
        Some(Command::Router(config)) => router::command(config).await.context(RouterError),
        Some(Command::Test(config)) => test::command(config).await.context(TestError),
    }
}
