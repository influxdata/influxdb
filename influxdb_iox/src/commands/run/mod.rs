use snafu::{ResultExt, Snafu};
use structopt::StructOpt;

use crate::structopt_blocks::run_config::RunConfig;

pub mod query;

#[derive(Debug, Snafu)]
pub enum Error {
    #[snafu(display("Error in query subcommand: {}", source))]
    QueryError { source: query::Error },
}

pub type Result<T, E = Error> = std::result::Result<T, E>;

#[derive(Debug, StructOpt)]
pub struct Config {
    // TODO(marco) remove this
    /// Config for query mode, for backwards compatibility reasons.
    #[structopt(flatten)]
    query_config: query::Config,

    #[structopt(subcommand)]
    command: Option<Command>,
}

impl Config {
    pub fn run_config(&self) -> &RunConfig {
        match &self.command {
            None => &self.query_config.run_config,
            Some(Command::Query(config)) => &config.run_config,
        }
    }
}

#[derive(Debug, StructOpt)]
enum Command {
    Query(query::Config),
}

pub async fn command(config: Config) -> Result<()> {
    match config.command {
        None => {
            println!("WARNING: Not specifying the run-mode is deprecated. Defaulting to 'query'.");
            query::command(config.query_config)
                .await
                .context(QueryError)
        }
        Some(Command::Query(config)) => query::command(config).await.context(QueryError),
    }
}
