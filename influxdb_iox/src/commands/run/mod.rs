use snafu::{ResultExt, Snafu};
use trogging::cli::LoggingConfig;

mod all_in_one;
mod compactor;
mod ingester;
mod main;
mod querier;
mod router2;
mod test;

#[derive(Debug, Snafu)]
#[allow(clippy::enum_variant_names)]
pub enum Error {
    #[snafu(display("Error in compactor subcommand: {}", source))]
    CompactorError { source: compactor::Error },

    #[snafu(display("Error in querier subcommand: {}", source))]
    QuerierError { source: querier::Error },

    #[snafu(display("Error in router2 subcommand: {}", source))]
    Router2Error { source: router2::Error },

    #[snafu(display("Error in ingester subcommand: {}", source))]
    IngesterError { source: ingester::Error },

    #[snafu(display("Error in all in one subcommand: {}", source))]
    AllInOneError { source: all_in_one::Error },

    #[snafu(display("Error in test subcommand: {}", source))]
    TestError { source: test::Error },
}

pub type Result<T, E = Error> = std::result::Result<T, E>;

#[derive(Debug, clap::Parser)]
pub struct Config {
    #[clap(subcommand)]
    command: Command,
}

impl Config {
    pub fn logging_config(&self) -> &LoggingConfig {
        match &self.command {
            Command::Compactor(config) => config.run_config.logging_config(),
            Command::Querier(config) => config.run_config.logging_config(),
            Command::Router2(config) => config.run_config.logging_config(),
            Command::Ingester(config) => config.run_config.logging_config(),
            Command::AllInOne(config) => &config.logging_config,
            Command::Test(config) => config.run_config.logging_config(),
        }
    }
}

#[derive(Debug, clap::Parser)]
enum Command {
    /// Run the server in compactor mode
    Compactor(compactor::Config),

    /// Run the server in querier mode
    Querier(querier::Config),

    /// Run the server in router2 mode
    Router2(router2::Config),

    /// Run the server in ingester mode
    Ingester(ingester::Config),

    /// Run the server in "all in one" mode
    AllInOne(all_in_one::Config),

    /// Run the server in test mode
    Test(test::Config),
}

pub async fn command(config: Config) -> Result<()> {
    match config.command {
        Command::Compactor(config) => compactor::command(config).await.context(CompactorSnafu),
        Command::Querier(config) => querier::command(config).await.context(QuerierSnafu),
        Command::Router2(config) => router2::command(config).await.context(Router2Snafu),
        Command::Ingester(config) => ingester::command(config).await.context(IngesterSnafu),
        Command::AllInOne(config) => all_in_one::command(config).await.context(AllInOneSnafu),
        Command::Test(config) => test::command(config).await.context(TestSnafu),
    }
}
