use snafu::{ResultExt, Snafu};
use trogging::cli::LoggingConfig;

pub(crate) mod all_in_one;
mod compactor;
mod garbage_collector;
mod ingester;
#[cfg(feature = "rpc_write")]
mod ingester2;
mod main;
mod querier;
mod router;
#[cfg(feature = "rpc_write")]
mod router_rpc_write;
mod test;

#[derive(Debug, Snafu)]
#[allow(clippy::enum_variant_names)]
pub enum Error {
    #[snafu(display("Error in compactor subcommand: {}", source))]
    CompactorError { source: compactor::Error },

    #[snafu(display("Error in garbage collector subcommand: {}", source))]
    GarbageCollectorError { source: garbage_collector::Error },

    #[snafu(display("Error in querier subcommand: {}", source))]
    QuerierError { source: querier::Error },

    #[snafu(display("Error in router subcommand: {}", source))]
    RouterError { source: router::Error },

    #[cfg(feature = "rpc_write")]
    #[snafu(display("Error in router-rpc-write subcommand: {}", source))]
    RouterRpcWriteError { source: router_rpc_write::Error },

    #[snafu(display("Error in ingester subcommand: {}", source))]
    IngesterError { source: ingester::Error },

    #[cfg(feature = "rpc_write")]
    #[snafu(display("Error in ingester2 subcommand: {}", source))]
    Ingester2Error { source: ingester2::Error },

    #[snafu(display("Error in all in one subcommand: {}", source))]
    AllInOneError { source: all_in_one::Error },

    #[snafu(display("Error in test subcommand: {}", source))]
    TestError { source: test::Error },
}

pub type Result<T, E = Error> = std::result::Result<T, E>;

#[derive(Debug, clap::Parser)]
pub struct Config {
    /// Supports having all-in-one be the default command.
    #[clap(flatten)]
    all_in_one_config: all_in_one::Config,

    #[clap(subcommand)]
    command: Option<Command>,
}

impl Config {
    pub fn logging_config(&self) -> &LoggingConfig {
        match &self.command {
            None => &self.all_in_one_config.logging_config,
            Some(Command::Compactor(config)) => config.run_config.logging_config(),
            Some(Command::GarbageCollector(config)) => config.run_config.logging_config(),
            Some(Command::Querier(config)) => config.run_config.logging_config(),
            Some(Command::Router(config)) => config.run_config.logging_config(),
            #[cfg(feature = "rpc_write")]
            Some(Command::RouterRpcWrite(config)) => config.run_config.logging_config(),
            Some(Command::Ingester(config)) => config.run_config.logging_config(),
            #[cfg(feature = "rpc_write")]
            Some(Command::Ingester2(config)) => config.run_config.logging_config(),
            Some(Command::AllInOne(config)) => &config.logging_config,
            Some(Command::Test(config)) => config.run_config.logging_config(),
        }
    }
}

#[derive(Debug, clap::Parser)]
enum Command {
    /// Run the server in compactor mode
    Compactor(compactor::Config),

    /// Run the server in querier mode
    Querier(querier::Config),

    /// Run the server in router mode
    Router(router::Config),

    /// Run the server in router mode using the RPC write path.
    #[cfg(feature = "rpc_write")]
    RouterRpcWrite(router_rpc_write::Config),

    /// Run the server in ingester mode
    Ingester(ingester::Config),

    /// Run the server in ingester2 mode
    #[cfg(feature = "rpc_write")]
    Ingester2(ingester2::Config),

    /// Run the server in "all in one" mode (Default)
    AllInOne(all_in_one::Config),

    /// Run the server in test mode
    Test(test::Config),

    /// Run the server in garbage collecter mode
    GarbageCollector(garbage_collector::Config),
}

pub async fn command(config: Config) -> Result<()> {
    match config.command {
        None => all_in_one::command(config.all_in_one_config)
            .await
            .context(AllInOneSnafu),
        Some(Command::Compactor(config)) => {
            compactor::command(config).await.context(CompactorSnafu)
        }
        Some(Command::GarbageCollector(config)) => garbage_collector::command(config)
            .await
            .context(GarbageCollectorSnafu),
        Some(Command::Querier(config)) => querier::command(config).await.context(QuerierSnafu),
        Some(Command::Router(config)) => router::command(config).await.context(RouterSnafu),
        #[cfg(feature = "rpc_write")]
        Some(Command::RouterRpcWrite(config)) => router_rpc_write::command(config)
            .await
            .context(RouterRpcWriteSnafu),
        Some(Command::Ingester(config)) => ingester::command(config).await.context(IngesterSnafu),
        #[cfg(feature = "rpc_write")]
        Some(Command::Ingester2(config)) => {
            ingester2::command(config).await.context(Ingester2Snafu)
        }
        Some(Command::AllInOne(config)) => all_in_one::command(config).await.context(AllInOneSnafu),
        Some(Command::Test(config)) => test::command(config).await.context(TestSnafu),
    }
}
