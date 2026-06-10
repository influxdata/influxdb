use std::error::Error;

pub mod catalog;

/// `influxdb3 debug` — offline inspection of on-disk InfluxDB 3 files.
#[derive(Debug, clap::Parser)]
pub struct Config {
    #[clap(subcommand)]
    cmd: DebugCommand,
}

#[derive(Debug, clap::Subcommand)]
enum DebugCommand {
    /// Inspect catalog files
    Catalog(catalog::Config),
}

pub async fn command(config: Config) -> Result<(), Box<dyn Error>> {
    match config.cmd {
        DebugCommand::Catalog(cfg) => catalog::command(cfg).await,
    }
}
