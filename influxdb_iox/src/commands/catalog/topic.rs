//! This module implements the `catalog topic` CLI subcommand

use thiserror::Error;

use clap_blocks::catalog_dsn::CatalogDsnConfig;

use crate::process_info::setup_metric_registry;

#[allow(clippy::enum_variant_names)]
#[derive(Debug, Error)]
pub enum Error {
    #[error("Error updating catalog: {0}")]
    UpdateCatalogError(#[from] iox_catalog::interface::Error),

    #[error("Catalog DSN error: {0}")]
    CatalogDsn(#[from] clap_blocks::catalog_dsn::Error),
}

/// Manage IOx chunks
#[derive(Debug, clap::Parser)]
pub struct Config {
    #[clap(subcommand)]
    command: Command,
}

/// Create or update a topic
#[derive(Debug, clap::Parser)]
struct Update {
    #[clap(flatten)]
    catalog_dsn: CatalogDsnConfig,

    /// The name of the topic
    #[clap(action)]
    db_name: String,
}

/// All possible subcommands for topic
#[derive(Debug, clap::Parser)]
enum Command {
    Update(Update),
}

pub async fn command(config: Config) -> Result<(), Error> {
    match config.command {
        Command::Update(update) => {
            let metrics = setup_metric_registry();
            let catalog = update.catalog_dsn.get_catalog("cli", metrics).await?;
            let mut repos = catalog.repositories().await;
            let topic = repos.topics().create_or_get(&update.db_name).await?;
            println!("{}", topic.id);
            Ok(())
        }
    }
}
