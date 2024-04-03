use std::sync::Arc;

use anyhow::Context;
use clap::Parser;

use crate::commands::{query::run_query_load, write::run_write_load};

use super::{common::InfluxDb3Config, query::QueryConfig, write::WriteConfig};

#[derive(Debug, Parser)]
pub(crate) struct Config {
    /// Common InfluxDB 3.0 config
    #[clap(flatten)]
    common: InfluxDb3Config,

    /// Query-specific config
    #[clap(flatten)]
    query: QueryConfig,

    /// Write-specific config
    #[clap(flatten)]
    write: WriteConfig,
}

pub(crate) async fn command(mut config: Config) -> Result<(), anyhow::Error> {
    let (client, mut load_config) = config
        .common
        .initialize_full(
            config.query.querier_spec_path.take(),
            config.write.writer_spec_path.take(),
        )
        .await?;

    // spawn system stats collection:
    let stats = load_config.system_reporter()?;
    let database_name = load_config.database_name.clone();

    // setup query load:
    let query_spec = load_config.query_spec()?;
    let (query_results_file_path, query_reporter) = load_config.query_reporter()?;
    let qr = Arc::clone(&query_reporter);
    let query_client = client.clone();
    let query_handle = tokio::spawn(async move {
        run_query_load(
            query_spec,
            qr,
            query_client,
            database_name.clone(),
            config.query,
        )
        .await
    });

    // setup write load:
    let write_spec = load_config.write_spec()?;
    let (write_results_file_path, write_reporter) = load_config.write_reporter()?;
    let wr = Arc::clone(&write_reporter);
    let write_handle = tokio::spawn(async move {
        run_write_load(
            write_spec,
            wr,
            client,
            load_config.database_name,
            config.write,
        )
        .await
    });

    let (query_task, write_task) = tokio::try_join!(query_handle, write_handle)
        .context("failed to join query and write tasks")?;

    query_task?;
    write_task?;

    write_reporter.shutdown();
    println!("write results saved in: {write_results_file_path}");

    // shutdown query reporter:
    query_reporter.shutdown();
    println!("query results saved in: {query_results_file_path}");

    if let Some((stats_file_path, stats_reporter)) = stats {
        println!("system stats saved in: {stats_file_path}");
        stats_reporter.shutdown();
    }

    Ok(())
}
