use std::sync::Arc;

use clap::Parser;
use tokio::task::JoinSet;

use crate::commands::{query::run_query_load, write::run_write_load};

use super::{common::InfluxDb3Config, query::QueryConfig, write::WriteConfig};

#[derive(Debug, Parser)]
pub struct Config {
    /// Common InfluxDB 3 Enterprise config
    #[clap(flatten)]
    common: InfluxDb3Config,

    /// Query-specific config
    #[clap(flatten)]
    query: QueryConfig,

    /// Write-specific config
    #[clap(flatten)]
    write: WriteConfig,
}

pub async fn command(mut config: Config) -> Result<(), anyhow::Error> {
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

    let mut tasks = JoinSet::new();

    // setup query load:
    let query_spec = load_config.query_spec()?;
    let (query_results_file_path, query_reporter) = load_config.query_reporter()?;
    let qr = Arc::clone(&query_reporter);
    let query_client = client.clone();
    tasks.spawn(async move {
        run_query_load(
            query_spec,
            qr,
            query_client,
            database_name.clone(),
            load_config.end_time,
            config.query,
        )
        .await
    });

    // setup write load:
    let write_spec = load_config.write_spec()?;
    let (write_results_file_path, write_reporter) = load_config.write_reporter()?;
    let wr = Arc::clone(&write_reporter);
    tasks.spawn(async move {
        run_write_load(
            write_spec,
            wr,
            client,
            load_config.database_name,
            load_config.end_time,
            config.write,
        )
        .await
    });

    while let Some(res) = tasks.join_next().await {
        res??;
    }

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
