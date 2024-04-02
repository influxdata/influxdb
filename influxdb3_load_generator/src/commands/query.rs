use std::{collections::HashMap, sync::Arc, time::Duration};

use bytes::Bytes;
use chrono::Local;
use clap::Parser;
use influxdb3_client::Client;
use serde_json::Value;
use tokio::time::Instant;

use crate::{
    commands::common::LoadType,
    query_generator::{create_queriers, Format, Querier},
    report::QueryReporter,
};

use super::common::InfluxDb3Config;

#[derive(Debug, Parser)]
#[clap(visible_alias = "q", trailing_var_arg = true)]
pub(crate) struct Config {
    /// Common InfluxDB 3.0 config
    #[clap(flatten)]
    influxdb3_config: InfluxDb3Config,

    /// Sampling interval for the queriers. They will perform queries at this interval and
    /// sleep for the remainder of the interval. If not specified, queriers will not wait
    /// before performing the next query.
    #[clap(
        short = 'I',
        long = "query-interval",
        env = "INFLUXDB3_LOAD_QUERY_SAMPLING_INTERVAL"
    )]
    sampling_interval: Option<humantime::Duration>,

    /// Number of simultaneous queriers. Each querier will perform queries at the specified `interval`.
    #[clap(
        short = 'Q',
        long = "querier-count",
        env = "INFLUXDB3_LOAD_QUERIER_COUNT",
        default_value = "1"
    )]
    querier_count: usize,

    #[clap(
        short = 'F',
        long = "query-format",
        env = "INFLUXDB3_LOAD_QUERY_FORMAT",
        value_enum,
        default_value = "json"
    )]
    query_response_format: Format,
}

pub(crate) async fn command(config: Config) -> Result<(), anyhow::Error> {
    let (client, load_config) = config.influxdb3_config.initialize(LoadType::Query).await?;
    let spec = load_config.query_spec.unwrap();
    let results_file = load_config.query_results_file.unwrap();
    let results_file_path = load_config.query_results_file_path.unwrap();

    // spin up the queriers
    let queriers = create_queriers(&spec, config.query_response_format, config.querier_count)?;

    // set up a results reporter and spawn a thread to flush results
    println!("generating results in: {results_file_path}");
    let query_reporter = Arc::new(QueryReporter::new(results_file)?);
    let reporter = Arc::clone(&query_reporter);
    tokio::task::spawn_blocking(move || {
        reporter.flush_reports();
    });

    // create a InfluxDB Client and spawn tasks for each querier
    let mut tasks = Vec::new();
    for querier in queriers {
        let reporter = Arc::clone(&query_reporter);
        let sampling_interval = config.sampling_interval.map(Into::into);
        let task = tokio::spawn(run_querier(
            querier,
            client.clone(),
            load_config.database_name.clone(),
            reporter,
            sampling_interval,
        ));
        tasks.push(task);
    }

    // await tasks, shutdown reporter and exit
    for task in tasks {
        task.await?;
    }
    println!("all queriers finished");

    query_reporter.shutdown();
    println!("results saved in: {results_file_path}");

    Ok(())
}

async fn run_querier(
    mut querier: Querier,
    client: Client,
    database_name: String,
    reporter: Arc<QueryReporter>,
    sampling_interval: Option<Duration>,
) {
    let mut interval = sampling_interval.map(tokio::time::interval);
    loop {
        if let Some(ref mut i) = interval {
            i.tick().await;
        }
        for query in &mut querier.queries {
            let start_request = Instant::now();
            let mut builder = client
                .api_v3_query_sql(&database_name, query.query())
                .format(querier.format.into());
            for p in query.params_mut() {
                let v = p.generate();
                builder = builder
                    .with_try_param(p.name(), v)
                    .expect("expected primitive JSON value");
            }
            let res = builder.send().await;
            let response_time = start_request.elapsed().as_millis() as u64;
            let (status, rows) = match res {
                Ok(b) => (200, count_rows(b, querier.format)),
                Err(influxdb3_client::Error::ApiError { code, message: _ }) => (code.as_u16(), 0),
                Err(other_error) => {
                    panic!("unexpected error while performing query: {other_error}")
                }
            };

            reporter.report(
                querier.querier_id,
                status,
                response_time,
                rows,
                Local::now(),
            );
        }
    }
}

fn count_rows(response: Bytes, format: Format) -> u64 {
    match format {
        Format::Json => {
            let v: Vec<HashMap<String, Value>> =
                serde_json::from_slice(&response).expect("valid formatted JSON");
            v.len().try_into().unwrap()
        }
        Format::Csv => {
            let mut count = 0;
            for _ in response.split(|c| *c == b'\n') {
                count += 1;
            }
            count
        }
    }
}
