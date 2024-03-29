use std::{collections::HashMap, sync::Arc, time::Duration};

use anyhow::Context;
use bytes::Bytes;
use chrono::Local;
use clap::Parser;
use influxdb3_client::Client;
use serde_json::Value;
use tokio::time::Instant;

use crate::{
    query_generator::{create_queriers, Format, Querier},
    report::QueryReporter,
    specification::QuerierSpec,
};

use super::common::{create_client, InfluxDb3Config};

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

    /// The file that will be used to write the results of the run. If not specified, results
    /// will be written to <spec_name>_results.csv in the current directory.
    #[clap(
        short = 'R',
        long = "results",
        env = "INFLUXDB3_LOAD_QUERY_RESULTS_FILE"
    )]
    results_file: Option<String>,
}

pub(crate) async fn command(config: Config) -> Result<(), anyhow::Error> {
    let InfluxDb3Config {
        host_url,
        database_name,
        auth_token,
        spec_path,
        builtin_spec,
        print_spec,
    }: InfluxDb3Config = config.influxdb3_config;

    if spec_path.is_none() && print_spec.is_none() && builtin_spec.is_none() {
        println!("You didn't provide a spec path.");
        return Ok(());
    }

    let built_in_specs = crate::specs::built_in_specs();

    // if print spec is set, print the spec and exit
    if let Some(spec_name) = print_spec {
        let spec = built_in_specs
            .iter()
            .find(|spec| spec.query_spec.name == spec_name)
            .with_context(|| format!("Spec with name '{spec_name}' not found"))?;
        println!("{}", spec.query_spec.to_json_string_pretty()?);
        return Ok(());
    }

    // if builtin spec is set, use that instead of the spec path
    let spec = if let Some(b) = builtin_spec {
        let builtin = built_in_specs
            .into_iter()
            .find(|spec| spec.query_spec.name == b)
            .with_context(|| format!("built-in spec with name '{b}' not found"))?;
        println!("using built-in spec: {}", builtin.query_spec.name);
        builtin.query_spec
    } else {
        QuerierSpec::from_path(&spec_path.unwrap())?
    };

    // spin up the queriers
    let queriers = create_queriers(&spec, config.query_response_format, config.querier_count)?;

    // set up a results reporter and spawn a thread to flush results
    let results_file = config
        .results_file
        .unwrap_or_else(|| format!("{}_query_results.csv", spec.name));
    let query_reporter = Arc::new(QueryReporter::new(&results_file)?);
    let reporter = Arc::clone(&query_reporter);
    tokio::task::spawn_blocking(move || {
        reporter.flush_reports();
    });

    // create a InfluxDB Client and spawn tasks for each querier
    let client = create_client(host_url, auth_token)?;
    let mut tasks = Vec::new();
    for querier in queriers {
        let reporter = Arc::clone(&query_reporter);
        let sampling_interval = config.sampling_interval.map(Into::into);
        let task = tokio::spawn(run_querier(
            querier,
            client.clone(),
            database_name.clone(),
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
    println!("reporter closed and results written to {}", results_file);

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
