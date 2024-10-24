use std::{collections::HashMap, path::PathBuf, sync::Arc};

use bytes::Bytes;
use chrono::{DateTime, Local};
use clap::{Parser, ValueEnum};
use influxdb3_client::Client;
use influxdb_iox_client::flightsql::FlightSqlClient;
use serde_json::Value;
use tokio::time::Instant;

use crate::{
    query_generator::{create_queriers, Format, Querier},
    report::QueryReporter,
    specification::QuerierSpec,
};

use super::common::InfluxDb3Config;

#[derive(Debug, Parser)]
#[clap(visible_alias = "q", trailing_var_arg = true)]
pub(crate) struct Config {
    /// Common InfluxDB 3.0 config
    #[clap(flatten)]
    common: InfluxDb3Config,

    /// Query-specific config
    #[clap(flatten)]
    query: QueryConfig,
}

#[derive(ValueEnum, Debug, Clone)]
enum QueryType {
    Http,
    Flight,
}

#[derive(Debug, Parser)]
pub(crate) struct QueryConfig {
    /// Number of simultaneous queriers. Each querier will perform queries at the specified `interval`.
    #[clap(
        short = 'q',
        long = "querier-count",
        env = "INFLUXDB3_LOAD_QUERIER_COUNT",
        default_value = "1"
    )]
    querier_count: usize,

    /// querier type, use HTTP by default
    #[clap(
        value_enum,
        long = "query-type",
        env = "INFLUXDB3_LOAD_QUERIER_TYPE",
        default_value_t = QueryType::Http,
    )]
    querier_type: QueryType,

    /// The path to the querier spec file to use for this run.
    ///
    /// Alternatively, specify a name of a builtin spec to use. If neither are specified, the
    /// generator will output a list of builtin specs along with help and an example for writing
    /// your own.
    #[clap(long = "querier-spec", env = "INFLUXDB3_LOAD_QUERIER_SPEC_PATH")]
    pub(crate) querier_spec_path: Option<PathBuf>,

    #[clap(
        long = "query-format",
        env = "INFLUXDB3_LOAD_QUERY_FORMAT",
        value_enum,
        default_value = "json"
    )]
    query_response_format: Format,
}

pub(crate) async fn command(mut config: Config) -> Result<(), anyhow::Error> {
    match config.query.querier_type {
        QueryType::Http => {
            let (client, mut load_config) = config
                    .common
                    .initialize_query(config.query.querier_spec_path.take())
                    .await?;

            let spec = load_config.query_spec()?;
            let (results_file_path, reporter) = load_config.query_reporter()?;

            // spawn system stats collection
            let stats = load_config.system_reporter()?;
            run_query_load(
                spec,
                Arc::clone(&reporter),
                client,
                load_config.database_name,
                load_config.end_time,
                config.query,
            )
            .await?;

            reporter.shutdown();
            println!("results saved in: {results_file_path}");

            if let Some((stats_file_path, stats_reporter)) = stats {
                println!("system stats saved in: {stats_file_path}");
                stats_reporter.shutdown();
            }

        },
        QueryType::Flight => {
            let host_url = config.common.host_url.as_ref().to_owned();
            let (_, mut load_config) = config.common.initialize_query_flight(config.query.querier_spec_path.take()).await?;
            let spec = load_config.query_spec()?;
            let (results_file_path, reporter) = load_config.query_reporter()?;

            // spawn system stats collection
            let stats = load_config.system_reporter()?;
            run_query_load_flight(
                spec,
                Arc::clone(&reporter),
                host_url,
                load_config.database_name,
                load_config.end_time,
                config.query,
            )
            .await?;

            reporter.shutdown();
            println!("results saved in: {results_file_path}");

            if let Some((stats_file_path, stats_reporter)) = stats {
                println!("system stats saved in: {stats_file_path}");
                stats_reporter.shutdown();
            }

        }
    }
    Ok(())
}

pub(crate) async fn run_query_load(
    spec: QuerierSpec,
    reporter: Arc<QueryReporter>,
    client: influxdb3_client::Client,
    database_name: String,
    end_time: Option<DateTime<Local>>,
    config: QueryConfig,
) -> Result<(), anyhow::Error> {
    let QueryConfig {
        querier_count,
        query_response_format,
        ..
    } = config;
    // spin up the queriers
    let queriers = create_queriers(&spec, query_response_format, querier_count)?;

    // spawn tasks for each querier
    let mut tasks = Vec::new();
    for querier in queriers {
        let reporter = Arc::clone(&reporter);
        let task = tokio::spawn(run_querier(
            querier,
            client.clone(),
            database_name.clone(),
            end_time,
            reporter,
        ));
        tasks.push(task);
    }

    // await tasks, shutdown reporter and exit
    for task in tasks {
        task.await?;
    }
    println!("all queriers finished");

    Ok(())
}

pub(crate) async fn run_query_load_flight(
    spec: QuerierSpec,
    reporter: Arc<QueryReporter>,
    client_addr: String,
    database_name: String,
    end_time: Option<DateTime<Local>>,
    config: QueryConfig,
) -> Result<(), anyhow::Error> {
    let QueryConfig {
        querier_count,
        query_response_format,
        ..
    } = config;
    // spin up the queriers
    let queriers = create_queriers(&spec, query_response_format, querier_count)?;

    // spawn tasks for each querier
    let mut tasks = Vec::new();

    for querier in queriers {
        let reporter = Arc::clone(&reporter);
        let task = tokio::spawn(run_querier_flight(
            querier,
            client_addr.clone(),
            database_name.clone(),
            end_time,
            reporter,
        ));
        tasks.push(task);
    }

    // await tasks, shutdown reporter and exit
    for task in tasks {
        task.await?;
    }
    println!("all queriers finished");

    Ok(())
}

async fn run_querier(
    mut querier: Querier,
    client: Client,
    database_name: String,
    end_time: Option<DateTime<Local>>,
    reporter: Arc<QueryReporter>,
) {
    loop {
        if end_time.is_some_and(|t| Local::now() > t) {
            println!(
                "querier {id} completed at {time}",
                id = querier.querier_id,
                time = Local::now()
            );
            break;
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

async fn flight_sql_client(client_addr: String) -> FlightSqlClient {
    let channel = tonic::transport::Channel::from_shared(client_addr)
        .expect("create tonic channel")
        .connect()
        .await
        .expect("connect to gRPC client");
    FlightSqlClient::new(channel)
}

async fn run_querier_flight(
    mut querier: Querier,
    client_addr: String,
    database_name: String,
    end_time: Option<DateTime<Local>>,
    reporter: Arc<QueryReporter>,
) {
    loop {
        let mut client = flight_sql_client(client_addr.clone()).await;
        if end_time.is_some_and(|t| Local::now() > t) {
            println!(
                "querier {id} completed at {time}",
                id = querier.querier_id,
                time = Local::now()
            );
            break;
        }
        for query in &mut querier.queries {
            let start_request = Instant::now();
            let query_string = "SELECT int_val, float_val FROM measurement_data WHERE series_id = 100";
            let res = client.query(query_string);
            // let mut builder = client
            //     .api_v3_query_sql(&database_name, query.query())
            //     .format(querier.format.into());
            // for p in query.params_mut() {
            //     let v = p.generate();
            //     builder = builder
            //         .with_try_param(p.name(), v)
            //         .expect("expected primitive JSON value");
            // }
            // let res = builder.send().await;
            let mut all_times = vec![];
            let response_time = start_request.elapsed().as_millis() as u64;
            // let (status, rows) = match res {
            //     Ok(b) => (200, count_rows(b, querier.format)),
            //     Err(influxdb3_client::Error::ApiError { code, message: _ }) => (code.as_u16(), 0),
            //     Err(other_error) => {
            //         panic!("unexpected error while performing query: {other_error}")
            //     }
            // };
            all_times.push(response_time);

            let min = all_times.iter().min();
            let max = all_times.iter().max();
            let avg = (all_times.iter().sum::<num::bigint::BigInt>()) / (all_times.len());
            println!("Time taken for all requests min: {:?}, max: {:?}, avg: {:?}", min, max, avg);

            // reporter.report(
            //     querier.querier_id,
            //     status,
            //     response_time,
            //     rows,
            //     Local::now(),
            // );
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
