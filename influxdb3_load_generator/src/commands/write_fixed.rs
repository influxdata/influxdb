//! This always writes to,
//!   - `foo_` dbs
//!   - `series_` tables
//!   - `tag_` tags
//!   - `field_1, field_2` fields
//!
//! Data is generated based on num_writers setup and a fixed tput and it writes until it's been
//! shutdown
use std::{
    convert::Infallible,
    slice::Iter,
    time::{Duration, Instant},
};

use clap::Parser;
use futures::future::join_all;
use rand::{Rng, SeedableRng, rngs::SmallRng};
use reqwest::header::{AUTHORIZATION, HeaderMap};
use secrecy::{ExposeSecret, Secret};
use url::Url;

#[derive(Debug, Parser)]
pub struct WriteFixedConfig {
    /// The host URL of the running InfluxDB 3 Enterprise server
    #[clap(
        short = 'h',
        long = "host",
        env = "INFLUXDB3_HOST_URL",
        default_value = "http://127.0.0.1:8181"
    )]
    pub(crate) host_url: Url,

    /// The token for authentication with the InfluxDB 3 Enterprise server
    #[clap(long = "token", env = "INFLUXDB3_AUTH_TOKEN")]
    pub(crate) auth_token: Option<Secret<String>>,

    /// Write-specific config:
    #[clap(flatten)]
    write: WriteConstrainedConfig,
}

#[derive(Debug, Parser)]
pub(crate) struct WriteConstrainedConfig {
    /// Number of simultaneous writers. Each writer will generate data at the specified interval.
    #[clap(
        short = 'w',
        long = "writer-count",
        env = "INFLUXDB3_LOAD_WRITER_COUNT",
        default_value = "1"
    )]
    pub writer_count: usize,

    /// Throughput to generate data for (in MiB/s)
    #[clap(
        short = 't',
        long = "tput",
        env = "INFLUXDB3_LOAD_WRITER_THROUGHPUT",
        default_value = "1.0"
    )]
    pub tput_mebibytes_per_sec: f64,

    /// number of databases to distribute the writes
    #[clap(
        short = 'n',
        long = "num-databases",
        env = "INFLUXDB3_LOAD_WRITER_NUM_DATABASES",
        default_value = "1"
    )]
    pub num_databases: u64,
}

/// The idea here is to generate data based on throughput required shared across multiple writers.
/// Each line is derived from a template so we can calculate how much it would be in bytes roughly,
/// and then generate the data for each client pre-emptively.
///
/// This is a very naive approach but it allows for more fine grained control over "volume" of data
/// per client that needs to be generated to match(roughly) the expected throughput
///
/// Currently data here can be duplicated as they rely on just rng with small ranges. This can be
/// extended quite easily to create data that has no dups by chaining them together to create an
/// iterator for a single line that loops through unique combinations. All of this data is reported
/// every second (similar to how many sensors in different locations will transmit data say per
/// second)
fn generate_data_points(expected_tput_in_mb: f64, num_writers: usize) -> (Vec<Vec<String>>, f64) {
    let tput_bytes = expected_tput_in_mb * 1024.0 * 1024.0;
    let mut curr_size = 0.0;
    let num_bytes_per_client = tput_bytes / (num_writers as f64);
    println!(
        "setup tput_bytes {}, num_writers {}, num_bytes_per_client {}",
        tput_bytes, num_writers, num_bytes_per_client
    );

    let mut rng = SmallRng::seed_from_u64(123_456_789);
    let mut all_lines = vec![];

    'outer: loop {
        let mut per_client_lines = vec![];
        let mut per_client_bytes = 0.0;
        loop {
            // write to 2 tables
            let db_series_val = rng.gen_range(1..=2);
            // 3 tags (random for now)
            let tag1_val = rng.gen_range(1..3);
            let tag2_val = rng.gen_range(1..3);
            let tag3_val = rng.gen_range(1..3);

            // use string format instead to avoid using string literal here
            let line = format!(
                // this literal, and the associated generators can be moved to a constant to set
                // the data up
                "series_{},tag_1=value{},tag_2=value{},tag_3=value{} field_1=100,field_2=1.0",
                db_series_val, tag1_val, tag2_val, tag3_val
            );

            if (curr_size + line.len() as f64) > tput_bytes {
                break 'outer;
            }

            if (per_client_bytes + line.len() as f64) >= num_bytes_per_client.ceil() {
                break;
            }
            curr_size += line.len() as f64;
            per_client_bytes += line.len() as f64;
            per_client_lines.push(line);
        }
        all_lines.push(per_client_lines);
    }

    println!(
        "all lines size(bytes) {}, num writers {}",
        as_mib(curr_size),
        all_lines.len()
    );
    (all_lines, curr_size)
}

fn as_mib(bytes: f64) -> f64 {
    bytes / (1024.0 * 1024.0)
}

struct CustomDbIter<'a> {
    iter: std::iter::Cycle<Iter<'a, String>>,
}

impl<'a> Iterator for CustomDbIter<'a> {
    type Item = &'a String;

    fn next(&mut self) -> Option<Self::Item> {
        self.iter.next()
    }
}

pub async fn command(config: WriteFixedConfig) -> Result<(), Infallible> {
    let max_size = config.write.tput_mebibytes_per_sec;
    let num_writers = config.write.writer_count;
    let (data_points, total_size) = generate_data_points(max_size, num_writers);
    let mut headers = HeaderMap::new();
    if let Some(auth_token) = config.auth_token {
        headers.insert(
            AUTHORIZATION,
            format!("Token {}", auth_token.expose_secret())
                .parse()
                .unwrap(),
        );
    }

    let client = reqwest::Client::builder()
        .pool_max_idle_per_host(config.write.writer_count)
        .pool_idle_timeout(Duration::from_secs(10))
        .timeout(Duration::from_secs(30))
        .default_headers(headers)
        .build()
        .unwrap();

    let mut all_dbs = vec![];
    for i in 1..=config.write.num_databases {
        all_dbs.push(format!("foo_{}", i));
    }
    let mut db_iter = CustomDbIter {
        iter: all_dbs.iter().cycle(),
    };
    let write_uri = format!("{}api/v3/write_lp", config.host_url);
    println!("writing to {}", write_uri);

    loop {
        let mut futs = vec![];
        let mut total_lines = 0;
        for data in &data_points {
            total_lines += data.len();
            let db_name = db_iter.next().unwrap();
            let fut = client
                .post(&write_uri)
                .query(&[("db", &db_name)])
                .body(data.join("\n"))
                .send();
            futs.push(fut);
        }
        let start = Instant::now();
        join_all(futs).await;
        let elapsed = start.elapsed().as_millis() as u64;
        let sleep_time_ms = if elapsed > 1000 { 10 } else { 1000 - elapsed };
        println!(
            "Took {:?}ms, sleep {:?}ms, tput: {:.2}MiB/s, lines: {:?}",
            elapsed,
            sleep_time_ms,
            as_mib(total_size),
            total_lines,
        );
        tokio::time::sleep(Duration::from_millis(sleep_time_ms)).await;
    }
}
