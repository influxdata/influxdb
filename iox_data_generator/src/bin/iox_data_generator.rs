//! Entry point for generator CLI.
#![deny(rustdoc::broken_intra_doc_links, rustdoc::bare_urls, rust_2018_idioms)]
#![warn(
    missing_copy_implementations,
    missing_debug_implementations,
    missing_docs,
    clippy::explicit_iter_loop,
    clippy::future_not_send,
    clippy::use_self,
    clippy::clone_on_ref_ptr
)]

use chrono::prelude::*;
use chrono_english::{parse_date_string, Dialect};
use iox_data_generator::{specification::DataSpec, write::PointsWriterBuilder};
use std::{
    fs::File,
    io::{self, BufRead},
};
use tracing::info;

#[derive(clap::Parser)]
#[clap(
    name = "iox_data_generator",
    about = "IOx data point generator",
    long_about = r#"IOx data point generator

Examples:
    # Generate data points using the specification in `spec.toml` and save in the `lp` directory
    iox_data_generator -s spec.toml -o lp

    # Generate data points and write to the server running at localhost:8080 with the provided org,
    # bucket and authorization token
    iox_data_generator -s spec.toml -h localhost:8080 --org myorg --bucket mybucket --token mytoken

    # Generate data points for the 24 hours between midnight 2020-01-01 and 2020-01-02
    iox_data_generator -s spec.toml -o lp --start 2020-01-01 --end 2020-01-02

    # Generate data points starting from an hour ago until now, generating the historical data as
    # fast as possible. Then generate data according to the sampling interval until terminated.
    iox_data_generator -s spec.toml -o lp --start "1 hr ago" --continue

Logging:
    Use the RUST_LOG environment variable to configure the desired logging level.
    For example:

    # Enable INFO level logging for all of iox_data_generator
    RUST_LOG=iox_data_generator=info iox_data_generator -s spec.toml -o lp
"#,
    author,
    version,
    disable_help_flag = true,
    arg(
        clap::Arg::new("help")
            .long("help")
            .help("Print help information")
            .action(clap::ArgAction::Help)
            .global(true)
    ),
)]
struct Config {
    /// Path to the specification TOML file describing the data generation
    #[clap(long, short, action)]
    specification: String,

    /// Print the generated line protocol from a single sample collection to the terminal
    #[clap(long, action)]
    print: bool,

    /// Runs the generation with agents writing to a sink. Useful for quick stress test to see how
    /// much resources the generator will take
    #[clap(long, action)]
    noop: bool,

    /// The directory to write line protocol to
    #[clap(long, short, action)]
    output: Option<String>,

    /// The directory to write Parquet files to
    #[clap(long, short, action)]
    parquet: Option<String>,

    /// The host name part of the API endpoint to write to
    #[clap(long, short, action)]
    host: Option<String>,

    /// The organization name to write to
    #[clap(long, action)]
    org: Option<String>,

    /// The bucket name to write to
    #[clap(long, action)]
    bucket: Option<String>,

    /// File name with a list of databases. 1 per line with <org>_<bucket> format
    #[clap(long, action)]
    database_list: Option<String>,

    /// The API authorization token used for all requests
    #[clap(long, action)]
    token: Option<String>,

    /// The date and time at which to start the timestamps of the generated data.
    ///
    /// Can be an exact datetime like `2020-01-01T01:23:45-05:00` or a fuzzy
    /// specification like `1 hour ago`. If not specified, defaults to no.
    #[clap(long, action)]
    start: Option<String>,

    /// The date and time at which to stop the timestamps of the generated data.
    ///
    /// Can be an exact datetime like `2020-01-01T01:23:45-05:00` or a fuzzy
    /// specification like `1 hour ago`. If not specified, defaults to now.
    #[clap(long, action)]
    end: Option<String>,

    /// Generate live data using the intervals from the spec after generating historical data.
    ///
    /// This option has no effect if you specify an end time.
    #[clap(long = "continue", action)]
    do_continue: bool,

    /// Generate this many samplings to batch into a single API call. Good for sending a bunch of
    /// historical data in quickly if paired with a start time from long ago.
    #[clap(long, action, default_value = "1")]
    batch_size: usize,

    /// Generate jaeger debug header with given key during write
    #[clap(long, action)]
    jaeger_debug_header: Option<String>,
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let config: Config = clap::Parser::parse();

    if !config.print {
        tracing_subscriber::fmt::init();
    }

    let execution_start_time = Local::now();

    let start_datetime = datetime_nanoseconds(config.start.as_deref(), execution_start_time);
    let end_datetime = datetime_nanoseconds(config.end.as_deref(), execution_start_time);

    let start_display = start_datetime.unwrap_or_else(|| execution_start_time.timestamp_nanos());
    let end_display = end_datetime.unwrap_or_else(|| execution_start_time.timestamp_nanos());

    let continue_on = config.do_continue;

    info!(
        "Starting at {}, ending at {} ({}){}",
        start_display,
        end_display,
        (end_display - start_display) / 1_000_000_000,
        if continue_on { " then continuing" } else { "" },
    );

    let data_spec = DataSpec::from_file(&config.specification)?;

    let mut points_writer_builder = if let Some(line_protocol_filename) = config.output {
        PointsWriterBuilder::new_file(line_protocol_filename)?
    } else if let Some(parquet_directory) = config.parquet {
        PointsWriterBuilder::new_parquet(parquet_directory)?
    } else if let Some(ref host) = config.host {
        let token = config.token.expect("--token must be specified");

        PointsWriterBuilder::new_api(host, token, config.jaeger_debug_header.as_deref()).await?
    } else if config.print {
        PointsWriterBuilder::new_std_out()
    } else if config.noop {
        PointsWriterBuilder::new_no_op(true)
    } else {
        panic!("One of --print or --output or --host must be provided.");
    };

    let buckets = if config.host.is_some() {
        // Buckets are only relevant if we're writing to the API
        match (config.org, config.bucket, config.database_list) {
            (Some(org), Some(bucket), None) => {
                vec![format!("{}_{}", org, bucket)]
            }
            (None, None, Some(bucket_list)) => {
                let f = File::open(bucket_list).expect("unable to open database_list file");

                io::BufReader::new(f)
                    .lines()
                    .map(|l| l.expect("unable to read database from database_list file"))
                    .collect::<Vec<_>>()
            }
            _ => panic!("must specify either --org AND --bucket OR --database_list"),
        }
    } else {
        // But we need at least one database or nothing will be written anywhere
        vec![String::from("org_bucket")]
    };

    let result = iox_data_generator::generate(
        &data_spec,
        buckets,
        &mut points_writer_builder,
        start_datetime,
        end_datetime,
        execution_start_time.timestamp_nanos(),
        continue_on,
        config.batch_size,
        config.print,
    )
    .await;

    match result {
        Ok(total_points) => {
            if !config.print {
                eprintln!("Submitted {} total points", total_points);
            }
        }
        Err(e) => eprintln!("Execution failed: \n{}", e),
    }

    Ok(())
}

fn datetime_nanoseconds(arg: Option<&str>, now: DateTime<Local>) -> Option<i64> {
    arg.map(|s| {
        let datetime = parse_date_string(s, now, Dialect::Us).expect("Could not parse time");
        datetime.timestamp_nanos()
    })
}

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn none_datetime_is_none_nanoseconds() {
        let ns = datetime_nanoseconds(None, Local::now());
        assert!(ns.is_none());
    }

    #[test]
    #[ignore] // TODO: I think chrono-english isn't handling timezones the way I'd expect
    fn rfc3339() {
        let ns = datetime_nanoseconds(Some("2020-01-01T01:23:45-05:00"), Local::now());
        assert_eq!(ns, Some(1577859825000000000));
    }

    #[test]
    fn relative() {
        let fixed_now = Local::now();
        let ns = datetime_nanoseconds(Some("1hr ago"), fixed_now);
        let expected = (fixed_now - chrono::Duration::hours(1)).timestamp_nanos();
        assert_eq!(ns, Some(expected));
    }
}
