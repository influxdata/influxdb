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
use clap::{crate_authors, crate_version, App, Arg};
use iox_data_generator::{specification::DataSpec, write::PointsWriterBuilder};
use std::fs::File;
use std::io::{self, BufRead};
use tracing::info;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let help = r#"IOx data point generator

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


"#;

    let matches = App::new(help)
        .version(crate_version!())
        .author(crate_authors!())
        .about("IOx data point generator")
        .arg(
            Arg::new("SPECIFICATION")
                .short('s')
                .long("spec")
                .help("Path to the specification TOML file describing the data generation")
                .takes_value(true)
                .required(true),
        )
        .arg(Arg::new("PRINT")
                .long("print")
            .help("Print the generated line protocol from a single sample collection to the terminal")
        )
        .arg(Arg::new("NOOP")
                .long("noop")
            .help("Runs the generation with agents writing to a sink. Useful for quick stress test to see how much resources the generator will take")
        )
        .arg(
            Arg::new("OUTPUT")
                .short('o')
                .long("output")
                .help("The filename to write line protocol")
                .takes_value(true),
        )
        .arg(
            Arg::new("HOST")
                .short('h')
                .long("host")
                .help("The host name part of the API endpoint to write to")
                .takes_value(true),
        )
        .arg(
            Arg::new("ORG")
                .long("org")
                .help("The organization name to write to")
                .takes_value(true),
        )
        .arg(
            Arg::new("BUCKET")
                .long("bucket")
                .help("The bucket name to write to")
                .takes_value(true),
        )
        .arg(
            Arg::new("DATABASE_LIST")
                .long("database_list")
                .help("File name with a list of databases. 1 per line with <org>_<bucket> format")
                .takes_value(true),
        )
        .arg(
            Arg::new("TOKEN")
                .long("token")
                .help("The API authorization token used for all requests")
                .takes_value(true),
        )
        .arg(
            Arg::new("START")
                .long("start")
                .help(
                    "The date and time at which to start the timestamps of the generated data. \
                       Can be an exact datetime like `2020-01-01T01:23:45-05:00` or a fuzzy \
                       specification like `1 hour ago`. If not specified, defaults to now.",
                )
                .takes_value(true),
        )
        .arg(
            Arg::new("END")
                .long("end")
                .help(
                    "The date and time at which to stop the timestamps of the generated data. \
                       Can be an exact datetime like `2020-01-01T01:23:45-05:00` or a fuzzy \
                       specification like `1 hour ago`. If not specified, defaults to now.",
                )
                .takes_value(true),
        )
        .arg(Arg::new("continue").long("continue").help(
            "Generate live data using the intervals from the spec after generating historical  \
              data. This option has no effect if you specify an end time.",
        ))
        .arg(
            Arg::new("batch_size")
                .long("batch_size")
                .help("Generate this many samplings to batch into a single API call. Good for sending a bunch of historical data in quickly if paired with a start time from long ago.")
                .takes_value(true)
        )
        .arg(
            Arg::new("jaeger_debug_header")
                .long("jaeger_debug_header")
                .help("Generate jaeger debug header with given key during write")
                .takes_value(true)
        )
        .get_matches();

    let disable_log_output = matches.is_present("PRINT");
    if !disable_log_output {
        tracing_subscriber::fmt::init();
    }

    let spec_filename = matches
        .value_of("SPECIFICATION")
        // This should never fail if clap is working properly
        .expect("SPECIFICATION is a required argument");

    let execution_start_time = Local::now();

    let start_datetime = datetime_nanoseconds(matches.value_of("START"), execution_start_time);
    let end_datetime = datetime_nanoseconds(matches.value_of("END"), execution_start_time);

    let start_display = start_datetime.unwrap_or_else(|| execution_start_time.timestamp_nanos());
    let end_display = end_datetime.unwrap_or_else(|| execution_start_time.timestamp_nanos());

    let continue_on = matches.is_present("continue");

    let batch_size = matches
        .value_of("batch_size")
        .map(|v| v.parse::<usize>().unwrap())
        .unwrap_or(1);

    info!(
        "Starting at {}, ending at {} ({}){}",
        start_display,
        end_display,
        (end_display - start_display) / 1_000_000_000,
        if continue_on { " then continuing" } else { "" },
    );

    let data_spec = DataSpec::from_file(spec_filename)?;

    // TODO: parquet output

    let mut points_writer_builder = if let Some(line_protocol_filename) = matches.value_of("OUTPUT")
    {
        PointsWriterBuilder::new_file(line_protocol_filename)?
    } else if let Some(host) = matches.value_of("HOST") {
        let token = matches
            .value_of("TOKEN")
            .expect("--token must be specified");

        PointsWriterBuilder::new_api(host, token, matches.value_of("jaeger_debug_header")).await?
    } else if matches.is_present("PRINT") {
        PointsWriterBuilder::new_std_out()
    } else if matches.is_present("NOOP") {
        PointsWriterBuilder::new_no_op(true)
    } else {
        panic!("One of --print or --output or --host must be provided.");
    };

    let buckets = match (
        matches.value_of("ORG"),
        matches.value_of("BUCKET"),
        matches.value_of("DATABASE_LIST"),
    ) {
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
    };

    let result = iox_data_generator::generate(
        &data_spec,
        buckets,
        &mut points_writer_builder,
        start_datetime,
        end_datetime,
        execution_start_time.timestamp_nanos(),
        continue_on,
        batch_size,
        disable_log_output,
    )
    .await;

    match result {
        Ok(total_points) => {
            if !disable_log_output {
                eprintln!("Submitted {} total points", total_points);
            }
        }
        Err(e) => panic!("Execution failed: \n{}", e),
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
