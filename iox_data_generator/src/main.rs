#![deny(rust_2018_idioms)]
#![warn(
    missing_copy_implementations,
    missing_debug_implementations,
    clippy::explicit_iter_loop,
    clippy::use_self
)]

use chrono::prelude::*;
use chrono_english::{parse_date_string, Dialect};
use clap::{crate_authors, crate_version, App, Arg};
use iox_data_generator::{specification::DataSpec, write::PointsWriterBuilder};
use tracing::info;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    tracing_subscriber::fmt::init();

    let help = r#"IOx data point generator

Examples:
    # Generate data points using the specification in `spec.toml` and save in the `lp` directory
    iox_data_generator -s spec.toml -o lp

    # Generate data points and write to the server running at localhost:8080 with the provided org,
    # bucket and authorization token, creating the bucket
    iox_data_generator -s spec.toml -h localhost:8080 --org myorg --org_id 0000111100001111 \
        --bucket mybucket --token mytoken --create

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
            Arg::with_name("SPECIFICATION")
                .short("s")
                .long("spec")
                .help("Path to the specification TOML file describing the data generation")
                .takes_value(true)
                .required(true),
        )
        .arg(
            Arg::with_name("OUTPUT")
                .short("o")
                .long("output")
                .help("The filename to write line protocol")
                .takes_value(true),
        )
        .arg(
            Arg::with_name("HOST")
                .short("h")
                .long("host")
                .help("The host name part of the API endpoint to write to")
                .takes_value(true),
        )
        .arg(
            Arg::with_name("ORG")
                .long("org")
                .help("The organization name to write to")
                .takes_value(true),
        )
        .arg(
            Arg::with_name("ORG_ID")
                .long("org_id")
                .help("The 16-digit hex ID of the organization. Only needed if passing `--create`.")
                .takes_value(true),
        )
        .arg(
            Arg::with_name("BUCKET")
                .long("bucket")
                .help("The bucket name to write to")
                .takes_value(true),
        )
        .arg(
            Arg::with_name("TOKEN")
                .long("token")
                .help("The API authorization token used for all requests")
                .takes_value(true),
        )
        .arg(
            Arg::with_name("START")
                .long("start")
                .help(
                    "The date and time at which to start the timestamps of the generated data. \
                       Can be an exact datetime like `2020-01-01T01:23:45-05:00` or a fuzzy \
                       specification like `1 hour ago`. If not specified, defaults to now.",
                )
                .takes_value(true),
        )
        .arg(
            Arg::with_name("END")
                .long("end")
                .help(
                    "The date and time at which to stop the timestamps of the generated data. \
                       Can be an exact datetime like `2020-01-01T01:23:45-05:00` or a fuzzy \
                       specification like `1 hour ago`. If not specified, defaults to now.",
                )
                .takes_value(true),
        )
        .arg(
            Arg::with_name("create")
                .long("create")
                .help("Create the bucket specified before sending points. Requires `--org_id`"),
        )
        .arg(Arg::with_name("continue").long("continue").help(
            "Generate live data using the intervals from the spec after generating historical  \
              data. This option has no effect if you specify an end time.",
        ))
        .get_matches();

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
        let (host, org, bucket, token, create_bucket, org_id) = validate_api_arguments(
            host,
            matches.value_of("ORG"),
            matches.value_of("BUCKET"),
            matches.value_of("TOKEN"),
            matches.is_present("create"),
            matches.value_of("ORG_ID"),
        );

        PointsWriterBuilder::new_api(host, org, bucket, token, create_bucket, org_id).await?
    } else {
        panic!("One of --output or --host must be provided.");
    };

    let result = iox_data_generator::generate::<rand::rngs::SmallRng>(
        &data_spec,
        &mut points_writer_builder,
        start_datetime,
        end_datetime,
        execution_start_time.timestamp_nanos(),
        continue_on,
    )
    .await;

    match result {
        Ok(total_points) => eprintln!("Submitted {} total points", total_points),
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

fn validate_api_arguments<'a>(
    host: &'a str,
    org: Option<&'a str>,
    bucket: Option<&'a str>,
    token: Option<&'a str>,
    create_bucket: bool,
    org_id: Option<&'a str>,
) -> (&'a str, &'a str, &'a str, &'a str, bool, Option<&'a str>) {
    let mut errors = vec![];

    if create_bucket && org_id.is_none() {
        panic!("When `--create` is specified, `--org_id` is required, but it was missing.");
    }

    if org.is_none() {
        errors.push("`--org` is missing");
    }
    if bucket.is_none() {
        errors.push("`--bucket` is missing");
    }
    if token.is_none() {
        errors.push("`--token` is missing");
    }

    if errors.is_empty() {
        // These `unwrap`s are safe because otherwise errors wouldn't be empty
        (
            host,
            org.unwrap(),
            bucket.unwrap(),
            token.unwrap(),
            create_bucket,
            org_id,
        )
    } else {
        panic!(
            "When `--host` is specified, `--org`, `--bucket`, and `--token` are required, \
                but {}",
            errors.join(", ")
        );
    }
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
