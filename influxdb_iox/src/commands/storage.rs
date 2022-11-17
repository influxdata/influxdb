pub(crate) mod request;
pub(crate) mod response;

use generated_types::{
    aggregate::AggregateType, influxdata::platform::storage::read_group_request::Group, Predicate,
};
use influxdb_storage_client::{connection::Connection, Client, OrgAndBucket};
use influxrpc_parser::predicate;
use iox_time;
use snafu::{ensure, OptionExt, ResultExt, Snafu};
use std::{num::NonZeroU64, time::Duration};
use tonic::Status;

#[derive(Debug, Snafu)]
pub enum ParseError {
    #[snafu(display("unable to parse timestamp '{:?}'", t))]
    Timestamp { t: String },

    #[snafu(display("unable to parse namespace name '{:?}'", db_name))]
    NamespaceName { db_name: String },

    #[snafu(display("unable to parse predicate: {:?}", source))]
    Predicate { source: predicate::Error },

    #[snafu(display("server error: {:?}", source))]
    ServerError { source: Status },

    #[snafu(display("error building request: {:?}", source))]
    Request { source: request::Error },

    #[snafu(display("error building response: {:?}", source))]
    ResponseError { source: response::Error },

    #[snafu(display(
        "value {} not supported for format. Expected 'pretty' or 'quiet'",
        value
    ))]
    UnsupportedFormat { value: String },

    #[snafu(display("unsupported aggregate type: '{:?}'", agg))]
    Aggregate { agg: String },

    #[snafu(display(
        "unsupported group. Expected '0', 'none', '2', or 'by': got '{:?}'",
        group
    ))]
    Group { group: String },
}

pub type Result<T, E = ParseError> = std::result::Result<T, E>;

/// Craft and submit different types of storage read requests
#[derive(Debug, clap::Parser)]
pub struct Config {
    #[clap(subcommand)]
    command: Command,

    /// The name of the namespace
    #[clap(
        value_parser = parse_namespace_name,
    )]
    db_name: OrgAndBucket,

    /// The requested start time (inclusive) of the time-range (also accepts RFC3339 format).
    #[clap(
        global = true,
        long,
        default_value = "-9223372036854775806",
        value_parser = parse_range,
    )]
    pub start: i64,

    /// The requested stop time (exclusive) of the time-range (also accepts RFC3339 format).
    #[clap(
        global = true,
        long,
        default_value = "9223372036854775806",
        value_parser = parse_range,
        )]
    pub stop: i64,

    /// A predicate to filter results by. Effectively InfluxQL predicate format (see examples).
    #[clap(
        global = true,
        long,
        default_value = "",
        value_parser = parse_predicate,
    )]
    pub predicate: Predicate,

    #[clap(
        global = true,
        long,
        default_value = "pretty",
        value_parser = parse_format,
    )]
    pub format: Format,
}

// Attempts to parse either a stringified `i64` value. or alternatively parse an
// RFC3339 formatted timestamp into an `i64` value representing nanoseconds
// since the epoch.
fn parse_range(s: &str) -> Result<i64, ParseError> {
    match s.parse::<i64>() {
        Ok(v) => Ok(v),
        Err(_) => {
            // try to parse timestamp
            let t = iox_time::Time::from_rfc3339(s).or_else(|_| TimestampSnafu { t: s }.fail())?;
            Ok(t.timestamp_nanos())
        }
    }
}

// Attempts to parse the optional predicate into an `Predicate` RPC node. This
// node is then used as part of a read request.
fn parse_predicate(expr: &str) -> Result<Predicate, ParseError> {
    if expr.is_empty() {
        return Ok(Predicate::default());
    }

    predicate::expr_to_rpc_predicate(expr).context(PredicateSnafu)
}

// Attempts to parse the namespace name into and org and bucket ID.
fn parse_namespace_name(db_name: &str) -> Result<OrgAndBucket, ParseError> {
    let parts = db_name.split('_').collect::<Vec<_>>();

    ensure!(parts.len() == 2, NamespaceNameSnafu { db_name });

    let org_id = usize::from_str_radix(parts[0], 16)
        .ok()
        .context(NamespaceNameSnafu { db_name })?;

    let bucket_id = usize::from_str_radix(parts[1], 16)
        .ok()
        .context(NamespaceNameSnafu { db_name })?;

    Ok(OrgAndBucket::new(
        NonZeroU64::new(org_id as u64).context(NamespaceNameSnafu { db_name })?,
        NonZeroU64::new(bucket_id as u64).context(NamespaceNameSnafu { db_name })?,
    ))
}

// Attempts to parse the optional format.
fn parse_format(format: &str) -> Result<Format, ParseError> {
    match format {
        "pretty" => Ok(Format::Pretty),
        "quiet" => Ok(Format::Quiet),
        // TODO - raw frame format?
        _ => Err(ParseError::UnsupportedFormat {
            value: format.to_owned(),
        }),
    }
}

#[derive(Clone, Copy, Debug, clap::Parser)]
pub enum Format {
    Pretty,
    Quiet,
}

/// All possible subcommands for storage
#[derive(Debug, clap::Parser)]
enum Command {
    MeasurementFields(MeasurementFields),
    MeasurementTagKeys(MeasurementTagKeys),
    ReadFilter,
    ReadGroup(ReadGroup),
    ReadWindowAggregate(ReadWindowAggregate),
    TagValues(TagValues),
}

#[derive(Debug, clap::Parser)]
struct MeasurementFields {
    #[clap(action)]
    measurement: String,
}

#[derive(Debug, clap::Parser)]
struct MeasurementTagKeys {
    #[clap(action)]
    measurement: String,
}

#[derive(Debug, clap::Parser)]
struct ReadGroup {
    #[clap(
        long,
        value_parser = parse_aggregate,
    )]
    aggregate: Option<AggregateType>,

    #[clap(
        long,
        value_parser = parse_group,
    )]
    group: Group,

    #[clap(long, action)]
    group_keys: Vec<String>,
}

#[derive(Debug, clap::Parser)]
struct ReadWindowAggregate {
    #[clap(
        long,
        value_parser = humantime::parse_duration,
    )]
    window_every: Duration,

    #[clap(
        long,
        value_parser = humantime::parse_duration,
    )]
    offset: Duration,

    #[clap(
        long,
        value_parser = parse_aggregate,
    )]
    aggregate: Vec<AggregateType>,
}

// Attempts to parse the optional format.
fn parse_aggregate(aggs: &str) -> Result<AggregateType, ParseError> {
    match aggs.to_lowercase().as_str() {
        "none" => Ok(AggregateType::None),
        "count" => Ok(AggregateType::Count),
        "sum" => Ok(AggregateType::Sum),
        "min" => Ok(AggregateType::Min),
        "max" => Ok(AggregateType::Max),
        "mean" => Ok(AggregateType::Mean),
        "first" => Ok(AggregateType::First),
        "last" => Ok(AggregateType::Last),
        _ => AggregateSnafu { agg: aggs }.fail(),
    }
}

fn parse_group(g: &str) -> Result<Group, ParseError> {
    match g.to_lowercase().as_str() {
        "0" | "none" => Ok(Group::None),
        "2" | "by" => Ok(Group::By),
        _ => GroupSnafu { group: g }.fail(),
    }
}

#[derive(Debug, clap::Parser)]
struct TagValues {
    /// The tag key value to interrogate for tag values.
    #[clap(action)]
    tag_key: String,
}

/// Create and issue read request
pub async fn command(connection: Connection, config: Config) -> Result<()> {
    let mut client = influxdb_storage_client::Client::new(connection);

    // convert predicate with no root node into None.
    let predicate = config.predicate.root.is_some().then_some(config.predicate);

    let source = Client::read_source(&config.db_name, 0);
    let now = std::time::Instant::now();
    match config.command {
        Command::MeasurementFields(m) => {
            let result = client
                .measurement_fields(request::measurement_fields(
                    source,
                    m.measurement,
                    config.start,
                    config.stop,
                    predicate,
                ))
                .await
                .context(ServerSnafu)?;
            match config.format {
                Format::Pretty => response::pretty_print_strings(result).context(ResponseSnafu)?,
                Format::Quiet => {}
            }
        }
        Command::MeasurementTagKeys(m) => {
            let result = client
                .measurement_tag_keys(request::measurement_tag_keys(
                    source,
                    m.measurement,
                    config.start,
                    config.stop,
                    predicate,
                ))
                .await
                .context(ServerSnafu)?;
            match config.format {
                Format::Pretty => response::pretty_print_strings(result).context(ResponseSnafu)?,
                Format::Quiet => {}
            }
        }
        Command::ReadFilter => {
            let result = client
                .read_filter(request::read_filter(
                    source,
                    config.start,
                    config.stop,
                    predicate,
                ))
                .await
                .context(ServerSnafu)?;
            match config.format {
                Format::Pretty => response::pretty_print_frames(&result).context(ResponseSnafu)?,
                Format::Quiet => {}
            }
        }
        Command::ReadGroup(rg) => {
            let result = client
                .read_group(request::read_group(
                    source,
                    config.start,
                    config.stop,
                    predicate,
                    rg.aggregate,
                    rg.group,
                    rg.group_keys,
                ))
                .await
                .context(ServerSnafu)?;
            match config.format {
                Format::Pretty => response::pretty_print_frames(&result).context(ResponseSnafu)?,
                Format::Quiet => {}
            }
        }
        Command::ReadWindowAggregate(rwa) => {
            let result = client
                .read_window_aggregate(
                    request::read_window_aggregate(
                        source,
                        config.start,
                        config.stop,
                        predicate,
                        rwa.window_every,
                        rwa.offset,
                        rwa.aggregate,
                        None, // TODO(edd): determine if window needs to be set
                    )
                    .context(RequestSnafu)?,
                )
                .await
                .context(ServerSnafu)?;

            match config.format {
                Format::Pretty => response::pretty_print_frames(&result).context(ResponseSnafu)?,
                Format::Quiet => {}
            }
        }
        Command::TagValues(tv) => {
            let result = client
                .tag_values(request::tag_values(
                    source,
                    config.start,
                    config.stop,
                    predicate,
                    tv.tag_key,
                ))
                .await
                .context(ServerSnafu)?;
            match config.format {
                Format::Pretty => response::pretty_print_strings(result).context(ResponseSnafu)?,
                Format::Quiet => {}
            }
        }
    };
    println!("Query execution: {:?}", now.elapsed());
    Ok(())
}

#[cfg(test)]
mod test_super {
    use super::*;

    #[test]
    fn test_parse_range() {
        let cases = vec![
            ("1965-06-11T15:22:22.1234Z", -143800657876600000),
            ("1970-01-01T00:00:00Z", 0),
            ("1970-01-01T00:00:00.00000001Z", 10),
            ("2028-01-01T15:00:00Z", 1830351600000000000),
            ("1830351600000000000", 1830351600000000000),
            ("-12345", -12345),
        ];

        for (input, exp) in cases {
            let got = parse_range(input).unwrap();
            assert_eq!(
                got, exp,
                "got {:?} for input {:?}, expected {:?}",
                got, input, exp
            );
        }
    }
}
