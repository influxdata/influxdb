use generated_types::Predicate;
use influxrpc_parser::predicate;
use time;

use snafu::{ResultExt, Snafu};

#[derive(Debug, Snafu)]
pub enum Error {
    #[snafu(display("Unable to parse timestamp '{:?}'", t))]
    TimestampParseError { t: String },

    #[snafu(display("Unable to parse predicate: {:?}", source))]
    PredicateParseError { source: predicate::Error },
}

pub type Result<T, E = Error> = std::result::Result<T, E>;

/// Craft and submit different types of storage read requests
#[derive(Debug, clap::Parser)]
pub struct Config {
    #[clap(subcommand)]
    command: Command,

    /// The requested start time (inclusive) of the time-range (also accepts RFC3339 format).
    #[clap(long, default_value = "-9223372036854775806", parse(try_from_str = parse_range))]
    start: i64,

    /// The requested stop time (exclusive) of the time-range (also accepts RFC3339 format).
    #[clap(long, default_value = "9223372036854775806", parse(try_from_str = parse_range))]
    stop: i64,

    /// A predicate to filter results by. Effectively InfluxQL predicate format (see examples).
    #[clap(long, default_value = "", parse(try_from_str = parse_predicate))]
    predicate: Predicate,
}

// Attempts to parse either a stringified `i64` value. or alternatively parse an
// RFC3339 formatted timestamp into an `i64` value representing nanoseconds
// since the epoch.
fn parse_range(s: &str) -> Result<i64, Error> {
    match s.parse::<i64>() {
        Ok(v) => Ok(v),
        Err(_) => {
            // try to parse timestamp
            let t = time::Time::from_rfc3339(s).or_else(|_| TimestampParseSnafu { t: s }.fail())?;
            Ok(t.timestamp_nanos())
        }
    }
}

// Attempts to parse the optional predicate into an `Predicate` RPC node. This
// node is then used as part of a read request.
fn parse_predicate(expr: &str) -> Result<Predicate, Error> {
    if expr.is_empty() {
        return Ok(Predicate::default());
    }

    predicate::expr_to_rpc_predicate(expr).context(PredicateParseSnafu)
}

/// All possible subcommands for storage
#[derive(Debug, clap::Parser)]
enum Command {
    /// Issue a read_filter request
    ReadFilter(ReadFilter),
}

/// Create a new database
#[derive(Debug, clap::Parser)]
struct ReadFilter {}

/// Create and issue read request
pub async fn command(config: Config) -> Result<()> {
    // TODO(edd): handle command/config and execute request
    println!("Unimplemented: config is {:?}", config);
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
