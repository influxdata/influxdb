use snafu::Snafu;

/// Craft and submit different types of storage read requests
#[derive(Debug, clap::Parser)]
pub struct Config {
    #[clap(subcommand)]
    command: Command,

    /// The requested start time (inclusive) of the time-range (also accepts RFC3339 format).
    #[clap(long, default_value = "-9223372036854775806")]
    start: String,

    /// The requested stop time (exclusive) of the time-range (also accepts RFC3339 format).
    #[clap(long, default_value = "9223372036854775806")]
    stop: String,

    /// A predicate to filter results by. Effectively InfluxQL predicate format (see examples).
    #[clap(long, default_value = "")]
    predicate: String,
}

/// All possible subcommands for storage_rpc
#[derive(Debug, clap::Parser)]
enum Command {
    /// Issue a read_filter request
    ReadFilter(ReadFilter),
}

/// Create a new database
#[derive(Debug, clap::Parser)]
struct ReadFilter {}

#[derive(Debug, Snafu)]
pub enum Error {}

pub type Result<T, E = Error> = std::result::Result<T, E>;

/// Create and issue read request
pub async fn command(config: Config) -> Result<()> {
    Ok(())
}
