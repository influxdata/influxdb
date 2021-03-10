//! Entrypoint of InfluxDB IOx binary
#![deny(rust_2018_idioms)]
#![warn(
    missing_debug_implementations,
    clippy::explicit_iter_loop,
    clippy::use_self,
    clippy::clone_on_ref_ptr
)]

use std::str::FromStr;

use dotenv::dotenv;
use structopt::StructOpt;
use tokio::runtime::Runtime;
use tracing::{debug, error, warn};

use commands::logging::LoggingLevel;
use ingest::parquet::writer::CompressionLevel;

mod commands {
    pub mod convert;
    pub mod database;
    mod input;
    pub mod logging;
    pub mod meta;
    pub mod server;
    pub mod stats;
    pub mod writer;
}

pub mod influxdb_ioxd;

enum ReturnCode {
    Failure = 1,
}

#[derive(Debug, StructOpt)]
#[structopt(
    name = "influxdb_iox",
    about = "InfluxDB IOx server and command line tools",
    long_about = r#"InfluxDB IOx server and command line tools

Examples:
    # Run the InfluxDB IOx server:
    influxdb_iox

    # Display all server settings
    influxdb_iox server --help

    # Run the InfluxDB IOx server with extra verbose logging
    influxdb_iox -v

    # Run InfluxDB IOx with full debug logging specified with RUST_LOG
    RUST_LOG=debug influxdb_iox

    # converts line protocol formatted data in temperature.lp to out.parquet
    influxdb_iox convert temperature.lp out.parquet

    # Dumps metadata information about 000000000013.tsm to stdout
    influxdb_iox meta 000000000013.tsm

    # Dumps storage statistics about out.parquet to stdout
    influxdb_iox stats out.parquet
"#
)]
struct Config {
    #[structopt(short, long, parse(from_occurrences))]
    verbose: u64,

    /// gRPC address of IOx server to connect to
    #[structopt(
        short,
        long,
        global = true,
        env = "IOX_ADDR",
        default_value = "http://127.0.0.1:8082"
    )]
    host: String, /* TODO: This must be on the root due to https://github.com/clap-rs/clap/pull/2253 */

    #[structopt(long)]
    /// Set the maximum number of threads to use. Defaults to the number of
    /// cores on the system
    num_threads: Option<usize>,

    #[structopt(subcommand)]
    command: Option<Command>,
}

#[derive(Debug, StructOpt)]
#[structopt(setting = structopt::clap::AppSettings::SubcommandRequiredElseHelp)]
enum Command {
    /// Convert one storage format to another
    Convert {
        /// The input files to read from
        input: String,
        /// The filename or directory to write the output
        output: String,
        /// How much to compress the output data. 'max' compresses the most;
        /// 'compatibility' compresses in a manner more likely to be readable by
        /// other tools.
        #[structopt(
        short, long, default_value = "compatibility",
        possible_values = & ["max", "compatibility"])]
        compression_level: String,
    },

    /// Print out metadata information about a storage file
    Meta {
        /// The input filename to read from
        input: String,
    },
    Database(commands::database::Config),
    Stats(commands::stats::Config),
    // Clippy recommended boxing this variant because it's much larger than the others
    Server(Box<commands::server::Config>),
    Writer(commands::writer::Config),
}

fn main() -> Result<(), std::io::Error> {
    // load all environment variables from .env before doing anything
    load_dotenv();

    let config = Config::from_args();

    // Logging level is determined via:
    // 1. If RUST_LOG environment variable is set, use that value
    // 2. if `-vv` (multiple instances of verbose), use DEFAULT_DEBUG_LOG_LEVEL
    // 2. if `-v` (single instances of verbose), use DEFAULT_VERBOSE_LOG_LEVEL
    // 3. Otherwise use DEFAULT_LOG_LEVEL
    let logging_level = LoggingLevel::new(config.verbose);

    let tokio_runtime = get_runtime(config.num_threads)?;
    tokio_runtime.block_on(async move {
        let host = config.host;

        match config.command {
            Some(Command::Convert {
                input,
                output,
                compression_level,
            }) => {
                logging_level.setup_basic_logging();

                let compression_level = CompressionLevel::from_str(&compression_level).unwrap();
                match commands::convert::convert(&input, &output, compression_level) {
                    Ok(()) => debug!("Conversion completed successfully"),
                    Err(e) => {
                        eprintln!("Conversion failed: {}", e);
                        std::process::exit(ReturnCode::Failure as _)
                    }
                }
            }
            Some(Command::Meta { input }) => {
                logging_level.setup_basic_logging();
                match commands::meta::dump_meta(&input) {
                    Ok(()) => debug!("Metadata dump completed successfully"),
                    Err(e) => {
                        eprintln!("Metadata dump failed: {}", e);
                        std::process::exit(ReturnCode::Failure as _)
                    }
                }
            }
            Some(Command::Stats(config)) => {
                logging_level.setup_basic_logging();
                match commands::stats::stats(&config).await {
                    Ok(()) => debug!("Storage statistics dump completed successfully"),
                    Err(e) => {
                        eprintln!("Stats dump failed: {}", e);
                        std::process::exit(ReturnCode::Failure as _)
                    }
                }
            }
            Some(Command::Database(config)) => {
                logging_level.setup_basic_logging();
                if let Err(e) = commands::database::command(host, config).await {
                    eprintln!("{}", e);
                    std::process::exit(ReturnCode::Failure as _)
                }
            }
            Some(Command::Writer(config)) => {
                logging_level.setup_basic_logging();
                if let Err(e) = commands::writer::command(host, config).await {
                    eprintln!("{}", e);
                    std::process::exit(ReturnCode::Failure as _)
                }
            }
            Some(Command::Server(config)) => {
                // Note don't set up basic logging here, different logging rules apply in server
                // mode
                let res = influxdb_ioxd::main(logging_level, Some(config)).await;

                if let Err(e) = res {
                    error!("Server shutdown with error: {}", e);
                    std::process::exit(ReturnCode::Failure as _);
                }
            }
            None => {
                unreachable!(
                    "SubcommandRequiredElseHelp will print help if there is no subcommand"
                );
            }
        }
    });

    Ok(())
}

/// Creates the tokio runtime for executing IOx
///
/// if nthreads is none, uses the default scheduler
/// otherwise, creates a scheduler with the number of threads
fn get_runtime(num_threads: Option<usize>) -> Result<Runtime, std::io::Error> {
    // NOTE: no log macros will work here!
    //
    // That means use eprintln!() instead of error!() and so on. The log emitter
    // requires a running tokio runtime and is initialised after this function.

    use tokio::runtime::Builder;
    let kind = std::io::ErrorKind::Other;
    match num_threads {
        None => Runtime::new(),
        Some(num_threads) => {
            println!(
                "Setting number of threads to '{}' per command line request",
                num_threads
            );

            match num_threads {
                0 => {
                    let msg = format!(
                        "Invalid num-threads: '{}' must be greater than zero",
                        num_threads
                    );
                    Err(std::io::Error::new(kind, msg))
                }
                1 => Builder::new_current_thread().enable_all().build(),
                _ => Builder::new_multi_thread()
                    .enable_all()
                    .worker_threads(num_threads)
                    .build(),
            }
        }
    }
}

/// Source the .env file before initialising the Config struct - this sets
/// any envs in the file, which the Config struct then uses.
///
/// Precedence is given to existing env variables.
fn load_dotenv() {
    match dotenv() {
        Ok(_) => {}
        Err(dotenv::Error::Io(err)) if err.kind() == std::io::ErrorKind::NotFound => {
            // Ignore this - a missing env file is not an error, defaults will
            // be applied when initialising the Config struct.
        }
        Err(e) => {
            eprintln!("FATAL Error loading config from: {}", e);
            eprintln!("Aborting");
            std::process::exit(1);
        }
    };
}
