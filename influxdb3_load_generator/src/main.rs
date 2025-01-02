//! Entrypoint of the influxdb3_load_generator binary
#![recursion_limit = "512"] // required for print_cpu
#![deny(rustdoc::broken_intra_doc_links, rustdoc::bare_urls, rust_2018_idioms)]
#![warn(
    missing_debug_implementations,
    clippy::explicit_iter_loop,
    clippy::use_self,
    clippy::clone_on_ref_ptr,
    clippy::future_not_send
)]

pub mod line_protocol_generator;
pub mod query_generator;
pub mod report;
pub mod specification;
mod specs;

pub mod commands {
    pub mod common;
    pub mod full;
    pub mod query;
    pub mod write;
}

use dotenvy::dotenv;
use observability_deps::tracing::warn;
use std::sync::{
    atomic::{AtomicUsize, Ordering},
    Arc,
};
use tokio::runtime::Runtime;

enum ReturnCode {
    Failure = 1,
}

#[derive(Debug, clap::Parser)]
#[clap(
name = "influxdb3_load_generator",
disable_help_flag = true,
arg(
clap::Arg::new("help")
.long("help")
.help("Print help information")
.action(clap::ArgAction::Help)
.global(true)
),
about = "InfluxDB 3.0 Load Generator for writes and queries",
long_about = r#"InfluxDB 3.0 Load Generator for writes and queries

Examples:
    # Run the write load generator
    influxdb3_load_generator write --help

    # Generate a sample write spec
    influxdb3_load_generator write --generate-spec

    # Run the the query load generator
    influxdb3_load_generator query  --help

    # Generate a sample query spec
    influxdb3_load_generator query --generate-spec

    # Display all commands
    influxdb3_load_generator --help
"#
)]
struct Config {
    #[clap(subcommand)]
    command: Option<Command>,
}

// Ignoring clippy here since this enum is just used for running
// the CLI command
#[allow(clippy::large_enum_variant)]
#[derive(Debug, clap::Parser)]
#[allow(clippy::large_enum_variant)]
enum Command {
    /// Perform a query against a running InfluxDB 3.0 server
    Query(commands::query::Config),

    /// Perform a set of writes to a running InfluxDB 3.0 server
    Write(commands::write::Config),

    /// Perform both writes and queries against a running InfluxDB 3.0 server
    Full(commands::full::Config),
}

fn main() -> Result<(), std::io::Error> {
    // load all environment variables from .env before doing anything
    load_dotenv();

    let config: Config = clap::Parser::parse();

    let tokio_runtime = get_runtime(None)?;
    tokio_runtime.block_on(async move {
        match config.command {
            None => println!("command required, --help for help"),
            Some(Command::Query(config)) => {
                if let Err(e) = commands::query::command(config).await {
                    eprintln!("Query command exited: {e:?}");
                    std::process::exit(ReturnCode::Failure as _)
                }
            }
            Some(Command::Write(config)) => {
                if let Err(e) = commands::write::command(config).await {
                    eprintln!("Write command exited: {e:?}");
                    std::process::exit(ReturnCode::Failure as _)
                }
            }
            Some(Command::Full(config)) => {
                if let Err(e) = commands::full::command(config).await {
                    eprintln!("Full Write/Query command exited: {e:?}");
                    std::process::exit(ReturnCode::Failure as _)
                }
            }
        }
    });

    Ok(())
}

/// Creates the tokio runtime for executing
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
            println!("Setting number of threads to '{num_threads}' per command line request");

            let thread_counter = Arc::new(AtomicUsize::new(1));
            match num_threads {
                0 => {
                    let msg =
                        format!("Invalid num-threads: '{num_threads}' must be greater than zero");
                    Err(std::io::Error::new(kind, msg))
                }
                1 => Builder::new_current_thread().enable_all().build(),
                _ => Builder::new_multi_thread()
                    .enable_all()
                    .thread_name_fn(move || {
                        format!(
                            "inflxudb3_load_generator main {}",
                            thread_counter.fetch_add(1, Ordering::SeqCst)
                        )
                    })
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
        Err(dotenvy::Error::Io(err)) if err.kind() == std::io::ErrorKind::NotFound => {
            // Ignore this - a missing env file is not an error, defaults will
            // be applied when initialising the Config struct.
        }
        Err(e) => {
            eprintln!("FATAL Error loading config from: {e}");
            eprintln!("Aborting");
            std::process::exit(1);
        }
    };
}
