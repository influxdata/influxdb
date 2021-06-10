//! Entrypoint of InfluxDB IOx binary
#![deny(broken_intra_doc_links, rust_2018_idioms)]
#![warn(
    missing_debug_implementations,
    clippy::explicit_iter_loop,
    clippy::use_self,
    clippy::clone_on_ref_ptr,
    clippy::future_not_send
)]

use dotenv::dotenv;
use structopt::StructOpt;
use tokio::runtime::Runtime;

use commands::tracing::{init_logs_and_tracing, init_simple_logs};
use observability_deps::tracing::warn;

use crate::commands::tracing::TracingGuard;
use observability_deps::tracing::dispatcher::SetGlobalDefaultError;
use tikv_jemallocator::Jemalloc;

mod commands {
    pub mod database;
    pub mod operations;
    pub mod run;
    pub mod server;
    pub mod server_remote;
    pub mod sql;
    pub mod tracing;
}

pub mod influxdb_ioxd;

#[global_allocator]
static GLOBAL: Jemalloc = Jemalloc;

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

    # Run the interactive SQL prompt
    influxdb_iox sql

    # Display all server settings
    influxdb_iox run --help

    # Run the InfluxDB IOx server with extra verbose logging
    influxdb_iox run -v

    # Run InfluxDB IOx with full debug logging specified with RUST_LOG
    RUST_LOG=debug influxdb_iox run

    # converts line protocol formatted data in temperature.lp to out.parquet
    influxdb_iox convert temperature.lp out.parquet

    # Dumps metadata information about 000000000013.tsm to stdout
    influxdb_iox meta 000000000013.tsm

    # Dumps storage statistics about out.parquet to stdout
    influxdb_iox stats out.parquet

Command are generally structured in the form:
    <type of object> <action> <arguments>

For example, a command such as the following shows all actions
    available for database chunks, including get and list.

    influxdb_iox database chunk --help
"#
)]
struct Config {
    /// Log filter short-hand.
    ///
    /// Convenient way to set log severity level filter.
    /// Overrides --log-filter / LOG_FILTER.
    ///
    /// -v   'info'
    ///
    /// -vv  'debug,hyper::proto::h1=info,h2=info'
    ///
    /// -vvv 'trace,hyper::proto::h1=info,h2=info'
    #[structopt(
        short = "-v",
        long = "--verbose",
        multiple = true,
        takes_value = false,
        parse(from_occurrences)
    )]
    pub log_verbose_count: u8,

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
    command: Command,
}

#[derive(Debug, StructOpt)]
enum Command {
    Database(commands::database::Config),
    // Clippy recommended boxing this variant because it's much larger than the others
    Run(Box<commands::run::Config>),
    Server(commands::server::Config),
    Operation(commands::operations::Config),
    Sql(commands::sql::Config),
}

fn main() -> Result<(), std::io::Error> {
    // load all environment variables from .env before doing anything
    load_dotenv();

    let config = Config::from_args();

    let tokio_runtime = get_runtime(config.num_threads)?;
    tokio_runtime.block_on(async move {
        let host = config.host;
        let log_verbose_count = config.log_verbose_count;

        fn handle_init_logs(r: Result<TracingGuard, SetGlobalDefaultError>) -> TracingGuard {
            match r {
                Ok(guard) => guard,
                Err(e) => {
                    eprintln!("Initializing logs failed: {}", e);
                    std::process::exit(ReturnCode::Failure as _);
                }
            }
        }

        match config.command {
            Command::Database(config) => {
                let _tracing_guard = handle_init_logs(init_simple_logs(log_verbose_count));
                if let Err(e) = commands::database::command(host, config).await {
                    eprintln!("{}", e);
                    std::process::exit(ReturnCode::Failure as _)
                }
            }
            Command::Operation(config) => {
                let _tracing_guard = handle_init_logs(init_simple_logs(log_verbose_count));
                if let Err(e) = commands::operations::command(host, config).await {
                    eprintln!("{}", e);
                    std::process::exit(ReturnCode::Failure as _)
                }
            }
            Command::Server(config) => {
                let _tracing_guard = handle_init_logs(init_simple_logs(log_verbose_count));
                if let Err(e) = commands::server::command(host, config).await {
                    eprintln!("Server command failed: {}", e);
                    std::process::exit(ReturnCode::Failure as _)
                }
            }
            Command::Run(config) => {
                let _tracing_guard =
                    handle_init_logs(init_logs_and_tracing(log_verbose_count, &config));
                if let Err(e) = commands::run::command(*config).await {
                    eprintln!("Server command failed: {}", e);
                    std::process::exit(ReturnCode::Failure as _)
                }
            }
            Command::Sql(config) => {
                let _tracing_guard = handle_init_logs(init_simple_logs(log_verbose_count));
                if let Err(e) = commands::sql::command(host, config).await {
                    eprintln!("{}", e);
                    std::process::exit(ReturnCode::Failure as _)
                }
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
