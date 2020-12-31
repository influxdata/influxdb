//! Entrypoint of InfluxDB IOx binary
#![deny(rust_2018_idioms)]
#![warn(
    missing_copy_implementations,
    missing_debug_implementations,
    clippy::explicit_iter_loop,
    clippy::use_self
)]

use clap::{crate_authors, crate_version, value_t, App, Arg, ArgMatches, SubCommand};
use ingest::parquet::writer::CompressionLevel;
use tokio::runtime::Runtime;
use tracing::{debug, error, info, warn};

mod panic;
pub mod server;

mod commands {
    pub mod config;
    pub mod convert;
    pub mod file_meta;
    mod input;
    pub mod logging;
    pub mod server;
    pub mod stats;
}

use commands::logging::LoggingLevel;

enum ReturnCode {
    ConversionFailed = 1,
    MetadataDumpFailed = 2,
    StatsFailed = 3,
    ServerExitedAbnormally = 4,
}

fn main() -> Result<(), std::io::Error> {
    let help = r#"InfluxDB IOx server and command line tools

Examples:
    # Run the InfluxDB IOx server:
    influxdb_iox

    # Display all current config settings
    influxdb_iox config show

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
"#;

    let matches = App::new(help)
        .version(crate_version!())
        .author(crate_authors!())
        .about("InfluxDB IOx server and command line tools")
        .subcommand(
            SubCommand::with_name("convert")
                .about("Convert one storage format to another")
                .arg(
                    Arg::with_name("INPUT")
                        .help("The input files to read from")
                        .required(true)
                        .index(1),
                )
                .arg(
                    Arg::with_name("OUTPUT")
                        .takes_value(true)
                        .help("The filename or directory to write the output.")
                        .required(true)
                        .index(2),
                )
                .arg(
                    Arg::with_name("compression_level")
                        .short("c")
                        .long("compression-level")
                        .help("How much to compress the output data. 'max' compresses the most; 'compatibility' compresses in a manner more likely to be readable by other tools.")
                        .takes_value(true)
                        .possible_values(&["max", "compatibility"])
                        .default_value("compatibility"),
                ),
        )
        .subcommand(
            SubCommand::with_name("meta")
                .about("Print out metadata information about a storage file")
                .arg(
                    Arg::with_name("INPUT")
                        .help("The input filename to read from")
                        .required(true)
                        .index(1),
                ),
        )
        .subcommand(
            SubCommand::with_name("stats")
                .about("Print out storage statistics information to stdout. \
                        If a directory is specified, checks all files recursively")
                .arg(
                    Arg::with_name("INPUT")
                        .help("The input filename or directory to read from")
                        .required(true)
                        .index(1),
                )
                .arg(
                    Arg::with_name("per-column")
                        .long("per-column")
                        .help("Include detailed information per column")
                )
                .arg(
                    Arg::with_name("per-file")
                        .long("per-file")
                        .help("Include detailed information per file")
                ),
        )
        .subcommand(
            SubCommand::with_name("config")
                .about("Configuration display and manipulation")
                .subcommand(SubCommand::with_name("show").help("show current configuration information"))
                .subcommand(SubCommand::with_name("help").help("explain detailed configuration options"))
        )
         .subcommand(
            SubCommand::with_name("server")
                .about("Runs in server mode (default)")
        )
        .arg(Arg::with_name("verbose").short("v").long("verbose").multiple(true).help(
            "Enables verbose logging (use 'vv' for even more verbosity). You can also set log level via \
                       the environment variable RUST_LOG=<value>",
        ))
        .arg(Arg::with_name("num-threads").long("num-threads").takes_value(true).help(
            "Set the maximum number of threads to use. Defaults to the number of cores on the system",
        ))
        .arg(Arg::with_name("ignore-config-file").long("ignore-config-file").takes_value(false).help(
            "If specified, ignores the default configuration file, if any. Configuration is read from the environment only",
        ))
        .get_matches();

    let mut tokio_runtime = get_runtime(matches.value_of("num-threads"))?;
    tokio_runtime.block_on(dispatch_args(matches));

    info!("InfluxDB IOx server shutting down");
    Ok(())
}

async fn dispatch_args(matches: ArgMatches<'_>) {
    // Logging level is determined via:
    // 1. If RUST_LOG environment variable is set, use that value
    // 2. if `-vv` (multiple instances of verbose), use DEFAULT_DEBUG_LOG_LEVEL
    // 2. if `-v` (single instances of verbose), use DEFAULT_VERBOSE_LOG_LEVEL
    // 3. Otherwise use DEFAULT_LOG_LEVEL
    let logging_level = LoggingLevel::new(matches.occurrences_of("verbose"));

    let ignore_config_file = matches.occurrences_of("ignore-config-file") > 0;

    match matches.subcommand() {
        ("convert", Some(sub_matches)) => {
            logging_level.setup_basic_logging();
            let input_path = sub_matches.value_of("INPUT").unwrap();
            let output_path = sub_matches.value_of("OUTPUT").unwrap();
            let compression_level =
                value_t!(sub_matches, "compression_level", CompressionLevel).unwrap();
            match commands::convert::convert(&input_path, &output_path, compression_level) {
                Ok(()) => debug!("Conversion completed successfully"),
                Err(e) => {
                    eprintln!("Conversion failed: {}", e);
                    std::process::exit(ReturnCode::ConversionFailed as _)
                }
            }
        }
        ("meta", Some(sub_matches)) => {
            logging_level.setup_basic_logging();
            let input_filename = sub_matches.value_of("INPUT").unwrap();
            match commands::file_meta::dump_meta(&input_filename) {
                Ok(()) => debug!("Metadata dump completed successfully"),
                Err(e) => {
                    eprintln!("Metadata dump failed: {}", e);
                    std::process::exit(ReturnCode::MetadataDumpFailed as _)
                }
            }
        }
        ("stats", Some(sub_matches)) => {
            logging_level.setup_basic_logging();
            let config = commands::stats::StatsConfig {
                input_path: sub_matches.value_of("INPUT").unwrap().into(),
                per_file: sub_matches.is_present("per-file"),
                per_column: sub_matches.is_present("per-column"),
            };

            match commands::stats::stats(&config).await {
                Ok(()) => debug!("Storage statistics dump completed successfully"),
                Err(e) => {
                    eprintln!("Stats dump failed: {}", e);
                    std::process::exit(ReturnCode::StatsFailed as _)
                }
            }
        }
        ("config", Some(sub_matches)) => {
            logging_level.setup_basic_logging();
            match sub_matches.subcommand() {
                ("show", _) => commands::config::show_config(ignore_config_file),
                ("help", _) => commands::config::describe_config(ignore_config_file),
                (command, _) => panic!("Unknown subcommand for config: {}", command),
            }
        }
        ("server", Some(_)) | (_, _) => {
            // Note don't set up basic logging here, different logging rules appy in server
            // mode
            println!("InfluxDB IOx server starting");
            match commands::server::main(logging_level, ignore_config_file).await {
                Ok(()) => eprintln!("Shutdown OK"),
                Err(e) => {
                    error!("Server shutdown with error: {}", e);
                    std::process::exit(ReturnCode::ServerExitedAbnormally as _);
                }
            }
        }
    }
}

/// Creates the tokio runtime for executing IOx
///
/// if nthreads is none, uses the default scheduler
/// otherwise, creates a scheduler with the number of threads
fn get_runtime(num_threads: Option<&str>) -> Result<Runtime, std::io::Error> {
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
            let n = num_threads.parse::<usize>().map_err(|e| {
                let msg = format!(
                    "Invalid num-threads: can not parse '{}' as an integer: {}",
                    num_threads, e
                );
                std::io::Error::new(kind, msg)
            })?;

            match n {
                0 => {
                    let msg = format!(
                        "Invalid num-threads: '{}' must be greater than zero",
                        num_threads
                    );
                    Err(std::io::Error::new(kind, msg))
                }
                1 => Builder::new().basic_scheduler().enable_all().build(),
                _ => Builder::new()
                    .threaded_scheduler()
                    .enable_all()
                    .core_threads(n)
                    .build(),
            }
        }
    }
}
