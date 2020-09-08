#![deny(rust_2018_idioms)]
#![warn(
    missing_copy_implementations,
    missing_debug_implementations,
    clippy::explicit_iter_loop,
    clippy::use_self
)]

use clap::{crate_authors, crate_version, value_t, App, Arg, SubCommand};
use delorean_parquet::writer::CompressionLevel;
use log::{debug, error, warn};

pub mod server;

mod commands {
    pub mod convert;
    pub mod file_meta;
    mod input;
    pub mod server;
    pub mod stats;
    pub mod write_buffer;
}

enum ReturnCode {
    ConversionFailed = 1,
    MetadataDumpFailed = 2,
    StatsFailed = 3,
    ServerExitedAbnormally = 4,
}

#[tokio::main]
async fn main() {
    let help = r#"Delorean server and command line tools

Examples:
    # Run the Delorean server:
    delorean

    # Run the Delorean server with extra verbose logging
    delorean -v

    # Run delorean with full debug logging specified with RUST_LOG
    RUST_LOG=debug delorean

    # converts line protocol formatted data in temperature.lp to out.parquet
    delorean convert temperature.lp out.parquet

    # Dumps metadata information about 000000000013.tsm to stdout
    delorean meta 000000000013.tsm

    # Dumps storage statistics about out.parquet to stdout
    delorean stats out.parquet
"#;

    let matches = App::new(help)
        .version(crate_version!())
        .author(crate_authors!())
        .about("Delorean server and command line tools")
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
            SubCommand::with_name("write-buffer")
                .about("Starts the delorean server using the write buffer database implementation.")
        )
        .subcommand(
            SubCommand::with_name("server")
                .about("Runs in server mode (default)")
                .arg(
                    Arg::with_name("write-buffer")
                        .help("Run the server with the write buffer database enabled rather than MemDB")
                        .short("wb"),
                ),
        )
        .arg(Arg::with_name("verbose").short("v").long("verbose").multiple(true).help(
            "Enables verbose logging (use 'vv' for even more verbosity). You can also set log level via \
                       the environment variable RUST_LOG=<value>",
        ))
        .get_matches();

    setup_logging(matches.occurrences_of("verbose"));

    match matches.subcommand() {
        ("convert", Some(sub_matches)) => {
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
        ("write-buffer", Some(_sub_matches)) => {
            println!("Starting delorean server using WriteBuffer implementation...");
            match commands::write_buffer::main().await {
                Ok(()) => eprintln!("Shutdown OK"),
                Err(e) => {
                    error!("Server shutdown with error: {:?}", e);
                    std::process::exit(ReturnCode::ServerExitedAbnormally as _);
                }
            }
        }
        ("server", Some(_)) | (_, _) => {
            println!("Starting delorean server...");
            match commands::server::main().await {
                Ok(()) => eprintln!("Shutdown OK"),
                Err(e) => {
                    error!("Server shutdown with error: {:?}", e);
                    std::process::exit(ReturnCode::ServerExitedAbnormally as _);
                }
            }
        }
    }
}

/// Default debug level is debug for everything except
/// some especially noisy low level libraries
const DEFAULT_DEBUG_LOG_LEVEL: &str = "debug,h2=info";

// Default verbose log level is info level for all components
const DEFAULT_VERBOSE_LOG_LEVEL: &str = "info";

// Default log level is warn level for all components
const DEFAULT_LOG_LEVEL: &str = "warn";

/// Configures logging in the following precedence:
///
/// 1. If RUST_LOG environment variable is set, use that value
/// 2. if `-vv` (multiple instances of verbose), use DEFAULT_DEBUG_LOG_LEVEL
/// 2. if `-v` (single instances of verbose), use DEFAULT_VERBOSE_LOG_LEVEL
/// 3. Otherwise use DEFAULT_LOG_LEVEL
fn setup_logging(num_verbose: u64) {
    let rust_log_env = std::env::var("RUST_LOG");

    match rust_log_env {
        Ok(lvl) => {
            if num_verbose > 0 {
                eprintln!(
                    "WARNING: Using RUST_LOG='{}' environment, ignoring -v command line",
                    lvl
                );
            }
        }
        Err(_) => match num_verbose {
            0 => std::env::set_var("RUST_LOG", DEFAULT_LOG_LEVEL),
            1 => std::env::set_var("RUST_LOG", DEFAULT_VERBOSE_LOG_LEVEL),
            _ => std::env::set_var("RUST_LOG", DEFAULT_DEBUG_LOG_LEVEL),
        },
    }

    env_logger::init();
}
