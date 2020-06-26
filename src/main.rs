#![deny(rust_2018_idioms)]
#![warn(
    missing_copy_implementations,
    missing_debug_implementations,
    clippy::explicit_iter_loop,
    clippy::use_self
)]

use clap::{crate_authors, crate_version, App, Arg, SubCommand};
use log::{debug, error, warn};

mod commands {
    pub mod convert;
    pub mod file_meta;
    mod input;
    pub mod stats;
}
mod rpc;
mod server;

enum ReturnCode {
    ConversionFailed = 1,
    MetadataDumpFailed = 2,
    StatsFailed = 3,
    ServerExitedAbnormally = 4,
}

fn main() {
    let help = r#"Delorean server and command line tools

Examples:
    # Run the Delorean server:
    delorean

    # Run the Delorean server with extra verbose logging
    delorean -v

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
                        .help("The input filename to read from")
                        .required(true)
                        .index(1),
                )
                .arg(
                    Arg::with_name("OUTPUT")
                        .takes_value(true)
                        .help("The filename or directory to write the output.")
                        .required(true)
                        .index(2),
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
                .about("Print out storage statistics information about a file to stdout")
                .arg(
                    Arg::with_name("INPUT")
                        .help("The input filename to read from")
                        .required(true)
                        .index(1),
                ),
        )
        .subcommand(SubCommand::with_name("server").about("Runs in server mode (default)"))
        .arg(
            Arg::with_name("verbose")
                .short("v")
                .long("verbose")
                .help("Enables verbose output"),
        )
        .get_matches();

    if matches.is_present("verbose") {
        std::env::set_var("RUST_LOG", "delorean=debug,hyper=info");
    } else {
        std::env::set_var("RUST_LOG", "delorean=info,hyper=info");
    }
    env_logger::init();

    match matches.subcommand() {
        ("convert", Some(sub_matches)) => {
            let input_filename = sub_matches.value_of("INPUT").unwrap();
            let output_filename = sub_matches.value_of("OUTPUT").unwrap();
            match commands::convert::convert(&input_filename, &output_filename) {
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
            let input_filename = sub_matches.value_of("INPUT").unwrap();
            match commands::stats::stats(&input_filename) {
                Ok(()) => debug!("Storage statistics dump completed successfully"),
                Err(e) => {
                    eprintln!("Stats dump failed: {}", e);
                    std::process::exit(ReturnCode::StatsFailed as _)
                }
            }
        }
        ("server", Some(_)) | (_, _) => {
            println!("Staring delorean server...");
            match server::main() {
                Ok(()) => eprintln!("Shutdown OK"),
                Err(e) => {
                    error!("Server shutdown with error: {:?}", e);
                    std::process::exit(ReturnCode::ServerExitedAbnormally as _);
                }
            }
        }
    }
}
