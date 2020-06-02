use std::fs;
use std::sync::Arc;

use log::{debug, info};

use clap::{crate_authors, crate_version, App, Arg, SubCommand};
use snafu::Snafu;

#[derive(Debug, Snafu)]
pub enum Error {
    #[snafu(display(r#"IO Error: {} ({})"#, message, source))]
    IOError {
        message: String,
        source: Arc<dyn std::error::Error>,
    },
}

type Result<T, E = Error> = std::result::Result<T, E>;

enum ReturnCode {
    InternalError = 1,
    ConversionFailed = 2,
}

fn convert(input_filename: &str, output_filename: &str) -> Result<()> {
    info!("dstool starting");
    debug!("reading from input file {}", input_filename);
    debug!("writing to output file {}", output_filename);

    // TODO: make a streaming parser that you can stream data through in blocks.
    // for now, just read the whole thing into RAM...
    let buf = fs::read_to_string(input_filename).map_err(|e| {
        let msg = format!("Error reading {}", input_filename);
        Error::IOError {
            message: msg,
            source: Arc::new(e),
        }
    })?;
    info!("Read {} bytes from {}", buf.len(), input_filename);

    unimplemented!("The actual conversion");
}

fn main() {
    let help = r#"Delorean Storage Tool

Examples:
    # converts line protocol formatted data in temperature.txt to out.parquet
    dstool convert temperature.txt out.parquet

"#;

    let matches = App::new(help)
        .version(crate_version!())
        .author(crate_authors!())
        .about("Storage file manipulation and inspection utility")
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
                        .help("The filename to write the output.")
                        .required(true)
                        .index(2),
                ),
        )
        .arg(
            Arg::with_name("verbose")
                .short("v")
                .long("verbose")
                .help("Enables verbose output"),
        )
        .get_matches();

    if matches.is_present("verbose") {
        std::env::set_var("RUST_LOG", "debug");
    } else {
        std::env::set_var("RUST_LOG", "info");
    }
    env_logger::init();

    if let Some(matches) = matches.subcommand_matches("convert") {
        // clap says Calling .unwrap() is safe here because "INPUT"
        // and "OUTPUT" are required (if "INPUT" wasn't required we
        // could have used an 'if let' to conditionally get the value)
        let input_filename = matches.value_of("INPUT").unwrap();
        let output_filename = matches.value_of("OUTPUT").unwrap();
        match convert(&input_filename, &output_filename) {
            Ok(()) => debug!("Conversion completed successfully"),
            Err(e) => {
                eprintln!("Conversion failed: {}", e);
                std::process::exit(ReturnCode::ConversionFailed as _)
            }
        }
    } else {
        eprintln!("Internal error: no command found");
        std::process::exit(ReturnCode::InternalError as _);
    }
}
