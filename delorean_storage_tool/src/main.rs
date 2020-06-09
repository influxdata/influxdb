#![deny(rust_2018_idioms)]
#![warn(missing_debug_implementations, clippy::explicit_iter_loop)]

use clap::{crate_authors, crate_version, App, Arg, SubCommand};
use delorean_ingest::LineProtocolConverter;
use delorean_line_parser::{parse_lines, ParsedLine};
use delorean_parquet::writer::{DeloreanTableWriter, Error as DeloreanTableWriterError};
use log::{debug, info, warn};
use snafu::{ResultExt, Snafu};
use std::fs;

#[derive(Debug, Snafu)]
pub enum Error {
    #[snafu(display("Error reading {} ({})", name.display(), source))]
    UnableToReadInput {
        name: std::path::PathBuf,
        source: std::io::Error,
    },

    UnableToCreateFile {
        name: std::path::PathBuf,
        source: std::io::Error,
    },

    #[snafu(context(false))]
    Conversion {
        source: delorean_ingest::Error,
    },

    UnableToCreateTableWriter {
        source: DeloreanTableWriterError,
    },

    UnableToWriteSchemaSample {
        source: DeloreanTableWriterError,
    },

    UnableToWriteGoodLines {
        source: DeloreanTableWriterError,
    },

    UnableToCloseTableWriter {
        source: DeloreanTableWriterError,
    },
}

type Result<T, E = Error> = std::result::Result<T, E>;

enum ReturnCode {
    InternalError = 1,
    ConversionFailed = 2,
}

static SCHEMA_SAMPLE_SIZE: usize = 5;

fn convert(input_filename: &str, output_filename: &str) -> Result<()> {
    info!("dstool starting");
    debug!("Reading from input file {}", input_filename);
    debug!("Writing to output file {}", output_filename);

    // TODO: make a streaming parser that you can stream data through in blocks.
    // for now, just read the whole input file into RAM...
    let buf = fs::read_to_string(input_filename).context(UnableToReadInput {
        name: input_filename,
    })?;
    info!("Read {} bytes from {}", buf.len(), input_filename);

    // FIXME: Design something sensible to do with lines that don't
    // parse rather than just dropping them on the floor
    let mut only_good_lines = parse_lines(&buf).filter_map(|r| match r {
        Ok(line) => Some(line),
        Err(e) => {
            warn!("Ignorning line with parse error: {}", e);
            None
        }
    });

    let schema_sample: Vec<ParsedLine<'_>> =
        only_good_lines.by_ref().take(SCHEMA_SAMPLE_SIZE).collect();

    // The idea here is to use the first few parsed lines to deduce the schema
    let converter = LineProtocolConverter::new(&schema_sample)?;
    debug!("Using schema deduced from sample: {:?}", converter.schema());

    info!("Schema deduced. Writing output to {} ...", output_filename);
    let output_file = fs::File::create(output_filename).context(UnableToCreateFile {
        name: output_filename,
    })?;

    let mut writer = DeloreanTableWriter::new(converter.schema(), output_file)
        .context(UnableToCreateTableWriter)?;

    // Write the sample and then the remaining lines
    writer
        .write_batch(&converter.pack_lines(schema_sample.into_iter()))
        .context(UnableToWriteSchemaSample)?;
    writer
        .write_batch(&converter.pack_lines(only_good_lines))
        .context(UnableToWriteGoodLines)?;
    writer.close().context(UnableToCloseTableWriter)?;
    info!("Completing writing {} successfully", output_filename);
    Ok(())
}

fn main() {
    let help = r#"Delorean Storage Tool

Examples:
    # converts line protocol formatted data in temperature.lp to out.parquet
    dstool convert temperature.lp out.parquet

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
