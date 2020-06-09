#![deny(rust_2018_idioms)]
#![warn(missing_debug_implementations, clippy::explicit_iter_loop)]

use log::{debug, info, warn};
use std::fs;

use clap::{crate_authors, crate_version, App, Arg, SubCommand};

use delorean_ingest::LineProtocolConverter;
use delorean_line_parser::{parse_lines, ParsedLine};
use delorean_parquet::writer::DeloreanParquetTableWriter;
use delorean_table::{DeloreanTableWriter, DeloreanTableWriterSource, Error as TableError};
use delorean_table_schema::Schema;

mod error;
mod file_meta;
mod input;

use error::{Error, Result};

use file_meta::dump_meta;

enum ReturnCode {
    InternalError = 1,
    ConversionFailed = 2,
    MetadataDumpFailed = 3,
}

static SCHEMA_SAMPLE_SIZE: usize = 5;

/// Creates  `DeloreanParquetTableWriter` suitable for writing
#[derive(Debug)]
struct ParquetWriterSource {
    output_filename: String,
    // This creator only supports  a single filename at this time
    // so track if it has alread been made, for errors
    made_file: bool,
}

impl DeloreanTableWriterSource for ParquetWriterSource {
    // Returns a `DeloreanTableWriter suitable for writing data from packers.
    fn next_writer(&mut self, schema: &Schema) -> Result<Box<dyn DeloreanTableWriter>, TableError> {
        if self.made_file {
            return Err(TableError::Other {
                source: Box::new(Error::NotImplemented {
                    operation_name: String::from("Multiple measurements"),
                }),
            });
        }

        let output_file = fs::File::create(&self.output_filename).map_err(|e| TableError::IO {
            message: format!("Error creating output file {}", self.output_filename),
            source: e,
        })?;
        info!("Writing output to {} ...", self.output_filename);

        let writer = DeloreanParquetTableWriter::new(schema, output_file).map_err(|e| {
            TableError::Other {
                source: Box::new(Error::UnableToCreateParquetTableWriter { source: e }),
            }
        })?;
        self.made_file = true;
        Ok(Box::new(writer))
    }
}

fn convert(input_filename: &str, output_filename: &str) -> Result<()> {
    info!("dstool convert starting");
    debug!("Reading from input file {}", input_filename);
    debug!("Writing to output file {}", output_filename);

    // TODO: make a streaming parser that you can stream data through in blocks.
    // for now, just read the whole input file into RAM...
    let buf = fs::read_to_string(input_filename).map_err(|e| Error::UnableToReadInput {
        name: String::from(input_filename),
        source: e,
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

    let writer_source = Box::new(ParquetWriterSource {
        output_filename: String::from(output_filename),
        made_file: false,
    });

    let schema_sample: Vec<ParsedLine<'_>> =
        only_good_lines.by_ref().take(SCHEMA_SAMPLE_SIZE).collect();

    // The idea here is to use the first few parsed lines to deduce the schema
    let mut converter = LineProtocolConverter::new(&schema_sample, writer_source)
        .map_err(|e| Error::UnableToCreateLineProtocolConverter { source: e })?;

    debug!("Using schema deduced from sample: {:?}", converter.schema());

    info!("Schema deduced. Writing output to {} ...", output_filename);

    // Write the sample and then the remaining lines
    converter
        .ingest_lines(schema_sample.into_iter())
        .map_err(|e| Error::UnableToWriteSchemaSample { source: e })?;
    converter
        .ingest_lines(only_good_lines)
        .map_err(|e| Error::UnableToWriteGoodLines { source: e })?;
    converter
        .finalize()
        .map_err(|e| Error::UnableToCloseTableWriter { source: e })?;
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
    } else if let Some(matches) = matches.subcommand_matches("meta") {
        let input_filename = matches.value_of("INPUT").unwrap();
        match dump_meta(&input_filename) {
            Ok(()) => debug!("Metadata dump completed successfully"),
            Err(e) => {
                eprintln!("Metadata dump failed: {}", e);
                std::process::exit(ReturnCode::MetadataDumpFailed as _)
            }
        }
    } else {
        eprintln!("Internal error: no command found");
        std::process::exit(ReturnCode::InternalError as _);
    }
}
