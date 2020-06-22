use delorean_ingest::{ConversionSettings, LineProtocolConverter, TSMFileConverter};
use delorean_line_parser::parse_lines;
use delorean_parquet::writer::DeloreanParquetTableWriter;
use delorean_table::{DeloreanTableWriter, DeloreanTableWriterSource, Error as TableError};
use delorean_table_schema::Schema;
use log::{debug, info, warn};
use std::convert::TryInto;
use std::fs;
use std::io::Read;
use std::path::{Path, PathBuf};

use crate::commands::error::{Error, Result};
use crate::commands::input::{FileType, InputReader};
/// Creates  `DeloreanParquetTableWriter` suitable for writing to a single file
#[derive(Debug)]
struct ParquetFileWriterSource {
    output_filename: String,
    // This creator only supports  a single filename at this time
    // so track if it has alread been made, for errors
    made_file: bool,
}

impl DeloreanTableWriterSource for ParquetFileWriterSource {
    // Returns a `DeloreanTableWriter suitable for writing data from packers.
    fn next_writer(&mut self, schema: &Schema) -> Result<Box<dyn DeloreanTableWriter>, TableError> {
        if self.made_file {
            return Err(TableError::Other {
                source: Box::new(Error::MultipleMeasurementsToSingleFile {
                    new_measurement_name: String::from(schema.measurement()),
                }),
            });
        }

        let output_file = fs::File::create(&self.output_filename).map_err(|e| TableError::IO {
            message: format!("Error creating output file {}", self.output_filename),
            source: e,
        })?;
        info!(
            "Writing output for measurement {} to {} ...",
            schema.measurement(),
            self.output_filename
        );

        let writer = DeloreanParquetTableWriter::new(schema, output_file).map_err(|e| {
            TableError::Other {
                source: Box::new(Error::UnableToCreateParquetTableWriter { source: e }),
            }
        })?;
        self.made_file = true;
        Ok(Box::new(writer))
    }
}

/// Creates `DeloreanParquetTableWriter` for each measurement by
/// writing each to a separate file (measurement1.parquet,
/// measurement2.parquet, etc)
#[derive(Debug)]
struct ParquetDirectoryWriterSource {
    output_dir_path: PathBuf,
}

impl DeloreanTableWriterSource for ParquetDirectoryWriterSource {
    /// Returns a `DeloreanTableWriter` suitable for writing data from packers.
    /// named in the template of <measurement.parquet>
    fn next_writer(&mut self, schema: &Schema) -> Result<Box<dyn DeloreanTableWriter>, TableError> {
        let mut output_file_path: PathBuf = self.output_dir_path.clone();

        output_file_path.push(schema.measurement());
        output_file_path.set_extension("parquet");

        let output_file = fs::File::create(&output_file_path).map_err(|e| TableError::IO {
            message: format!("Error creating output file {:?}", output_file_path),
            source: e,
        })?;
        info!(
            "Writing output for measurement {} to {:?} ...",
            schema.measurement(),
            output_file_path
        );

        let writer = DeloreanParquetTableWriter::new(schema, output_file).map_err(|e| {
            TableError::Other {
                source: Box::new(Error::UnableToCreateParquetTableWriter { source: e }),
            }
        })?;
        Ok(Box::new(writer))
    }
}

pub fn is_directory(p: impl AsRef<Path>) -> bool {
    fs::metadata(p)
        .map(|metadata| metadata.is_dir())
        .unwrap_or(false)
}

pub fn convert(input_filename: &str, output_name: &str) -> Result<()> {
    info!("convert starting");
    debug!("Reading from input file {}", input_filename);

    let input_reader = InputReader::new(input_filename)?;
    info!(
        "Preparing to convert {} bytes from {}",
        input_reader.len(),
        input_filename
    );

    match input_reader.file_type() {
        FileType::LineProtocol => {
            convert_line_protocol_to_parquet(input_filename, input_reader, output_name)
        }
        FileType::TSM => {
            // TODO(edd): we can remove this when I figure out the best way to share
            // the reader between the TSM index reader and the Block decoder.
            let input_block_reader = InputReader::new(input_filename)?;
            let len = input_reader.len() as usize;
            convert_tsm_to_parquet(input_reader, len, input_block_reader, output_name)
        }
        FileType::Parquet => Err(Error::NotImplemented {
            operation_name: String::from("Parquet format conversion"),
        }),
    }
}

fn convert_line_protocol_to_parquet(
    input_filename: &str,
    mut input_reader: InputReader,
    output_name: &str,
) -> Result<()> {
    // TODO: make a streaming parser that you can stream data through in blocks.
    // for now, just read the whole input at once into a string
    let mut buf = String::with_capacity(
        input_reader
            .len()
            .try_into()
            .expect("Can not allocate buffer"),
    );
    input_reader
        .read_to_string(&mut buf)
        .map_err(|e| Error::UnableToReadInput {
            name: String::from(input_filename),
            source: e,
        })?;

    // FIXME: Design something sensible to do with lines that don't
    // parse rather than just dropping them on the floor
    let only_good_lines = parse_lines(&buf).filter_map(|r| match r {
        Ok(line) => Some(line),
        Err(e) => {
            warn!("Ignorning line with parse error: {}", e);
            None
        }
    });

    // setup writing
    let writer_source: Box<dyn DeloreanTableWriterSource> = if is_directory(&output_name) {
        info!("Writing to output directory {:?}", output_name);
        Box::new(ParquetDirectoryWriterSource {
            output_dir_path: PathBuf::from(output_name),
        })
    } else {
        info!("Writing to output file {}", output_name);
        Box::new(ParquetFileWriterSource {
            output_filename: String::from(output_name),
            made_file: false,
        })
    };

    let settings = ConversionSettings::default();
    let mut converter = LineProtocolConverter::new(settings, writer_source);
    converter
        .convert(only_good_lines)
        .map_err(|e| Error::UnableToWriteGoodLines { source: e })?;
    converter
        .finalize()
        .map_err(|e| Error::UnableToCloseTableWriter { source: e })?;
    info!("Completing writing to {} successfully", output_name);
    Ok(())
}

fn convert_tsm_to_parquet(
    index_stream: InputReader,
    index_stream_size: usize,
    block_stream: InputReader,
    output_name: &str,
) -> Result<()> {
    // setup writing
    let writer_source: Box<dyn DeloreanTableWriterSource> = if is_directory(&output_name) {
        info!("Writing to output directory {:?}", output_name);
        Box::new(ParquetDirectoryWriterSource {
            output_dir_path: PathBuf::from(output_name),
        })
    } else {
        info!("Writing to output file {}", output_name);
        Box::new(ParquetFileWriterSource {
            output_filename: String::from(output_name),
            made_file: false,
        })
    };

    let mut converter = TSMFileConverter::new(writer_source);
    converter
        .convert(index_stream, index_stream_size, block_stream)
        .map_err(|e| Error::UnableToCloseTableWriter { source: e })
}
