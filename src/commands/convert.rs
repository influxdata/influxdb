use log::{debug, info, warn};
use std::fs;

use delorean_ingest::LineProtocolConverter;
use delorean_line_parser::{parse_lines, ParsedLine};
use delorean_parquet::writer::DeloreanParquetTableWriter;
use delorean_table::{DeloreanTableWriter, DeloreanTableWriterSource, Error as TableError};
use delorean_table_schema::Schema;

static SCHEMA_SAMPLE_SIZE: usize = 5;

use crate::commands::error::{Error, Result};

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

pub fn convert(input_filename: &str, output_filename: &str) -> Result<()> {
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
