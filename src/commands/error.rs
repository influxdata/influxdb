use snafu::Snafu;

use delorean::storage::StorageError;
use delorean_ingest::Error as IngestError;
use delorean_parquet::writer::Error as ParquetWriterError;

#[derive(Debug, Snafu)]
pub enum Error {
    #[snafu(display("Error reading {} ({})", name, source))]
    UnableToReadInput {
        name: String,
        source: std::io::Error,
    },

    #[snafu(display("Unable to create output file {} ({})", name, source))]
    UnableToCreateFile {
        name: String,
        source: std::io::Error,
    },

    #[snafu(display("Not implemented: {}", operation_name))]
    NotImplemented { operation_name: String },

    #[snafu(display("Unknown input type: {} for {}", details, input_name))]
    UnknownInputType { details: String, input_name: String },

    #[snafu(display("Can't convert filename to utf-8, : {}", input_name))]
    FileNameDecode { input_name: String },

    #[snafu(display("Can't read gzip data : {}", input_name))]
    ReadingGzip {
        input_name: String,
        source: std::io::Error,
    },

    #[snafu(context(false))]
    #[snafu(display("Error converting data {}", source))]
    Conversion { source: delorean_ingest::Error },

    #[snafu(display("Error creating a parquet table writer {}", source))]
    UnableToCreateParquetTableWriter { source: ParquetWriterError },

    #[snafu(display("Error creating a line protocol converter {}", source))]
    UnableToCreateLineProtocolConverter { source: IngestError },

    #[snafu(display("Error writing the sample schema {}", source))]
    UnableToWriteSchemaSample { source: IngestError },

    #[snafu(display("Error writing remaining lines {}", source))]
    UnableToWriteGoodLines { source: IngestError },

    #[snafu(display("Error while closing the table writer {}", source))]
    UnableToCloseTableWriter { source: IngestError },

    #[snafu(display(r#"Error reading TSM data: {}"#, source))]
    TSM { source: StorageError },

    #[snafu(display(r#"Error parsing data: {}"#, source))]
    Parsing { source: delorean_line_parser::Error },
}

pub type Result<T, E = Error> = std::result::Result<T, E>;
