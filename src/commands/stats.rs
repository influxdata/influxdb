//! This module contains code to report compression statistics for storage files

use delorean_parquet::{error::Error as DeloreanParquetError, stats::col_stats};
use log::info;
use snafu::{ResultExt, Snafu};

use crate::commands::input::{FileType, InputReader};

pub type Result<T, E = Error> = std::result::Result<T, E>;

/// Print statistics about the file name in input_filename to stdout
pub fn stats(input_filename: &str) -> Result<()> {
    info!("stats starting");

    let input_reader = InputReader::new(input_filename).context(OpenInput)?;

    let (input_len, col_stats) = match input_reader.file_type() {
        FileType::LineProtocol => {
            return NotImplemented {
                operation_name: "Line protocol storage statistics",
            }
            .fail()
        }
        FileType::TSM => {
            return NotImplemented {
                operation_name: "TSM storage statistics",
            }
            .fail()
        }
        FileType::Parquet => {
            let input_len = input_reader.len();
            (
                input_len,
                col_stats(input_reader).context(UnableDumpToParquetMetadata)?,
            )
        }
    };

    let mut total_rows = 0;

    println!("Storage statistics:");
    let num_cols = col_stats.len();
    for c in col_stats {
        println!("Column Stats '{}' [{}]", c.column_name, c.column_index);
        println!(
            "  Total rows: {}, DataType: {:?}, Compression: {}",
            c.num_rows, c.data_type, c.compression_description
        );
        println!(
            "  Compressed/Uncompressed Bytes: ({:8}/{:8}) {:.4} bits per row",
            c.num_compressed_bytes,
            c.num_uncompressed_bytes,
            8.0 * (c.num_compressed_bytes as f64) / (c.num_rows as f64)
        );

        if total_rows == 0 {
            total_rows = c.num_rows;
        }

        assert_eq!(
            total_rows, c.num_rows,
            "Internal error: columns had different numbers of rows {}, {}",
            total_rows, c.num_rows
        );
    }

    println!();
    println!(
        "{}: total columns/rows/bytes: ({:8}/{:8}/{:8}) {:.4} bits per row",
        input_filename,
        num_cols,
        total_rows,
        input_len,
        8.0 * (input_len as f64) / (total_rows as f64)
    );

    Ok(())
}

#[derive(Debug, Snafu)]
pub enum Error {
    #[snafu(display("Not implemented: {}", operation_name))]
    NotImplemented { operation_name: String },

    #[snafu(display("Error opening input {}", source))]
    OpenInput { source: super::input::Error },

    #[snafu(display("Unable to dump parquet file metadata: {}", source))]
    UnableDumpToParquetMetadata { source: DeloreanParquetError },
}
