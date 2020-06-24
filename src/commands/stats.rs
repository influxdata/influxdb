//! This module contains code to report compression statistics for storage files

use log::info;

use crate::{
    commands::error::{Error, Result},
    commands::input::{FileType, InputReader},
};

use delorean_parquet::stats::col_stats;

/// Print statistics about the file name in input_filename to stdout
pub fn stats(input_filename: &str) -> Result<()> {
    info!("stats starting");

    let input_reader = InputReader::new(input_filename)?;

    let (input_len, col_stats) = match input_reader.file_type() {
        FileType::LineProtocol => {
            return Err(Error::NotImplemented {
                operation_name: String::from("Line protocol storage statistics"),
            });
        }
        FileType::TSM => {
            return Err(Error::NotImplemented {
                operation_name: String::from("TSM storage statistics"),
            })
        }
        FileType::Parquet => {
            let input_len = input_reader.len();
            (
                input_len,
                col_stats(input_reader)
                    .map_err(|e| Error::UnableDumpToParquetMetadata { source: e })?,
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
