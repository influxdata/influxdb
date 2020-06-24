//! Provide storage statistics for parquet files
use std::collections::BTreeMap;
use std::convert::TryInto;
use std::io::{Read, Seek};

use log::debug;
use parquet::basic::{Compression, Encoding};
use parquet::file::reader::{FileReader, SerializedFileReader};

use delorean_table::stats::{ColumnStats, ColumnStatsBuilder};

use crate::{
    error::{Error, Result},
    metadata::data_type_from_parquet_type,
    Length, TryClone,
};

/// Calculate storage statistics for a particular parquet file that can
/// be read from `input`, with a total size of `input_size` byes
///
/// Returns a Vec of ColumnStats, one for each column in the input
pub fn col_stats<R: 'static>(input: R) -> Result<Vec<ColumnStats>>
where
    R: Read + Seek + TryClone + Length,
{
    let reader = SerializedFileReader::new(input).map_err(|e| Error::ParquetLibraryError {
        message: String::from("Creating parquet reader"),
        source: e,
    })?;

    let mut stats_builders = BTreeMap::new();

    let parquet_metadata = reader.metadata();
    for (rg_idx, rg_metadata) in parquet_metadata.row_groups().iter().enumerate() {
        debug!(
            "Looking at Row Group [{}] (total uncompressed byte size {})",
            rg_idx,
            rg_metadata.total_byte_size()
        );

        for (cc_idx, cc_metadata) in rg_metadata.columns().iter().enumerate() {
            let col_path = cc_metadata.column_path();
            let builder = stats_builders.entry(col_path.string()).or_insert_with(|| {
                let data_type = data_type_from_parquet_type(cc_metadata.column_type());
                ColumnStatsBuilder::new(col_path.string(), cc_idx, data_type)
            });

            builder
                .compression(&format!(
                    "Enc: {}, Comp: {}",
                    encoding_display(&cc_metadata.encodings()),
                    compression_display(&cc_metadata.compression()),
                ))
                .add_rows(
                    cc_metadata
                        .num_values()
                        .try_into()
                        .expect("positive number of values"),
                )
                .add_compressed_bytes(
                    cc_metadata
                        .compressed_size()
                        .try_into()
                        .expect("positive compressed size"),
                )
                .add_uncompressed_bytes(
                    cc_metadata
                        .uncompressed_size()
                        .try_into()
                        .expect("positive uncompressed size"),
                );
        }
    }

    // now, marshal up all the results
    let mut v = stats_builders
        .into_iter()
        .map(|(_k, b)| b.build())
        .collect::<Vec<_>>();

    // ensure output is not sorted by column name, but by column index
    v.sort_by_key(|stats| stats.column_index);

    Ok(v)
}

/// Create a more user friendly display of the encodings
fn encoding_display(encodings: &[Encoding]) -> String {
    if encodings.iter().any(|&e| e == Encoding::RLE_DICTIONARY) {
        // parquet represents "dictionary" encoding as [PLAIN,
        // RLE_DICTIONARY, RLE] , which is somewhat confusing -- it means
        // to point out that the dictionary page uses plain encoding,
        // whereas the data page uses RLE encoding.
        "Dictionary".into()
    } else {
        return format!("{:?}", encodings);
    }
}

/// Create a user friendly display of the encodings
fn compression_display(compression: &Compression) -> String {
    return format!("{}", compression);
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_encoding_display() {
        assert_eq!(&encoding_display(&[Encoding::PLAIN]), "[PLAIN]");
        assert_eq!(
            &encoding_display(&[Encoding::PLAIN, Encoding::RLE]),
            "[PLAIN, RLE]"
        );
        assert_eq!(
            &encoding_display(&[Encoding::PLAIN, Encoding::RLE, Encoding::RLE_DICTIONARY]),
            "Dictionary"
        );
        assert_eq!(
            &encoding_display(&[Encoding::DELTA_BYTE_ARRAY, Encoding::RLE_DICTIONARY]),
            "Dictionary"
        );
    }

    #[test]
    fn test_compression_display() {
        assert_eq!(&compression_display(&Compression::GZIP), "GZIP");
    }
}
