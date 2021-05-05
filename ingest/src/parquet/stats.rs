//! Provide storage statistics for parquet files
use observability_deps::tracing::debug;
use packers::{
    stats::{ColumnStatsBuilder, FileStats, FileStatsBuilder},
    Name,
};
use parquet::{
    basic::{Compression, Encoding},
    file::reader::{ChunkReader, FileReader, SerializedFileReader},
};
use snafu::ResultExt;
use std::{collections::BTreeMap, convert::TryInto};

use super::{
    error::{ParquetLibraryError, Result},
    metadata::data_type_from_parquet_type,
};

/// Calculate storage statistics for a particular parquet "file" that can
/// be read from `input`, with a total size of `input_size` byes
///
/// Returns a `FileStats` object representing statistics for all
/// columns across all column chunks.
pub fn file_stats<R: 'static>(input: R) -> Result<FileStats>
where
    R: ChunkReader + Name,
{
    let mut file_stats_builder = FileStatsBuilder::new(&input.name(), input.len());
    let reader = SerializedFileReader::new(input).context(ParquetLibraryError {
        message: "Creating parquet reader",
    })?;

    let mut stats_builders: BTreeMap<String, ColumnStatsBuilder> = BTreeMap::new();

    let parquet_metadata = reader.metadata();
    for (rg_idx, rg_metadata) in parquet_metadata.row_groups().iter().enumerate() {
        debug!(
            "Looking at Row Group [{}] (total uncompressed byte size {})",
            rg_idx,
            rg_metadata.total_byte_size()
        );

        for (cc_idx, cc_metadata) in rg_metadata.columns().iter().enumerate() {
            let col_path = cc_metadata.column_path();
            let builder = stats_builders
                .remove(&col_path.string())
                .unwrap_or_else(|| {
                    let data_type = data_type_from_parquet_type(cc_metadata.column_type());
                    ColumnStatsBuilder::new(col_path.string(), cc_idx, data_type)
                })
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

            // put the builder back in the map
            stats_builders.insert(col_path.string(), builder);
        }
    }

    // now, marshal up all the results
    for (_k, b) in stats_builders {
        file_stats_builder = file_stats_builder.add_column(b.build());
    }

    Ok(file_stats_builder.build())
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
