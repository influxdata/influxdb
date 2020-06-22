//! Structures for computing and reporting on storage statistics

use delorean_table_schema::DataType;
use std::collections::BTreeSet;

/// Represents statistics for data stored in a particular chunk
#[derive(Debug, PartialEq, Eq)]
pub struct ColumnStats {
    pub column_index: usize,
    pub column_name: String,
    pub compression_description: String,
    pub num_rows: u64,
    pub num_compressed_bytes: u64,
    /// "uncompressed" means how large the data is after decompression
    /// (e.g. GZIP) not the raw (decoded) size
    pub num_uncompressed_bytes: u64,
    pub data_type: DataType,
}

/// Represents statistics for data stored in a particular chunk
///
/// # Example:
/// ```
/// use delorean_table_schema::DataType;
/// use delorean_table::stats::ColumnStatsBuilder;
///
/// let stats = ColumnStatsBuilder::new("My Column", 3, DataType::Float)
///    .compression("GZIP")
///    .add_rows(3)
///    .compression("SNAPPY")
///    .add_rows(7)
///    .build();
///
/// assert_eq!(stats.compression_description, r#"{"GZIP", "SNAPPY"}"#);
/// assert_eq!(stats.num_rows, 10);
/// ```
#[derive(Debug)]
pub struct ColumnStatsBuilder {
    column_index: usize,
    column_name: String,
    compression_descriptions: BTreeSet<String>,
    num_rows: u64,
    num_compressed_bytes: u64,
    num_uncompressed_bytes: u64,
    data_type: DataType,
}

impl ColumnStatsBuilder {
    pub fn new(
        column_name: impl Into<String>,
        column_index: usize,
        data_type: DataType,
    ) -> ColumnStatsBuilder {
        ColumnStatsBuilder {
            column_name: column_name.into(),
            column_index,
            compression_descriptions: BTreeSet::new(),
            num_rows: 0,
            num_compressed_bytes: 0,
            num_uncompressed_bytes: 0,
            data_type,
        }
    }

    /// Add a compression description to this column. As there may be
    /// several all compression descriptions that apply to any single column,
    /// the builder tracks a list of descriptions.
    pub fn compression(&mut self, compression: &str) -> &mut Self {
        if !self.compression_descriptions.contains(compression) {
            self.compression_descriptions.insert(compression.into());
        }
        self
    }

    /// Adds `row_count` to the running row count
    pub fn add_rows(&mut self, row_count: u64) -> &mut Self {
        self.num_rows += row_count;
        self
    }

    /// Adds `byte_count` to the running compressed byte count
    pub fn add_compressed_bytes(&mut self, byte_count: u64) -> &mut Self {
        self.num_compressed_bytes += byte_count;
        self
    }

    /// Adds `byte_count` to the running uncompressed byte count
    pub fn add_uncompressed_bytes(&mut self, byte_count: u64) -> &mut Self {
        self.num_uncompressed_bytes += byte_count;
        self
    }

    /// Create the resulting ColumnStats
    pub fn build(&self) -> ColumnStats {
        ColumnStats {
            column_name: self.column_name.clone(),
            column_index: self.column_index,
            compression_description: format!("{:?}", self.compression_descriptions),
            num_rows: self.num_rows,
            num_compressed_bytes: self.num_compressed_bytes,
            num_uncompressed_bytes: self.num_uncompressed_bytes,
            data_type: self.data_type,
        }
    }
}

#[cfg(test)]
mod test {
    use super::*;

    impl Default for ColumnStats {
        fn default() -> Self {
            ColumnStats {
                column_index: 0,
                column_name: String::from(""),
                compression_description: String::from("{}"),
                num_rows: 0,
                num_compressed_bytes: 0,
                num_uncompressed_bytes: 0,
                data_type: DataType::Float,
            }
        }
    }

    #[test]
    fn stats_builder_create() {
        let stats = ColumnStatsBuilder::new("My Column", 7, DataType::Integer).build();

        assert_eq!(
            stats,
            ColumnStats {
                column_index: 7,
                column_name: String::from("My Column"),
                data_type: DataType::Integer,
                ..Default::default()
            }
        );
    }

    #[test]
    fn stats_builder_compression() {
        let stats = ColumnStatsBuilder::new("My Column", 3, DataType::Float)
            .compression("GZIP")
            .compression("DEFLATE")
            .compression("GZIP")
            .build();

        assert_eq!(
            stats,
            ColumnStats {
                column_index: 3,
                column_name: String::from("My Column"),
                compression_description: String::from(r#"{"DEFLATE", "GZIP"}"#),
                ..Default::default()
            }
        );
    }

    #[test]
    fn stats_builder_add_rows() {
        let stats = ColumnStatsBuilder::new("My Column", 3, DataType::Float)
            .add_rows(7)
            .add_rows(3)
            .build();

        assert_eq!(
            stats,
            ColumnStats {
                column_index: 3,
                column_name: String::from("My Column"),
                num_rows: 10,
                ..Default::default()
            }
        );
    }

    #[test]
    fn stats_builder_add_compressed_bytes() {
        let stats = ColumnStatsBuilder::new("My Column", 3, DataType::Float)
            .add_compressed_bytes(7)
            .add_compressed_bytes(3)
            .build();

        assert_eq!(
            stats,
            ColumnStats {
                column_index: 3,
                column_name: String::from("My Column"),
                num_compressed_bytes: 10,
                ..Default::default()
            }
        );
    }

    #[test]
    fn stats_builder_add_uncompressed_bytes() {
        let stats = ColumnStatsBuilder::new("My Column", 3, DataType::Float)
            .add_uncompressed_bytes(7)
            .add_uncompressed_bytes(3)
            .build();

        assert_eq!(
            stats,
            ColumnStats {
                column_index: 3,
                column_name: "My Column".into(),
                num_uncompressed_bytes: 10,
                ..Default::default()
            }
        );
    }
}
