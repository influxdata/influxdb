//! Structures for computing and reporting on storage statistics
use std::collections::{BTreeMap, BTreeSet};
use std::fmt;

use arrow::datatypes::DataType;

fn format_size(sz: u64) -> String {
    let mut s = sz.to_string();
    let mut i = s.len();
    loop {
        i = i.saturating_sub(3);
        if i == 0 {
            break;
        }
        s.insert(i, ',')
    }
    s
}

/// Represents statistics for data stored in a particular chunk
#[derive(Debug, PartialEq, Eq, Clone)]
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

impl ColumnStats {
    /// Accumulates the statistics of other into this column stats
    pub fn merge(&mut self, other: &Self) {
        // ignore column_index (some files can have different numbers of columns)
        assert_eq!(self.column_name, other.column_name, "expected same column");
        if self.compression_description != other.compression_description {
            self.compression_description = String::from("MIXED");
        }
        self.num_rows += other.num_rows;
        self.num_compressed_bytes += other.num_compressed_bytes;
        self.num_uncompressed_bytes += other.num_uncompressed_bytes;
        assert_eq!(self.data_type, other.data_type, "expected same datatype");
    }
}

impl fmt::Display for ColumnStats {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        writeln!(
            f,
            "Column Stats '{}' [{}]",
            self.column_name, self.column_index
        )?;
        writeln!(
            f,
            "  Total rows: {}, DataType: {:?}, Compression: {}",
            format_size(self.num_rows),
            self.data_type,
            self.compression_description
        )?;
        writeln!(
            f,
            "  Compressed/Uncompressed Bytes: {} / {}",
            format_size(self.num_compressed_bytes),
            format_size(self.num_uncompressed_bytes),
        )?;
        write!(
            f,
            "  Bits per row: {:.4}",
            8.0 * (self.num_compressed_bytes as f64) / (self.num_rows as f64)
        )
    }
}

/// Represents statistics for data stored in a particular chunk
///
/// # Example:
/// ```
/// use arrow::datatypes::DataType;
/// use packers::stats::ColumnStatsBuilder;
///
/// let stats = ColumnStatsBuilder::new("My Column", 3, DataType::Float64)
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
    pub fn new(column_name: impl Into<String>, column_index: usize, data_type: DataType) -> Self {
        Self {
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
    pub fn compression(mut self, compression: &str) -> Self {
        if !self.compression_descriptions.contains(compression) {
            self.compression_descriptions.insert(compression.into());
        }
        self
    }

    /// Adds `row_count` to the running row count
    pub fn add_rows(mut self, row_count: u64) -> Self {
        self.num_rows += row_count;
        self
    }

    /// Adds `byte_count` to the running compressed byte count
    pub fn add_compressed_bytes(mut self, byte_count: u64) -> Self {
        self.num_compressed_bytes += byte_count;
        self
    }

    /// Adds `byte_count` to the running uncompressed byte count
    pub fn add_uncompressed_bytes(mut self, byte_count: u64) -> Self {
        self.num_uncompressed_bytes += byte_count;
        self
    }

    /// Create the resulting ColumnStats
    pub fn build(self) -> ColumnStats {
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

/// Represents File level statistics
#[derive(Debug, PartialEq, Eq)]
pub struct FileStats {
    /// Name of the input file
    pub file_name: String,
    /// size of input file, in bytes
    pub input_len: u64,
    /// total number of rows in the input file
    pub total_rows: u64,
    /// Column by column statistics
    pub col_stats: Vec<ColumnStats>,
}

impl fmt::Display for FileStats {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "{}: total columns {}, rows: {}, size: {}, bits per row: {:.4}",
            self.file_name,
            format_size(self.col_stats.len() as u64),
            format_size(self.total_rows),
            format_size(self.input_len),
            8.0 * (self.input_len as f64) / (self.total_rows as f64)
        )
    }
}

#[derive(Debug, PartialEq, Eq)]
pub struct FileStatsBuilder {
    inner: FileStats,
}

impl FileStatsBuilder {
    pub fn new(file_name: &str, input_len: u64) -> Self {
        Self {
            inner: FileStats {
                file_name: file_name.into(),
                input_len,
                total_rows: 0,
                col_stats: Vec::new(),
            },
        }
    }

    pub fn build(mut self) -> FileStats {
        // ensure output is not sorted by column name, but by column index
        self.inner.col_stats.sort_by_key(|stats| stats.column_index);
        self.inner
    }

    pub fn add_column(mut self, c: ColumnStats) -> Self {
        if self.inner.total_rows == 0 {
            self.inner.total_rows = c.num_rows;
        }

        assert_eq!(
            self.inner.total_rows, c.num_rows,
            "Internal error: columns had different numbers of rows {}, {}",
            self.inner.total_rows, c.num_rows
        );

        self.inner.col_stats.push(c);
        self
    }
}
/// Represents statistics for a set of files
#[derive(Debug, PartialEq, Eq, Default)]
pub struct FileSetStats {
    /// total size of input file, in bytes
    pub total_len: u64,
    /// total number of rows in the input files
    pub total_rows: u64,
    /// maximum columns in any file
    pub max_columns: u64,
    // Column by column statistics
    pub col_stats: Vec<ColumnStats>,
}

impl fmt::Display for FileSetStats {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "ALL: total columns: {}, rows: {}, size: {}, bits per row: {:.4}",
            format_size(self.col_stats.len() as u64),
            format_size(self.total_rows),
            format_size(self.total_len),
            8.0 * (self.total_len as f64) / (self.total_rows as f64)
        )
    }
}
#[derive(Debug, PartialEq, Eq, Default)]
pub struct FileSetStatsBuilder {
    inner: FileSetStats,

    // key: column name
    // value: index into inner.col_stats
    col_stats_map: BTreeMap<String, usize>,
}

impl FileSetStatsBuilder {
    /// Add a file's statstics
    pub fn accumulate(mut self, file_stats: &FileStats) -> Self {
        self.inner.total_len += file_stats.input_len;
        self.inner.total_rows += file_stats.total_rows;
        self.inner.max_columns =
            std::cmp::max(self.inner.max_columns, file_stats.col_stats.len() as u64);

        for c in &file_stats.col_stats {
            match self.col_stats_map.get(&c.column_name) {
                Some(index) => {
                    self.inner.col_stats[*index].merge(c);
                }
                None => {
                    self.inner.col_stats.push(c.clone());
                    let index = self.inner.col_stats.len() - 1;
                    self.col_stats_map.insert(c.column_name.clone(), index);
                }
            }
        }
        self
    }

    pub fn build(self) -> FileSetStats {
        self.inner
    }
}

#[cfg(test)]
mod test {
    use super::*;

    impl Default for ColumnStats {
        fn default() -> Self {
            Self {
                column_index: 0,
                column_name: String::from(""),
                compression_description: String::from("{}"),
                num_rows: 0,
                num_compressed_bytes: 0,
                num_uncompressed_bytes: 0,
                data_type: DataType::Float64,
            }
        }
    }

    #[test]
    fn test_format_size() {
        assert_eq!(format_size(0).as_str(), "0");
        assert_eq!(format_size(1).as_str(), "1");
        assert_eq!(format_size(11).as_str(), "11");
        assert_eq!(format_size(154).as_str(), "154");
        assert_eq!(format_size(1332).as_str(), "1,332");
        assert_eq!(format_size(45224).as_str(), "45,224");
        assert_eq!(format_size(123456789).as_str(), "123,456,789");
    }

    #[test]
    fn col_stats_builder_create() {
        let stats = ColumnStatsBuilder::new("My Column", 7, DataType::Int64).build();

        assert_eq!(
            stats,
            ColumnStats {
                column_index: 7,
                column_name: String::from("My Column"),
                data_type: DataType::Int64,
                ..Default::default()
            }
        );
    }

    #[test]
    fn col_stats_builder_compression() {
        let stats = ColumnStatsBuilder::new("My Column", 3, DataType::Float64)
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
    fn col_stats_builder_add_rows() {
        let stats = ColumnStatsBuilder::new("My Column", 3, DataType::Float64)
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
    fn col_stats_builder_add_compressed_bytes() {
        let stats = ColumnStatsBuilder::new("My Column", 3, DataType::Float64)
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
    fn col_stats_builder_add_uncompressed_bytes() {
        let stats = ColumnStatsBuilder::new("My Column", 3, DataType::Float64)
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

    #[test]
    fn col_stats_display() {
        let stats = ColumnStats {
            column_index: 11,
            column_name: String::from("The Column"),
            compression_description: String::from("Maximum"),
            num_rows: 123456789,
            num_compressed_bytes: 1234122,
            num_uncompressed_bytes: 5588833,
            data_type: DataType::Float64,
        };
        assert_eq!(
            stats.to_string(),
            r#"Column Stats 'The Column' [11]
  Total rows: 123,456,789, DataType: Float64, Compression: Maximum
  Compressed/Uncompressed Bytes: 1,234,122 / 5,588,833
  Bits per row: 0.0800"#
        );
    }

    #[test]
    fn col_stats_merge() {
        let mut stats1 = ColumnStats {
            column_index: 11,
            column_name: String::from("The Column"),
            compression_description: String::from("Maximum"),
            num_rows: 123456789,
            num_compressed_bytes: 1234122,
            num_uncompressed_bytes: 5588833,
            data_type: DataType::Float64,
        };

        let stats2 = ColumnStats {
            column_index: 11,
            column_name: String::from("The Column"),
            compression_description: String::from("Maximum"),
            num_rows: 1,
            num_compressed_bytes: 2,
            num_uncompressed_bytes: 3,
            data_type: DataType::Float64,
        };

        stats1.merge(&stats2);
        assert_eq!(
            stats1,
            ColumnStats {
                column_index: 11,
                column_name: String::from("The Column"),
                compression_description: String::from("Maximum"),
                num_rows: 123456790,
                num_compressed_bytes: 1234124,
                num_uncompressed_bytes: 5588836,
                data_type: DataType::Float64,
            }
        );

        // now, merge in stats with different compression description
        let stats3 = ColumnStats {
            column_name: String::from("The Column"),
            compression_description: String::from("Minimum"),
            ..Default::default()
        };

        stats1.merge(&stats3);
        assert_eq!(stats1.compression_description, "MIXED");
    }

    #[test]
    fn file_stats_builder() {
        let file_stats = FileStatsBuilder::new("the_filename", 1337)
            .add_column(ColumnStats {
                column_index: 11,
                column_name: String::from("The Column"),
                compression_description: String::from("Maximum"),
                num_rows: 2,
                num_compressed_bytes: 3,
                num_uncompressed_bytes: 4,
                data_type: DataType::Float64,
            })
            .add_column(ColumnStats {
                column_index: 11,
                column_name: String::from("The Second Column"),
                compression_description: String::from("Minimum"),
                num_rows: 2,
                num_compressed_bytes: 30,
                num_uncompressed_bytes: 40,
                data_type: DataType::Float64,
            })
            .build();

        assert_eq!(file_stats.file_name, "the_filename");
        assert_eq!(file_stats.input_len, 1337);
        assert_eq!(file_stats.total_rows, 2);
        assert_eq!(file_stats.col_stats[0].column_name, "The Column");
        assert_eq!(file_stats.col_stats[1].column_name, "The Second Column");
    }

    #[test]
    fn file_stats_display() {
        let file_stats = FileStats {
            file_name: "the_filename".into(),
            input_len: 1337,
            total_rows: 11,
            col_stats: vec![ColumnStats::default()],
        };
        assert_eq!(
            file_stats.to_string(),
            "the_filename: total columns 1, rows: 11, size: 1,337, bits per row: 972.3636"
        );
    }

    #[test]
    fn file_set_stats_build() {
        let file_set_stats = FileSetStatsBuilder::default()
            .accumulate(&FileStats {
                file_name: "the_filename".into(),
                input_len: 1337,
                total_rows: 11,
                col_stats: vec![ColumnStats {
                    // Accumulate just enough to demonstrate that `merge` is being called
                    num_rows: 1,
                    ..ColumnStats::default()
                }],
            })
            .accumulate(&FileStats {
                file_name: "the_other_filename".into(),
                input_len: 555,
                total_rows: 22,
                col_stats: vec![
                    ColumnStats {
                        num_rows: 11,
                        ..ColumnStats::default()
                    },
                    ColumnStats {
                        column_name: "Another".into(),
                        num_rows: 13,
                        ..ColumnStats::default()
                    },
                ],
            })
            .build();

        assert_eq!(
            file_set_stats,
            FileSetStats {
                total_len: 1892,
                total_rows: 33,
                max_columns: 2,
                col_stats: vec![
                    ColumnStats {
                        column_index: 0,
                        column_name: "".into(),
                        compression_description: "{}".into(),
                        num_rows: 12,
                        num_compressed_bytes: 0,
                        num_uncompressed_bytes: 0,
                        data_type: DataType::Float64,
                    },
                    ColumnStats {
                        column_index: 0,
                        column_name: "Another".into(),
                        compression_description: "{}".into(),
                        num_rows: 13,
                        num_compressed_bytes: 0,
                        num_uncompressed_bytes: 0,
                        data_type: DataType::Float64,
                    },
                ],
            }
        );
    }

    #[test]
    fn file_set_stats_display() {
        let file_set_stats = FileSetStats {
            total_len: 1892,
            total_rows: 33,
            max_columns: 2,
            col_stats: vec![ColumnStats::default(), ColumnStats::default()],
        };

        assert_eq!(
            file_set_stats.to_string(),
            "ALL: total columns: 2, rows: 33, size: 1,892, bits per row: 458.6667"
        );
    }
}
