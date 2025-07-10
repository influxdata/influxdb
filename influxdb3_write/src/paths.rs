use chrono::prelude::*;
use influxdb3_catalog::catalog::CatalogSequenceNumber;
use influxdb3_id::{DbId, TableId, TableIndexId};
use influxdb3_wal::{SnapshotSequenceNumber, WalFileSequenceNumber};
use object_store::path::Path as ObjPath;
use regex::Regex;
use std::ops::Deref;
use std::str::FromStr;
use std::sync::LazyLock;
use thiserror::Error;

/// Errors that can occur when parsing paths
#[derive(Debug, Error)]
pub enum PathError {
    #[error("Invalid path format: expected {expected}, got: {actual}")]
    InvalidFormat { expected: String, actual: String },

    #[error("Invalid database ID: {0}")]
    InvalidDbId(String),

    #[error("Invalid table ID: {0}")]
    InvalidTableId(String),

    #[error("Invalid sequence number in {context}: {filename}")]
    InvalidSequenceNumber { context: String, filename: String },
}

/// File extension for catalog files
pub const CATALOG_LOG_FILE_EXTENSION: &str = "catalog";

/// File extension for catalog files
pub const CATALOG_SNAPSHOT_FILE_EXTENSION: &str = "catalog.snapshot";

/// File extension for parquet files
pub const PARQUET_FILE_EXTENSION: &str = "parquet";

/// File extension for snapshot info files
pub const SNAPSHOT_INFO_FILE_EXTENSION: &str = "info.json";

fn object_store_file_stem(n: u64) -> u64 {
    u64::MAX - n
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CatalogFilePath(ObjPath);

impl CatalogFilePath {
    pub fn log(host_prefix: &str, catalog_sequence_number: CatalogSequenceNumber) -> Self {
        let num = u64::MAX - catalog_sequence_number.get();
        let path = ObjPath::from(format!(
            "{host_prefix}/catalogs/{num:020}.{CATALOG_LOG_FILE_EXTENSION}",
        ));
        Self(path)
    }

    pub fn snapshot(host_prefix: &str, catalog_sequence_number: CatalogSequenceNumber) -> Self {
        let num = u64::MAX - catalog_sequence_number.get();
        let path = ObjPath::from(format!(
            "{host_prefix}/catalogs/{num:020}.{CATALOG_SNAPSHOT_FILE_EXTENSION}",
        ));
        Self(path)
    }

    pub fn dir(host_prefix: &str) -> Self {
        Self(ObjPath::from(format!("{host_prefix}/catalogs")))
    }
}

impl Deref for CatalogFilePath {
    type Target = ObjPath;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl AsRef<ObjPath> for CatalogFilePath {
    fn as_ref(&self) -> &ObjPath {
        &self.0
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ParquetFilePath(ObjPath);

impl ParquetFilePath {
    /// Generate a parquet file path using the given arguments. This will convert the provided
    /// `chunk_time` into a date time string with format `'YYYY-MM-DD/HH-MM'`
    pub fn new(
        host_prefix: &str,
        db_name: &str,
        db_id: u32,
        table_name: &str,
        table_id: u32,
        chunk_time: i64,
        wal_file_sequence_number: WalFileSequenceNumber,
    ) -> Self {
        let date_time = DateTime::<Utc>::from_timestamp_nanos(chunk_time);
        let path = ObjPath::from(format!(
            "{host_prefix}/dbs/{db_name}-{db_id}/{table_name}-{table_id}/{date_string}/{wal_seq:010}.{ext}",
            date_string = date_time.format("%Y-%m-%d/%H-%M"),
            wal_seq = wal_file_sequence_number.as_u64(),
            ext = PARQUET_FILE_EXTENSION
        ));
        Self(path)
    }
}

impl Deref for ParquetFilePath {
    type Target = ObjPath;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl AsRef<ObjPath> for ParquetFilePath {
    fn as_ref(&self) -> &ObjPath {
        &self.0
    }
}

/// This path, when it exists in the object store, signifies that a full conversion from
/// PersistedSnapshots to TableIndices has been completed at least once, allowing us to skip the
/// memory-intensive operation on subsequent startups.
///
/// The contents of the path should be a simple JSON data structure containing the last snapshot
/// sequence number at the time of conversion.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TableIndexConversionCompletedPath(ObjPath);

impl TableIndexConversionCompletedPath {
    pub fn new(host_prefix: &str) -> Self {
        let path = ObjPath::from(format!("{host_prefix}/table-index-conversion-completed",));
        Self(path)
    }
}

impl AsRef<ObjPath> for TableIndexConversionCompletedPath {
    fn as_ref(&self) -> &ObjPath {
        &self.0
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TableIndexSnapshotPath(ObjPath);

static TABLE_INDEX_SNAPSHOT_PATH_REGEX: LazyLock<Regex> = LazyLock::new(|| {
    Regex::new(r"^.*/table-snapshots/(\d+)/(\d+)/(\d{20})\.info\.json$")
        .expect("regex must be valid")
});

impl TableIndexSnapshotPath {
    pub fn new(
        host_prefix: &str,
        db_id: u32,
        table_id: u32,
        snapshot_sequence_number: SnapshotSequenceNumber,
    ) -> Self {
        let path = ObjPath::from(format!(
            "{host_prefix}/table-snapshots/{db_id}/{table_id}/{seq:020}.{ext}",
            seq = snapshot_sequence_number.as_u64(),
            ext = SNAPSHOT_INFO_FILE_EXTENSION,
        ));
        Self(path)
    }

    pub fn from_path(path: ObjPath) -> Result<Self, PathError> {
        Self::try_from(path)
    }

    pub fn dir(host_prefix: &str) -> Self {
        Self(ObjPath::from(format!("{host_prefix}/table-snapshots",)))
    }

    pub fn prefix(host_prefix: &str, db_id: u32, table_id: u32) -> ObjPath {
        ObjPath::from(format!("{host_prefix}/table-snapshots/{db_id}/{table_id}/"))
    }

    pub fn all_snapshots_prefix(host_prefix: &str) -> ObjPath {
        ObjPath::from(format!("{host_prefix}/table-snapshots/"))
    }

    pub fn parse_sequence_number(filename: &str) -> Option<SnapshotSequenceNumber> {
        // Extract the filename from the path if it's a full path
        let filename = filename.split('/').next_back().unwrap_or(filename);

        filename
            .strip_suffix(".info.json")
            .and_then(|seq_str| seq_str.parse::<u64>().ok())
            .map(SnapshotSequenceNumber::new)
    }

    /// Extract the full table ID (node_id, db_id, table_id) from the path.
    ///
    /// This method will panic if called on a path not created by `TableIndexPath::new`,
    /// but this should never happen in practice since we control path construction.
    pub fn full_table_id(&self) -> TableIndexId {
        let mut parts = self.0.parts();

        let node_id = parts
            .next()
            .expect("constructors ensure there is a valid node id")
            .as_ref()
            .to_string();

        // skip static part of path
        let _ = parts.next();

        let db_id = parts
            .next()
            .and_then(|p| DbId::from_str(p.as_ref()).ok())
            .expect("constructors ensure there is a valid db id");
        let table_id = parts
            .next()
            .and_then(|p| TableId::from_str(p.as_ref()).ok())
            .expect("constructors ensure there is a valid table id");

        TableIndexId::new(node_id, db_id, table_id)
    }
}

impl Deref for TableIndexSnapshotPath {
    type Target = ObjPath;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl AsRef<ObjPath> for TableIndexSnapshotPath {
    fn as_ref(&self) -> &ObjPath {
        &self.0
    }
}

impl TryFrom<ObjPath> for TableIndexSnapshotPath {
    type Error = PathError;

    fn try_from(path: ObjPath) -> Result<Self, Self::Error> {
        // Clone the regex to avoid thread contention
        let re = TABLE_INDEX_SNAPSHOT_PATH_REGEX.clone();

        let path_str = path.as_ref();

        let caps = re
            .captures(path_str)
            .ok_or_else(|| PathError::InvalidFormat {
                expected: "*/table-snapshots/<db_id>/<table_id>/<20-digits>.info.json".to_string(),
                actual: path_str.to_string(),
            })?;

        // Validate db_id
        caps.get(1)
            .and_then(|m| m.as_str().parse::<u32>().ok())
            .ok_or_else(|| PathError::InvalidDbId(path_str.to_string()))?;

        // Validate table_id
        caps.get(2)
            .and_then(|m| m.as_str().parse::<u32>().ok())
            .ok_or_else(|| PathError::InvalidTableId(path_str.to_string()))?;

        // Validate sequence number
        if let Some(filename) = path_str.split('/').next_back() {
            if TableIndexSnapshotPath::parse_sequence_number(filename).is_none() {
                return Err(PathError::InvalidSequenceNumber {
                    context: "table snapshot".to_string(),
                    filename: filename.to_string(),
                });
            }
        }

        Ok(Self(path))
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TableIndexPath(ObjPath);

static TABLE_INDEX_PATH_REGEX: LazyLock<Regex> = LazyLock::new(|| {
    Regex::new(r"^.*/db-indices/(\d+)/(\d+)/index\.info\.json$").expect("regex must be valid")
});

impl TableIndexPath {
    pub fn new(host_prefix: &str, db_id: u32, table_id: u32) -> Self {
        let path = ObjPath::from(format!(
            "{host_prefix}/db-indices/{db_id}/{table_id}/index.{SNAPSHOT_INFO_FILE_EXTENSION}",
        ));
        Self(path)
    }

    pub fn db_prefix(host_prefix: &str, db_id: DbId) -> ObjPath {
        ObjPath::from(format!("{host_prefix}/db-indices/{db_id}",))
    }

    pub fn all_db_indices_prefix(host_prefix: &str) -> ObjPath {
        ObjPath::from(format!("{host_prefix}/db-indices"))
    }

    /// Extract the full table ID (node_id, db_id, table_id) from the path.
    ///
    /// This method will panic if called on a path not created by `TableIndexPath::new`,
    /// but this should never happen in practice since we control path construction.
    pub fn full_table_id(&self) -> TableIndexId {
        let mut parts = self.0.parts();

        let node_id = parts
            .next()
            .expect("constructors ensure there is a valid node id")
            .as_ref()
            .to_string();

        // skip static part of path
        let _ = parts.next();

        let db_id = parts
            .next()
            .and_then(|p| DbId::from_str(p.as_ref()).ok())
            .expect("constructors ensure there is a valid db id");
        let table_id = parts
            .next()
            .and_then(|p| TableId::from_str(p.as_ref()).ok())
            .expect("constructors ensure there is a valid table id");

        TableIndexId::new(node_id, db_id, table_id)
    }

    /// Create a TableIndexPath from an object store path, validating its format.
    /// Returns an error if the path doesn't match the expected pattern.
    pub fn from_path(path: ObjPath) -> Result<Self, PathError> {
        Self::try_from(path)
    }
}

impl Deref for TableIndexPath {
    type Target = ObjPath;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl AsRef<ObjPath> for TableIndexPath {
    fn as_ref(&self) -> &ObjPath {
        &self.0
    }
}

impl TryFrom<ObjPath> for TableIndexPath {
    type Error = PathError;

    fn try_from(path: ObjPath) -> Result<Self, Self::Error> {
        // Clone the regex to avoid thread contention
        let re = TABLE_INDEX_PATH_REGEX.clone();

        let path_str = path.as_ref();

        let caps = re
            .captures(path_str)
            .ok_or_else(|| PathError::InvalidFormat {
                expected: "*/db-indices/<db_id>/<table_id>/index.info.json".to_string(),
                actual: path_str.to_string(),
            })?;

        // Validate db_id
        caps.get(1)
            .and_then(|m| m.as_str().parse::<u32>().ok())
            .ok_or_else(|| PathError::InvalidDbId(path_str.to_string()))?;

        // Validate table_id
        caps.get(2)
            .and_then(|m| m.as_str().parse::<u32>().ok())
            .ok_or_else(|| PathError::InvalidTableId(path_str.to_string()))?;

        Ok(Self(path))
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SnapshotInfoFilePath(ObjPath);

static SNAPSHOT_INFO_FILE_PATH_REGEX: LazyLock<Regex> = LazyLock::new(|| {
    Regex::new(r"^.*/snapshots/(\d{20})\.info\.json$").expect("regex must be valid")
});

impl SnapshotInfoFilePath {
    pub fn new(host_prefix: &str, snapshot_sequence_number: SnapshotSequenceNumber) -> Self {
        let path = ObjPath::from(format!(
            "{host_prefix}/snapshots/{:020}.{}",
            object_store_file_stem(snapshot_sequence_number.as_u64()),
            SNAPSHOT_INFO_FILE_EXTENSION
        ));
        Self(path)
    }

    pub fn from_path(path: ObjPath) -> Result<Self, PathError> {
        Self::try_from(path)
    }

    pub fn dir(host_prefix: &str) -> Self {
        Self(ObjPath::from(format!("{host_prefix}/snapshots")))
    }

    pub fn obj_path(&self) -> ObjPath {
        self.0.clone()
    }

    pub fn parse_sequence_number(filename: &str) -> Option<SnapshotSequenceNumber> {
        // Extract the filename from the path if it's a full path
        let filename = filename.split('/').next_back().unwrap_or(filename);

        filename
            .strip_suffix(".info.json")
            .and_then(|seq_str| seq_str.parse::<u64>().ok())
            .map(|inverted| u64::MAX - inverted) // Undo the object_store_file_stem inversion
            .map(SnapshotSequenceNumber::new)
    }
}

impl Deref for SnapshotInfoFilePath {
    type Target = ObjPath;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl AsRef<ObjPath> for SnapshotInfoFilePath {
    fn as_ref(&self) -> &ObjPath {
        &self.0
    }
}

impl TryFrom<ObjPath> for SnapshotInfoFilePath {
    type Error = PathError;

    fn try_from(path: ObjPath) -> Result<Self, Self::Error> {
        // Clone the regex to avoid thread contention
        let re = SNAPSHOT_INFO_FILE_PATH_REGEX.clone();

        let path_str = path.as_ref();

        if !re.is_match(path_str) {
            return Err(PathError::InvalidFormat {
                expected: "*/snapshots/<20-digits>.info.json".to_string(),
                actual: path_str.to_string(),
            });
        }

        // Additional validation: ensure we can parse the sequence number
        if let Some(filename) = path_str.split('/').next_back() {
            if SnapshotInfoFilePath::parse_sequence_number(filename).is_none() {
                return Err(PathError::InvalidSequenceNumber {
                    context: "snapshot".to_string(),
                    filename: filename.to_string(),
                });
            }
        }

        Ok(Self(path))
    }
}

#[test]
fn catalog_file_path_new() {
    assert_eq!(
        *CatalogFilePath::log("my_host", CatalogSequenceNumber::new(0)),
        ObjPath::from("my_host/catalogs/18446744073709551615.catalog")
    );
}

#[test]
fn parquet_file_path_new() {
    assert_eq!(
        *ParquetFilePath::new(
            "my_host",
            "my_db",
            0,
            "my_table",
            0,
            Utc.with_ymd_and_hms(2038, 1, 19, 3, 14, 7)
                .unwrap()
                .timestamp_nanos_opt()
                .unwrap(),
            WalFileSequenceNumber::new(1337),
        ),
        ObjPath::from("my_host/dbs/my_db-0/my_table-0/2038-01-19/03-14/0000001337.parquet")
    );
}

#[test]
fn parquet_file_percent_encoded() {
    assert_eq!(
        ParquetFilePath::new(
            "..",
            "..",
            0,
            "..",
            0,
            Utc.with_ymd_and_hms(2038, 1, 19, 3, 14, 7)
                .unwrap()
                .timestamp_nanos_opt()
                .unwrap(),
            WalFileSequenceNumber::new(100),
        )
        .as_ref()
        .as_ref(),
        "%2E%2E/dbs/..-0/..-0/2038-01-19/03-14/0000000100.parquet"
    );
}

#[test]
fn snapshot_info_file_path_new() {
    assert_eq!(
        *SnapshotInfoFilePath::new("my_host", SnapshotSequenceNumber::new(0)),
        ObjPath::from("my_host/snapshots/18446744073709551615.info.json")
    );
}

#[cfg(test)]
mod test {
    use super::*;
    use rstest::rstest;

    #[test]
    fn validate_table_index_snapshot_path_regex() {
        struct Case {
            path: ObjPath,
            expected: TableIndexId,
        }

        let path_fn = |host_id: &str, d: u32, t: u32, seq: u64| -> ObjPath {
            format!("{host_id}/table-snapshots/{d}/{t}/{seq:020}.info.json").into()
        };

        let cases = vec![
            Case {
                path: path_fn("host-id", 0, 0, 0),
                expected: TableIndexId::new("host-id", 0.into(), 0.into()),
            },
            Case {
                path: path_fn("host-id", u32::MAX, u32::MAX, u64::MAX),
                expected: TableIndexId::new("host-id", u32::MAX.into(), u32::MAX.into()),
            },
        ];

        for case in cases {
            let actual = TableIndexSnapshotPath::try_from(case.path.clone())
                .expect("always succeed constructing from valid path");
            let actual = actual.full_table_id();
            assert_eq!(case.expected, actual, "failed to parse {}", case.path);
        }
    }

    #[test]
    fn validate_table_index_snapshot_sequence_number_extraction() {
        // Round-trip validation tests
        let test_cases = vec![
            (123u64, "host-prefix"),
            (42u64, "prefix"),
            (0u64, "test-host"),
            (u64::MAX, "another-host"),
        ];

        for (seq_num, host_prefix) in test_cases {
            let snapshot_seq = SnapshotSequenceNumber::new(seq_num);
            let db_id = 10;
            let table_id = 20;

            // Construct a new TableIndexSnapshotPath
            let path = TableIndexSnapshotPath::new(host_prefix, db_id, table_id, snapshot_seq);

            // Extract the inner object store path and convert to string
            let path_str = path.as_ref().to_string();

            // Validate the full path structure
            let expected_path = format!(
                "{host_prefix}/table-snapshots/{db_id}/{table_id}/{seq_num:020}.info.json",
            );
            assert_eq!(
                path_str, expected_path,
                "Path structure is incorrect for sequence number {seq_num}",
            );

            // Parse the sequence number from the filename
            let parsed = TableIndexSnapshotPath::parse_sequence_number(&path_str);

            // Verify round-trip
            assert_eq!(
                parsed,
                Some(snapshot_seq),
                "Round-trip failed for sequence number {seq_num} with path {path_str}",
            );
        }

        // Direct parsing tests for valid cases
        assert_eq!(
            TableIndexSnapshotPath::parse_sequence_number("0000000123.info.json"),
            Some(SnapshotSequenceNumber::new(123))
        );
        assert_eq!(
            TableIndexSnapshotPath::parse_sequence_number("0000000000.info.json"),
            Some(SnapshotSequenceNumber::new(0))
        );
        assert_eq!(
            TableIndexSnapshotPath::parse_sequence_number("9999999999.info.json"),
            Some(SnapshotSequenceNumber::new(9999999999))
        );

        // Invalid cases - missing .info.json suffix
        assert_eq!(
            TableIndexSnapshotPath::parse_sequence_number("0000000123"),
            None
        );
        assert_eq!(
            TableIndexSnapshotPath::parse_sequence_number("0000000123.json"),
            None
        );
        assert_eq!(
            TableIndexSnapshotPath::parse_sequence_number("0000000123.info"),
            None
        );

        // Invalid cases - non-numeric prefix
        assert_eq!(
            TableIndexSnapshotPath::parse_sequence_number("abc123.info.json"),
            None
        );
        assert_eq!(
            TableIndexSnapshotPath::parse_sequence_number("not_a_number.info.json"),
            None
        );

        // Edge case - empty string
        assert_eq!(TableIndexSnapshotPath::parse_sequence_number(""), None);
        assert_eq!(
            TableIndexSnapshotPath::parse_sequence_number(".info.json"),
            None
        );
    }

    #[rstest]
    #[case::standard_path("host-prefix/snapshots/12345678901234567890.info.json")]
    #[case::nested_prefix("another/host/prefix/snapshots/00000000000000000000.info.json")]
    #[case::max_inverted_value("simple/snapshots/18446744073709551615.info.json")] // u64::MAX - 0
    fn test_snapshot_info_file_path_valid_paths(#[case] path: &str) {
        use object_store::path::Path as ObjPath;

        assert!(SnapshotInfoFilePath::from_path(ObjPath::from(path)).is_ok());
    }

    #[rstest]
    #[case::invalid_format("invalid/path", PathError::InvalidFormat { expected: String::new(), actual: String::new() })]
    #[case::wrong_directory(
        "prefix/wrong-dir/12345678901234567890.info.json",
        PathError::InvalidFormat { expected: String::new(), actual: String::new() }
    )]
    #[case::wrong_directory_singular(
        "host-prefix/snapshot/12345678901234567890.info.json",
        PathError::InvalidFormat { expected: String::new(), actual: String::new() }
    )]
    #[case::wrong_directory_name(
        "host-prefix/other-dir/12345678901234567890.info.json",
        PathError::InvalidFormat { expected: String::new(), actual: String::new() }
    )]
    #[case::too_few_digits(
        "prefix/snapshots/123.info.json",
        PathError::InvalidFormat { expected: String::new(), actual: String::new() }
    )]
    #[case::wrong_extension_json_only(
        "prefix/snapshots/12345678901234567890.json",
        PathError::InvalidFormat { expected: String::new(), actual: String::new() }
    )]
    #[case::wrong_extension_info_only(
        "host-prefix/snapshots/12345678901234567890.info",
        PathError::InvalidFormat { expected: String::new(), actual: String::new() }
    )]
    #[case::not_a_number(
        "host-prefix/snapshots/not-a-number.info.json",
        PathError::InvalidFormat { expected: String::new(), actual: String::new() }
    )]
    #[case::nineteen_digits(
        "host-prefix/snapshots/1234567890123456789.info.json",
        PathError::InvalidFormat { expected: String::new(), actual: String::new() }
    )]
    #[case::twenty_one_digits(
        "host-prefix/snapshots/123456789012345678901.info.json",
        PathError::InvalidFormat { expected: String::new(), actual: String::new() }
    )]
    #[case::missing_prefix(
        "snapshots/12345678901234567890.info.json",
        PathError::InvalidFormat { expected: String::new(), actual: String::new() }
    )]
    #[case::no_directory(
        "12345678901234567890.info.json",
        PathError::InvalidFormat { expected: String::new(), actual: String::new() }
    )]
    fn test_snapshot_info_file_path_errors(#[case] path: &str, #[case] expected_error: PathError) {
        use object_store::path::Path as ObjPath;
        use std::mem::discriminant;

        let result = SnapshotInfoFilePath::from_path(ObjPath::from(path));
        assert!(result.is_err());

        let err = result.unwrap_err();
        assert_eq!(
            discriminant(&err),
            discriminant(&expected_error),
            "Expected error type {expected_error:?}, got {err:?}",
        );
    }

    #[rstest]
    #[case::standard_path("host-prefix/table-snapshots/123/456/12345678901234567890.info.json")]
    #[case::nested_prefix_with_zeros(
        "another/prefix/table-snapshots/0/0/00000000000000000000.info.json"
    )]
    #[case::max_u32_values(
        "simple/table-snapshots/4294967295/4294967295/12345678901234567890.info.json"
    )]
    fn test_table_index_snapshot_path_valid_paths(#[case] path: &str) {
        use object_store::path::Path as ObjPath;

        assert!(TableIndexSnapshotPath::from_path(ObjPath::from(path)).is_ok());
    }

    #[rstest]
    #[case::invalid_format("invalid/path", PathError::InvalidFormat { expected: String::new(), actual: String::new() })]
    #[case::missing_directory(
        "prefix/wrong-dir/123/456/12345678901234567890.info.json",
        PathError::InvalidFormat { expected: String::new(), actual: String::new() }
    )]
    #[case::wrong_directory_singular(
        "host-prefix/table-snapshot/123/456/12345678901234567890.info.json",
        PathError::InvalidFormat { expected: String::new(), actual: String::new() }
    )]
    #[case::wrong_directory_name(
        "host-prefix/snapshots/123/456/12345678901234567890.info.json",
        PathError::InvalidFormat { expected: String::new(), actual: String::new() }
    )]
    #[case::invalid_extension(
        "prefix/table-snapshots/123/456/12345678901234567890.json",
        PathError::InvalidFormat { expected: String::new(), actual: String::new() }
    )]
    #[case::wrong_extension_info_only(
        "prefix/table-snapshots/123/456/12345678901234567890.info",
        PathError::InvalidFormat { expected: String::new(), actual: String::new() }
    )]
    #[case::too_few_digits_in_sequence(
        "prefix/table-snapshots/123/456/123.info.json",
        PathError::InvalidFormat { expected: String::new(), actual: String::new() }
    )]
    #[case::not_a_number_filename(
        "host-prefix/table-snapshots/123/456/not-a-number.info.json",
        PathError::InvalidFormat { expected: String::new(), actual: String::new() }
    )]
    #[case::invalid_db_id_not_number(
        "host-prefix/table-snapshots/not-a-number/456/12345678901234567890.info.json",
        PathError::InvalidFormat { expected: String::new(), actual: String::new() }
    )]
    #[case::invalid_table_id_not_number(
        "host-prefix/table-snapshots/123/not-a-number/12345678901234567890.info.json",
        PathError::InvalidFormat { expected: String::new(), actual: String::new() }
    )]
    #[case::table_id_too_large_for_u32(
        "host-prefix/table-snapshots/123/4294967296/12345678901234567890.info.json",
        PathError::InvalidTableId(String::new())
    )]
    #[case::db_id_too_large_for_u32(
        "host-prefix/table-snapshots/99999999999999999999/456/12345678901234567890.info.json",
        PathError::InvalidDbId(String::new())
    )]
    #[case::sequence_number_too_large_for_u32(
        "host-prefix/table-snapshots/123/456/99999999999999999999.info.json",
        PathError::InvalidSequenceNumber{context: String::new(), filename: String::new()}
    )]
    #[case::missing_table_id(
        "host-prefix/table-snapshots/123/12345678901234567890.info.json",
        PathError::InvalidFormat { expected: String::new(), actual: String::new() }
    )]
    #[case::missing_prefix(
        "table-snapshots/123/456/12345678901234567890.info.json",
        PathError::InvalidFormat { expected: String::new(), actual: String::new() }
    )]
    #[case::extra_path_components(
        "host-prefix/table-snapshots/123/456/extra/12345678901234567890.info.json",
        PathError::InvalidFormat { expected: String::new(), actual: String::new() }
    )]
    fn test_table_index_snapshot_path_errors(
        #[case] path: &str,
        #[case] expected_error: PathError,
    ) {
        use object_store::path::Path as ObjPath;
        use std::mem::discriminant;

        let result = TableIndexSnapshotPath::from_path(ObjPath::from(path));
        assert!(result.is_err());

        let err = result.unwrap_err();
        assert_eq!(
            discriminant(&err),
            discriminant(&expected_error),
            "Expected error type {expected_error:?}, got {err:?}",
        );
    }

    #[rstest]
    #[case::zero_ids(0, 0)]
    #[case::regular_ids(123, 456)]
    #[case::max_ids(u32::MAX, u32::MAX)]
    #[case::mixed_ids(0, u32::MAX)]
    #[case::another_mixed(u32::MAX, 0)]
    fn test_table_index_snapshot_path_extract_ids(#[case] db_id: u32, #[case] table_id: u32) {
        use object_store::path::Path as ObjPath;

        // Create a path with known IDs
        let path = TableIndexSnapshotPath::new(
            "test-prefix",
            db_id,
            table_id,
            SnapshotSequenceNumber::new(12345),
        );

        // Extract IDs and verify they match
        let id = path.full_table_id();
        assert_eq!(id.node_id(), "test-prefix");
        assert_eq!(id.db_id(), DbId::from(db_id));
        assert_eq!(id.table_id(), TableId::from(table_id));

        // Also test with a path created via from_path
        let path_str = format!(
            "test-prefix/table-snapshots/{db_id}/{table_id}/12345678901234567890.info.json"
        );
        let parsed_path = TableIndexSnapshotPath::from_path(ObjPath::from(path_str))
            .expect("path should be valid");

        let parsed_id = parsed_path.full_table_id();
        assert_eq!(parsed_id.node_id(), "test-prefix");
        assert_eq!(parsed_id.db_id(), DbId::from(db_id));
        assert_eq!(parsed_id.table_id(), TableId::from(table_id));
    }

    #[rstest]
    #[case::missing_db_indices(
        "invalid/path",
        PathError::InvalidFormat { expected: String::new(), actual: String::new() }
    )]
    #[case::invalid_db_id(
        "prefix/db-indices/not-a-number/456/index.info.json",
        PathError::InvalidFormat { expected: String::new(), actual: String::new() }
    )]
    #[case::invalid_table_id(
        "prefix/db-indices/123/not-a-number/index.info.json",
        PathError::InvalidFormat { expected: String::new(), actual: String::new() }
    )]
    #[case::table_id_overflow_u32(
        "prefix/db-indices/123/4294967296/index.info.json",
        PathError::InvalidTableId(String::new())
    )]
    #[case::db_id_overflow_u32(
        "prefix/db-indices/4294967296/123/index.info.json",
        PathError::InvalidDbId(String::new())
    )]
    #[case::wrong_filename(
        "prefix/db-indices/123/456/wrong-file.json",
        PathError::InvalidFormat { expected: String::new(), actual: String::new() }
    )]
    fn test_table_index_path_errors(#[case] path: &str, #[case] expected_error: PathError) {
        use object_store::path::Path as ObjPath;
        use std::mem::discriminant;

        let result = TableIndexPath::from_path(ObjPath::from(path));
        assert!(result.is_err());

        let err = result.unwrap_err();
        assert_eq!(
            discriminant(&err),
            discriminant(&expected_error),
            "Expected error type {expected_error:?}, got {err:?}",
        );
    }
}
