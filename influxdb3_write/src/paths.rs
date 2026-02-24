use crate::YearMonth;
use chrono::prelude::*;
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

/// File extension for parquet files
pub const PARQUET_FILE_EXTENSION: &str = "parquet";

/// File extension for snapshot info files
pub const SNAPSHOT_INFO_FILE_EXTENSION: &str = "info.json";

/// File extension for checkpoint files
pub const CHECKPOINT_FILE_EXTENSION: &str = "checkpoint.json";

fn object_store_file_stem(n: u64) -> u64 {
    u64::MAX - n
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ParquetFilePath(ObjPath);

impl ParquetFilePath {
    /// Generate a parquet file path using the given arguments. This will convert the provided
    /// `chunk_time` into a date time string with format `'YYYY-MM-DD/HH-MM'`
    pub fn new(
        host_prefix: &str,
        db_id: u32,
        table_id: u32,
        chunk_time: i64,
        wal_file_sequence_number: WalFileSequenceNumber,
    ) -> Self {
        let date_time = DateTime::<Utc>::from_timestamp_nanos(chunk_time);
        let path = ObjPath::from(format!(
            "{host_prefix}/dbs/{db_id}/{table_id}/{date_string}/{wal_seq:010}.{ext}",
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
        if let Some(filename) = path_str.split('/').next_back()
            && TableIndexSnapshotPath::parse_sequence_number(filename).is_none()
        {
            return Err(PathError::InvalidSequenceNumber {
                context: "table snapshot".to_string(),
                filename: filename.to_string(),
            });
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
        if let Some(filename) = path_str.split('/').next_back()
            && SnapshotInfoFilePath::parse_sequence_number(filename).is_none()
        {
            return Err(PathError::InvalidSequenceNumber {
                context: "snapshot".to_string(),
                filename: filename.to_string(),
            });
        }

        Ok(Self(path))
    }
}

/// Path for snapshot checkpoints, organized by year-month.
/// Pattern: `{node_id}/snapshot-checkpoints/{year-month}/{inverted_seq:020}.checkpoint.json`
///
/// Checkpoints are organized by month to enable efficient loading of only the latest
/// checkpoint per month during server startup.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SnapshotCheckpointPath(ObjPath);

static SNAPSHOT_CHECKPOINT_PATH_REGEX: LazyLock<Regex> = LazyLock::new(|| {
    Regex::new(r"^.*/snapshot-checkpoints/(\d{4}-\d{2})/(\d{20})\.checkpoint\.json$")
        .expect("regex must be valid")
});

impl SnapshotCheckpointPath {
    /// Create a new checkpoint path for the given host, year-month, and snapshot sequence number.
    ///
    /// The checkpoint filename embeds the `last_snapshot_sequence_number` from the checkpoint,
    /// enabling efficient filtering by sequence during startup without loading checkpoint content.
    pub fn new(
        host_prefix: &str,
        year_month: &YearMonth,
        snapshot_sequence_number: SnapshotSequenceNumber,
    ) -> Self {
        let path = ObjPath::from(format!(
            "{host_prefix}/snapshot-checkpoints/{year_month}/{:020}.{ext}",
            object_store_file_stem(snapshot_sequence_number.as_u64()),
            ext = CHECKPOINT_FILE_EXTENSION
        ));
        Self(path)
    }

    /// Get the directory path for all checkpoints for a host
    pub fn dir(host_prefix: &str) -> ObjPath {
        ObjPath::from(format!("{host_prefix}/snapshot-checkpoints"))
    }

    /// Get the directory path for checkpoints of a specific month
    pub fn month_dir(host_prefix: &str, year_month: &YearMonth) -> ObjPath {
        ObjPath::from(format!("{host_prefix}/snapshot-checkpoints/{year_month}/"))
    }

    /// Parse the year-month from a checkpoint path
    pub fn parse_year_month(path: &str) -> Option<YearMonth> {
        let re = SNAPSHOT_CHECKPOINT_PATH_REGEX.clone();
        re.captures(path)
            .and_then(|caps| caps.get(1).map(|m| m.as_str()))
            .and_then(|s| s.parse().ok())
    }

    /// Parse the snapshot sequence number from a checkpoint filename.
    ///
    /// The checkpoint filename contains the `last_snapshot_sequence_number` that was
    /// included in the checkpoint, encoded via `object_store_file_stem` (inverted for sorting).
    pub fn parse_sequence_number(filename: &str) -> Option<SnapshotSequenceNumber> {
        // Extract the filename from the path if it's a full path
        let filename = filename.split('/').next_back().unwrap_or(filename);
        filename
            .strip_suffix(".checkpoint.json")
            .and_then(|seq_str| seq_str.parse::<u64>().ok())
            .map(|inverted| u64::MAX - inverted)
            .map(SnapshotSequenceNumber::new)
    }

    pub fn from_path(path: ObjPath) -> Result<Self, PathError> {
        Self::try_from(path)
    }
}

impl Deref for SnapshotCheckpointPath {
    type Target = ObjPath;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl AsRef<ObjPath> for SnapshotCheckpointPath {
    fn as_ref(&self) -> &ObjPath {
        &self.0
    }
}

impl TryFrom<ObjPath> for SnapshotCheckpointPath {
    type Error = PathError;

    fn try_from(path: ObjPath) -> Result<Self, Self::Error> {
        let re = SNAPSHOT_CHECKPOINT_PATH_REGEX.clone();
        let path_str = path.as_ref();

        if !re.is_match(path_str) {
            return Err(PathError::InvalidFormat {
                expected: "*/snapshot-checkpoints/<YYYY-MM>/<20-digits>.checkpoint.json"
                    .to_string(),
                actual: path_str.to_string(),
            });
        }

        // Additional validation: ensure we can parse the sequence number
        if let Some(filename) = path_str.split('/').next_back()
            && SnapshotCheckpointPath::parse_sequence_number(filename).is_none()
        {
            return Err(PathError::InvalidSequenceNumber {
                context: "snapshot checkpoint".to_string(),
                filename: filename.to_string(),
            });
        }

        Ok(Self(path))
    }
}

#[test]
fn parquet_file_path_new() {
    assert_eq!(
        *ParquetFilePath::new(
            "my_host",
            0,
            0,
            Utc.with_ymd_and_hms(2038, 1, 19, 3, 14, 7)
                .unwrap()
                .timestamp_nanos_opt()
                .unwrap(),
            WalFileSequenceNumber::new(1337),
        ),
        ObjPath::from("my_host/dbs/0/0/2038-01-19/03-14/0000001337.parquet")
    );
}

#[test]
fn parquet_file_percent_encoded() {
    assert_eq!(
        ParquetFilePath::new(
            "..",
            0,
            0,
            Utc.with_ymd_and_hms(2038, 1, 19, 3, 14, 7)
                .unwrap()
                .timestamp_nanos_opt()
                .unwrap(),
            WalFileSequenceNumber::new(100),
        )
        .as_ref()
        .as_ref(),
        "%2E%2E/dbs/0/0/2038-01-19/03-14/0000000100.parquet"
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
mod test;
