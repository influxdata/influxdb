use chrono::prelude::*;
use influxdb3_wal::WalFileSequenceNumber;
use object_store::path::Path as ObjPath;
use std::ops::Deref;

/// File extension for catalog files
pub const CATALOG_FILE_EXTENSION: &str = "json";

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
    pub fn new(wal_file_sequence_number: WalFileSequenceNumber) -> Self {
        let path = ObjPath::from(format!(
            "catalogs/{:020}.{}",
            object_store_file_stem(wal_file_sequence_number.get()),
            CATALOG_FILE_EXTENSION
        ));
        Self(path)
    }

    pub fn dir() -> Self {
        Self(ObjPath::from("catalogs"))
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
    pub fn new(
        db_name: &str,
        table_name: &str,
        date: DateTime<Utc>,
        wal_file_sequence_number: WalFileSequenceNumber,
    ) -> Self {
        let path = ObjPath::from(format!(
            "dbs/{db_name}/{table_name}/{}/{}.{}",
            date.format("%Y-%m-%d/%H-%M"),
            wal_file_sequence_number.get(),
            PARQUET_FILE_EXTENSION
        ));
        Self(path)
    }

    pub fn new_with_chunk_time(
        db_name: &str,
        table_name: &str,
        chunk_time: i64,
        wal_file_sequence_number: WalFileSequenceNumber,
    ) -> Self {
        // Convert the chunk time into a date time string for YYYY-MM-DDTHH-MM
        let date_time = DateTime::<Utc>::from_timestamp_nanos(chunk_time);
        let path = ObjPath::from(format!(
            "dbs/{db_name}/{table_name}/{}/{:010}.{}",
            date_time.format("%Y-%m-%d/%H-%M"),
            wal_file_sequence_number.get(),
            PARQUET_FILE_EXTENSION
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

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SnapshotInfoFilePath(ObjPath);

impl SnapshotInfoFilePath {
    pub fn new(wal_file_sequence_number: WalFileSequenceNumber) -> Self {
        let path = ObjPath::from(format!(
            "snapshots/{:020}.{}",
            object_store_file_stem(wal_file_sequence_number.get()),
            SNAPSHOT_INFO_FILE_EXTENSION
        ));
        Self(path)
    }

    pub fn dir() -> Self {
        Self(ObjPath::from("snapshots"))
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

#[test]
fn catalog_file_path_new() {
    assert_eq!(
        *CatalogFilePath::new(WalFileSequenceNumber::new(0)),
        ObjPath::from("catalogs/18446744073709551615.json")
    );
}

#[test]
fn parquet_file_path_new() {
    assert_eq!(
        *ParquetFilePath::new(
            "my_db",
            "my_table",
            Utc.with_ymd_and_hms(2038, 1, 19, 3, 14, 7).unwrap(),
            WalFileSequenceNumber::new(0),
        ),
        ObjPath::from("dbs/my_db/my_table/2038-01-19/03-14/0.parquet")
    );
}

#[test]
fn parquet_file_percent_encoded() {
    assert_eq!(
        ParquetFilePath::new(
            "..",
            "..",
            Utc.with_ymd_and_hms(2038, 1, 19, 3, 14, 7).unwrap(),
            WalFileSequenceNumber::new(0),
        )
        .as_ref()
        .as_ref(),
        "dbs/%2E%2E/%2E%2E/2038-01-19/03-14/0.parquet"
    );
}

#[test]
fn snapshot_info_file_path_new() {
    assert_eq!(
        *SnapshotInfoFilePath::new(WalFileSequenceNumber::new(0)),
        ObjPath::from("snapshots/18446744073709551615.info.json")
    );
}
