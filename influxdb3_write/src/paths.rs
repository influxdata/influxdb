use chrono::prelude::*;
use influxdb3_wal::{SnapshotSequenceNumber, WalFileSequenceNumber};
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
    pub fn new(host_prefix: &str, wal_file_sequence_number: WalFileSequenceNumber) -> Self {
        let path = ObjPath::from(format!(
            "{host_prefix}/catalogs/{:020}.{}",
            object_store_file_stem(wal_file_sequence_number.as_u64()),
            CATALOG_FILE_EXTENSION
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
    pub fn new(
        host_prefix: &str,
        db_name: &str,
        table_name: &str,
        date: DateTime<Utc>,
        wal_file_sequence_number: WalFileSequenceNumber,
    ) -> Self {
        let path = ObjPath::from(format!(
            "{host_prefix}/dbs/{db_name}/{table_name}/{}/{}.{}",
            date.format("%Y-%m-%d/%H-%M"),
            wal_file_sequence_number.as_u64(),
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
            wal_file_sequence_number.as_u64(),
            PARQUET_FILE_EXTENSION
        ));
        Self(path)
    }

    pub fn clone_inner(&self) -> ObjPath {
        self.0.clone()
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
    pub fn new(host_prefix: &str, snapshot_sequence_number: SnapshotSequenceNumber) -> Self {
        let path = ObjPath::from(format!(
            "{host_prefix}/snapshots/{:020}.{}",
            object_store_file_stem(snapshot_sequence_number.as_u64()),
            SNAPSHOT_INFO_FILE_EXTENSION
        ));
        Self(path)
    }

    pub fn dir(host_prefix: &str) -> Self {
        Self(ObjPath::from(format!("{host_prefix}/snapshots")))
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
        *CatalogFilePath::new("my_host", WalFileSequenceNumber::new(0)),
        ObjPath::from("my_host/catalogs/18446744073709551615.json")
    );
}

#[test]
fn parquet_file_path_new() {
    assert_eq!(
        *ParquetFilePath::new(
            "my_host",
            "my_db",
            "my_table",
            Utc.with_ymd_and_hms(2038, 1, 19, 3, 14, 7).unwrap(),
            WalFileSequenceNumber::new(0),
        ),
        ObjPath::from("my_host/dbs/my_db/my_table/2038-01-19/03-14/0.parquet")
    );
}

#[test]
fn parquet_file_percent_encoded() {
    assert_eq!(
        ParquetFilePath::new(
            "my_host",
            "..",
            "..",
            Utc.with_ymd_and_hms(2038, 1, 19, 3, 14, 7).unwrap(),
            WalFileSequenceNumber::new(0),
        )
        .as_ref()
        .as_ref(),
        "my_host/dbs/%2E%2E/%2E%2E/2038-01-19/03-14/0.parquet"
    );
}

#[test]
fn snapshot_info_file_path_new() {
    assert_eq!(
        *SnapshotInfoFilePath::new("my_host", SnapshotSequenceNumber::new(0)),
        ObjPath::from("my_host/snapshots/18446744073709551615.info.json")
    );
}
