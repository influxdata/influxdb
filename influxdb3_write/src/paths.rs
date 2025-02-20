use chrono::prelude::*;
use influxdb3_catalog::catalog::CatalogSequenceNumber;
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
    pub fn new(host_prefix: &str, catalog_sequence_number: CatalogSequenceNumber) -> Self {
        let num = u64::MAX - catalog_sequence_number.as_u32() as u64;
        let path = ObjPath::from(format!(
            "{host_prefix}/catalogs/{:020}.{}",
            num, CATALOG_FILE_EXTENSION
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
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        host_prefix: &str,
        db_name: &str,
        db_id: u32,
        table_name: &str,
        table_id: u32,
        chunk_time: i64,
        wal_file_sequence_number: WalFileSequenceNumber,
        sub_chunk_index: Option<u64>,
    ) -> Self {
        let date_time = DateTime::<Utc>::from_timestamp_nanos(chunk_time);
        let path = if sub_chunk_index.is_some() {
            ObjPath::from(format!(
                "{host_prefix}/dbs/{db_name}-{db_id}/{table_name}-{table_id}/{date_string}/{wal_seq:010}-{chunk_idx}.{ext}",
                date_string = date_time.format("%Y-%m-%d/%H-%M"),
                wal_seq = wal_file_sequence_number.as_u64(),
                chunk_idx = sub_chunk_index.unwrap(),
                ext = PARQUET_FILE_EXTENSION
            ))

        } else {
            ObjPath::from(format!(
                "{host_prefix}/dbs/{db_name}-{db_id}/{table_name}-{table_id}/{date_string}/{wal_seq:010}.{ext}",
                date_string = date_time.format("%Y-%m-%d/%H-%M"),
                wal_seq = wal_file_sequence_number.as_u64(),
                ext = PARQUET_FILE_EXTENSION
            ))
        };
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
        *CatalogFilePath::new("my_host", CatalogSequenceNumber::new(0)),
        ObjPath::from("my_host/catalogs/18446744073709551615.json")
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
            None,
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
            None,
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
