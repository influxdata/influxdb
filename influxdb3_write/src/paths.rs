use crate::SegmentId;
use chrono::prelude::*;
use std::convert::AsRef;
use std::ops::Deref;
use std::path::Path;
use std::path::PathBuf;

/// File extension for catalog files
const CATALOG_FILE_EXTENSION: &str = "json";

/// File extension for parquet files
const PARQUET_FILE_EXTENSION: &str = "parquet";

/// File extension for segment info files
const SEGMENT_INFO_FILE_EXTENSION: &str = "info.json";

/// File extension for segment wal files
const SEGMENT_WAL_FILE_EXTENSION: &str = "wal";

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CatalogFilePath(PathBuf);

impl CatalogFilePath {
    pub fn new(prefix: impl Into<PathBuf>, sequence_number: u32) -> Self {
        let mut path = prefix.into();
        path.push("catalogs");
        path.push(format!("{:010}", u32::MAX - sequence_number));
        path.set_extension(CATALOG_FILE_EXTENSION);
        Self(path)
    }
}

impl Deref for CatalogFilePath {
    type Target = Path;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl AsRef<Path> for CatalogFilePath {
    fn as_ref(&self) -> &Path {
        &self.0
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ParquetFilePath(PathBuf);

impl ParquetFilePath {
    pub fn new(
        prefix: impl Into<PathBuf>,
        db_name: &str,
        table_name: &str,
        date: DateTime<Utc>,
        file_number: u32,
    ) -> Self {
        let mut path = prefix.into();
        path.push("dbs");
        path.push(db_name);
        path.push(table_name);
        path.push(format!("{}", date.format("%Y-%m-%d")));
        path.push(format!("{:010}", u32::MAX - file_number));
        path.set_extension(PARQUET_FILE_EXTENSION);
        Self(path)
    }
}

impl Deref for ParquetFilePath {
    type Target = Path;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl AsRef<Path> for ParquetFilePath {
    fn as_ref(&self) -> &Path {
        &self.0
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SegmentWalFilePath(PathBuf);

impl SegmentWalFilePath {
    pub fn new(dir: impl Into<PathBuf>, segment_id: SegmentId) -> Self {
        let mut path = dir.into();
        path.push(format!("{:010}", segment_id.0));
        path.set_extension(SEGMENT_WAL_FILE_EXTENSION);
        Self(path)
    }
}

impl Deref for SegmentWalFilePath {
    type Target = Path;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl AsRef<Path> for SegmentWalFilePath {
    fn as_ref(&self) -> &Path {
        &self.0
    }
}

impl SegmentInfoFilePath {
    pub fn new(prefix: impl Into<PathBuf>, segment_id: SegmentId) -> Self {
        let mut path = prefix.into();
        path.push("segments");
        path.push(format!("{:010}", u32::MAX - segment_id.0));
        path.set_extension(SEGMENT_INFO_FILE_EXTENSION);
        Self(path)
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SegmentInfoFilePath(PathBuf);

impl Deref for SegmentInfoFilePath {
    type Target = Path;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl AsRef<Path> for SegmentInfoFilePath {
    fn as_ref(&self) -> &Path {
        &self.0
    }
}

#[test]
fn catalog_file_path_new() {
    assert_eq!(
        *CatalogFilePath::new("prefix/dir", 0),
        PathBuf::from("prefix/dir/catalogs/4294967295.json").as_ref()
    );
}

#[test]
fn parquet_file_path_new() {
    assert_eq!(
        *ParquetFilePath::new(
            "prefix/dir",
            "my_db",
            "my_table",
            Utc.with_ymd_and_hms(2038, 01, 19, 3, 14, 7).unwrap(),
            0
        ),
        PathBuf::from("prefix/dir/dbs/my_db/my_table/2038-01-19/4294967295.parquet").as_ref()
    );
}

#[test]
fn segment_info_file_path_new() {
    assert_eq!(
        *SegmentInfoFilePath::new("prefix/dir", SegmentId::new(0)),
        PathBuf::from("prefix/dir/segments/4294967295.info.json").as_ref()
    );
}

#[test]
fn segment_wal_file_path_new() {
    assert_eq!(
        *SegmentWalFilePath::new("dir", SegmentId::new(0)),
        PathBuf::from("dir/0000000000.wal").as_ref()
    );
}
