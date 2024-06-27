use crate::SegmentId;
use chrono::prelude::*;
use object_store::path::Path as ObjPath;
use std::fmt;
use std::ops::Deref;
use std::path::Path;
use std::path::PathBuf;

/// File extension for catalog files
pub const CATALOG_FILE_EXTENSION: &str = "json";

/// File extension for parquet files
pub const PARQUET_FILE_EXTENSION: &str = "parquet";

/// File extension for segment info files
pub const SEGMENT_INFO_FILE_EXTENSION: &str = "info.json";

/// File extension for segment wal files
pub const SEGMENT_WAL_FILE_EXTENSION: &str = "wal";

fn object_store_file_stem(n: u32) -> u32 {
    u32::MAX - n
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CatalogFilePath(ObjPath);

impl CatalogFilePath {
    pub fn new(segment_id: SegmentId) -> Self {
        let path = ObjPath::from(format!(
            "catalogs/{:010}.{}",
            object_store_file_stem(segment_id.0),
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
        segment_id: SegmentId,
        file_number: u32,
    ) -> Self {
        let path = ObjPath::from(format!(
            "dbs/{db_name}/{table_name}/{}/{:010}/{}.{}",
            date.format("%Y-%m-%d"),
            object_store_file_stem(segment_id.0),
            file_number,
            PARQUET_FILE_EXTENSION
        ));
        Self(path)
    }

    pub fn new_with_partition_key(
        db_name: &str,
        table_name: &str,
        partition_key: &str,
        segment_id: SegmentId,
        file_number: u32,
    ) -> Self {
        let path = ObjPath::from(format!(
            "dbs/{db_name}/{table_name}/{}/{:010}/{}.{}",
            partition_key,
            object_store_file_stem(segment_id.0),
            file_number,
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
pub struct SegmentWalFilePath(PathBuf);

impl SegmentWalFilePath {
    pub fn new(dir: impl Into<PathBuf>, segment_id: SegmentId) -> Self {
        let mut path = dir.into();
        path.push(format!("{:010}", segment_id.0));
        path.set_extension(SEGMENT_WAL_FILE_EXTENSION);
        Self(path)
    }
}

impl fmt::Display for SegmentWalFilePath {
    fn fmt(&self, f: &mut fmt::Formatter) -> Result<(), fmt::Error> {
        write!(f, "{}", self.0.to_string_lossy())
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

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SegmentInfoFilePath(ObjPath);

impl SegmentInfoFilePath {
    pub fn new(segment_id: SegmentId) -> Self {
        let path = ObjPath::from(format!(
            "segments/{:010}.{}",
            object_store_file_stem(segment_id.0),
            SEGMENT_INFO_FILE_EXTENSION
        ));
        Self(path)
    }

    pub fn dir() -> Self {
        Self(ObjPath::from("segments"))
    }
}

impl Deref for SegmentInfoFilePath {
    type Target = ObjPath;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl AsRef<ObjPath> for SegmentInfoFilePath {
    fn as_ref(&self) -> &ObjPath {
        &self.0
    }
}

#[test]
fn catalog_file_path_new() {
    assert_eq!(
        *CatalogFilePath::new(SegmentId::new(0)),
        ObjPath::from("catalogs/4294967295.json")
    );
}

#[test]
fn parquet_file_path_new() {
    assert_eq!(
        *ParquetFilePath::new(
            "my_db",
            "my_table",
            Utc.with_ymd_and_hms(2038, 1, 19, 3, 14, 7).unwrap(),
            SegmentId::new(0),
            0
        ),
        ObjPath::from("dbs/my_db/my_table/2038-01-19/4294967295/0.parquet")
    );
}

#[test]
fn parquet_file_percent_encoded() {
    assert_eq!(
        ParquetFilePath::new(
            "..",
            "..",
            Utc.with_ymd_and_hms(2038, 1, 19, 3, 14, 7).unwrap(),
            SegmentId::new(0),
            0
        )
        .as_ref()
        .as_ref(),
        "dbs/%2E%2E/%2E%2E/2038-01-19/4294967295/0.parquet"
    );
}

#[test]
fn segment_info_file_path_new() {
    assert_eq!(
        *SegmentInfoFilePath::new(SegmentId::new(0)),
        ObjPath::from("segments/4294967295.info.json")
    );
}

#[test]
fn segment_wal_file_path_new() {
    assert_eq!(
        *SegmentWalFilePath::new("dir", SegmentId::new(0)),
        PathBuf::from("dir/0000000000.wal").as_ref()
    );
}
