use crate::SegmentId;
use std::convert::AsRef;
use std::ops::Deref;
use std::path::Path;
use std::path::PathBuf;

/// File extension for catalog files
const CATALOG_FILE_EXTENSION: &str = "json";

/// File extension for parquet files
const PARQUET_FILE_EXTENSION: &str = "parquet";

/// File extension for segment files
const SEGMENT_FILE_EXTENSION: &str = "wal";

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CatalogFilePath(PathBuf);

impl CatalogFilePath {
    pub fn new(prefix: impl Into<PathBuf>, sequence_number: u64) -> Self {
        let mut path = prefix.into();
        path.push("catalogs");
        path.push(format!("{sequence_number:010}"));
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
        year: u16,
        month: u8,
        day: u8,
        file_number: usize,
    ) -> Self {
        let mut path = prefix.into();
        path.push("dbs");
        path.push(db_name);
        path.push(table_name);
        path.push(format!("{year}-{month:02}-{day:02}"));
        path.push(format!("{file_number:010}"));
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
pub struct SegmentFilePath(PathBuf);

impl SegmentFilePath {
    pub fn new(prefix: impl Into<PathBuf>, segment_id: SegmentId) -> Self {
        let mut path = prefix.into();
        path.push("segments");
        path.push(format!("{:010}", segment_id.0));
        path.set_extension(SEGMENT_FILE_EXTENSION);
        Self(path)
    }
}

impl Deref for SegmentFilePath {
    type Target = Path;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl AsRef<Path> for SegmentFilePath {
    fn as_ref(&self) -> &Path {
        &self.0
    }
}

#[test]
fn catalog_file_path_new() {
    assert_eq!(
        *CatalogFilePath::new("prefix/dir", 0),
        PathBuf::from("prefix/dir/catalogs/0000000000.json").as_ref()
    );
}
#[test]
fn parquet_file_path_new() {
    assert_eq!(
        *ParquetFilePath::new("prefix/dir", "my_db", "my_table", 2038, 1, 19, 0),
        PathBuf::from("prefix/dir/dbs/my_db/my_table/2038-01-19/0000000000.parquet").as_ref()
    );
}
#[test]
fn segment_file_path_new() {
    assert_eq!(
        *SegmentFilePath::new("prefix/dir", SegmentId::new(0)),
        PathBuf::from("prefix/dir/segments/0000000000.wal").as_ref()
    );
}
