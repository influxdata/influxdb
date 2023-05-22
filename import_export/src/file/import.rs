//! Utilities for importing catalog and data from files
//! MORE COMING SOON: <https://github.com/influxdata/influxdb_iox/issues/7744>

use observability_deps::tracing::{debug, warn};
use std::{
    borrow::Cow,
    path::{Path, PathBuf},
};
use thiserror::Error;

#[derive(Debug, Error)]
pub enum ImportError {
    #[error("Reading {path:?}: {e}")]
    Reading { path: PathBuf, e: std::io::Error },

    #[error("Not a directory: {0:?}")]
    NotDirectory(PathBuf),
}

impl ImportError {
    fn reading(path: impl Into<PathBuf>, e: std::io::Error) -> Self {
        let path = path.into();
        Self::Reading { path, e }
    }
}

type Result<T, E = ImportError> = std::result::Result<T, E>;

/// Represents the contents of a directory exported using [`RemoteExporter`]
///
/// [`RemoteExporter`]: crate::file::RemoteExporter
#[derive(Debug, Default)]
pub struct ExportedContents {
    // .parquet files
    parquet_files: Vec<PathBuf>,

    // .parquet.json files (json that correspond to the parquet files)
    parquet_json_files: Vec<PathBuf>,

    // table .json files
    table_json_files: Vec<PathBuf>,

    // partition .json files
    partition_json_files: Vec<PathBuf>,
}

impl ExportedContents {
    /// Read the contents of the directory in `dir_path`, categorizing
    /// files in that directory.
    pub fn try_new(dir_path: &Path) -> Result<Self> {
        if !dir_path.is_dir() {
            return Err(ImportError::NotDirectory(dir_path.into()));
        };

        let entries: Vec<_> = dir_path
            .read_dir()
            .map_err(|e| ImportError::reading(dir_path, e))?
            .flatten()
            .collect();

        debug!(?entries, "Directory contents");

        let mut new_self = Self::default();

        for entry in entries {
            let path = entry.path();
            let extension = if let Some(extension) = path.extension() {
                extension
            } else {
                warn!(?path, "IGNORING file with no extension");
                continue;
            };

            if extension == "parquet" {
                // names like "<UUID>.parquet"
                new_self.parquet_files.push(path)
            } else if extension == "json" {
                let name = file_name(&path);
                if name.starts_with("table.") {
                    new_self.table_json_files.push(path);
                } else if name.starts_with("partition") {
                    // names like "partitition.<id>.json"
                    new_self.partition_json_files.push(path);
                } else if name.ends_with(".parquet.json") {
                    // names like  "<UUID>.parquet.json"
                    new_self.parquet_json_files.push(path);
                } else {
                    warn!(?path, "IGNORING unknown JSON file");
                }
            } else {
                warn!(?path, "IGNORING unknown file");
            }
        }

        Ok(new_self)
    }

    /// Returns the name of the i'th entry in `self.parquet_files`, if
    /// any
    pub fn parquet_file_name(&self, i: usize) -> Option<Cow<'_, str>> {
        self.parquet_files.get(i).map(|p| file_name(p))
    }

    pub fn parquet_files(&self) -> &[PathBuf] {
        self.parquet_files.as_ref()
    }

    pub fn parquet_json_files(&self) -> &[PathBuf] {
        self.parquet_json_files.as_ref()
    }

    pub fn table_json_files(&self) -> &[PathBuf] {
        self.table_json_files.as_ref()
    }

    pub fn partition_json_files(&self) -> &[PathBuf] {
        self.partition_json_files.as_ref()
    }
}

/// Returns the name of the file
fn file_name(p: &Path) -> Cow<'_, str> {
    p.file_name()
        .map(|p| p.to_string_lossy())
        .unwrap_or_else(|| Cow::Borrowed(""))
}
