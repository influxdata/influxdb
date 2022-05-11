//! This module contains the IOx implementation for using local disk as the
//! object store.
use crate::path::{Path, DELIMITER};
use crate::{GetResult, ListResult, ObjectMeta, ObjectStoreApi, Result};
use async_trait::async_trait;
use bytes::Bytes;
use futures::{
    stream::{self, BoxStream},
    StreamExt,
};
use snafu::{OptionExt, ResultExt, Snafu};
use std::{collections::BTreeSet, convert::TryFrom, io};
use tokio::fs;
use walkdir::{DirEntry, WalkDir};

/// A specialized `Error` for filesystem object store-related errors
#[derive(Debug, Snafu)]
#[allow(missing_docs)]
pub enum Error {
    #[snafu(display("File size for {} did not fit in a usize: {}", path, source))]
    FileSizeOverflowedUsize {
        source: std::num::TryFromIntError,
        path: String,
    },

    #[snafu(display("Unable to walk dir: {}", source))]
    UnableToWalkDir {
        source: walkdir::Error,
    },

    #[snafu(display("Unable to access metadata for {}: {}", path, source))]
    UnableToAccessMetadata {
        source: Box<dyn std::error::Error + Send + Sync + 'static>,
        path: String,
    },

    #[snafu(display("Unable to copy data to file: {}", source))]
    UnableToCopyDataToFile {
        source: io::Error,
    },

    #[snafu(display("Unable to create dir {}: {}", path.display(), source))]
    UnableToCreateDir {
        source: io::Error,
        path: std::path::PathBuf,
    },

    #[snafu(display("Unable to create file {}: {}", path.display(), err))]
    UnableToCreateFile {
        path: std::path::PathBuf,
        err: io::Error,
    },

    #[snafu(display("Unable to delete file {}: {}", path.display(), source))]
    UnableToDeleteFile {
        source: io::Error,
        path: std::path::PathBuf,
    },

    #[snafu(display("Unable to open file {}: {}", path.display(), source))]
    UnableToOpenFile {
        source: io::Error,
        path: std::path::PathBuf,
    },

    #[snafu(display("Unable to read data from file {}: {}", path.display(), source))]
    UnableToReadBytes {
        source: io::Error,
        path: std::path::PathBuf,
    },

    NotFound {
        path: String,
        source: io::Error,
    },
}

impl From<Error> for super::Error {
    fn from(source: Error) -> Self {
        match source {
            Error::NotFound { path, source } => Self::NotFound {
                path,
                source: source.into(),
            },
            _ => Self::Generic {
                store: "LocalFileSystem",
                source: Box::new(source),
            },
        }
    }
}

/// Local filesystem storage suitable for testing or for opting out of using a
/// cloud storage provider.
#[derive(Debug)]
pub struct LocalFileSystem {
    root: std::path::PathBuf,
}

impl std::fmt::Display for LocalFileSystem {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "LocalFileSystem({})", self.root.display())
    }
}

#[async_trait]
impl ObjectStoreApi for LocalFileSystem {
    async fn put(&self, location: &Path, bytes: Bytes) -> Result<()> {
        let content = bytes::BytesMut::from(&*bytes);

        let path = self.path_to_filesystem(location);

        let mut file = match fs::File::create(&path).await {
            Ok(f) => f,
            Err(err) if err.kind() == std::io::ErrorKind::NotFound => {
                let parent = path
                    .parent()
                    .context(UnableToCreateFileSnafu { path: &path, err })?;
                fs::create_dir_all(&parent)
                    .await
                    .context(UnableToCreateDirSnafu { path: parent })?;

                match fs::File::create(&path).await {
                    Ok(f) => f,
                    Err(err) => return Err(Error::UnableToCreateFile { path, err }.into()),
                }
            }
            Err(err) => return Err(Error::UnableToCreateFile { path, err }.into()),
        };

        tokio::io::copy(&mut &content[..], &mut file)
            .await
            .context(UnableToCopyDataToFileSnafu)?;

        Ok(())
    }

    async fn get(&self, location: &Path) -> Result<GetResult> {
        let (file, path) = self.get_file(location).await?;
        Ok(GetResult::File(file, path))
    }

    async fn head(&self, location: &Path) -> Result<ObjectMeta> {
        let (file, _) = self.get_file(location).await?;
        let metadata = file
            .metadata()
            .await
            .map_err(|e| Error::UnableToAccessMetadata {
                source: e.into(),
                path: location.to_string(),
            })?;

        convert_metadata(metadata, location.clone())
    }

    async fn delete(&self, location: &Path) -> Result<()> {
        let path = self.path_to_filesystem(location);
        fs::remove_file(&path)
            .await
            .context(UnableToDeleteFileSnafu { path })?;
        Ok(())
    }

    async fn list<'a>(
        &'a self,
        prefix: Option<&'a Path>,
    ) -> Result<BoxStream<'a, Result<ObjectMeta>>> {
        let root_path = match prefix {
            Some(prefix) => self.path_to_filesystem(prefix),
            None => self.root.to_path_buf(),
        };

        let walkdir = WalkDir::new(&root_path)
            // Don't include the root directory itself
            .min_depth(1);

        let s =
            walkdir.into_iter().flat_map(move |result_dir_entry| {
                match convert_walkdir_result(result_dir_entry) {
                    Err(e) => Some(Err(e)),
                    Ok(None) => None,
                    Ok(entry @ Some(_)) => entry
                        .filter(|dir_entry| dir_entry.file_type().is_file())
                        .map(|entry| {
                            let location = self.filesystem_to_path(entry.path()).unwrap();
                            convert_entry(entry, location)
                        }),
                }
            });

        Ok(stream::iter(s).boxed())
    }

    async fn list_with_delimiter(&self, prefix: &Path) -> Result<ListResult> {
        let resolved_prefix = self.path_to_filesystem(prefix);

        let walkdir = WalkDir::new(&resolved_prefix).min_depth(1).max_depth(1);

        let mut common_prefixes = BTreeSet::new();
        let mut objects = Vec::new();

        for entry_res in walkdir.into_iter().map(convert_walkdir_result) {
            if let Some(entry) = entry_res? {
                let is_directory = entry.file_type().is_dir();
                let entry_location = self.filesystem_to_path(entry.path()).unwrap();

                let mut parts = match entry_location.prefix_match(prefix) {
                    Some(parts) => parts,
                    None => continue,
                };

                let common_prefix = match parts.next() {
                    Some(p) => p,
                    None => continue,
                };

                drop(parts);

                if is_directory {
                    common_prefixes.insert(prefix.child(common_prefix));
                } else {
                    objects.push(convert_entry(entry, entry_location)?);
                }
            }
        }

        Ok(ListResult {
            next_token: None,
            common_prefixes: common_prefixes.into_iter().collect(),
            objects,
        })
    }
}

fn convert_entry(entry: DirEntry, location: Path) -> Result<ObjectMeta> {
    let metadata = entry
        .metadata()
        .map_err(|e| Error::UnableToAccessMetadata {
            source: e.into(),
            path: location.to_string(),
        })?;
    convert_metadata(metadata, location)
}

fn convert_metadata(metadata: std::fs::Metadata, location: Path) -> Result<ObjectMeta> {
    let last_modified = metadata
        .modified()
        .expect("Modified file time should be supported on this platform")
        .into();

    let size = usize::try_from(metadata.len()).context(FileSizeOverflowedUsizeSnafu {
        path: location.to_raw(),
    })?;

    Ok(ObjectMeta {
        location,
        last_modified,
        size,
    })
}

/// Convert walkdir results and converts not-found errors into `None`.
fn convert_walkdir_result(
    res: std::result::Result<walkdir::DirEntry, walkdir::Error>,
) -> Result<Option<walkdir::DirEntry>> {
    match res {
        Ok(entry) => Ok(Some(entry)),
        Err(walkdir_err) => match walkdir_err.io_error() {
            Some(io_err) => match io_err.kind() {
                io::ErrorKind::NotFound => Ok(None),
                _ => Err(Error::UnableToWalkDir {
                    source: walkdir_err,
                }
                .into()),
            },
            None => Err(Error::UnableToWalkDir {
                source: walkdir_err,
            }
            .into()),
        },
    }
}

impl LocalFileSystem {
    /// Create new filesystem storage.
    pub fn new(root: impl Into<std::path::PathBuf>) -> Self {
        Self { root: root.into() }
    }

    /// Return filesystem path of the given location
    fn path_to_filesystem(&self, location: &Path) -> std::path::PathBuf {
        let mut path = self.root.clone();
        for component in location.to_raw().split(DELIMITER) {
            path.push(component)
        }
        path.to_path_buf()
    }

    fn filesystem_to_path(&self, location: &std::path::Path) -> Option<Path> {
        let stripped = location.strip_prefix(&self.root).ok()?;
        let path = stripped.to_string_lossy();
        let split = path.split(std::path::MAIN_SEPARATOR);

        Some(Path::from_iter(split))
    }

    async fn get_file(&self, location: &Path) -> Result<(fs::File, std::path::PathBuf)> {
        let path = self.path_to_filesystem(location);

        let file = fs::File::open(&path).await.map_err(|e| {
            if e.kind() == std::io::ErrorKind::NotFound {
                Error::NotFound {
                    path: location.to_string(),
                    source: e,
                }
            } else {
                Error::UnableToOpenFile {
                    path: path.clone(),
                    source: e,
                }
            }
        })?;
        Ok((file, path))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        tests::{
            get_nonexistent_object, list_uses_directories_correctly, list_with_delimiter,
            put_get_delete_list,
        },
        Error as ObjectStoreError, ObjectStoreApi,
    };
    use std::{fs::set_permissions, os::unix::prelude::PermissionsExt};
    use tempfile::TempDir;

    #[tokio::test]
    async fn file_test() {
        let root = TempDir::new().unwrap();
        let integration = LocalFileSystem::new(root.path());

        put_get_delete_list(&integration).await.unwrap();
        list_uses_directories_correctly(&integration).await.unwrap();
        list_with_delimiter(&integration).await.unwrap();
    }

    #[tokio::test]
    async fn creates_dir_if_not_present() {
        let root = TempDir::new().unwrap();
        let integration = LocalFileSystem::new(root.path());

        let location = Path::from_raw("nested/file/test_file");

        let data = Bytes::from("arbitrary data");
        let expected_data = data.clone();

        integration.put(&location, data).await.unwrap();

        let read_data = integration
            .get(&location)
            .await
            .unwrap()
            .bytes()
            .await
            .unwrap();
        assert_eq!(&*read_data, expected_data);
    }

    #[tokio::test]
    async fn unknown_length() {
        let root = TempDir::new().unwrap();
        let integration = LocalFileSystem::new(root.path());

        let location = Path::from_raw("some_file");

        let data = Bytes::from("arbitrary data");
        let expected_data = data.clone();

        integration.put(&location, data).await.unwrap();

        let read_data = integration
            .get(&location)
            .await
            .unwrap()
            .bytes()
            .await
            .unwrap();
        assert_eq!(&*read_data, expected_data);
    }

    #[tokio::test]
    async fn bubble_up_io_errors() {
        let root = TempDir::new().unwrap();

        // make non-readable
        let metadata = root.path().metadata().unwrap();
        let mut permissions = metadata.permissions();
        permissions.set_mode(0o000);
        set_permissions(root.path(), permissions).unwrap();

        let store = LocalFileSystem::new(root.path());

        // `list` must fail
        match store.list(None).await {
            Err(_) => {
                // ok, error found
            }
            Ok(mut stream) => {
                let mut any_err = false;
                while let Some(res) = stream.next().await {
                    if res.is_err() {
                        any_err = true;
                    }
                }
                assert!(any_err);
            }
        }

        // `list_with_delimiter
        assert!(store.list_with_delimiter(&Path::default()).await.is_err());
    }

    const NON_EXISTENT_NAME: &str = "nonexistentname";

    #[tokio::test]
    async fn get_nonexistent_location() {
        let root = TempDir::new().unwrap();
        let integration = LocalFileSystem::new(root.path());

        let location = Path::from_raw(NON_EXISTENT_NAME);

        let err = get_nonexistent_object(&integration, Some(location))
            .await
            .unwrap_err();
        if let Some(ObjectStoreError::NotFound { path, source }) =
            err.downcast_ref::<ObjectStoreError>()
        {
            let source_variant = source.downcast_ref::<std::io::Error>();
            assert!(
                matches!(source_variant, Some(std::io::Error { .. }),),
                "got: {:?}",
                source_variant
            );
            assert_eq!(path, NON_EXISTENT_NAME);
        } else {
            panic!("unexpected error type: {:?}", err);
        }
    }
}
