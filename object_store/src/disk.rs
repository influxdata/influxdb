//! This module contains the IOx implementation for using local disk as the
//! object store.
use crate::cache::Cache;
use crate::path::Path;
use crate::{path::file::FilePath, ListResult, ObjectMeta, ObjectStore, ObjectStoreApi};
use async_trait::async_trait;
use bytes::Bytes;
use futures::{
    stream::{self, BoxStream},
    StreamExt, TryStreamExt,
};
use snafu::{OptionExt, ResultExt, Snafu};
use std::sync::Arc;
use std::{collections::BTreeSet, convert::TryFrom, io, path::PathBuf};
use tokio::fs;
use tokio_util::codec::{BytesCodec, FramedRead};
use walkdir::WalkDir;

/// A specialized `Result` for filesystem object store-related errors
pub type Result<T, E = Error> = std::result::Result<T, E>;

/// A specialized `Error` for filesystem object store-related errors
#[derive(Debug, Snafu)]
#[allow(missing_docs)]
pub enum Error {
    #[snafu(display("File size for {} did not fit in a usize: {}", path.display(), source))]
    FileSizeOverflowedUsize {
        source: std::num::TryFromIntError,
        path: PathBuf,
    },

    #[snafu(display("Unable to walk dir: {}", source))]
    UnableToWalkDir { source: walkdir::Error },

    #[snafu(display("Unable to access metadata for {}: {}", path.display(), source))]
    UnableToAccessMetadata {
        source: walkdir::Error,
        path: PathBuf,
    },

    #[snafu(display("Unable to copy data to file: {}", source))]
    UnableToCopyDataToFile { source: io::Error },

    #[snafu(display("Unable to create dir {}: {}", path.display(), source))]
    UnableToCreateDir { source: io::Error, path: PathBuf },

    #[snafu(display("Unable to create file {}: {}", path.display(), err))]
    UnableToCreateFile { path: PathBuf, err: io::Error },

    #[snafu(display("Unable to delete file {}: {}", path.display(), source))]
    UnableToDeleteFile { source: io::Error, path: PathBuf },

    #[snafu(display("Unable to open file {}: {}", path.display(), source))]
    UnableToOpenFile { source: io::Error, path: PathBuf },

    #[snafu(display("Unable to read data from file {}: {}", path.display(), source))]
    UnableToReadBytes { source: io::Error, path: PathBuf },
}

/// Local filesystem storage suitable for testing or for opting out of using a
/// cloud storage provider.
#[derive(Debug)]
pub struct File {
    root: FilePath,
}

#[async_trait]
impl ObjectStoreApi for File {
    type Path = FilePath;
    type Error = Error;

    fn new_path(&self) -> Self::Path {
        FilePath::default()
    }

    async fn put(&self, location: &Self::Path, bytes: Bytes) -> Result<()> {
        let content = bytes::BytesMut::from(&*bytes);

        let path = self.path(location);

        let mut file = match fs::File::create(&path).await {
            Ok(f) => f,
            Err(err) if err.kind() == std::io::ErrorKind::NotFound => {
                let parent = path
                    .parent()
                    .context(UnableToCreateFile { path: &path, err })?;
                fs::create_dir_all(&parent)
                    .await
                    .context(UnableToCreateDir { path: parent })?;

                match fs::File::create(&path).await {
                    Ok(f) => f,
                    Err(err) => return UnableToCreateFile { path, err }.fail(),
                }
            }
            Err(err) => return UnableToCreateFile { path, err }.fail(),
        };

        tokio::io::copy(&mut &content[..], &mut file)
            .await
            .context(UnableToCopyDataToFile)?;

        Ok(())
    }

    async fn get(&self, location: &Self::Path) -> Result<BoxStream<'static, Result<Bytes>>> {
        let path = self.path(location);

        let file = fs::File::open(&path)
            .await
            .context(UnableToOpenFile { path: &path })?;

        let s = FramedRead::new(file, BytesCodec::new())
            .map_ok(|b| b.freeze())
            .map_err(move |source| Error::UnableToReadBytes {
                source,
                path: path.clone(),
            });
        Ok(s.boxed())
    }

    async fn delete(&self, location: &Self::Path) -> Result<()> {
        let path = self.path(location);
        fs::remove_file(&path)
            .await
            .context(UnableToDeleteFile { path })?;
        Ok(())
    }

    async fn list<'a>(
        &'a self,
        prefix: Option<&'a Self::Path>,
    ) -> Result<BoxStream<'a, Result<Vec<Self::Path>>>> {
        let root_path = self.root.to_raw();
        let walkdir = WalkDir::new(&root_path)
            // Don't include the root directory itself
            .min_depth(1);

        let s =
            walkdir.into_iter().filter_map(move |result_dir_entry| {
                match convert_walkdir_result(result_dir_entry) {
                    Err(e) => Some(Err(e)),
                    Ok(None) => None,
                    Ok(entry @ Some(_)) => entry
                        .filter(|dir_entry| dir_entry.file_type().is_file())
                        .map(|file| {
                            let relative_path = file.path().strip_prefix(&root_path).expect(
                                "Must start with root path because this came from walking the root",
                            );
                            FilePath::raw(relative_path, false)
                        })
                        .filter(|name| prefix.map_or(true, |p| name.prefix_matches(p)))
                        .map(|name| Ok(vec![name])),
                }
            });

        Ok(stream::iter(s).boxed())
    }

    async fn list_with_delimiter(&self, prefix: &Self::Path) -> Result<ListResult<Self::Path>> {
        // Always treat prefix as relative because the list operations don't know
        // anything about where on disk the root of this object store is; they
        // only care about what's within this object store's directory. See
        // documentation for `push_path`: it deliberately does *not* behave  as
        // `PathBuf::push` does: there is no way to replace the root. So even if
        // `prefix` isn't relative, we treat it as such here.
        let mut resolved_prefix = self.root.clone();
        resolved_prefix.push_path(prefix);

        // It is valid to specify a prefix with directories `[foo, bar]` and filename
        // `baz`, in which case we want to treat it like a glob for
        // `foo/bar/baz*` and there may not actually be a file or directory
        // named `foo/bar/baz`. We want to look at all the entries in
        // `foo/bar/`, so remove the file name.
        let mut search_path = resolved_prefix.clone();
        search_path.unset_file_name();

        let walkdir = WalkDir::new(&search_path.to_raw())
            .min_depth(1)
            .max_depth(1);

        let mut common_prefixes = BTreeSet::new();
        let mut objects = Vec::new();

        let root_path = self.root.to_raw();
        for entry_res in walkdir.into_iter().map(convert_walkdir_result) {
            if let Some(entry) = entry_res? {
                let entry_location = FilePath::raw(entry.path(), false);

                if entry_location.prefix_matches(&resolved_prefix) {
                    let metadata = entry
                        .metadata()
                        .context(UnableToAccessMetadata { path: entry.path() })?;

                    if metadata.is_dir() {
                        let parts = entry_location
                            .parts_after_prefix(&resolved_prefix)
                            .expect("must have prefix because of the if prefix_matches condition");

                        let mut relative_location = prefix.to_owned();
                        relative_location.push_part_as_dir(&parts[0]);
                        common_prefixes.insert(relative_location);
                    } else {
                        let path = entry
                            .path()
                            .strip_prefix(&root_path)
                            .expect("must have prefix because of the if prefix_matches condition");
                        let location = FilePath::raw(path, false);

                        let last_modified = metadata
                            .modified()
                            .expect("Modified file time should be supported on this platform")
                            .into();
                        let size = usize::try_from(metadata.len())
                            .context(FileSizeOverflowedUsize { path: entry.path() })?;

                        objects.push(ObjectMeta {
                            location,
                            last_modified,
                            size,
                        });
                    }
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

#[async_trait]
impl Cache for File {
    fn evict(&self, _path: &Path) -> crate::cache::Result<()> {
        todo!()
    }

    async fn fs_path_or_cache(
        &self,
        _path: &Path,
        _store: Arc<ObjectStore>,
    ) -> crate::cache::Result<&str> {
        todo!()
    }

    fn size(&self) -> u64 {
        todo!()
    }

    fn limit(&self) -> u64 {
        todo!()
    }
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
                }),
            },
            None => Err(Error::UnableToWalkDir {
                source: walkdir_err,
            }),
        },
    }
}

impl File {
    /// Create new filesystem storage.
    pub fn new(root: impl Into<PathBuf>) -> Self {
        Self {
            root: FilePath::raw(root, true),
        }
    }

    /// Return full path of the given location
    pub fn path(&self, location: &FilePath) -> PathBuf {
        let mut path = self.root.clone();
        path.push_path(location);
        path.to_raw()
    }
}

#[cfg(test)]
mod tests {
    use std::{fs::set_permissions, os::unix::prelude::PermissionsExt};

    use super::*;

    use crate::{
        tests::{list_with_delimiter, put_get_delete_list},
        ObjectStore, ObjectStoreApi, ObjectStorePath,
    };
    use tempfile::TempDir;

    #[tokio::test]
    async fn file_test() {
        let root = TempDir::new().unwrap();
        let integration = ObjectStore::new_file(root.path());

        put_get_delete_list(&integration).await.unwrap();
        list_with_delimiter(&integration).await.unwrap();
    }

    #[tokio::test]
    async fn creates_dir_if_not_present() {
        let root = TempDir::new().unwrap();
        let integration = ObjectStore::new_file(root.path());

        let mut location = integration.new_path();
        location.push_all_dirs(&["nested", "file", "test_file"]);

        let data = Bytes::from("arbitrary data");
        let expected_data = data.clone();

        integration.put(&location, data).await.unwrap();

        let read_data = integration
            .get(&location)
            .await
            .unwrap()
            .map_ok(|b| bytes::BytesMut::from(&b[..]))
            .try_concat()
            .await
            .unwrap();
        assert_eq!(&*read_data, expected_data);
    }

    #[tokio::test]
    async fn unknown_length() {
        let root = TempDir::new().unwrap();
        let integration = ObjectStore::new_file(root.path());

        let mut location = integration.new_path();
        location.set_file_name("some_file");

        let data = Bytes::from("arbitrary data");
        let expected_data = data.clone();

        integration.put(&location, data).await.unwrap();

        let read_data = integration
            .get(&location)
            .await
            .unwrap()
            .map_ok(|b| bytes::BytesMut::from(&b[..]))
            .try_concat()
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

        let store = File::new(root.path());

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
        assert!(store.list_with_delimiter(&store.new_path()).await.is_err());
    }
}
