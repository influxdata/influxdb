//! This module contains the IOx implementation for using local disk as the
//! object store.
use crate::{
    path::file::FilePath, DataDoesNotMatchLength, ListResult, ObjectStoreApi, Result,
    UnableToCopyDataToFile, UnableToCreateDir, UnableToCreateFile, UnableToDeleteFile,
    UnableToOpenFile, UnableToPutDataInMemory, UnableToReadBytes,
};
use async_trait::async_trait;
use bytes::Bytes;
use futures::{
    stream::{self, BoxStream},
    Stream, StreamExt, TryStreamExt,
};
use snafu::{ensure, futures::TryStreamExt as _, OptionExt, ResultExt};
use std::{io, path::PathBuf};
use tokio::fs;
use tokio_util::codec::{BytesCodec, FramedRead};
use walkdir::WalkDir;

/// Local filesystem storage suitable for testing or for opting out of using a
/// cloud storage provider.
#[derive(Debug)]
pub struct File {
    root: FilePath,
}

#[async_trait]
impl ObjectStoreApi for File {
    type Path = FilePath;

    fn new_path(&self) -> Self::Path {
        FilePath::default()
    }

    async fn put<S>(&self, location: &Self::Path, bytes: S, length: usize) -> Result<()>
    where
        S: Stream<Item = io::Result<Bytes>> + Send + Sync + 'static,
    {
        let content = bytes
            .map_ok(|b| bytes::BytesMut::from(&b[..]))
            .try_concat()
            .await
            .context(UnableToPutDataInMemory)?;

        ensure!(
            content.len() == length,
            DataDoesNotMatchLength {
                actual: content.len(),
                expected: length,
            }
        );

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
            .context(UnableToReadBytes { path });
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

        let s = walkdir.into_iter().filter_map(move |result_dir_entry| {
            result_dir_entry
                .ok()
                .filter(|dir_entry| dir_entry.file_type().is_file())
                .map(|file| {
                    let relative_path = file.path().strip_prefix(&root_path).expect(
                        "Must start with root path because this came from walking the root",
                    );
                    FilePath::raw(relative_path)
                })
                .filter(|name| prefix.map_or(true, |p| name.prefix_matches(p)))
                .map(|name| Ok(vec![name]))
        });

        Ok(stream::iter(s).boxed())
    }

    async fn list_with_delimiter(&self, _prefix: &Self::Path) -> Result<ListResult<Self::Path>> {
        unimplemented!()
    }
}

impl File {
    /// Create new filesystem storage.
    pub fn new(root: impl Into<PathBuf>) -> Self {
        Self {
            root: FilePath::raw(root),
        }
    }

    fn path(&self, location: &FilePath) -> PathBuf {
        let mut path = self.root.clone();
        path.push_path(location);
        path.to_raw()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    type TestError = Box<dyn std::error::Error + Send + Sync + 'static>;
    type Result<T, E = TestError> = std::result::Result<T, E>;

    use tempfile::TempDir;

    use crate::{tests::put_get_delete_list, Error, ObjectStoreApi, ObjectStorePath};
    use futures::stream;

    #[tokio::test]
    async fn file_test() -> Result<()> {
        let root = TempDir::new()?;
        let integration = File::new(root.path());

        put_get_delete_list(&integration).await?;
        Ok(())
    }

    #[tokio::test]
    async fn length_mismatch_is_an_error() -> Result<()> {
        let root = TempDir::new()?;
        let integration = File::new(root.path());

        let bytes = stream::once(async { Ok(Bytes::from("hello world")) });
        let mut location = integration.new_path();
        location.set_file_name("junk");
        let res = integration.put(&location, bytes, 0).await;

        assert!(matches!(
            res.err().unwrap(),
            Error::DataDoesNotMatchLength {
                expected: 0,
                actual: 11,
            }
        ));

        Ok(())
    }

    #[tokio::test]
    async fn creates_dir_if_not_present() -> Result<()> {
        let root = TempDir::new()?;
        let integration = File::new(root.path());

        let data = Bytes::from("arbitrary data");
        let mut location = integration.new_path();
        location.push_all_dirs(&["nested", "file", "test_file"]);

        let stream_data = std::io::Result::Ok(data.clone());
        integration
            .put(
                &location,
                futures::stream::once(async move { stream_data }),
                data.len(),
            )
            .await?;

        let read_data = integration
            .get(&location)
            .await?
            .map_ok(|b| bytes::BytesMut::from(&b[..]))
            .try_concat()
            .await?;
        assert_eq!(&*read_data, data);

        Ok(())
    }
}
