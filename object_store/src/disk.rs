//! This module contains the IOx implementation for using local disk as the
//! object store.
use crate::{
    DataDoesNotMatchLength, Result, UnableToCopyDataToFile, UnableToCreateDir, UnableToCreateFile,
    UnableToDeleteFile, UnableToGetFileName, UnableToListDirectory, UnableToOpenFile,
    UnableToProcessEntry, UnableToPutDataInMemory, UnableToReadBytes,
};
use bytes::Bytes;
use futures::{Stream, TryStreamExt};
use snafu::{ensure, futures::TryStreamExt as _, OptionExt, ResultExt};
use std::{io, path::PathBuf};
use tokio::fs;
use tokio_util::codec::{BytesCodec, FramedRead};

/// Local filesystem storage suitable for testing or for opting out of using a
/// cloud storage provider.
#[derive(Debug)]
pub struct File {
    root: PathBuf,
}

impl File {
    /// Create new filesystem storage.
    pub fn new(root: impl Into<PathBuf>) -> Self {
        Self { root: root.into() }
    }

    fn path(&self, location: &str) -> PathBuf {
        self.root.join(location)
    }

    /// Save the provided bytes to the specified location.
    pub async fn put<S>(&self, location: &str, bytes: S, length: usize) -> Result<()>
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

    /// Return the bytes that are stored at the specified location.
    pub async fn get(&self, location: &str) -> Result<impl Stream<Item = Result<Bytes>>> {
        let path = self.path(location);

        let file = fs::File::open(&path)
            .await
            .context(UnableToOpenFile { path: &path })?;

        let s = FramedRead::new(file, BytesCodec::new())
            .map_ok(|b| b.freeze())
            .context(UnableToReadBytes { path });
        Ok(s)
    }

    /// Delete the object at the specified location.
    pub async fn delete(&self, location: &str) -> Result<()> {
        let path = self.path(location);
        fs::remove_file(&path)
            .await
            .context(UnableToDeleteFile { path })?;
        Ok(())
    }

    /// List all the objects with the given prefix.
    pub async fn list<'a>(
        &'a self,
        prefix: Option<&'a str>,
    ) -> Result<impl Stream<Item = Result<Vec<String>>> + 'a> {
        let dirs = fs::read_dir(&self.root)
            .await
            .context(UnableToListDirectory { path: &self.root })?;

        let s = dirs
            .context(UnableToProcessEntry)
            .and_then(|entry| {
                let name = entry
                    .file_name()
                    .into_string()
                    .ok()
                    .context(UnableToGetFileName);
                async move { name }
            })
            .try_filter(move |name| {
                let matches = prefix.map_or(true, |p| name.starts_with(p));
                async move { matches }
            })
            .map_ok(|name| vec![name]);
        Ok(s)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    type TestError = Box<dyn std::error::Error + Send + Sync + 'static>;
    type Result<T, E = TestError> = std::result::Result<T, E>;

    use tempfile::TempDir;

    use crate::{tests::put_get_delete_list, Error, ObjectStore};
    use futures::stream;

    #[tokio::test]
    async fn file_test() -> Result<()> {
        let root = TempDir::new()?;
        let integration = ObjectStore::new_file(File::new(root.path()));

        put_get_delete_list(&integration).await?;
        Ok(())
    }

    #[tokio::test]
    async fn length_mismatch_is_an_error() -> Result<()> {
        let root = TempDir::new()?;
        let integration = ObjectStore::new_file(File::new(root.path()));

        let bytes = stream::once(async { Ok(Bytes::from("hello world")) });
        let res = integration.put("junk", bytes, 0).await;

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
        let storage = ObjectStore::new_file(File::new(root.path()));

        let data = Bytes::from("arbitrary data");
        let location = "nested/file/test_file";

        let stream_data = std::io::Result::Ok(data.clone());
        storage
            .put(
                location,
                futures::stream::once(async move { stream_data }),
                data.len(),
            )
            .await?;

        let read_data = storage
            .get(location)
            .await?
            .map_ok(|b| bytes::BytesMut::from(&b[..]))
            .try_concat()
            .await?;
        assert_eq!(&*read_data, data);

        Ok(())
    }
}
