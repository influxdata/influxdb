#![deny(rustdoc::broken_intra_doc_links, rustdoc::bare_urls, rust_2018_idioms)]
#![warn(
    missing_copy_implementations,
    missing_debug_implementations,
    missing_docs,
    clippy::explicit_iter_loop,
    clippy::future_not_send,
    clippy::use_self,
    clippy::clone_on_ref_ptr
)]

//! # object_store
//!
//! This crate provides APIs for interacting with object storage services. It
//! currently supports PUT, GET, DELETE, and list for Google Cloud Storage,
//! Amazon S3, in-memory and local file storage.
//!
//! Future compatibility will include Azure Blob Storage, Minio, and Ceph.

#[cfg(feature = "aws")]
pub mod aws;
#[cfg(feature = "azure")]
pub mod azure;
pub mod disk;
#[cfg(feature = "gcp")]
pub mod gcp;
pub mod memory;
pub mod path;
pub mod throttle;

use crate::path::Path;
use async_trait::async_trait;
use bytes::Bytes;
use chrono::{DateTime, Utc};
use futures::{stream::BoxStream, StreamExt, TryStreamExt};
use snafu::Snafu;
use std::fmt::{Debug, Formatter};

/// An alias for a dynamically dispatched object store implementation.
pub type DynObjectStore = dyn ObjectStoreApi;

/// Universal API to multiple object store services.
#[async_trait]
pub trait ObjectStoreApi: std::fmt::Display + Send + Sync + Debug + 'static {
    /// Save the provided bytes to the specified location.
    async fn put(&self, location: &Path, bytes: Bytes) -> Result<()>;

    /// Return the bytes that are stored at the specified location.
    async fn get(&self, location: &Path) -> Result<GetResult>;

    /// Delete the object at the specified location.
    async fn delete(&self, location: &Path) -> Result<()>;

    /// List all the objects with the given prefix.
    ///
    /// Prefixes are evaluated on a path segment basis, i.e. `foo/bar/` is a prefix of `foo/bar/x` but not of
    /// `foo/bar_baz/x`.
    async fn list<'a>(
        &'a self,
        prefix: Option<&'a Path>,
    ) -> Result<BoxStream<'a, Result<ObjectMeta>>>;

    /// List objects with the given prefix and an implementation specific
    /// delimiter. Returns common prefixes (directories) in addition to object
    /// metadata.
    ///
    /// Prefixes are evaluated on a path segment basis, i.e. `foo/bar/` is a prefix of `foo/bar/x` but not of
    /// `foo/bar_baz/x`.
    async fn list_with_delimiter(&self, prefix: &Path) -> Result<ListResult>;
}

/// Result of a list call that includes objects, prefixes (directories) and a
/// token for the next set of results. Individual result sets may be limited to
/// 1,000 objects based on the underlying object storage's limitations.
#[derive(Debug)]
pub struct ListResult {
    /// Token passed to the API for the next page of list results.
    pub next_token: Option<String>,
    /// Prefixes that are common (like directories)
    pub common_prefixes: Vec<Path>,
    /// Object metadata for the listing
    pub objects: Vec<ObjectMeta>,
}

/// The metadata that describes an object.
#[derive(Debug)]
pub struct ObjectMeta {
    /// The full path to the object
    pub location: Path,
    /// The last modified time
    pub last_modified: DateTime<Utc>,
    /// The size in bytes of the object
    pub size: usize,
}

/// Result for a get request
pub enum GetResult {
    /// A file
    File(tokio::fs::File, std::path::PathBuf),
    /// An asynchronous stream
    Stream(BoxStream<'static, Result<Bytes>>),
}

impl Debug for GetResult {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            GetResult::File(_, _) => write!(f, "GetResult(File)"),
            GetResult::Stream(_) => write!(f, "GetResult(Stream)"),
        }
    }
}

impl GetResult {
    /// Collects the data into a [`Vec<u8>`]
    pub async fn bytes(self) -> Result<Vec<u8>> {
        let mut stream = self.into_stream();
        let mut bytes = Vec::new();

        while let Some(next) = stream.next().await {
            bytes.extend_from_slice(next?.as_ref())
        }

        Ok(bytes)
    }

    /// Converts this into a byte stream
    pub fn into_stream(self) -> BoxStream<'static, Result<Bytes>> {
        match self {
            Self::File(file, path) => {
                tokio_util::codec::FramedRead::new(file, tokio_util::codec::BytesCodec::new())
                    .map_ok(|b| b.freeze())
                    .map_err(move |source| {
                        disk::Error::UnableToReadBytes {
                            source,
                            path: path.clone(),
                        }
                        .into()
                    })
                    .boxed()
            }
            Self::Stream(s) => s,
        }
    }
}

/// A specialized `Result` for object store-related errors
pub type Result<T, E = Error> = std::result::Result<T, E>;

/// A specialized `Error` for object store-related errors
#[derive(Debug, Snafu)]
#[allow(missing_docs)]
pub enum Error {
    #[snafu(display("Generic {} error: {}", store, source))]
    Generic {
        store: &'static str,
        source: Box<dyn std::error::Error + Send + Sync + 'static>,
    },

    #[snafu(display("Object at location {} not found: {}", path, source))]
    NotFound {
        path: String,
        source: Box<dyn std::error::Error + Send + Sync + 'static>,
    },
}

/// Convenience functions for object stores that are only appropriate to use in tests. Not marked
/// with cfg(test) to make this accessible to other crates.
#[async_trait]
pub trait ObjectStoreTestConvenience {
    /// A convenience function for getting all results from a list operation without a prefix. Only
    /// appropriate for tests because production code should handle the stream of potentially a
    /// large number of returned paths.
    async fn list_all(&self) -> Result<Vec<ObjectMeta>>;
}

#[async_trait]
impl ObjectStoreTestConvenience for dyn ObjectStoreApi {
    async fn list_all(&self) -> Result<Vec<ObjectMeta>> {
        self.list(None).await?.try_collect().await
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    type Error = Box<dyn std::error::Error + Send + Sync + 'static>;
    type Result<T, E = Error> = std::result::Result<T, E>;

    async fn flatten_list_stream(
        storage: &DynObjectStore,
        prefix: Option<&Path>,
    ) -> super::Result<Vec<Path>> {
        storage
            .list(prefix)
            .await?
            .map_ok(|meta| meta.location)
            .try_collect::<Vec<Path>>()
            .await
    }

    pub(crate) async fn put_get_delete_list(storage: &DynObjectStore) -> Result<()> {
        delete_fixtures(storage).await;

        let content_list = flatten_list_stream(storage, None).await?;
        assert!(
            content_list.is_empty(),
            "Expected list to be empty; found: {:?}",
            content_list
        );

        let location = Path::from_raw("test_dir/test_file.json");

        let data = Bytes::from("arbitrary data");
        let expected_data = data.clone();
        storage.put(&location, data).await?;

        // List everything
        let content_list = flatten_list_stream(storage, None).await?;
        assert_eq!(content_list, &[location.clone()]);

        // List everything starting with a prefix that should return results
        let prefix = Path::from_raw("test_dir");
        let content_list = flatten_list_stream(storage, Some(&prefix)).await?;
        assert_eq!(content_list, &[location.clone()]);

        // List everything starting with a prefix that shouldn't return results
        let prefix = Path::from_raw("something");
        let content_list = flatten_list_stream(storage, Some(&prefix)).await?;
        assert!(content_list.is_empty());

        let read_data = storage.get(&location).await?.bytes().await?;
        assert_eq!(&*read_data, expected_data);

        storage.delete(&location).await?;

        let content_list = flatten_list_stream(storage, None).await?;
        assert!(content_list.is_empty());

        Ok(())
    }

    pub(crate) async fn list_uses_directories_correctly(storage: &DynObjectStore) -> Result<()> {
        delete_fixtures(storage).await;

        let content_list = flatten_list_stream(storage, None).await?;
        assert!(
            content_list.is_empty(),
            "Expected list to be empty; found: {:?}",
            content_list
        );

        let location1 = Path::from_raw("foo/x.json");
        let location2 = Path::from_raw("foo.bar/y.json");

        let data = Bytes::from("arbitrary data");
        storage.put(&location1, data.clone()).await?;
        storage.put(&location2, data).await?;

        let prefix = Path::from_raw("foo");
        let content_list = flatten_list_stream(storage, Some(&prefix)).await?;
        assert_eq!(content_list, &[location1.clone()]);

        let prefix = Path::from_raw("foo/x");
        let content_list = flatten_list_stream(storage, Some(&prefix)).await?;
        assert_eq!(content_list, &[]);

        Ok(())
    }

    pub(crate) async fn list_with_delimiter(storage: &DynObjectStore) -> Result<()> {
        delete_fixtures(storage).await;

        // ==================== check: store is empty ====================
        let content_list = flatten_list_stream(storage, None).await?;
        assert!(content_list.is_empty());

        // ==================== do: create files ====================
        let data = Bytes::from("arbitrary data");

        let files: Vec<_> = [
            "test_file",
            "mydb/wb/000/000/000.segment",
            "mydb/wb/000/000/001.segment",
            "mydb/wb/000/000/002.segment",
            "mydb/wb/001/001/000.segment",
            "mydb/wb/foo.json",
            "mydb/wbwbwb/111/222/333.segment",
            "mydb/data/whatevs",
        ]
        .iter()
        .map(|&s| Path::from_raw(s))
        .collect();

        for f in &files {
            let data = data.clone();
            storage.put(f, data).await.unwrap();
        }

        // ==================== check: prefix-list `mydb/wb` (directory) ====================
        let prefix = Path::from_raw("mydb/wb");

        let expected_000 = Path::from_raw("mydb/wb/000");
        let expected_001 = Path::from_raw("mydb/wb/001");
        let expected_location = Path::from_raw("mydb/wb/foo.json");

        let result = storage.list_with_delimiter(&prefix).await.unwrap();

        assert_eq!(result.common_prefixes, vec![expected_000, expected_001]);
        assert_eq!(result.objects.len(), 1);

        let object = &result.objects[0];

        assert_eq!(object.location, expected_location);
        assert_eq!(object.size, data.len());

        // ==================== check: prefix-list `mydb/wb/000/000/001` (partial filename doesn't match) ====================
        let prefix = Path::from_raw("mydb/wb/000/000/001");

        let result = storage.list_with_delimiter(&prefix).await.unwrap();
        assert!(result.common_prefixes.is_empty());
        assert_eq!(result.objects.len(), 0);

        // ==================== check: prefix-list `not_there` (non-existing prefix) ====================
        let prefix = Path::from_raw("not_there");

        let result = storage.list_with_delimiter(&prefix).await.unwrap();
        assert!(result.common_prefixes.is_empty());
        assert!(result.objects.is_empty());

        // ==================== do: remove all files ====================
        for f in &files {
            storage.delete(f).await.unwrap();
        }

        // ==================== check: store is empty ====================
        let content_list = flatten_list_stream(storage, None).await?;
        assert!(content_list.is_empty());

        Ok(())
    }

    #[allow(dead_code)]
    pub(crate) async fn get_nonexistent_object(
        storage: &DynObjectStore,
        location: Option<Path>,
    ) -> Result<Vec<u8>> {
        let location = location.unwrap_or_else(|| Path::from_raw("this_file_should_not_exist"));

        let content_list = flatten_list_stream(storage, Some(&location)).await?;
        assert!(content_list.is_empty());

        Ok(storage.get(&location).await?.bytes().await?)
    }

    async fn delete_fixtures(storage: &DynObjectStore) {
        let files: Vec<_> = [
            "test_file",
            "test_dir/test_file.json",
            "mydb/wb/000/000/000.segment",
            "mydb/wb/000/000/001.segment",
            "mydb/wb/000/000/002.segment",
            "mydb/wb/001/001/000.segment",
            "mydb/wb/foo.json",
            "mydb/data/whatevs",
            "mydb/wbwbwb/111/222/333.segment",
            "foo/x.json",
            "foo.bar/y.json",
        ]
        .iter()
        .map(|&s| Path::from_raw(s))
        .collect();

        for f in &files {
            // don't care if it errors, should fail elsewhere
            let _ = storage.delete(f).await;
        }
    }

    // Tests TODO:
    // GET nonexisting location (in_memory/file)
    // DELETE nonexisting location
    // PUT overwriting
}
