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
mod aws;
#[cfg(feature = "azure")]
mod azure;
mod buffer;
mod disk;
#[cfg(feature = "gcp")]
mod gcp;
mod memory;
pub mod path;
mod throttle;

pub mod cache;
pub mod dummy;

#[cfg(not(feature = "aws"))]
use dummy as aws;
#[cfg(not(feature = "azure"))]
use dummy as azure;
#[cfg(not(feature = "gcp"))]
use dummy as gcp;

use aws::AmazonS3;
use azure::MicrosoftAzure;
use disk::File;
use gcp::GoogleCloudStorage;
use memory::InMemory;
use path::{parsed::DirsAndFileName, ObjectStorePath};
use throttle::ThrottledStore;

/// Publically expose throttling configuration
pub use throttle::ThrottleConfig;

use crate::{
    cache::{Cache, LocalFSCache},
    path::Path,
};
use async_trait::async_trait;
use bytes::Bytes;
use chrono::{DateTime, Utc};
use futures::{stream::BoxStream, StreamExt, TryFutureExt, TryStreamExt};
use snafu::{ResultExt, Snafu};
use std::{path::PathBuf, sync::Arc};

/// Universal API to multiple object store services.
#[async_trait]
pub trait ObjectStoreApi: Send + Sync + 'static {
    /// The type of the locations used in interacting with this object store.
    type Path: path::ObjectStorePath;

    /// The error returned from fallible methods
    type Error: std::error::Error + Send + Sync + 'static;

    /// Return a new location path appropriate for this object storage
    fn new_path(&self) -> Self::Path;

    /// Save the provided bytes to the specified location.
    async fn put(&self, location: &Self::Path, bytes: Bytes) -> Result<(), Self::Error>;

    /// Return the bytes that are stored at the specified location.
    async fn get(
        &self,
        location: &Self::Path,
    ) -> Result<BoxStream<'static, Result<Bytes, Self::Error>>, Self::Error>;

    /// Delete the object at the specified location.
    async fn delete(&self, location: &Self::Path) -> Result<(), Self::Error>;

    /// List all the objects with the given prefix.
    async fn list<'a>(
        &'a self,
        prefix: Option<&'a Self::Path>,
    ) -> Result<BoxStream<'a, Result<Vec<Self::Path>, Self::Error>>, Self::Error>;

    /// List objects with the given prefix and an implementation specific
    /// delimiter. Returns common prefixes (directories) in addition to object
    /// metadata.
    async fn list_with_delimiter(
        &self,
        prefix: &Self::Path,
    ) -> Result<ListResult<Self::Path>, Self::Error>;
}

/// Universal interface to multiple object store services.
#[derive(Debug)]
pub struct ObjectStore {
    /// The object store
    pub integration: ObjectStoreIntegration,
    cache: Option<ObjectStoreFileCache>,
}

impl ObjectStore {
    /// Configure a connection to Amazon S3.
    pub fn new_amazon_s3(
        access_key_id: Option<impl Into<String>>,
        secret_access_key: Option<impl Into<String>>,
        region: impl Into<String>,
        bucket_name: impl Into<String>,
        endpoint: Option<impl Into<String>>,
        session_token: Option<impl Into<String>>,
    ) -> Result<Self> {
        let s3 = aws::new_s3(
            access_key_id,
            secret_access_key,
            region,
            bucket_name,
            endpoint,
            session_token,
        )?;
        Ok(Self {
            integration: ObjectStoreIntegration::AmazonS3(s3),
            cache: None,
        })
    }

    /// Configure a connection to Google Cloud Storage.
    pub fn new_google_cloud_storage(
        service_account_path: impl AsRef<std::ffi::OsStr>,
        bucket_name: impl Into<String>,
    ) -> Result<Self> {
        let gcs = gcp::new_gcs(service_account_path, bucket_name)?;
        Ok(Self {
            integration: ObjectStoreIntegration::GoogleCloudStorage(gcs),
            cache: None,
        })
    }

    /// Configure in-memory storage.
    pub fn new_in_memory() -> Self {
        let in_mem = InMemory::new();
        Self {
            integration: ObjectStoreIntegration::InMemory(in_mem),
            cache: None,
        }
    }

    /// For Testing: Configure throttled in-memory storage.
    pub fn new_in_memory_throttled(config: ThrottleConfig) -> Self {
        let in_mem = InMemory::new();
        let in_mem_throttled = ThrottledStore::new(in_mem, config);
        Self {
            integration: ObjectStoreIntegration::InMemoryThrottled(in_mem_throttled),
            cache: None,
        }
    }

    /// For Testing: Configure a object store with invalid credentials
    /// that will always fail on operations (hopefully)
    pub fn new_failing_store() -> Result<Self> {
        let s3 = aws::new_failing_s3()?;
        Ok(Self {
            integration: ObjectStoreIntegration::AmazonS3(s3),
            cache: None,
        })
    }

    /// Configure local file storage, rooted at `root`
    pub fn new_file(root: impl Into<PathBuf>) -> Self {
        let file = File::new(root);
        Self {
            integration: ObjectStoreIntegration::File(file),
            cache: None,
        }
    }

    /// Configure a connection to Microsoft Azure Blob store.
    pub fn new_microsoft_azure(
        account: impl Into<String>,
        access_key: impl Into<String>,
        container_name: impl Into<String>,
    ) -> Result<Self> {
        let azure = azure::new_azure(account, access_key, container_name)?;
        Ok(Self {
            integration: ObjectStoreIntegration::MicrosoftAzure(Box::new(azure)),
            cache: None,
        })
    }

    /// Create implementation-specific path from parsed representation.
    pub fn path_from_dirs_and_filename(&self, path: DirsAndFileName) -> path::Path {
        use ObjectStoreIntegration::*;
        match &self.integration {
            AmazonS3(_) => path::Path::AmazonS3(path.into()),
            GoogleCloudStorage(_) => path::Path::GoogleCloudStorage(path.into()),
            InMemory(_) => path::Path::InMemory(path),
            InMemoryThrottled(_) => path::Path::InMemory(path),
            File(_) => path::Path::File(path.into()),
            MicrosoftAzure(_) => path::Path::MicrosoftAzure(path.into()),
        }
    }

    /// Returns the filesystem cache if configured
    pub fn cache(&self) -> &Option<ObjectStoreFileCache> {
        &self.cache
    }
}

#[async_trait]
impl ObjectStoreApi for ObjectStore {
    type Path = path::Path;
    type Error = Error;

    fn new_path(&self) -> Self::Path {
        use ObjectStoreIntegration::*;
        match &self.integration {
            AmazonS3(s3) => path::Path::AmazonS3(s3.new_path()),
            GoogleCloudStorage(gcs) => path::Path::GoogleCloudStorage(gcs.new_path()),
            InMemory(in_mem) => path::Path::InMemory(in_mem.new_path()),
            InMemoryThrottled(in_mem_throttled) => {
                path::Path::InMemory(in_mem_throttled.new_path())
            }
            File(file) => path::Path::File(file.new_path()),
            MicrosoftAzure(azure) => path::Path::MicrosoftAzure(azure.new_path()),
        }
    }

    async fn put(&self, location: &Self::Path, bytes: Bytes) -> Result<()> {
        use ObjectStoreIntegration::*;
        match (&self.integration, location) {
            (AmazonS3(s3), path::Path::AmazonS3(location)) => s3.put(location, bytes).await?,
            (GoogleCloudStorage(gcs), path::Path::GoogleCloudStorage(location)) => gcs
                .put(location, bytes)
                .await
                .context(GcsObjectStoreError)?,
            (InMemory(in_mem), path::Path::InMemory(location)) => {
                in_mem.put(location, bytes).await?
            }
            (InMemoryThrottled(in_mem_throttled), path::Path::InMemory(location)) => {
                in_mem_throttled.put(location, bytes).await?
            }
            (File(file), path::Path::File(location)) => file
                .put(location, bytes)
                .await
                .context(FileObjectStoreError)?,
            (MicrosoftAzure(azure), path::Path::MicrosoftAzure(location)) => {
                azure.put(location, bytes).await?
            }
            _ => unreachable!(),
        }

        Ok(())
    }

    async fn get(&self, location: &Self::Path) -> Result<BoxStream<'static, Result<Bytes>>> {
        use ObjectStoreIntegration::*;
        Ok(match (&self.integration, location) {
            (AmazonS3(s3), path::Path::AmazonS3(location)) => {
                s3.get(location).await?.err_into().boxed()
            }
            (GoogleCloudStorage(gcs), path::Path::GoogleCloudStorage(location)) => {
                gcs.get(location).await?.err_into().boxed()
            }
            (InMemory(in_mem), path::Path::InMemory(location)) => {
                in_mem.get(location).await?.err_into().boxed()
            }
            (InMemoryThrottled(in_mem_throttled), path::Path::InMemory(location)) => {
                in_mem_throttled.get(location).await?.err_into().boxed()
            }
            (File(file), path::Path::File(location)) => file
                .get(location)
                .await
                .context(FileObjectStoreError)?
                .err_into()
                .boxed(),
            (MicrosoftAzure(azure), path::Path::MicrosoftAzure(location)) => {
                azure.get(location).await?.err_into().boxed()
            }
            _ => unreachable!(),
        })
    }

    async fn delete(&self, location: &Self::Path) -> Result<()> {
        use ObjectStoreIntegration::*;
        match (&self.integration, location) {
            (AmazonS3(s3), path::Path::AmazonS3(location)) => s3.delete(location).await?,
            (GoogleCloudStorage(gcs), path::Path::GoogleCloudStorage(location)) => {
                gcs.delete(location).await?
            }
            (InMemory(in_mem), path::Path::InMemory(location)) => in_mem.delete(location).await?,
            (InMemoryThrottled(in_mem_throttled), path::Path::InMemory(location)) => {
                in_mem_throttled.delete(location).await?
            }
            (File(file), path::Path::File(location)) => file.delete(location).await?,
            (MicrosoftAzure(azure), path::Path::MicrosoftAzure(location)) => {
                azure.delete(location).await?
            }
            _ => unreachable!(),
        }

        Ok(())
    }

    async fn list<'a>(
        &'a self,
        prefix: Option<&'a Self::Path>,
    ) -> Result<BoxStream<'a, Result<Vec<Self::Path>>>> {
        use ObjectStoreIntegration::*;
        Ok(match (&self.integration, prefix) {
            (AmazonS3(s3), Some(path::Path::AmazonS3(prefix))) => s3
                .list(Some(prefix))
                .await?
                .map_ok(|s| s.into_iter().map(path::Path::AmazonS3).collect())
                .err_into()
                .boxed(),
            (AmazonS3(s3), None) => s3
                .list(None)
                .await?
                .map_ok(|s| s.into_iter().map(path::Path::AmazonS3).collect())
                .err_into()
                .boxed(),

            (GoogleCloudStorage(gcs), Some(path::Path::GoogleCloudStorage(prefix))) => gcs
                .list(Some(prefix))
                .await?
                .map_ok(|s| s.into_iter().map(path::Path::GoogleCloudStorage).collect())
                .err_into()
                .boxed(),
            (GoogleCloudStorage(gcs), None) => gcs
                .list(None)
                .await?
                .map_ok(|s| s.into_iter().map(path::Path::GoogleCloudStorage).collect())
                .err_into()
                .boxed(),

            (InMemory(in_mem), Some(path::Path::InMemory(prefix))) => in_mem
                .list(Some(prefix))
                .await?
                .map_ok(|s| s.into_iter().map(path::Path::InMemory).collect())
                .err_into()
                .boxed(),
            (InMemory(in_mem), None) => in_mem
                .list(None)
                .await?
                .map_ok(|s| s.into_iter().map(path::Path::InMemory).collect())
                .err_into()
                .boxed(),

            (InMemoryThrottled(in_mem_throttled), Some(path::Path::InMemory(prefix))) => {
                in_mem_throttled
                    .list(Some(prefix))
                    .await?
                    .map_ok(|s| s.into_iter().map(path::Path::InMemory).collect())
                    .err_into()
                    .boxed()
            }
            (InMemoryThrottled(in_mem_throttled), None) => in_mem_throttled
                .list(None)
                .await?
                .map_ok(|s| s.into_iter().map(path::Path::InMemory).collect())
                .err_into()
                .boxed(),

            (File(file), Some(path::Path::File(prefix))) => file
                .list(Some(prefix))
                .await?
                .map_ok(|s| s.into_iter().map(path::Path::File).collect())
                .err_into()
                .boxed(),
            (File(file), None) => file
                .list(None)
                .await?
                .map_ok(|s| s.into_iter().map(path::Path::File).collect())
                .err_into()
                .boxed(),

            (MicrosoftAzure(azure), Some(path::Path::MicrosoftAzure(prefix))) => azure
                .list(Some(prefix))
                .await?
                .map_ok(|s| s.into_iter().map(path::Path::MicrosoftAzure).collect())
                .err_into()
                .boxed(),
            (MicrosoftAzure(azure), None) => azure
                .list(None)
                .await?
                .map_ok(|s| s.into_iter().map(path::Path::MicrosoftAzure).collect())
                .err_into()
                .boxed(),
            _ => unreachable!(),
        })
    }

    async fn list_with_delimiter(&self, prefix: &Self::Path) -> Result<ListResult<Self::Path>> {
        use ObjectStoreIntegration::*;
        match (&self.integration, prefix) {
            (AmazonS3(s3), path::Path::AmazonS3(prefix)) => s3
                .list_with_delimiter(prefix)
                .map_ok(|list_result| list_result.map_paths(path::Path::AmazonS3))
                .await
                .context(AwsObjectStoreError),
            (GoogleCloudStorage(gcs), path::Path::GoogleCloudStorage(prefix)) => gcs
                .list_with_delimiter(prefix)
                .map_ok(|list_result| list_result.map_paths(path::Path::GoogleCloudStorage))
                .await
                .context(GcsObjectStoreError),
            (InMemory(in_mem), path::Path::InMemory(prefix)) => in_mem
                .list_with_delimiter(prefix)
                .map_ok(|list_result| list_result.map_paths(path::Path::InMemory))
                .await
                .context(InMemoryObjectStoreError),
            (InMemoryThrottled(in_mem_throttled), path::Path::InMemory(prefix)) => in_mem_throttled
                .list_with_delimiter(prefix)
                .map_ok(|list_result| list_result.map_paths(path::Path::InMemory))
                .await
                .context(InMemoryObjectStoreError),
            (File(file), path::Path::File(prefix)) => file
                .list_with_delimiter(prefix)
                .map_ok(|list_result| list_result.map_paths(path::Path::File))
                .await
                .context(FileObjectStoreError),
            (MicrosoftAzure(azure), path::Path::MicrosoftAzure(prefix)) => azure
                .list_with_delimiter(prefix)
                .map_ok(|list_result| list_result.map_paths(path::Path::MicrosoftAzure))
                .await
                .context(AzureObjectStoreError),
            _ => unreachable!(),
        }
    }
}

/// All supported object storage integrations
#[derive(Debug)]
pub enum ObjectStoreIntegration {
    /// GCP storage
    GoogleCloudStorage(GoogleCloudStorage),
    /// Amazon storage
    AmazonS3(AmazonS3),
    /// In memory storage for testing
    InMemory(InMemory),
    /// Throttled in memory storage for testing
    InMemoryThrottled(ThrottledStore<InMemory>),
    /// Local file system storage
    File(File),
    /// Microsoft Azure Blob storage
    MicrosoftAzure(Box<MicrosoftAzure>),
}

/// Cache wrapper so local file object store can pass through to its implementation
/// while others use the `LocalFSCache`.
#[derive(Debug)]
pub enum ObjectStoreFileCache {
    /// If using the local filesystem for object store, don't create additional copies for caching
    Passthrough(File),
    /// Remote object stores should use the LocalFSCache implementation
    File(LocalFSCache),
}

#[async_trait]
impl Cache for ObjectStoreFileCache {
    fn evict(&self, path: &Path) -> crate::cache::Result<()> {
        match &self {
            Self::Passthrough(f) => f.evict(path),
            Self::File(f) => f.evict(path),
        }
    }

    async fn fs_path_or_cache(
        &self,
        path: &Path,
        store: Arc<ObjectStore>,
    ) -> crate::cache::Result<&str> {
        match &self {
            Self::Passthrough(f) => f.fs_path_or_cache(path, store).await,
            Self::File(f) => f.fs_path_or_cache(path, store).await,
        }
    }

    fn size(&self) -> u64 {
        match &self {
            Self::Passthrough(f) => f.size(),
            Self::File(f) => f.size(),
        }
    }

    fn limit(&self) -> u64 {
        match &self {
            Self::Passthrough(f) => f.size(),
            Self::File(f) => f.size(),
        }
    }
}

/// Result of a list call that includes objects, prefixes (directories) and a
/// token for the next set of results. Individual result sets may be limited to
/// 1,000 objects based on the underlying object storage's limitations.
#[derive(Debug)]
pub struct ListResult<P: ObjectStorePath> {
    /// Token passed to the API for the next page of list results.
    pub next_token: Option<String>,
    /// Prefixes that are common (like directories)
    pub common_prefixes: Vec<P>,
    /// Object metadata for the listing
    pub objects: Vec<ObjectMeta<P>>,
}

#[allow(clippy::use_self)] // https://github.com/rust-lang/rust-clippy/issues/3410
impl<P: ObjectStorePath> ListResult<P> {
    /// `c` is a function that can turn one type that implements an
    /// `ObjectStorePath` to another type that also implements
    /// `ObjectStorePath`.
    fn map_paths<Q: ObjectStorePath, C>(self, c: C) -> ListResult<Q>
    where
        C: Fn(P) -> Q,
    {
        let Self {
            next_token,
            common_prefixes,
            objects,
        } = self;

        ListResult {
            next_token,
            common_prefixes: common_prefixes.into_iter().map(&c).collect(),
            objects: objects.into_iter().map(|o| o.map_paths(&c)).collect(),
        }
    }
}

/// The metadata that describes an object.
#[derive(Debug)]
pub struct ObjectMeta<P: ObjectStorePath> {
    /// The full path to the object
    pub location: P,
    /// The last modified time
    pub last_modified: DateTime<Utc>,
    /// The size in bytes of the object
    pub size: usize,
}

#[allow(clippy::use_self)] // https://github.com/rust-lang/rust-clippy/issues/3410
impl<P: ObjectStorePath> ObjectMeta<P> {
    /// `c` is a function that can turn one type that implements an
    /// `ObjectStorePath` to another type that also implements
    /// `ObjectStorePath`.
    fn map_paths<Q: ObjectStorePath, C>(self, c: C) -> ObjectMeta<Q>
    where
        C: Fn(P) -> Q,
    {
        let Self {
            location,
            last_modified,
            size,
        } = self;

        ObjectMeta {
            location: c(location),
            last_modified,
            size,
        }
    }
}

/// A specialized `Result` for object store-related errors
pub type Result<T, E = Error> = std::result::Result<T, E>;

/// A specialized `Error` for object store-related errors
#[derive(Debug, Snafu)]
#[allow(missing_docs)]
pub enum Error {
    #[snafu(display("File-based Object Store error: {}", source))]
    FileObjectStoreError { source: disk::Error },

    #[snafu(display("Google Cloud Storage-based Object Store error: {}", source))]
    GcsObjectStoreError { source: gcp::Error },

    #[snafu(display("AWS S3-based Object Store error: {}", source))]
    AwsObjectStoreError { source: aws::Error },

    #[snafu(display("Azure Blob storage-based Object Store error: {}", source))]
    AzureObjectStoreError { source: azure::Error },

    #[snafu(display("In-memory-based Object Store error: {}", source))]
    InMemoryObjectStoreError { source: memory::Error },

    #[snafu(display("{}", source))]
    DummyObjectStoreError { source: dummy::Error },
}

impl From<disk::Error> for Error {
    fn from(source: disk::Error) -> Self {
        Self::FileObjectStoreError { source }
    }
}

#[cfg(feature = "gcp")]
impl From<gcp::Error> for Error {
    fn from(source: gcp::Error) -> Self {
        Self::GcsObjectStoreError { source }
    }
}

#[cfg(feature = "aws")]
impl From<aws::Error> for Error {
    fn from(source: aws::Error) -> Self {
        Self::AwsObjectStoreError { source }
    }
}

#[cfg(feature = "azure")]
impl From<azure::Error> for Error {
    fn from(source: azure::Error) -> Self {
        Self::AzureObjectStoreError { source }
    }
}

impl From<memory::Error> for Error {
    fn from(source: memory::Error) -> Self {
        Self::InMemoryObjectStoreError { source }
    }
}

impl From<dummy::Error> for Error {
    fn from(source: dummy::Error) -> Self {
        Self::DummyObjectStoreError { source }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::path::{cloud::CloudPath, parsed::DirsAndFileName, ObjectStorePath};

    use futures::stream;

    type Error = Box<dyn std::error::Error + Send + Sync + 'static>;
    type Result<T, E = Error> = std::result::Result<T, E>;

    async fn flatten_list_stream(
        storage: &ObjectStore,
        prefix: Option<&path::Path>,
    ) -> Result<Vec<path::Path>> {
        storage
            .list(prefix)
            .await?
            .map_ok(|v| stream::iter(v).map(Ok))
            .try_flatten()
            .try_collect()
            .await
    }

    pub(crate) async fn put_get_delete_list(storage: &ObjectStore) -> Result<()> {
        delete_fixtures(storage).await;

        let content_list = flatten_list_stream(storage, None).await?;
        assert!(
            content_list.is_empty(),
            "Expected list to be empty; found: {:?}",
            content_list
        );

        let mut location = storage.new_path();
        location.push_dir("test_dir");
        location.set_file_name("test_file.json");

        let data = Bytes::from("arbitrary data");
        let expected_data = data.clone();
        storage.put(&location, data).await?;

        // List everything
        let content_list = flatten_list_stream(storage, None).await?;
        assert_eq!(content_list, &[location.clone()]);

        // List everything starting with a prefix that should return results
        let mut prefix = storage.new_path();
        prefix.push_dir("test_dir");
        let content_list = flatten_list_stream(storage, Some(&prefix)).await?;
        assert_eq!(content_list, &[location.clone()]);

        // List everything starting with a prefix that shouldn't return results
        let mut prefix = storage.new_path();
        prefix.push_dir("something");
        let content_list = flatten_list_stream(storage, Some(&prefix)).await?;
        assert!(content_list.is_empty());

        let read_data = storage
            .get(&location)
            .await?
            .map_ok(|b| bytes::BytesMut::from(&b[..]))
            .try_concat()
            .await?;
        assert_eq!(&*read_data, expected_data);

        storage.delete(&location).await?;

        let content_list = flatten_list_stream(storage, None).await?;
        assert!(content_list.is_empty());

        Ok(())
    }

    pub(crate) async fn list_with_delimiter(storage: &ObjectStore) -> Result<()> {
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
        .map(|&s| str_to_path(storage, s))
        .collect();

        for f in &files {
            let data = data.clone();
            storage.put(f, data).await.unwrap();
        }

        // ==================== check: prefix-list `mydb/wb` (directory) ====================
        let mut prefix = storage.new_path();
        prefix.push_all_dirs(&["mydb", "wb"]);

        let mut expected_000 = prefix.clone();
        expected_000.push_dir("000");
        let mut expected_001 = prefix.clone();
        expected_001.push_dir("001");
        let mut expected_location = prefix.clone();
        expected_location.set_file_name("foo.json");

        let result = storage.list_with_delimiter(&prefix).await.unwrap();

        assert_eq!(result.common_prefixes, vec![expected_000, expected_001]);
        assert_eq!(result.objects.len(), 1);

        let object = &result.objects[0];

        assert_eq!(object.location, expected_location);
        assert_eq!(object.size, data.len());

        // ==================== check: prefix-list `mydb/wb/000/000/001` (partial filename) ====================
        let mut prefix = storage.new_path();
        prefix.push_all_dirs(&["mydb", "wb", "000", "000"]);
        prefix.set_file_name("001");

        let mut expected_location = storage.new_path();
        expected_location.push_all_dirs(&["mydb", "wb", "000", "000"]);
        expected_location.set_file_name("001.segment");

        let result = storage.list_with_delimiter(&prefix).await.unwrap();
        assert!(result.common_prefixes.is_empty());
        assert_eq!(result.objects.len(), 1);

        let object = &result.objects[0];

        assert_eq!(object.location, expected_location);

        // ==================== check: prefix-list `not_there` (non-existing prefix) ====================
        let mut prefix = storage.new_path();
        prefix.push_all_dirs(&["not_there"]);

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
        storage: &ObjectStore,
        location: Option<<ObjectStore as ObjectStoreApi>::Path>,
    ) -> Result<Bytes> {
        let location = location.unwrap_or_else(|| {
            let mut loc = storage.new_path();
            loc.set_file_name("this_file_should_not_exist");
            loc
        });

        let content_list = flatten_list_stream(storage, Some(&location)).await?;
        assert!(content_list.is_empty());

        Ok(storage
            .get(&location)
            .await?
            .map_ok(|b| bytes::BytesMut::from(&b[..]))
            .try_concat()
            .await?
            .freeze())
    }

    /// Parse a str as a `CloudPath` into a `DirAndFileName`, even though the
    /// associated storage might not be cloud storage, to reuse the cloud
    /// path parsing logic. Then convert into the correct type of path for
    /// the given storage.
    fn str_to_path(storage: &ObjectStore, val: &str) -> path::Path {
        let cloud_path = CloudPath::raw(val);
        let parsed: DirsAndFileName = cloud_path.into();

        let mut new_path = storage.new_path();
        for part in parsed.directories {
            new_path.push_dir(part.to_string());
        }

        if let Some(file_name) = parsed.file_name {
            new_path.set_file_name(file_name.to_string());
        }
        new_path
    }

    async fn delete_fixtures(storage: &ObjectStore) {
        let files: Vec<_> = [
            "test_file",
            "mydb/wb/000/000/000.segment",
            "mydb/wb/000/000/001.segment",
            "mydb/wb/000/000/002.segment",
            "mydb/wb/001/001/000.segment",
            "mydb/wb/foo.json",
            "mydb/data/whatevs",
        ]
        .iter()
        .map(|&s| str_to_path(storage, s))
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
