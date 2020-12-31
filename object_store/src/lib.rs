#![deny(rust_2018_idioms)]
#![warn(
    missing_copy_implementations,
    missing_debug_implementations,
    missing_docs,
    clippy::explicit_iter_loop,
    clippy::use_self
)]

//! # object_store
//!
//! This crate provides APIs for interacting with object storage services. It
//! currently supports PUT, GET, DELETE, and list for Google Cloud Storage,
//! Amazon S3, in-memory and local file storage.
//!
//! Future compatibility will include Azure Blob Storage, Minio, and Ceph.

pub mod aws;
pub mod disk;
pub mod gcp;
pub mod memory;

use aws::AmazonS3;
use disk::File;
use gcp::GoogleCloudStorage;
use memory::InMemory;

use bytes::Bytes;
use futures::{Stream, StreamExt, TryStreamExt};
use snafu::Snafu;
use std::{io, path::PathBuf};

/// Universal interface to multiple object store services.
#[derive(Debug)]
pub struct ObjectStore(pub ObjectStoreIntegration);

impl ObjectStore {
    /// Configure a connection to Amazon S3.
    pub fn new_amazon_s3(s3: AmazonS3) -> Self {
        Self(ObjectStoreIntegration::AmazonS3(s3))
    }

    /// Configure a connection to Google Cloud Storage.
    pub fn new_google_cloud_storage(gcs: GoogleCloudStorage) -> Self {
        Self(ObjectStoreIntegration::GoogleCloudStorage(gcs))
    }

    /// Configure in-memory storage.
    pub fn new_in_memory(in_mem: InMemory) -> Self {
        Self(ObjectStoreIntegration::InMemory(in_mem))
    }

    /// Configure local file storage.
    pub fn new_file(file: File) -> Self {
        Self(ObjectStoreIntegration::File(file))
    }

    /// Save the provided bytes to the specified location.
    pub async fn put<S>(&self, location: &str, bytes: S, length: usize) -> Result<()>
    where
        S: Stream<Item = io::Result<Bytes>> + Send + Sync + 'static,
    {
        use ObjectStoreIntegration::*;
        match &self.0 {
            AmazonS3(s3) => s3.put(location, bytes, length).await?,
            GoogleCloudStorage(gcs) => gcs.put(location, bytes, length).await?,
            InMemory(in_mem) => in_mem.put(location, bytes, length).await?,
            File(file) => file.put(location, bytes, length).await?,
        }

        Ok(())
    }

    /// Return the bytes that are stored at the specified location.
    pub async fn get(&self, location: &str) -> Result<impl Stream<Item = Result<Bytes>>> {
        use ObjectStoreIntegration::*;
        Ok(match &self.0 {
            AmazonS3(s3) => s3.get(location).await?.boxed(),
            GoogleCloudStorage(gcs) => gcs.get(location).await?.boxed(),
            InMemory(in_mem) => in_mem.get(location).await?.boxed(),
            File(file) => file.get(location).await?.boxed(),
        }
        .err_into())
    }

    /// Delete the object at the specified location.
    pub async fn delete(&self, location: &str) -> Result<()> {
        use ObjectStoreIntegration::*;
        match &self.0 {
            AmazonS3(s3) => s3.delete(location).await?,
            GoogleCloudStorage(gcs) => gcs.delete(location).await?,
            InMemory(in_mem) => in_mem.delete(location).await?,
            File(file) => file.delete(location).await?,
        }

        Ok(())
    }

    /// List all the objects with the given prefix.
    pub async fn list<'a>(
        &'a self,
        prefix: Option<&'a str>,
    ) -> Result<impl Stream<Item = Result<Vec<String>>> + 'a> {
        use ObjectStoreIntegration::*;
        Ok(match &self.0 {
            AmazonS3(s3) => s3.list(prefix).await?.boxed(),
            GoogleCloudStorage(gcs) => gcs.list(prefix).await?.boxed(),
            InMemory(in_mem) => in_mem.list(prefix).await?.boxed(),
            File(file) => file.list(prefix).await?.boxed(),
        }
        .err_into())
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
    /// Local file system storage
    File(File),
}

/// A specialized `Result` for object store-related errors
pub type Result<T, E = Error> = std::result::Result<T, E>;

/// A specialized `Error` for object store-related errors
#[derive(Debug, Snafu)]
#[allow(missing_docs)]
pub enum Error {
    DataDoesNotMatchLength {
        expected: usize,
        actual: usize,
    },

    UnableToPutDataToGcs {
        source: tokio::task::JoinError,
        bucket: String,
        location: String,
    },
    UnableToListDataFromGcs {
        source: tokio::task::JoinError,
        bucket: String,
    },
    UnableToListDataFromGcs2 {
        source: cloud_storage::Error,
        bucket: String,
    },
    UnableToDeleteDataFromGcs {
        source: tokio::task::JoinError,
        bucket: String,
        location: String,
    },
    UnableToDeleteDataFromGcs2 {
        source: cloud_storage::Error,
        bucket: String,
        location: String,
    },
    UnableToGetDataFromGcs {
        source: tokio::task::JoinError,
        bucket: String,
        location: String,
    },
    UnableToGetDataFromGcs2 {
        source: cloud_storage::Error,
        bucket: String,
        location: String,
    },

    UnableToPutDataToS3 {
        source: rusoto_core::RusotoError<rusoto_s3::PutObjectError>,
        bucket: String,
        location: String,
    },
    UnableToGetDataFromS3 {
        source: rusoto_core::RusotoError<rusoto_s3::GetObjectError>,
        bucket: String,
        location: String,
    },
    UnableToDeleteDataFromS3 {
        source: rusoto_core::RusotoError<rusoto_s3::DeleteObjectError>,
        bucket: String,
        location: String,
    },
    NoDataFromS3 {
        bucket: String,
        location: String,
    },
    UnableToReadBytesFromS3 {
        source: std::io::Error,
        bucket: String,
        location: String,
    },
    UnableToGetPieceOfDataFromS3 {
        source: std::io::Error,
        bucket: String,
        location: String,
    },
    UnableToListDataFromS3 {
        source: rusoto_core::RusotoError<rusoto_s3::ListObjectsV2Error>,
        bucket: String,
    },

    UnableToPutDataInMemory {
        source: std::io::Error,
    },
    NoDataInMemory,

    #[snafu(display("Unable to create file {}: {}", path.display(), err))]
    UnableToCreateFile {
        err: io::Error,
        path: PathBuf,
    },
    #[snafu(display("Unable to create dir {}: {}", path.display(), source))]
    UnableToCreateDir {
        source: io::Error,
        path: PathBuf,
    },
    #[snafu(display("Unable to open file {}: {}", path.display(), source))]
    UnableToOpenFile {
        source: io::Error,
        path: PathBuf,
    },
    #[snafu(display("Unable to read data from file {}: {}", path.display(), source))]
    UnableToReadBytes {
        source: io::Error,
        path: PathBuf,
    },
    #[snafu(display("Unable to delete file {}: {}", path.display(), source))]
    UnableToDeleteFile {
        source: io::Error,
        path: PathBuf,
    },
    #[snafu(display("Unable to list directory {}: {}", path.display(), source))]
    UnableToListDirectory {
        source: io::Error,
        path: PathBuf,
    },
    #[snafu(display("Unable to process directory entry: {}", source))]
    UnableToProcessEntry {
        source: io::Error,
    },
    #[snafu(display("Unable to copy data to file: {}", source))]
    UnableToCopyDataToFile {
        source: io::Error,
    },
    #[snafu(display("Unable to retrieve filename"))]
    UnableToGetFileName,
}

#[cfg(test)]
mod tests {
    use super::*;
    use futures::stream;

    type Error = Box<dyn std::error::Error + Send + Sync + 'static>;
    type Result<T, E = Error> = std::result::Result<T, E>;

    async fn flatten_list_stream(
        storage: &ObjectStore,
        prefix: Option<&str>,
    ) -> Result<Vec<String>> {
        storage
            .list(prefix)
            .await?
            .map_ok(|v| stream::iter(v).map(Ok))
            .try_flatten()
            .try_collect()
            .await
    }

    pub(crate) async fn put_get_delete_list(storage: &ObjectStore) -> Result<()> {
        let content_list = flatten_list_stream(storage, None).await?;
        assert!(content_list.is_empty());

        let data = Bytes::from("arbitrary data");
        let location = "test_file";

        let stream_data = std::io::Result::Ok(data.clone());
        storage
            .put(
                location,
                futures::stream::once(async move { stream_data }),
                data.len(),
            )
            .await?;

        // List everything
        let content_list = flatten_list_stream(storage, None).await?;
        assert_eq!(content_list, &[location]);

        // List everything starting with a prefix that should return results
        let content_list = flatten_list_stream(storage, Some("test")).await?;
        assert_eq!(content_list, &[location]);

        // List everything starting with a prefix that shouldn't return results
        let content_list = flatten_list_stream(storage, Some("something")).await?;
        assert!(content_list.is_empty());

        let read_data = storage
            .get(location)
            .await?
            .map_ok(|b| bytes::BytesMut::from(&b[..]))
            .try_concat()
            .await?;
        assert_eq!(&*read_data, data);

        storage.delete(location).await?;

        let content_list = flatten_list_stream(storage, None).await?;
        assert!(content_list.is_empty());

        Ok(())
    }

    #[cfg(any(test_aws, test_gcs))]
    pub(crate) async fn get_nonexistent_object(
        storage: &ObjectStore,
        location: Option<&str>,
    ) -> Result<Bytes> {
        let location = location.unwrap_or("this_file_should_not_exist");

        let content_list = flatten_list_stream(storage, Some(location)).await?;
        assert!(content_list.is_empty());

        Ok(storage
            .get(location)
            .await?
            .map_ok(|b| bytes::BytesMut::from(&b[..]))
            .try_concat()
            .await?
            .freeze())
    }

    // Tests TODO:
    // GET nonexisting location (in_memory/file)
    // DELETE nonexisting location
    // PUT overwriting
}
