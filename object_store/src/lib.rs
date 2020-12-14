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

use bytes::Bytes;
use futures::{stream, Stream, StreamExt, TryStreamExt};
use rusoto_core::ByteStream;
use rusoto_credential::ChainProvider;
use rusoto_s3::S3;
use snafu::{ensure, futures::TryStreamExt as _, OptionExt, ResultExt, Snafu};
use std::{collections::BTreeMap, fmt, io, path::PathBuf};
use tokio::{fs, sync::RwLock};
use tokio_util::codec::{BytesCodec, FramedRead};

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

/// Configuration for connecting to [Google Cloud Storage](https://cloud.google.com/storage/).
#[derive(Debug)]
pub struct GoogleCloudStorage {
    bucket_name: String,
}

impl GoogleCloudStorage {
    /// Configure a connection to Google Cloud Storage.
    pub fn new(bucket_name: impl Into<String>) -> Self {
        Self {
            bucket_name: bucket_name.into(),
        }
    }

    /// Save the provided bytes to the specified location.
    async fn put<S>(&self, location: &str, bytes: S, length: usize) -> InternalResult<()>
    where
        S: Stream<Item = io::Result<Bytes>> + Send + Sync + 'static,
    {
        let temporary_non_streaming = bytes
            .map_ok(|b| bytes::BytesMut::from(&b[..]))
            .try_concat()
            .await
            .expect("Should have been able to collect streaming data")
            .to_vec();

        ensure!(
            temporary_non_streaming.len() == length,
            DataDoesNotMatchLength {
                actual: temporary_non_streaming.len(),
                expected: length,
            }
        );

        let location_copy = location.to_string();
        let bucket_name = self.bucket_name.clone();

        let _ = tokio::task::spawn_blocking(move || {
            cloud_storage::Object::create(
                &bucket_name,
                &temporary_non_streaming,
                &location_copy,
                "application/octet-stream",
            )
        })
        .await
        .context(UnableToPutDataToGcs {
            bucket: &self.bucket_name,
            location,
        })?;

        Ok(())
    }

    async fn get(
        &self,
        location: &str,
    ) -> InternalResult<impl Stream<Item = InternalResult<Bytes>>> {
        let location_copy = location.to_string();
        let bucket_name = self.bucket_name.clone();

        let bytes = tokio::task::spawn_blocking(move || {
            cloud_storage::Object::download(&bucket_name, &location_copy)
        })
        .await
        .context(UnableToGetDataFromGcs {
            bucket: &self.bucket_name,
            location,
        })?
        .context(UnableToGetDataFromGcs2 {
            bucket: &self.bucket_name,
            location,
        })?;

        Ok(futures::stream::once(async move { Ok(bytes.into()) }))
    }

    /// Delete the object at the specified location.
    async fn delete(&self, location: &str) -> InternalResult<()> {
        let location_copy = location.to_string();
        let bucket_name = self.bucket_name.clone();

        tokio::task::spawn_blocking(move || {
            cloud_storage::Object::delete(&bucket_name, &location_copy)
        })
        .await
        .context(UnableToDeleteDataFromGcs {
            bucket: &self.bucket_name,
            location,
        })?
        .context(UnableToDeleteDataFromGcs2 {
            bucket: &self.bucket_name,
            location,
        })?;

        Ok(())
    }

    /// List all the objects with the given prefix.
    async fn list<'a>(
        &'a self,
        prefix: Option<&'a str>,
    ) -> InternalResult<impl Stream<Item = InternalResult<Vec<String>>> + 'a> {
        let bucket_name = self.bucket_name.clone();
        let prefix = prefix.map(|p| p.to_string());

        let objects = tokio::task::spawn_blocking(move || match prefix {
            Some(prefix) => cloud_storage::Object::list_prefix(&bucket_name, &prefix),
            None => cloud_storage::Object::list(&bucket_name),
        })
        .await
        .context(UnableToListDataFromGcs {
            bucket: &self.bucket_name,
        })?
        .context(UnableToListDataFromGcs2 {
            bucket: &self.bucket_name,
        })?;

        Ok(futures::stream::once(async move {
            Ok(objects.into_iter().map(|o| o.name).collect())
        }))
    }
}

/// Configuration for connecting to [Amazon S3](https://aws.amazon.com/s3/).
pub struct AmazonS3 {
    client: rusoto_s3::S3Client,
    bucket_name: String,
}

impl fmt::Debug for AmazonS3 {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("AmazonS3")
            .field("client", &"rusoto_s3::S3Client")
            .field("bucket_name", &self.bucket_name)
            .finish()
    }
}

impl AmazonS3 {
    /// Configure a connection to Amazon S3 in the specified Amazon region and
    /// bucket. Uses [`rusoto_credential::ChainProvider`][cp] to check for
    /// credentials in:
    ///
    /// 1. Environment variables: `AWS_ACCESS_KEY_ID` and
    ///    `AWS_SECRET_ACCESS_KEY`
    /// 2. `credential_process` command in the AWS config file, usually located
    ///    at `~/.aws/config`.
    /// 3. AWS credentials file. Usually located at `~/.aws/credentials`.
    /// 4. IAM instance profile. Will only work if running on an EC2 instance
    ///    with an instance profile/role.
    ///
    /// [cp]: https://docs.rs/rusoto_credential/0.43.0/rusoto_credential/struct.ChainProvider.html
    pub fn new(region: rusoto_core::Region, bucket_name: impl Into<String>) -> Self {
        let http_client = rusoto_core::request::HttpClient::new()
            .expect("Current implementation of rusoto_core has no way for this to fail");
        let credentials_provider = ChainProvider::new();
        Self {
            client: rusoto_s3::S3Client::new_with(http_client, credentials_provider, region),
            bucket_name: bucket_name.into(),
        }
    }

    /// Save the provided bytes to the specified location.
    async fn put<S>(&self, location: &str, bytes: S, length: usize) -> InternalResult<()>
    where
        S: Stream<Item = io::Result<Bytes>> + Send + Sync + 'static,
    {
        let bytes = ByteStream::new_with_size(bytes, length);

        let put_request = rusoto_s3::PutObjectRequest {
            bucket: self.bucket_name.clone(),
            key: location.to_string(),
            body: Some(bytes),
            ..Default::default()
        };

        self.client
            .put_object(put_request)
            .await
            .context(UnableToPutDataToS3 {
                bucket: &self.bucket_name,
                location: location.to_string(),
            })?;
        Ok(())
    }

    /// Return the bytes that are stored at the specified location.
    async fn get(
        &self,
        location: &str,
    ) -> InternalResult<impl Stream<Item = InternalResult<Bytes>>> {
        let get_request = rusoto_s3::GetObjectRequest {
            bucket: self.bucket_name.clone(),
            key: location.to_string(),
            ..Default::default()
        };
        Ok(self
            .client
            .get_object(get_request)
            .await
            .context(UnableToGetDataFromS3 {
                bucket: self.bucket_name.to_owned(),
                location: location.to_owned(),
            })?
            .body
            .context(NoDataFromS3 {
                bucket: self.bucket_name.to_owned(),
                location: location.to_string(),
            })?
            .context(UnableToGetPieceOfDataFromS3 {
                bucket: self.bucket_name.to_owned(),
                location: location.to_string(),
            })
            .err_into())
    }

    /// Delete the object at the specified location.
    async fn delete(&self, location: &str) -> InternalResult<()> {
        let delete_request = rusoto_s3::DeleteObjectRequest {
            bucket: self.bucket_name.clone(),
            key: location.to_string(),
            ..Default::default()
        };

        self.client
            .delete_object(delete_request)
            .await
            .context(UnableToDeleteDataFromS3 {
                bucket: self.bucket_name.to_owned(),
                location: location.to_owned(),
            })?;
        Ok(())
    }

    /// List all the objects with the given prefix.
    async fn list<'a>(
        &'a self,
        prefix: Option<&'a str>,
    ) -> InternalResult<impl Stream<Item = InternalResult<Vec<String>>> + 'a> {
        #[derive(Clone)]
        enum ListState {
            Start,
            HasMore(String),
            Done,
        }
        use ListState::*;

        Ok(stream::unfold(ListState::Start, move |state| async move {
            let mut list_request = rusoto_s3::ListObjectsV2Request {
                bucket: self.bucket_name.clone(),
                prefix: prefix.map(ToString::to_string),
                ..Default::default()
            };

            match state.clone() {
                HasMore(continuation_token) => {
                    list_request.continuation_token = Some(continuation_token);
                }
                Done => {
                    return None;
                }
                // If this is the first request we've made, we don't need to make any modifications
                // to the request
                Start => {}
            }

            let resp = match self.client.list_objects_v2(list_request).await {
                Ok(resp) => resp,
                Err(e) => {
                    return Some((
                        Err(InternalError::UnableToListDataFromS3 {
                            source: e,
                            bucket: self.bucket_name.clone(),
                        }),
                        state,
                    ))
                }
            };

            let contents = resp.contents.unwrap_or_default();
            let names = contents.into_iter().flat_map(|object| object.key).collect();

            // The AWS response contains a field named `is_truncated` as well as
            // `next_continuation_token`, and we're assuming that `next_continuation_token`
            // is only set when `is_truncated` is true (and therefore not
            // checking `is_truncated`).
            let next_state = if let Some(next_continuation_token) = resp.next_continuation_token {
                ListState::HasMore(next_continuation_token)
            } else {
                ListState::Done
            };

            Some((Ok(names), next_state))
        }))
    }
}

/// In-memory storage suitable for testing or for opting out of using a cloud
/// storage provider.
#[derive(Debug, Default)]
pub struct InMemory {
    storage: RwLock<BTreeMap<String, Bytes>>,
}

impl InMemory {
    /// Create new in-memory storage.
    pub fn new() -> Self {
        Self::default()
    }

    /// Creates a clone of the store
    pub async fn clone(&self) -> Self {
        let storage = self.storage.read().await;
        let storage = storage.clone();

        Self {
            storage: RwLock::new(storage),
        }
    }

    /// Save the provided bytes to the specified location.
    async fn put<S>(&self, location: &str, bytes: S, length: usize) -> InternalResult<()>
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

        let content = content.freeze();

        self.storage
            .write()
            .await
            .insert(location.to_string(), content);
        Ok(())
    }

    /// Return the bytes that are stored at the specified location.
    async fn get(
        &self,
        location: &str,
    ) -> InternalResult<impl Stream<Item = InternalResult<Bytes>>> {
        let data = self
            .storage
            .read()
            .await
            .get(location)
            .cloned()
            .context(NoDataInMemory)?;

        Ok(futures::stream::once(async move { Ok(data) }))
    }

    /// Delete the object at the specified location.
    async fn delete(&self, location: &str) -> InternalResult<()> {
        self.storage.write().await.remove(location);
        Ok(())
    }

    /// List all the objects with the given prefix.
    async fn list<'a>(
        &'a self,
        prefix: Option<&'a str>,
    ) -> InternalResult<impl Stream<Item = InternalResult<Vec<String>>> + 'a> {
        let list = if let Some(prefix) = prefix {
            self.storage
                .read()
                .await
                .keys()
                .filter(|k| k.starts_with(prefix))
                .cloned()
                .collect()
        } else {
            self.storage.read().await.keys().cloned().collect()
        };

        Ok(futures::stream::once(async move { Ok(list) }))
    }
}

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
    async fn put<S>(&self, location: &str, bytes: S, length: usize) -> InternalResult<()>
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
    async fn get(
        &self,
        location: &str,
    ) -> InternalResult<impl Stream<Item = InternalResult<Bytes>>> {
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
    async fn delete(&self, location: &str) -> InternalResult<()> {
        let path = self.path(location);
        fs::remove_file(&path)
            .await
            .context(UnableToDeleteFile { path })?;
        Ok(())
    }

    /// List all the objects with the given prefix.
    async fn list<'a>(
        &'a self,
        prefix: Option<&'a str>,
    ) -> InternalResult<impl Stream<Item = InternalResult<Vec<String>>> + 'a> {
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

/// A specialized `Result` for object store-related errors
pub type Result<T, E = Error> = std::result::Result<T, E>;
type InternalResult<T, E = InternalError> = std::result::Result<T, E>;

/// Opaque public `Error` type
#[derive(Debug, Snafu)]
pub struct Error(InternalError);

impl Error {
    #[cfg(test)]
    #[cfg(test_aws)]
    fn s3_error_due_to_credentials(&self) -> bool {
        use rusoto_core::RusotoError;
        use InternalError::*;

        match self.0 {
            UnableToPutDataToS3 {
                source: RusotoError::Credentials(_),
                bucket: _,
                location: _,
            } => true,
            UnableToGetDataFromS3 {
                source: RusotoError::Credentials(_),
                bucket: _,
                location: _,
            } => true,
            UnableToDeleteDataFromS3 {
                source: RusotoError::Credentials(_),
                bucket: _,
                location: _,
            } => true,
            UnableToListDataFromS3 {
                source: RusotoError::Credentials(_),
                bucket: _,
            } => true,
            _ => false,
        }
    }
}

#[derive(Debug, Snafu)]
enum InternalError {
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

    type Error = Box<dyn std::error::Error + Send + Sync + 'static>;
    type Result<T, E = Error> = std::result::Result<T, E>;

    #[cfg(any(test_aws, test_gcs))]
    const NON_EXISTENT_NAME: &str = "nonexistentname";

    macro_rules! assert_error {
        ($res:expr, $error_pat:pat$(,)?) => {
            assert!(
                matches!($res, Err(super::Error($error_pat))),
                "was: {:?}",
                $res,
            )
        };
    }

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

    async fn put_get_delete_list(storage: &ObjectStore) -> Result<()> {
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
    async fn get_nonexistent_object(
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

    #[cfg(test_gcs)]
    mod google_cloud_storage {
        use std::env;

        use super::*;

        fn bucket_name() -> Result<String> {
            dotenv::dotenv().ok();
            let bucket_name = env::var("GCS_BUCKET_NAME")
                .map_err(|_| "The environment variable GCS_BUCKET_NAME must be set")?;

            Ok(bucket_name)
        }

        #[tokio::test]
        async fn gcs_test() -> Result<()> {
            let bucket_name = bucket_name()?;

            let integration =
                ObjectStore::new_google_cloud_storage(GoogleCloudStorage::new(&bucket_name));
            put_get_delete_list(&integration).await?;
            Ok(())
        }

        #[tokio::test]
        async fn gcs_test_get_nonexistent_location() -> Result<()> {
            let bucket_name = bucket_name()?;
            let location_name = NON_EXISTENT_NAME;
            let integration =
                ObjectStore::new_google_cloud_storage(GoogleCloudStorage::new(&bucket_name));

            let result = get_nonexistent_object(&integration, Some(location_name)).await?;

            assert_eq!(
                result,
                Bytes::from(format!("No such object: {}/{}", bucket_name, location_name))
            );

            Ok(())
        }

        #[tokio::test]
        async fn gcs_test_get_nonexistent_bucket() -> Result<()> {
            let bucket_name = NON_EXISTENT_NAME;
            let location_name = NON_EXISTENT_NAME;
            let integration =
                ObjectStore::new_google_cloud_storage(GoogleCloudStorage::new(bucket_name));

            let result = get_nonexistent_object(&integration, Some(location_name)).await?;

            assert_eq!(result, Bytes::from("Not Found"));

            Ok(())
        }

        #[tokio::test]
        async fn gcs_test_delete_nonexistent_location() -> Result<()> {
            let bucket_name = bucket_name()?;
            let location_name = NON_EXISTENT_NAME;
            let integration =
                ObjectStore::new_google_cloud_storage(GoogleCloudStorage::new(&bucket_name));

            let err = integration.delete(location_name).await.unwrap_err();

            if let Error(InternalError::UnableToDeleteDataFromGcs2 {
                source,
                bucket,
                location,
            }) = err
            {
                assert!(matches!(source, cloud_storage::Error::Google(_)));
                assert_eq!(bucket, bucket_name);
                assert_eq!(location, location_name);
            } else {
                panic!("unexpected error type")
            }

            Ok(())
        }

        #[tokio::test]
        async fn gcs_test_delete_nonexistent_bucket() -> Result<()> {
            let bucket_name = NON_EXISTENT_NAME;
            let location_name = NON_EXISTENT_NAME;
            let integration =
                ObjectStore::new_google_cloud_storage(GoogleCloudStorage::new(bucket_name));

            let err = integration.delete(location_name).await.unwrap_err();

            if let Error(InternalError::UnableToDeleteDataFromGcs2 {
                source,
                bucket,
                location,
            }) = err
            {
                assert!(matches!(source, cloud_storage::Error::Google(_)));
                assert_eq!(bucket, bucket_name);
                assert_eq!(location, location_name);
            } else {
                panic!("unexpected error type")
            }

            Ok(())
        }

        #[tokio::test]
        async fn gcs_test_put_nonexistent_bucket() -> Result<()> {
            let bucket_name = NON_EXISTENT_NAME;
            let location_name = NON_EXISTENT_NAME;
            let integration =
                ObjectStore::new_google_cloud_storage(GoogleCloudStorage::new(bucket_name));
            let data = Bytes::from("arbitrary data");
            let stream_data = std::io::Result::Ok(data.clone());

            let result = integration
                .put(
                    location_name,
                    futures::stream::once(async move { stream_data }),
                    data.len(),
                )
                .await;
            assert!(result.is_ok());

            Ok(())
        }
    }

    #[cfg(test_aws)]
    mod amazon_s3 {
        use std::env;

        use super::*;

        fn region_and_bucket_name() -> Result<(rusoto_core::Region, String)> {
            dotenv::dotenv().ok();

            let region = env::var("AWS_DEFAULT_REGION")
                .map_err(|_| "The environment variable AWS_DEFAULT_REGION must be set to a value like `us-east-2`")?;
            let bucket_name = env::var("AWS_S3_BUCKET_NAME")
                .map_err(|_| "The environment variable AWS_S3_BUCKET_NAME must be set")?;

            Ok((region.parse()?, bucket_name))
        }

        fn check_credentials<T>(r: Result<T>) -> Result<T> {
            if let Err(e) = &r {
                let e = &**e;
                if let Some(e) = e.downcast_ref::<crate::Error>() {
                    if e.s3_error_due_to_credentials() {
                        eprintln!("Try setting the AWS_ACCESS_KEY_ID and AWS_SECRET_ACCESS_KEY environment variables");
                    }
                }
            }

            r
        }

        #[tokio::test]
        async fn s3_test() -> Result<()> {
            let (region, bucket_name) = region_and_bucket_name()?;

            let integration = ObjectStore::new_amazon_s3(AmazonS3::new(region, &bucket_name));
            check_credentials(put_get_delete_list(&integration).await)?;

            Ok(())
        }

        #[tokio::test]
        async fn s3_test_get_nonexistent_region() -> Result<()> {
            // Assumes environment variables do not provide credentials to AWS US West 1
            let (_, bucket_name) = region_and_bucket_name()?;
            let region = rusoto_core::Region::UsWest1;
            let integration = ObjectStore::new_amazon_s3(AmazonS3::new(region, &bucket_name));
            let location_name = NON_EXISTENT_NAME;

            let err = get_nonexistent_object(&integration, Some(location_name))
                .await
                .unwrap_err();
            if let Some(Error(InternalError::UnableToListDataFromS3 { source, bucket })) =
                err.downcast_ref::<crate::Error>()
            {
                assert!(matches!(source, rusoto_core::RusotoError::Unknown(_)));
                assert_eq!(bucket, &bucket_name);
            } else {
                panic!("unexpected error type")
            }

            Ok(())
        }

        #[tokio::test]
        async fn s3_test_get_nonexistent_location() -> Result<()> {
            let (region, bucket_name) = region_and_bucket_name()?;
            let integration = ObjectStore::new_amazon_s3(AmazonS3::new(region, &bucket_name));
            let location_name = NON_EXISTENT_NAME;

            let err = get_nonexistent_object(&integration, Some(location_name))
                .await
                .unwrap_err();
            if let Some(Error(InternalError::UnableToGetDataFromS3 {
                source,
                bucket,
                location,
            })) = err.downcast_ref::<crate::Error>()
            {
                assert!(matches!(
                    source,
                    rusoto_core::RusotoError::Service(rusoto_s3::GetObjectError::NoSuchKey(_))
                ));
                assert_eq!(bucket, &bucket_name);
                assert_eq!(location, location_name);
            } else {
                panic!("unexpected error type")
            }

            Ok(())
        }

        #[tokio::test]
        async fn s3_test_get_nonexistent_bucket() -> Result<()> {
            let (region, _) = region_and_bucket_name()?;
            let bucket_name = NON_EXISTENT_NAME;
            let integration = ObjectStore::new_amazon_s3(AmazonS3::new(region, bucket_name));
            let location_name = NON_EXISTENT_NAME;

            let err = get_nonexistent_object(&integration, Some(location_name))
                .await
                .unwrap_err();
            if let Some(Error(InternalError::UnableToListDataFromS3 { source, bucket })) =
                e.downcast_ref::<crate::Error>()
            {
                assert!(matches!(
                    source,
                    rusoto_core::RusotoError::Service(
                        rusoto_s3::ListObjectsV2Error::NoSuchBucket(_),
                    )
                ));
                assert_eq!(bucket, &bucket_name);
            } else {
                panic!("unexpected error type")
            }

            Ok(())
        }

        #[tokio::test]
        async fn s3_test_put_nonexistent_region() -> Result<()> {
            // Assumes environment variables do not provide credentials to AWS US West 1
            let (_, bucket_name) = region_and_bucket_name()?;
            let region = rusoto_core::Region::UsWest1;
            let integration = ObjectStore::new_amazon_s3(AmazonS3::new(region, &bucket_name));
            let location_name = NON_EXISTENT_NAME;
            let data = Bytes::from("arbitrary data");
            let stream_data = std::io::Result::Ok(data.clone());

            let err = integration
                .put(
                    location_name,
                    futures::stream::once(async move { stream_data }),
                    data.len(),
                )
                .await
                .unwrap_err();

            if let Error(InternalError::UnableToPutDataToS3 {
                source,
                bucket,
                location,
            }) = err
            {
                assert!(matches!(source, rusoto_core::RusotoError::Unknown(_)));
                assert_eq!(bucket, bucket_name);
                assert_eq!(location, location_name);
            } else {
                panic!("unexpected error type")
            }

            Ok(())
        }

        #[tokio::test]
        async fn s3_test_put_nonexistent_bucket() -> Result<()> {
            let (region, _) = region_and_bucket_name()?;
            let bucket_name = NON_EXISTENT_NAME;
            let integration = ObjectStore::new_amazon_s3(AmazonS3::new(region, bucket_name));
            let location_name = NON_EXISTENT_NAME;
            let data = Bytes::from("arbitrary data");
            let stream_data = std::io::Result::Ok(data.clone());

            let err = integration
                .put(
                    location_name,
                    futures::stream::once(async move { stream_data }),
                    data.len(),
                )
                .await
                .unwrap_err();

            if let Error(InternalError::UnableToPutDataToS3 {
                source,
                bucket,
                location,
            }) = err
            {
                assert!(matches!(source, rusoto_core::RusotoError::Unknown(_)));
                assert_eq!(bucket, bucket_name);
                assert_eq!(location, location_name);
            } else {
                panic!("unexpected error type")
            }

            Ok(())
        }

        #[tokio::test]
        async fn s3_test_delete_nonexistent_location() -> Result<()> {
            let (region, bucket_name) = region_and_bucket_name()?;
            let integration = ObjectStore::new_amazon_s3(AmazonS3::new(region, &bucket_name));
            let location_name = NON_EXISTENT_NAME;

            let result = integration.delete(location_name).await;

            assert!(result.is_ok());

            Ok(())
        }

        #[tokio::test]
        async fn s3_test_delete_nonexistent_region() -> Result<()> {
            // Assumes environment variables do not provide credentials to AWS US West 1
            let (_, bucket_name) = region_and_bucket_name()?;
            let region = rusoto_core::Region::UsWest1;
            let integration = ObjectStore::new_amazon_s3(AmazonS3::new(region, &bucket_name));
            let location_name = NON_EXISTENT_NAME;

            let err = integration.delete(location_name).await.unwrap_err();
            if let Error(InternalError::UnableToDeleteDataFromS3 {
                source,
                bucket,
                location,
            }) = err
            {
                assert!(matches!(source, rusoto_core::RusotoError::Unknown(_)));
                assert_eq!(bucket, bucket_name);
                assert_eq!(location, location_name);
            } else {
                panic!("unexpected error type")
            }

            Ok(())
        }

        #[tokio::test]
        async fn s3_test_delete_nonexistent_bucket() -> Result<()> {
            let (region, _) = region_and_bucket_name()?;
            let bucket_name = NON_EXISTENT_NAME;
            let integration = ObjectStore::new_amazon_s3(AmazonS3::new(region, bucket_name));
            let location_name = NON_EXISTENT_NAME;

            let err = integration.delete(location_name).await.unwrap_err();
            if let Error(InternalError::UnableToDeleteDataFromS3 {
                source,
                bucket,
                location,
            }) = err
            {
                assert!(matches!(source, rusoto_core::RusotoError::Unknown(_)));
                assert_eq!(bucket, bucket_name);
                assert_eq!(location, location_name);
            } else {
                panic!("unexpected error type")
            }

            Ok(())
        }
    }

    mod in_memory {
        use super::*;

        #[tokio::test]
        async fn in_memory_test() -> Result<()> {
            let integration = ObjectStore::new_in_memory(InMemory::new());

            put_get_delete_list(&integration).await?;
            Ok(())
        }

        #[tokio::test]
        async fn length_mismatch_is_an_error() -> Result<()> {
            let integration = ObjectStore::new_in_memory(InMemory::new());

            let bytes = stream::once(async { Ok(Bytes::from("hello world")) });
            let res = integration.put("junk", bytes, 0).await;

            assert_error!(
                res,
                InternalError::DataDoesNotMatchLength {
                    expected: 0,
                    actual: 11,
                },
            );

            Ok(())
        }
    }

    mod file {
        use tempfile::TempDir;

        use super::*;

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

            assert_error!(
                res,
                InternalError::DataDoesNotMatchLength {
                    expected: 0,
                    actual: 11,
                },
            );

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
}
