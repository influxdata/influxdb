//! CLI handling for object store config (via CLI arguments and environment variables).

use async_trait::async_trait;
use bytes::Bytes;
use futures::{StreamExt, stream::BoxStream};
use iox_time::{SystemProvider, TimeProvider};
use non_empty_string::NonEmptyString;
use object_store::{
    CredentialProvider, DynObjectStore, GetOptions, GetResult, ListResult, MultipartUpload,
    ObjectMeta, ObjectStore, PutMultipartOpts, PutOptions, PutPayload, PutResult,
    aws::AwsCredential,
    local::LocalFileSystem,
    memory::InMemory,
    path::Path,
    throttle::{ThrottleConfig, ThrottledStore},
};
use observability_deps::tracing::{info, warn};
use serde::{Deserialize, Serialize};
use snafu::{ResultExt, Snafu};
use std::{
    cmp::Ordering, convert::Infallible, fs, num::NonZeroUsize, ops::Range, path::PathBuf,
    sync::Arc, time::Duration,
};
use tokio::sync::RwLock;
use url::Url;

#[derive(Debug, Snafu)]
#[allow(missing_docs)]
pub enum ParseError {
    #[snafu(display("Unable to create database directory {:?}: {}", path, source))]
    CreatingDatabaseDirectory {
        path: PathBuf,
        source: std::io::Error,
    },

    #[snafu(display("Unable to create local store {:?}: {}", path, source))]
    CreateLocalFileSystem {
        path: PathBuf,
        source: object_store::Error,
    },

    #[snafu(display(
        "Specified {:?} for the object store, required configuration missing for {}",
        object_store,
        missing
    ))]
    MissingObjectStoreConfig {
        object_store: ObjectStoreType,
        missing: String,
    },

    // Creating a new S3 object store can fail if the region is *specified* but
    // not *parseable* as a rusoto `Region`. The other object store constructors
    // don't return `Result`.
    #[snafu(display("Error configuring Amazon S3: {}", source))]
    InvalidS3Config { source: object_store::Error },

    #[snafu(display("Error configuring GCS: {}", source))]
    InvalidGCSConfig { source: object_store::Error },

    #[snafu(display("Error configuring Microsoft Azure: {}", source))]
    InvalidAzureConfig { source: object_store::Error },

    #[snafu(display("Error reading AWS credentials from file: {}", source))]
    ReadingAwsFileCredentialsToString { source: std::io::Error },

    #[snafu(display("Error deserializing AWS file credentials: {}", source))]
    DeserializingAwsFileCredentials { source: serde_json::Error },
}

/// The AWS region to use for Amazon S3 based object storage if none is
/// specified.
pub const FALLBACK_AWS_REGION: &str = "us-east-1";

/// A `clap` `value_parser` which returns `None` when given an empty string and
/// `Some(NonEmptyString)` otherwise.
fn parse_optional_string(s: &str) -> Result<Option<NonEmptyString>, Infallible> {
    Ok(NonEmptyString::new(s.to_string()).ok())
}

/// Endpoint for S3 & Co.
///
/// This is a [`Url`] without a trailing slash.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct Endpoint(String);

impl std::fmt::Display for Endpoint {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.0.fmt(f)
    }
}

impl From<Endpoint> for String {
    fn from(value: Endpoint) -> Self {
        value.0
    }
}

impl std::str::FromStr for Endpoint {
    type Err = Box<dyn std::error::Error + Send + Sync>;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        // try to parse it
        Url::parse(s)?;

        // strip trailing slash
        let s = s.strip_suffix("/").unwrap_or(s);

        Ok(Self(s.to_owned()))
    }
}

#[derive(Debug)]
struct LocalFileSystemWithSortedListOp {
    inner: Arc<LocalFileSystem>,
}

impl std::fmt::Display for LocalFileSystemWithSortedListOp {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.inner.fmt(f)
    }
}

impl LocalFileSystemWithSortedListOp {
    fn new_with_prefix(prefix: impl AsRef<std::path::Path>) -> Result<Self, ParseError> {
        Ok(Self {
            inner: Arc::new(
                LocalFileSystem::new_with_prefix(prefix.as_ref())
                    .context(CreateLocalFileSystemSnafu {
                        path: prefix.as_ref().to_path_buf(),
                    })?
                    // Clean up intermediate directories automatically.
                    .with_automatic_cleanup(true),
            ),
        })
    }
}

#[async_trait]
impl ObjectStore for LocalFileSystemWithSortedListOp {
    async fn put(&self, location: &Path, bytes: PutPayload) -> object_store::Result<PutResult> {
        self.inner.put(location, bytes).await
    }

    async fn put_opts(
        &self,
        location: &Path,
        bytes: PutPayload,
        opts: PutOptions,
    ) -> object_store::Result<PutResult> {
        self.inner.put_opts(location, bytes, opts).await
    }

    async fn put_multipart(
        &self,
        location: &Path,
    ) -> object_store::Result<Box<dyn MultipartUpload>> {
        self.inner.put_multipart(location).await
    }

    async fn put_multipart_opts(
        &self,
        location: &Path,
        opts: PutMultipartOpts,
    ) -> object_store::Result<Box<dyn MultipartUpload>> {
        self.inner.put_multipart_opts(location, opts).await
    }

    async fn get(&self, location: &Path) -> object_store::Result<GetResult> {
        self.inner.get(location).await
    }

    async fn get_opts(
        &self,
        location: &Path,
        options: GetOptions,
    ) -> object_store::Result<GetResult> {
        self.inner.get_opts(location, options).await
    }

    async fn get_range(&self, location: &Path, range: Range<usize>) -> object_store::Result<Bytes> {
        self.inner.get_range(location, range).await
    }

    async fn get_ranges(
        &self,
        location: &Path,
        ranges: &[Range<usize>],
    ) -> object_store::Result<Vec<Bytes>> {
        self.inner.get_ranges(location, ranges).await
    }

    async fn head(&self, location: &Path) -> object_store::Result<ObjectMeta> {
        self.inner.head(location).await
    }

    async fn delete(&self, location: &Path) -> object_store::Result<()> {
        self.inner.delete(location).await
    }

    fn delete_stream<'a>(
        &'a self,
        locations: BoxStream<'a, object_store::Result<Path>>,
    ) -> BoxStream<'a, object_store::Result<Path>> {
        self.inner.delete_stream(locations)
    }

    /// Collect results from inner object store into a vec, sort them, then return a new boxed
    /// stream that iterates over the new vec.
    fn list(&self, prefix: Option<&Path>) -> BoxStream<'_, object_store::Result<ObjectMeta>> {
        if tokio::runtime::Handle::try_current().is_err() {
            // We should never reach this branch, but if we do then warn and return let the
            // inner implementation deal with it.
            warn!("no tokio runtime started, cannot sort object store list output");
            return self.inner.list(prefix);
        }

        let mut items: Vec<Result<ObjectMeta, _>> = futures::executor::block_on(async {
            // we could use TryStreamExt.collect() here to drop all collected results and
            // return the first error we encounter, but users of the ObjectStore API will
            // probably expect to have to deal with errors one element at a time anyway
            self.inner.list(prefix).collect().await
        });

        items.sort_unstable_by(|left, right| match (left, right) {
            (Ok(left_meta), Ok(right_meta)) => left_meta.location.cmp(&right_meta.location),
            // basically just move all the Err(E) instances to the end of the results.
            (Err(_), Ok(_)) => Ordering::Less,
            (Ok(_), Err(_)) => Ordering::Greater,
            (Err(_), Err(_)) => Ordering::Equal,
        });

        futures::stream::iter(items).boxed()
    }

    /// Collect results from inner object store into a vec, sort them, then return a new boxed
    /// stream that iterates over the new vec.
    fn list_with_offset(
        &self,
        prefix: Option<&Path>,
        offset: &Path,
    ) -> BoxStream<'_, object_store::Result<ObjectMeta>> {
        if tokio::runtime::Handle::try_current().is_err() {
            // We should never reach this branch, but if we do then warn and return let the
            // inner implementation deal with it.
            warn!("no tokio runtime started, cannot sort object store list output");
            return self.inner.list_with_offset(prefix, offset);
        }

        let mut items: Vec<Result<ObjectMeta, _>> = futures::executor::block_on(async {
            // we could use TryStreamExt.collect() here to drop all collected results and
            // return the first error we encounter, but users of the ObjectStore API will
            // probably expect to have to deal with errors one element at a time anyway
            self.inner.list_with_offset(prefix, offset).collect().await
        });

        items.sort_unstable_by(|left, right| match (left, right) {
            (Ok(left_meta), Ok(right_meta)) => left_meta.location.cmp(&right_meta.location),
            // basically just move all the Err(E) instances to the end of the results.
            (Err(_), Ok(_)) => Ordering::Less,
            (Ok(_), Err(_)) => Ordering::Greater,
            (Err(_), Err(_)) => Ordering::Equal,
        });

        futures::stream::iter(items).boxed()
    }

    /// Collect results from inner object store into a vec, sort them, then return a new boxed
    /// stream that iterates over the new vec.
    async fn list_with_delimiter(&self, prefix: Option<&Path>) -> object_store::Result<ListResult> {
        let mut items: ListResult = self.inner.list_with_delimiter(prefix).await?;

        items
            .objects
            .sort_unstable_by(|left, right| left.location.cmp(&right.location));
        items.common_prefixes.sort();

        Ok(items)
    }

    async fn copy(&self, from: &Path, to: &Path) -> object_store::Result<()> {
        self.inner.copy(from, to).await
    }

    async fn rename(&self, from: &Path, to: &Path) -> object_store::Result<()> {
        self.inner.rename(from, to).await
    }

    async fn copy_if_not_exists(&self, from: &Path, to: &Path) -> object_store::Result<()> {
        self.inner.copy_if_not_exists(from, to).await
    }

    async fn rename_if_not_exists(&self, from: &Path, to: &Path) -> object_store::Result<()> {
        self.inner.rename_if_not_exists(from, to).await
    }
}

/// Creation of an `ObjectStoreConfig` struct for `clap` argument handling.
///
/// This allows for multiple object store configurations to be produced when
/// needed, denoted by a particular use-case prefix.
macro_rules! object_store_config {
    ($prefix:expr) => {
        object_store_config_inner!($prefix);
    };
    () => {
        // Creates the original ObjectStoreConfig to maintain backwards
        // compatibility.
        object_store_config_inner!("_");
    };
}

/// Helper macro to generate the relevant name, used by the ID/long attributes
/// for `clap`.
macro_rules! gen_name {
    ($prefix:expr, $name:expr) => {
        paste::paste! {
            if $prefix == "_" {
                $name
            } else {
                concat!(stringify!([<$prefix:lower>]), "-", $name)
            }
        }
    };
}

/// Helper macro to generate the appropriate environment variable, used by the
/// env attribute for `clap`.
macro_rules! gen_env {
    ($prefix:expr, $name:expr) => {
        paste::paste! {
            if $prefix == "_" {
                $name
            } else {
                concat!(stringify!([<$prefix:upper>]), "_", $name)
            }
        }
    };
}

macro_rules! object_store_config_inner {
    ($prefix:expr) => {
        paste::paste! {
            /// CLI config for object stores.
            #[derive(Debug, Clone, clap::Parser)]
            pub struct [<$prefix:camel ObjectStoreConfig>] {
                /// Which object storage to use.
                ///
                /// Possible values (case insensitive):
                ///
                /// * memory: Effectively no object persistence.
                /// * memorythrottled: Like `memory` but with latency and throughput that somewhat resamble a cloud
                ///   object store. Useful for testing and benchmarking.
                /// * file: Stores objects in the local filesystem. Must also set `--data-dir`.
                /// * s3: Amazon S3. Must also set `--bucket`, `--aws-access-key-id`, `--aws-secret-access-key`, and
                ///   possibly `--aws-default-region`.
                /// * google: Google Cloud Storage. Must also set `--bucket` and `--google-service-account`.
                /// * azure: Microsoft Azure blob storage. Must also set `--bucket`, `--azure-storage-account`,
                ///   and `--azure-storage-access-key`.
                #[clap(
                    value_enum,
                    id = gen_name!($prefix, "object-store"),
                    long = gen_name!($prefix, "object-store"),
                    env = gen_env!($prefix, "INFLUXDB3_OBJECT_STORE"),
                    ignore_case = true,
                    action,
                    required = true,
                    verbatim_doc_comment
                )]
                pub object_store: ObjectStoreType,

                /// Name of the bucket to use for the object store. Must also set
                /// `--object-store` to a cloud object storage to have any effect.
                ///
                /// If using Google Cloud Storage for the object store, this item as well
                /// as `--google-service-account` must be set.
                ///
                /// If using S3 for the object store, must set this item as well
                /// as `--aws-access-key-id` and `--aws-secret-access-key`. Can also set
                /// `--aws-default-region` if not using the fallback region.
                ///
                /// If using Azure for the object store, set this item to the name of a
                /// container you've created in the associated storage account, under
                /// Blob Service > Containers. Must also set `--azure-storage-account` and
                /// `--azure-storage-access-key`.
                #[clap(
                    id = gen_name!($prefix, "bucket"),
                    long = gen_name!($prefix, "bucket"),
                    env = gen_env!($prefix, "INFLUXDB3_BUCKET"),
                    action
                )]
                pub bucket: Option<String>,

                /// The location InfluxDB 3 Core will use to store files locally.
                #[clap(
                    id = gen_name!($prefix, "data-dir"),
                    long = gen_name!($prefix, "data-dir"),
                    env = gen_env!($prefix, "INFLUXDB3_DB_DIR"),
                    action
                )]
                pub database_directory: Option<PathBuf>,

                /// When using Amazon S3 as the object store, set this to an access key that
                /// has permission to read from and write to the specified S3 bucket.
                ///
                /// Must also set `--object-store=s3`, `--bucket`, and
                /// `--aws-secret-access-key`. Can also set `--aws-default-region` if not
                /// using the fallback region.
                ///
                /// Prefer the environment variable over the command line flag in shared
                /// environments.
                ///
                /// An empty string value is equivalent to omitting the flag.
                /// Note: must refer to std::option::Option explicitly, see <https://github.com/clap-rs/clap/issues/4626>
                #[clap(
                    id = gen_name!($prefix, "aws-access-key-id"),
                    long = gen_name!($prefix, "aws-access-key-id"),
                    env = gen_env!($prefix, "AWS_ACCESS_KEY_ID"),
                    value_parser = parse_optional_string,
                    default_value = "",
                    action
                )]
                pub aws_access_key_id: std::option::Option<NonEmptyString>,

                /// When using Amazon S3 as the object store, set this to the secret access
                /// key that goes with the specified access key ID.
                ///
                /// Must also set `--object-store=s3`, `--bucket`, `--aws-access-key-id`.
                /// Can also set `--aws-default-region` if not using the fallback region.
                ///
                /// Prefer the environment variable over the command line flag in shared
                /// environments.
                ///
                /// An empty string value is equivalent to omitting the flag.
                /// Note: must refer to std::option::Option explicitly, see <https://github.com/clap-rs/clap/issues/4626>
                #[clap(
                    id = gen_name!($prefix, "aws-secret-access-key"),
                    long = gen_name!($prefix, "aws-secret-access-key"),
                    env = gen_env!($prefix, "AWS_SECRET_ACCESS_KEY"),
                    value_parser = parse_optional_string,
                    default_value = "",
                    action
                )]
                pub aws_secret_access_key: std::option::Option<NonEmptyString>,

                /// When using Amazon S3 as the object store, set this to the region
                /// that goes with the specified bucket if different from the fallback
                /// value.
                ///
                /// Must also set `--object-store=s3`, `--bucket`, `--aws-access-key-id`,
                /// and `--aws-secret-access-key`.
                #[clap(
                    id = gen_name!($prefix, "aws-default-region"),
                    long = gen_name!($prefix, "aws-default-region"),
                    env = gen_env!($prefix, "AWS_DEFAULT_REGION"),
                    default_value = FALLBACK_AWS_REGION,
                    action
                )]
                pub aws_default_region: String,

                /// When using Amazon S3 compatibility storage service, set this to the
                /// endpoint.
                ///
                /// Must also set `--object-store=s3`, `--bucket`. Can also set `--aws-default-region`
                /// if not using the fallback region.
                ///
                /// Prefer the environment variable over the command line flag in shared
                /// environments.
                #[clap(
                    id = gen_name!($prefix, "aws-endpoint"),
                    long = gen_name!($prefix, "aws-endpoint"),
                    env = gen_env!($prefix, "AWS_ENDPOINT"),
                    action
                )]
                pub aws_endpoint: Option<Endpoint>,

                /// When using Amazon S3 as an object store, set this to the session token. This is handy when using a federated
                /// login / SSO and you fetch credentials via the UI.
                ///
                /// It is assumed that the session is valid as long as the InfluxDB3 Core server is running.
                ///
                /// Prefer the environment variable over the command line flag in shared
                /// environments.
                #[clap(
                    id = gen_name!($prefix, "aws-session-token"),
                    long = gen_name!($prefix, "aws-session-token"),
                    env = gen_env!($prefix, "AWS_SESSION_TOKEN"),
                    action
                )]
                pub aws_session_token: Option<String>,

                /// Allow unencrypted HTTP connection to AWS.
                #[clap(
                    id = gen_name!($prefix, "aws-allow-http"),
                    long = gen_name!($prefix, "aws-allow-http"),
                    env = gen_env!($prefix, "AWS_ALLOW_HTTP"),
                    action
                )]
                pub aws_allow_http: bool,

                /// If enabled, S3 stores will not fetch credentials and will not sign requests.
                ///
                /// This can be useful when interacting with public S3 buckets that deny authorized requests or for when working
                /// with in-cluster proxies that handle the credentials already.
                #[clap(
                    id = gen_name!($prefix, "aws-skip-signature"),
                    long = gen_name!($prefix, "aws-skip-signature"),
                    env = gen_env!($prefix, "AWS_SKIP_SIGNATURE"),
                    action
                )]
                pub aws_skip_signature: bool,

                /// Specify this as an alternative to `--aws-access-key-id`,
                /// `--aws-secret-access-key`,  and `--aws-session-token`. This is a file path
                /// argument where the format of the file is as follows:
                ///
                /// ```
                /// {
                ///     "aws_access_key_id": "<key>",
                ///     "aws_secret_access_key": "<secret>",
                ///     "aws_session_token": "<token>",
                ///     "expiry": "<expiry_timestamp_seconds_since_epoch>"
                /// }
                /// ```
                ///
                /// The server will periodically check this file path for updated contents such
                /// that the credentials can be updated without restarting the server.
                #[clap(
                    id = gen_name!($prefix, "aws-credentials-file"),
                    long = gen_name!($prefix, "aws-credentials-file"),
                    env = gen_name!($prefix, "AWS_CREDENTIALS_FILE"),
                    action
                )]
                pub aws_credentials_file: Option<String>,

                /// When using Google Cloud Storage as the object store, set this to the
                /// path to the JSON file that contains the Google credentials.
                ///
                /// Must also set `--object-store=google` and `--bucket`.
                #[clap(
                    id = gen_name!($prefix, "google-service-account"),
                    long = gen_name!($prefix, "google-service-account"),
                    env = gen_env!($prefix, "GOOGLE_SERVICE_ACCOUNT"),
                    action
                )]
                pub google_service_account: Option<String>,

                /// When using Microsoft Azure as the object store, set this to the
                /// name you see when going to All Services > Storage accounts > `[name]`.
                ///
                /// Must also set `--object-store=azure`, `--bucket`, and
                /// `--azure-storage-access-key`.
                #[clap(
                    id = gen_name!($prefix, "azure-storage-account"),
                    long = gen_name!($prefix, "azure-storage-account"),
                    env = gen_env!($prefix, "AZURE_STORAGE_ACCOUNT"),
                    action
                )]
                pub azure_storage_account: Option<String>,

                /// When using Microsoft Azure as the object store, set this to one of the
                /// Key values in the Storage account's Settings > Access keys.
                ///
                /// Must also set `--object-store=azure`, `--bucket`, and
                /// `--azure-storage-account`.
                ///
                /// Prefer the environment variable over the command line flag in shared
                /// environments.
                #[clap(
                    id = gen_name!($prefix, "azure-storage-access-key"),
                    long = gen_name!($prefix, "azure-storage-access-key"),
                    env = gen_env!($prefix, "AZURE_STORAGE_ACCESS_KEY"),
                    action
                )]
                pub azure_storage_access_key: Option<String>,

                /// When using Microsoft Azure blob storage, you can set a custom endpoint.
                ///
                /// Useful for local development with Azure storage emulators like Azurite.
                ///
                /// Must also set `--object-store=azure`, `--bucket`, `--azure-storage-account`,
                /// and `--azure-storage-access-key`.
                #[clap(
                    id = gen_name!($prefix, "azure-endpoint"),
                    long = gen_name!($prefix, "azure-endpoint"),
                    env = gen_env!($prefix, "AZURE_ENDPOINT"),
                    action
                )]
                pub azure_endpoint: Option<Endpoint>,

                /// Allow unencrypted HTTP connection to Azure.
                #[clap(
                    id = gen_name!($prefix, "azure-allow-http"),
                    long = gen_name!($prefix, "azure-allow-http"),
                    env = gen_env!($prefix, "AZURE_ALLOW_HTTP"),
                    action
                )]
                pub azure_allow_http: bool,

                /// When using a network-based object store, limit the number of connection to this value.
                #[clap(
                    id = gen_name!($prefix, "object-store-connection-limit"),
                    long = gen_name!($prefix, "object-store-connection-limit"),
                    env = gen_env!($prefix, "OBJECT_STORE_CONNECTION_LIMIT"),
                    default_value = "16",
                    action
                )]
                pub object_store_connection_limit: NonZeroUsize,

                /// Force HTTP/2 connection to network-based object stores.
                ///
                /// This implies "prior knowledge" as per RFC7540 section 3.4.
                #[clap(
                    id = gen_name!($prefix, "object-store-http2-only"),
                    long = gen_name!($prefix, "object-store-http2-only"),
                    env = gen_env!($prefix, "OBJECT_STORE_HTTP2_ONLY"),
                    action
                )]
                pub http2_only: bool,

                /// Set max frame size (in bytes/octets) for HTTP/2 connection.
                ///
                /// If not set, this uses the `object_store`/`reqwest` default.
                ///
                /// Usually you want to set this as high as possible -- the maximum allowed by the standard is `2^24-1 = 16,777,215`.
                /// However under some circumstances (like buggy middleware or upstream providers getting unhappy), you may be
                /// required to pick something else.
                #[clap(
                    id = gen_name!($prefix, "object-store-http2-max-frame-size"),
                    long = gen_name!($prefix, "object-store-http2-max-frame-size"),
                    env = gen_env!($prefix, "OBJECT_STORE_HTTP2_MAX_FRAME_SIZE"),
                    action
                )]
                pub http2_max_frame_size: Option<u32>,

                /// The maximum number of times to retry a request
                ///
                /// Set to 0 to disable retries
                #[clap(
                    id = gen_name!($prefix, "object-store-max-retries"),
                    long = gen_name!($prefix, "object-store-max-retries"),
                    env = gen_env!($prefix, "OBJECT_STORE_MAX_RETRIES"),
                    action
                )]
                pub max_retries: Option<usize>,

                /// The maximum length of time from the initial request
                /// after which no further retries will be attempted
                ///
                /// This not only bounds the length of time before a server
                /// error will be surfaced to the application, but also bounds
                /// the length of time a request's credentials must remain valid.
                ///
                /// As requests are retried without renewing credentials or
                /// regenerating request payloads, this number should be kept
                /// below 5 minutes to avoid errors due to expired credentials
                /// and/or request payloads
                #[clap(
                    id = gen_name!($prefix, "object-store-retry-timeout"),
                    long = gen_name!($prefix, "object-store-retry-timeout"),
                    env = gen_env!($prefix, "OBJECT_STORE_RETRY_TIMEOUT"),
                    value_parser = humantime::parse_duration,
                    action
                )]
                pub retry_timeout: Option<Duration>,


                /// Endpoint of an S3 compatible, HTTP/2 enabled object store cache.
                #[clap(
                    id = gen_name!($prefix, "object-store-cache-endpoint"),
                    long = gen_name!($prefix, "object-store-cache-endpoint"),
                    env = gen_env!($prefix, "OBJECT_STORE_CACHE_ENDPOINT"),
                    action
                )]
                pub cache_endpoint: Option<Endpoint>,
            }

            impl [<$prefix:camel ObjectStoreConfig>] {

                /// Create a new instance for all-in-one mode, only allowing some arguments.
                pub fn new(database_directory: Option<PathBuf>) -> Self {
                    let object_store = match &database_directory {
                        Some(dir) => {
                            info!("Object store: File-based in `{}`", dir.display());
                            ObjectStoreType::File
                        }
                        None => {
                            info!("Object store: In-memory");
                            ObjectStoreType::Memory
                        }
                    };

                    Self {
                        aws_access_key_id: Default::default(),
                        aws_allow_http: Default::default(),
                        aws_default_region: FALLBACK_AWS_REGION.to_string(),
                        aws_endpoint: Default::default(),
                        aws_secret_access_key: Default::default(),
                        aws_session_token: Default::default(),
                        aws_skip_signature: Default::default(),
                        aws_credentials_file: Default::default(),
                        azure_storage_access_key: Default::default(),
                        azure_storage_account: Default::default(),
                        azure_endpoint: Default::default(),
                        azure_allow_http: Default::default(),
                        bucket: Default::default(),
                        database_directory,
                        google_service_account: Default::default(),
                        object_store,
                        object_store_connection_limit: NonZeroUsize::new(16).unwrap(),
                        http2_only: Default::default(),
                        http2_max_frame_size: Default::default(),
                        max_retries: Default::default(),
                        retry_timeout: Default::default(),
                        cache_endpoint: Default::default(),
                    }
                }

                #[cfg(any(feature = "aws", feature = "azure", feature = "gcp"))]
                fn client_options(&self) -> object_store::ClientOptions {
                    let mut options = object_store::ClientOptions::new();

                    if self.http2_only {
                        options = options.with_http2_only();
                    }
                    if let Some(sz) = self.http2_max_frame_size {
                        options = options.with_http2_max_frame_size(sz);
                    }

                    options
                }

                #[cfg(feature = "gcp")]
                fn new_gcs(&self) -> Result<Arc<DynObjectStore>, ParseError> {
                    use object_store::gcp::GoogleCloudStorageBuilder;
                    use object_store::limit::LimitStore;

                    info!(bucket=?self.bucket, object_store_type="GCS", "Object Store");

                    let mut builder = GoogleCloudStorageBuilder::new().with_client_options(self.client_options()).with_retry(self.retry_config());

                    if let Some(bucket) = &self.bucket {
                        builder = builder.with_bucket_name(bucket);
                    }
                    if let Some(account) = &self.google_service_account {
                        builder = builder.with_service_account_path(account);
                    }

                    Ok(Arc::new(LimitStore::new(
                        builder.build().context(InvalidGCSConfigSnafu)?,
                        self.object_store_connection_limit.get(),
                    )))
                }

                #[cfg(not(feature = "gcp"))]
                fn new_gcs(&self) -> Result<Arc<DynObjectStore>, ParseError> {
                    panic!("GCS support not enabled, recompile with the gcp feature enabled")
                }

                #[cfg(feature = "aws")]
                fn new_s3(&self) -> Result<Arc<DynObjectStore>, ParseError> {
                    use object_store::limit::LimitStore;

                    info!(
                        bucket=?self.bucket,
                        endpoint=?self.aws_endpoint,
                        object_store_type="S3",
                        "Object Store"
                    );

                    Ok(Arc::new(LimitStore::new(
                        self.build_s3()?,
                        self.object_store_connection_limit.get(),
                    )))
                }

                #[cfg(any(feature = "aws", feature = "azure", feature = "gcp"))]
                fn retry_config(&self) -> object_store::RetryConfig {
                    let mut retry_config = object_store::RetryConfig::default();

                    if let Some(max_retries) = self.max_retries {
                        retry_config.max_retries = max_retries;
                    }

                    if let Some(retry_timeout) = self.retry_timeout {
                        retry_config.retry_timeout = retry_timeout;
                    }

                    retry_config
                }

                /// If further configuration of S3 is needed beyond what this module provides, use this function
                /// to create an [`object_store::aws::AmazonS3Builder`] and further customize, then call `.build()`
                /// directly.
                #[cfg(feature = "aws")]
                pub fn s3_builder(&self) -> Result<object_store::aws::AmazonS3Builder, ParseError> {
                    use object_store::aws::AmazonS3Builder;
                    use object_store::aws::S3ConditionalPut;

                    let mut builder = AmazonS3Builder::from_env()
                        .with_client_options(self.client_options())
                        .with_allow_http(self.aws_allow_http)
                        .with_region(&self.aws_default_region)
                        .with_retry(self.retry_config())
                        .with_skip_signature(self.aws_skip_signature)
                        .with_imdsv1_fallback()
                        // Enable conditional PUT requests, so that the cluster-wide catalog
                        // can leverage PUT IF NOT EXISTS semantics when writing new catalog files
                        .with_conditional_put(S3ConditionalPut::ETagMatch);

                    if let Some(bucket) = &self.bucket {
                        builder = builder.with_bucket_name(bucket);
                    }
                    if let Some(endpoint) = &self.aws_endpoint {
                        builder = builder.with_endpoint(endpoint.clone());
                    }

                    Ok(builder)
                }

                #[cfg(feature = "aws")]
                fn build_s3(&self) -> Result<Arc<dyn ObjectStore>, ParseError> {
                    let mut builder = self.s3_builder()?;

                    let r = if let Some(path) = &self.aws_credentials_file {
                        let credentials = Arc::new(AwsCredentialReloader::new(path.into())? );
                        credentials.spawn_background_updates();

                        builder = builder.with_credentials(Arc::clone(&credentials) as _);
                        let r: Arc<dyn ObjectStore> = builder.build().map(Arc::new).context(InvalidS3ConfigSnafu)? as _;

                        let reauthing_object_store = ReauthingObjectStore::new_arc(r, credentials) ;

                        reauthing_object_store
                    } else {
                        if let Some(key_id) = &self.aws_access_key_id {
                        builder = builder.with_access_key_id(key_id.get());
                        }
                        if let Some(token) = &self.aws_session_token {
                            builder = builder.with_token(token);
                        }
                        if let Some(secret) = &self.aws_secret_access_key {
                            builder = builder.with_secret_access_key(secret.get());
                        }
                        let r: Arc<dyn ObjectStore> = builder.build().map(Arc::new).context(InvalidS3ConfigSnafu)? as _;

                        r
                    };

                    Ok(r)
                }

                #[cfg(feature = "aws")]
                pub fn build_s3_signer(&self) -> Result<object_store::aws::AmazonS3, ParseError> {
                    let builder = self.s3_builder()?;

                    builder.build().context(InvalidS3ConfigSnafu)
                }

                #[cfg(not(feature = "aws"))]
                fn new_s3(&self) -> Result<Arc<DynObjectStore>, ParseError> {
                    panic!("S3 support not enabled, recompile with the aws feature enabled")
                }

                #[cfg(feature = "azure")]
                fn new_azure(&self) -> Result<Arc<DynObjectStore>, ParseError> {
                    use object_store::azure::MicrosoftAzureBuilder;
                    use object_store::limit::LimitStore;

                    info!(bucket=?self.bucket, account=?self.azure_storage_account,
                          endpoint=?self.azure_endpoint, object_store_type="Azure", "Object Store");

                    let mut builder = MicrosoftAzureBuilder::new().with_client_options(self.client_options());

                    if let Some(bucket) = &self.bucket {
                        builder = builder.with_container_name(bucket);
                    }
                    if let Some(account) = &self.azure_storage_account {
                        builder = builder.with_account(account);
                    }
                    if let Some(key) = &self.azure_storage_access_key {
                        builder = builder.with_access_key(key);
                    }

                    builder = builder.with_allow_http(self.azure_allow_http);

                    if let Some(endpoint) = &self.azure_endpoint {
                        builder = builder.with_endpoint(endpoint.to_string());
                    }

                    Ok(Arc::new(LimitStore::new(
                        builder.build().context(InvalidAzureConfigSnafu)?,
                        self.object_store_connection_limit.get(),
                    )))
                }

                #[cfg(not(feature = "azure"))]
                fn new_azure(&self) -> Result<Arc<DynObjectStore>, ParseError> {
                    panic!("Azure blob storage support not enabled, recompile with the azure feature enabled")
                }

                /// Build cache store.
                #[cfg(feature = "aws")]
                pub fn make_cache_store(
                    &self
                ) -> Result<Option<Arc<DynObjectStore>>, ParseError> {
                    let Some(endpoint) = &self.cache_endpoint else {
                        return Ok(None);
                    };

                    let store = Arc::new(object_store::aws::AmazonS3Builder::from_env()
                        // bucket name is ignored by our cache server
                        .with_bucket_name(self.bucket.as_deref().unwrap_or("placeholder"))
                        .with_client_options(
                            object_store::ClientOptions::new()
                                .with_allow_http(true)
                                .with_http2_only()
                                // this is the maximum that is allowed by the HTTP/2 standard and is meant to lower the overhead of
                                // submitting TCP packages to the kernel
                                .with_http2_max_frame_size(16777215),
                        )
                        .with_endpoint(endpoint.clone())
                        .with_retry(object_store::RetryConfig {
                            max_retries: 3,
                            ..Default::default()
                        })
                        .with_skip_signature(true)
                        .build()
                        .context(InvalidS3ConfigSnafu)?);

                    Ok(Some(store))
                }

                /// Build cache store.
                #[cfg(not(feature = "aws"))]
                pub fn make_cache_store(
                    &self
                ) -> Result<Option<Arc<DynObjectStore>>, ParseError> {
                    match &self.cache_endpoint {
                        Some(_) => panic!("Cache support not enabled, recompile with the aws feature enabled"),
                        None => Ok(None),
                    }
                }

                /// Create config-dependant object store.
                pub fn make_object_store(&self) -> Result<Arc<DynObjectStore>, ParseError> {
                    if let Some(data_dir) = &self.database_directory {
                        if !matches!(&self.object_store, ObjectStoreType::File) {
                            warn!(?data_dir, object_store_type=?self.object_store,
                                  "--data-dir / `INFLUXDB3_DB_DIR` ignored. It only affects 'file' object stores");
                        }
                    }

                    let object_store: Arc<DynObjectStore> = match &self.object_store {
                        ObjectStoreType::Memory => {
                            info!(object_store_type = "Memory", "Object Store");
                            Arc::new(InMemory::new())
                        }
                        ObjectStoreType::MemoryThrottled => {
                            let config = ThrottleConfig {
                                // for every call: assume a 100ms latency
                                wait_delete_per_call: Duration::from_millis(100),
                                wait_get_per_call: Duration::from_millis(100),
                                wait_list_per_call: Duration::from_millis(100),
                                wait_list_with_delimiter_per_call: Duration::from_millis(100),
                                wait_put_per_call: Duration::from_millis(100),

                                // for list operations: assume we need 1 call per 1k entries at 100ms
                                wait_list_per_entry: Duration::from_millis(100) / 1_000,
                                wait_list_with_delimiter_per_entry: Duration::from_millis(100) / 1_000,

                                // for upload/download: assume 1GByte/s
                                wait_get_per_byte: Duration::from_secs(1) / 1_000_000_000,
                            };

                            info!(?config, object_store_type = "Memory", "Object Store");
                            Arc::new(ThrottledStore::new(InMemory::new(), config))
                        }

                        ObjectStoreType::Google => self.new_gcs()?,
                        ObjectStoreType::S3 => self.new_s3()?,
                        ObjectStoreType::Azure => self.new_azure()?,
                        ObjectStoreType::File => self.new_local_file_system()?,
                    };

                    Ok(object_store)
                }

                fn new_local_file_system(&self) -> Result<Arc<LocalFileSystemWithSortedListOp>, ParseError> {
                    match self.database_directory.as_ref() {
                        Some(db_dir) => {
                            info!(?db_dir, object_store_type = "Directory", "Object Store");
                            fs::create_dir_all(db_dir).context(CreatingDatabaseDirectorySnafu { path: db_dir })?;

                            let store = LocalFileSystemWithSortedListOp::new_with_prefix(db_dir)?;
                            Ok(Arc::new(store))
                        }
                        None => MissingObjectStoreConfigSnafu {
                            object_store: ObjectStoreType::File,
                            missing: "data-dir",
                        }
                        .fail()?,
                    }
                }

            }
        }
    };
}

object_store_config!("source"); // SourceObjectStoreConfig
object_store_config!("sink"); // SinkObjectStoreConfig
object_store_config!(); // ObjectStoreConfig

/// Object-store type.
#[derive(Debug, Copy, Clone, PartialEq, Eq, clap::ValueEnum)]
pub enum ObjectStoreType {
    /// In-memory.
    Memory,

    /// In-memory with additional throttling applied for testing
    MemoryThrottled,

    /// Filesystem.
    File,

    /// AWS S3.
    S3,

    /// GCS.
    Google,

    /// Azure object store.
    Azure,
}

impl ObjectStoreType {
    /// Map enum variant to static string, followed inverse of clap parsing rules.
    pub fn as_str(&self) -> &str {
        match self {
            Self::Memory => "memory",
            Self::MemoryThrottled => "memory-throttled",
            Self::File => "file",
            Self::S3 => "s3",
            Self::Google => "google",
            Self::Azure => "azure",
        }
    }
}

/// The `object_store::signer::Signer` trait is implemented for AWS and local file systems, so when
/// the AWS feature is enabled and the configured object store is S3 or the local file system,
/// return a signer.
#[cfg(feature = "aws")]
pub fn make_presigned_url_signer(
    config: &ObjectStoreConfig,
) -> Result<Option<Arc<dyn object_store::signer::Signer>>, ParseError> {
    match &config.object_store {
        ObjectStoreType::S3 => Ok(Some(Arc::new(config.build_s3_signer()?))),
        ObjectStoreType::File => Ok(Some(Arc::new(LocalUploadSigner::new(config)?))),
        _ => Ok(None),
    }
}

/// The `object_store::signer::Signer` trait is implemented for AWS and local file systems, so if
/// the AWS feature isn't enabled, only return a signer for local file systems.
#[cfg(not(feature = "aws"))]
pub fn make_presigned_url_signer(
    config: &ObjectStoreConfig,
) -> Result<Option<Arc<dyn object_store::signer::Signer>>, ParseError> {
    match &config.object_store {
        ObjectStoreType::File => Ok(Some(Arc::new(LocalUploadSigner::new(config)?))),
        _ => Ok(None),
    }
}

/// An implementation of `object_store::signer::Signer` suitable for local testing.
/// Does NOT actually create presigned URLs; only returns the given path resolved to an absolute `file://`
/// URL that the bulk ingester can write directly to only if the bulk ingester is running on the
/// same system.
///
/// Again, will not work and not intended to work in production, but is useful in local testing.
#[derive(Debug)]
pub struct LocalUploadSigner {
    inner: Arc<LocalFileSystemWithSortedListOp>,
}

impl LocalUploadSigner {
    fn new(config: &ObjectStoreConfig) -> Result<Self, ParseError> {
        Ok(Self {
            inner: config.new_local_file_system()?,
        })
    }
}

#[derive(Clone, Debug, Deserialize, Serialize)]
struct AwsFileCredential {
    aws_access_key_id: String,
    aws_secret_access_key: String,
    aws_session_token: Option<String>,
    expiry: Option<u64>,
}

impl From<AwsFileCredential> for AwsCredential {
    fn from(afc: AwsFileCredential) -> AwsCredential {
        AwsCredential {
            key_id: afc.aws_access_key_id,
            secret_key: afc.aws_secret_access_key,
            token: afc.aws_session_token,
        }
    }
}

#[derive(Clone, Debug)]
pub struct AwsCredentialReloader {
    path: PathBuf,
    current: Arc<RwLock<Arc<AwsCredential>>>,
    time_provider: Arc<dyn TimeProvider>,
}

// default AWS credential reloader interval is one hour, assuming we've exceeded the configured
// expiry
const AWS_CREDENTIAL_RELOADER_DEFAULT_INTERVAL_SECONDS: u64 = 60 * 60;

fn default_check_in() -> std::time::Duration {
    std::time::Duration::from_secs(AWS_CREDENTIAL_RELOADER_DEFAULT_INTERVAL_SECONDS)
}

impl AwsCredentialReloader {
    pub fn new(path: PathBuf) -> std::result::Result<Self, ParseError> {
        let cloned = path.clone();
        let afc = Self::get_file_credentials_sync(&cloned)?;
        let current = Arc::new(RwLock::new(Arc::new(afc.into())));
        Ok(Self {
            path,
            current,
            time_provider: Arc::new(SystemProvider::new()),
        })
    }

    pub fn spawn_background_updates(&self) {
        let cloned = self.clone();
        tokio::spawn(async move {
            loop {
                let next_check_in = cloned
                    .check_and_update()
                    .await
                    .and_then(|next_check_ts| {
                        let now = cloned.time_provider.now().timestamp() as u64;
                        if next_check_ts < now {
                            None
                        } else {
                            Some(Duration::from_secs(now - next_check_ts))
                        }
                    })
                    .unwrap_or_else(default_check_in);

                cloned.time_provider.sleep(next_check_in).await;
            }
        });
    }

    fn get_file_credentials_sync(
        path: &PathBuf,
    ) -> std::result::Result<AwsFileCredential, ParseError> {
        let contents =
            std::fs::read_to_string(path).context(ReadingAwsFileCredentialsToStringSnafu)?;

        let afc = serde_json::from_str(&contents).context(DeserializingAwsFileCredentialsSnafu)?;

        Ok(afc)
    }

    async fn get_file_credentials(
        path: &PathBuf,
    ) -> std::result::Result<AwsFileCredential, ParseError> {
        let contents = tokio::fs::read_to_string(path)
            .await
            .context(ReadingAwsFileCredentialsToStringSnafu)?;

        let afc = serde_json::from_str(&contents).context(DeserializingAwsFileCredentialsSnafu)?;

        Ok(afc)
    }

    async fn check_and_update(&self) -> Option<u64> {
        let file_credentials = match Self::get_file_credentials(&self.path).await {
            Ok(c) => c,
            Err(e) => {
                info!(error = ?e, path = ?self.path, "could not read aws credentials file");
                return None;
            }
        };
        let next_expiry = file_credentials.expiry;
        let credentials = file_credentials.into();

        let do_update = {
            let guard = self.current.read().await;
            guard.as_ref() != &credentials
        };

        if do_update {
            let mut guard = self.current.write().await;
            *guard = Arc::new(credentials);

            next_expiry
        } else {
            None
        }
    }
}

#[async_trait::async_trait]
impl CredentialProvider for AwsCredentialReloader {
    type Credential = AwsCredential;

    async fn get_credential(&self) -> object_store::Result<Arc<Self::Credential>> {
        let current = self.current.read().await;
        Ok(Arc::clone(&current))
    }
}

#[derive(Debug, Clone)]
struct ReauthingObjectStore {
    inner: Arc<dyn ObjectStore>,
    credential_reloader: Arc<AwsCredentialReloader>,
}

impl ReauthingObjectStore {
    fn new_arc(
        inner: Arc<dyn ObjectStore>,
        credential_reloader: Arc<AwsCredentialReloader>,
    ) -> Arc<dyn ObjectStore> {
        Arc::new(Self {
            inner,
            credential_reloader,
        })
    }
}

impl std::fmt::Display for ReauthingObjectStore {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.inner)
    }
}

macro_rules! retry_if_unauthenticated {
    ($self:ident, $expression:expr) => {
        match $expression {
            Ok(v) => Ok(v),
            Err(object_store::Error::Generic { store, source, }) => {
                let msg = format!("{source:?}");
                if msg.contains("ExpiredToken") {
                    let path = $self.credential_reloader.path.display();
                    warn!(error = ?source, "authentication with object store failed, attempting to reload credentials from {path}");
                    let _ = $self.credential_reloader.check_and_update().await;
                    $expression
                } else {
                    Err(object_store::Error::Generic{ store, source })
                }
            }
            Err(object_store::Error::Unauthenticated { source, .. }) => {
                let path = $self.credential_reloader.path.display();
                warn!(error = ?source, "authentication with object store failed, attempting to reload credentials from {path}");
                let _ = $self.credential_reloader.check_and_update().await;
                $expression
            }
            Err(e) => Err(e),
        }
    }
}

#[async_trait]
impl object_store::ObjectStore for ReauthingObjectStore {
    async fn copy(&self, from: &Path, to: &Path) -> object_store::Result<()> {
        retry_if_unauthenticated!(self, self.inner.as_ref().copy(from, to).await)
    }

    async fn copy_if_not_exists(&self, from: &Path, to: &Path) -> object_store::Result<()> {
        retry_if_unauthenticated!(self, self.inner.as_ref().copy_if_not_exists(from, to).await)
    }

    async fn delete(&self, location: &Path) -> object_store::Result<()> {
        retry_if_unauthenticated!(self, self.inner.as_ref().delete(location).await)
    }

    async fn get_opts(
        &self,
        location: &Path,
        options: GetOptions,
    ) -> object_store::Result<GetResult> {
        retry_if_unauthenticated!(
            self,
            self.inner
                .as_ref()
                .get_opts(location, options.clone())
                .await
        )
    }

    fn list(&self, prefix: Option<&Path>) -> BoxStream<'static, object_store::Result<ObjectMeta>> {
        let inner_cloned = Arc::clone(&self.inner);
        let items: Vec<object_store::Result<ObjectMeta, _>> = futures::executor::block_on(async {
            // we could use TryStreamExt.collect() here to drop all collected results and
            // return the first error we encounter, but users of the ObjectStore API will
            // probably expect to have to deal with errors one element at a time anyway
            inner_cloned.list(prefix).collect().await
        });

        if items.is_empty() {
            return futures::stream::iter(items).boxed();
        }

        if let Err(object_store::Error::Unauthenticated { source, .. }) = &items[0] {
            warn!(error = ?source, "authentication with object store failed, attempting to reload from disk");
            let items: Vec<Result<ObjectMeta, _>> = futures::executor::block_on(async {
                self.credential_reloader.check_and_update().await;
                // we could use TryStreamExt.collect() here to drop all collected results and
                // return the first error we encounter, but users of the ObjectStore API will
                // probably expect to have to deal with errors one element at a time anyway
                self.inner.as_ref().list(prefix).collect().await
            });
            return futures::stream::iter(items).boxed();
        }

        futures::stream::iter(items).boxed()
    }

    async fn list_with_delimiter(&self, prefix: Option<&Path>) -> object_store::Result<ListResult> {
        retry_if_unauthenticated!(self, self.inner.as_ref().list_with_delimiter(prefix).await)
    }

    async fn put_multipart_opts(
        &self,
        location: &Path,
        opts: PutMultipartOpts,
    ) -> object_store::Result<Box<dyn MultipartUpload>> {
        retry_if_unauthenticated!(
            self,
            self.inner
                .as_ref()
                .put_multipart_opts(location, opts.clone())
                .await
        )
    }

    async fn put_opts(
        &self,
        location: &Path,
        payload: PutPayload,
        options: PutOptions,
    ) -> object_store::Result<PutResult> {
        retry_if_unauthenticated!(
            self,
            self.inner
                .as_ref()
                .put_opts(location, payload.clone(), options.clone())
                .await
        )
    }
}

#[async_trait]
impl object_store::signer::Signer for LocalUploadSigner {
    async fn signed_url(
        &self,
        _method: http_1::Method,
        path: &Path,
        _expires_in: Duration,
    ) -> Result<Url, object_store::Error> {
        self.inner.inner.path_to_filesystem(path).and_then(|path| {
            Url::from_file_path(&path).map_err(|_| object_store::Error::InvalidPath {
                source: object_store::path::Error::InvalidPath { path },
            })
        })
    }
}

#[derive(Debug, Snafu)]
#[allow(missing_docs)]
pub enum CheckError {
    #[snafu(display("Cannot read from object store: {}", source))]
    CannotReadObjectStore { source: object_store::Error },
}

#[cfg(test)]
mod tests {
    use super::*;
    use clap::Parser;
    use iox_time::{MockProvider, Time};
    use object_store::ObjectStore;
    use std::{env, str::FromStr, sync::Mutex};
    use tempfile::TempDir;

    /// The current object store store configurations.
    enum StoreConfigs {
        Base(ObjectStoreConfig),
        Source(SourceObjectStoreConfig),
        Sink(SinkObjectStoreConfig),
    }

    impl StoreConfigs {
        pub(crate) fn make_object_store(&self) -> Result<Arc<dyn ObjectStore>, ParseError> {
            self.object_store_inner()
        }

        fn object_store_inner(&self) -> Result<Arc<dyn ObjectStore>, ParseError> {
            match self {
                Self::Base(o) => o.make_object_store(),
                Self::Source(o) => o.make_object_store(),
                Self::Sink(o) => o.make_object_store(),
            }
        }
    }

    #[test]
    fn object_store_flag_is_required() {
        // Since object-store is now required, parsing should fail without it
        assert!(ObjectStoreConfig::try_parse_from(["server"]).is_err());
        assert!(SourceObjectStoreConfig::try_parse_from(["server"]).is_err());
        assert!(SinkObjectStoreConfig::try_parse_from(["server"]).is_err());
    }

    #[test]
    fn explicitly_set_object_store_to_memory() {
        let configs = vec![
            StoreConfigs::Base(
                ObjectStoreConfig::try_parse_from(["server", "--object-store", "memory"]).unwrap(),
            ),
            StoreConfigs::Source(
                SourceObjectStoreConfig::try_parse_from([
                    "server",
                    "--source-object-store",
                    "memory",
                ])
                .unwrap(),
            ),
            StoreConfigs::Sink(
                SinkObjectStoreConfig::try_parse_from(["server", "--sink-object-store", "memory"])
                    .unwrap(),
            ),
        ];
        for config in configs {
            let object_store = config.make_object_store().unwrap();
            assert_eq!(&object_store.to_string(), "InMemory")
        }
    }

    #[test]
    fn default_url_signer_is_none() {
        let config =
            ObjectStoreConfig::try_parse_from(["server", "--object-store", "memory"]).unwrap();

        let signer = make_presigned_url_signer(&config).unwrap();
        assert!(signer.is_none(), "Expected None, got {signer:?}");
    }

    #[test]
    #[cfg(feature = "aws")]
    fn valid_s3_config() {
        let configs = vec![
            StoreConfigs::Base(
                ObjectStoreConfig::try_parse_from([
                    "server",
                    "--object-store",
                    "s3",
                    "--bucket",
                    "mybucket",
                    "--aws-access-key-id",
                    "NotARealAWSAccessKey",
                    "--aws-secret-access-key",
                    "NotARealAWSSecretAccessKey",
                ])
                .unwrap(),
            ),
            StoreConfigs::Source(
                SourceObjectStoreConfig::try_parse_from([
                    "server",
                    "--source-object-store",
                    "s3",
                    "--source-bucket",
                    "mybucket",
                    "--source-aws-access-key-id",
                    "NotARealAWSAccessKey",
                    "--source-aws-secret-access-key",
                    "NotARealAWSSecretAccessKey",
                ])
                .unwrap(),
            ),
            StoreConfigs::Sink(
                SinkObjectStoreConfig::try_parse_from([
                    "server",
                    "--sink-object-store",
                    "s3",
                    "--sink-bucket",
                    "mybucket",
                    "--sink-aws-access-key-id",
                    "NotARealAWSAccessKey",
                    "--sink-aws-secret-access-key",
                    "NotARealAWSSecretAccessKey",
                ])
                .unwrap(),
            ),
        ];

        for config in configs {
            let object_store = config.make_object_store().unwrap();
            assert_eq!(
                &object_store.to_string(),
                "LimitStore(16, AmazonS3(mybucket))"
            )
        }
    }

    #[test]
    #[cfg(feature = "aws")]
    fn valid_s3_endpoint_url() {
        ObjectStoreConfig::try_parse_from([
            "server",
            "--aws-endpoint",
            "http://whatever.com",
            "--object-store",
            "s3",
        ])
        .expect("must successfully parse config with absolute AWS endpoint URL");
    }

    #[test]
    #[cfg(feature = "aws")]
    fn invalid_s3_endpoint_url_fails_clap_parsing() {
        let result =
            ObjectStoreConfig::try_parse_from(["server", "--aws-endpoint", "whatever.com"]);
        assert!(result.is_err(), "{result:?}");
        let result = SourceObjectStoreConfig::try_parse_from([
            "server",
            "--source-aws-endpoint",
            "whatever.com",
        ]);
        assert!(result.is_err(), "{result:?}");
        let result = SinkObjectStoreConfig::try_parse_from([
            "server",
            "--sink-aws-endpoint",
            "whatever.com",
        ]);
        assert!(result.is_err(), "{result:?}");
    }

    #[test]
    #[cfg(feature = "aws")]
    fn s3_config_missing_params() {
        let mut config =
            ObjectStoreConfig::try_parse_from(["server", "--object-store", "s3"]).unwrap();

        // clean out eventual leaks via env variables
        config.bucket = None;

        let err = config.make_object_store().unwrap_err().to_string();

        assert_eq!(
            err,
            "Error configuring Amazon S3: Generic S3 error: Missing bucket name"
        );
    }

    #[test]
    #[cfg(feature = "aws")]
    fn valid_s3_url_signer() {
        let config = ObjectStoreConfig::try_parse_from([
            "server",
            "--object-store",
            "s3",
            "--bucket",
            "mybucket",
            "--aws-access-key-id",
            "NotARealAWSAccessKey",
            "--aws-secret-access-key",
            "NotARealAWSSecretAccessKey",
        ])
        .unwrap();

        assert!(make_presigned_url_signer(&config).unwrap().is_some());

        // Even with the aws feature on, object stores (other than local files) shouldn't create a
        // signer.
        let config =
            ObjectStoreConfig::try_parse_from(["server", "--object-store", "memory"]).unwrap();

        let signer = make_presigned_url_signer(&config).unwrap();
        assert!(signer.is_none(), "Expected None, got {signer:?}");
    }

    #[test]
    #[cfg(feature = "aws")]
    fn s3_url_signer_config_missing_params() {
        let mut config =
            ObjectStoreConfig::try_parse_from(["server", "--object-store", "s3"]).unwrap();

        // clean out eventual leaks via env variables
        config.bucket = None;

        let err = make_presigned_url_signer(&config).unwrap_err().to_string();

        assert_eq!(
            err,
            "Error configuring Amazon S3: Generic S3 error: Missing bucket name"
        );
    }

    #[test]
    #[cfg(feature = "gcp")]
    fn valid_google_config() {
        use std::io::Write;
        use tempfile::NamedTempFile;

        let mut file = NamedTempFile::new().expect("tempfile should be created");
        const FAKE_KEY: &str = r#"{"private_key": "private_key", "private_key_id": "private_key_id", "client_email":"client_email", "disable_oauth":true}"#;
        writeln!(file, "{FAKE_KEY}").unwrap();
        let path = file.path().to_str().expect("file path should exist");

        let configs = vec![
            StoreConfigs::Base(
                ObjectStoreConfig::try_parse_from([
                    "server",
                    "--object-store",
                    "google",
                    "--bucket",
                    "mybucket",
                    "--google-service-account",
                    path,
                ])
                .unwrap(),
            ),
            StoreConfigs::Source(
                SourceObjectStoreConfig::try_parse_from([
                    "server",
                    "--source-object-store",
                    "google",
                    "--source-bucket",
                    "mybucket",
                    "--source-google-service-account",
                    path,
                ])
                .unwrap(),
            ),
            StoreConfigs::Sink(
                SinkObjectStoreConfig::try_parse_from([
                    "server",
                    "--sink-object-store",
                    "google",
                    "--sink-bucket",
                    "mybucket",
                    "--sink-google-service-account",
                    path,
                ])
                .unwrap(),
            ),
        ];

        for config in configs {
            let object_store = config.make_object_store().unwrap();
            assert_eq!(
                &object_store.to_string(),
                "LimitStore(16, GoogleCloudStorage(mybucket))"
            )
        }
    }

    #[test]
    #[cfg(feature = "gcp")]
    fn google_config_missing_params() {
        let mut config =
            ObjectStoreConfig::try_parse_from(["server", "--object-store", "google"]).unwrap();

        // clean out eventual leaks via env variables
        config.bucket = None;

        let err = config.make_object_store().unwrap_err().to_string();

        assert_eq!(
            err,
            "Error configuring GCS: Generic GCS error: Missing bucket name"
        );
    }

    #[test]
    #[cfg(feature = "azure")]
    fn valid_azure_config() {
        let config = ObjectStoreConfig::try_parse_from([
            "server",
            "--object-store",
            "azure",
            "--bucket",
            "mybucket",
            "--azure-storage-account",
            "NotARealStorageAccount",
            "--azure-storage-access-key",
            "Zm9vYmFy", // base64 encoded "foobar"
        ])
        .unwrap();

        let object_store = config.make_object_store().unwrap();
        assert_eq!(
            &object_store.to_string(),
            "LimitStore(16, MicrosoftAzure { account: NotARealStorageAccount, container: mybucket })"
        )
    }

    #[test]
    #[cfg(feature = "azure")]
    fn azure_config_missing_params() {
        let mut config =
            ObjectStoreConfig::try_parse_from(["server", "--object-store", "azure"]).unwrap();

        // clean out eventual leaks via env variables
        config.bucket = None;

        let err = config.make_object_store().unwrap_err().to_string();

        assert_eq!(
            err,
            "Error configuring Microsoft Azure: Generic MicrosoftAzure error: Container name must be specified"
        );
    }

    #[test]
    fn valid_file_config() {
        let root = TempDir::new().unwrap();
        let root_path = root.path().to_str().unwrap();

        let config = ObjectStoreConfig::try_parse_from([
            "server",
            "--object-store",
            "file",
            "--data-dir",
            root_path,
        ])
        .unwrap();

        let object_store = config.make_object_store().unwrap().to_string();
        assert!(
            object_store.starts_with("LocalFileSystem"),
            "{}",
            object_store
        )
    }

    #[test]
    fn file_config_missing_params() {
        // this test tests for failure to configure the object store because of data-dir configuration missing
        // if the INFLUXDB3_DB_DIR env variable is set, the test fails because the configuration is
        // actually present.
        unsafe {
            env::remove_var("INFLUXDB3_DB_DIR");
        }

        let configs = vec![
            StoreConfigs::Base(
                ObjectStoreConfig::try_parse_from(["server", "--object-store", "file"]).unwrap(),
            ),
            StoreConfigs::Source(
                SourceObjectStoreConfig::try_parse_from([
                    "server",
                    "--source-object-store",
                    "file",
                ])
                .unwrap(),
            ),
            StoreConfigs::Sink(
                SinkObjectStoreConfig::try_parse_from(["server", "--sink-object-store", "file"])
                    .unwrap(),
            ),
        ];

        for config in configs {
            let err = config.make_object_store().unwrap_err().to_string();
            assert_eq!(
                err,
                "Specified File for the object store, required configuration missing for \
            data-dir"
            );
        }
    }

    #[tokio::test]
    async fn local_url_signer() {
        let root = TempDir::new().unwrap();
        let root_path = root.path().to_str().unwrap();
        let parquet_file_path = "1/2/something.parquet";

        let signer = make_presigned_url_signer(
            &ObjectStoreConfig::try_parse_from([
                "server",
                "--object-store",
                "file",
                "--data-dir",
                root_path,
            ])
            .unwrap(),
        )
        .unwrap()
        .unwrap();

        let object_store_parquet_file_path = Path::parse(parquet_file_path).unwrap();
        let upload_url = signer
            .signed_url(
                http_1::Method::PUT,
                &object_store_parquet_file_path,
                Duration::from_secs(100),
            )
            .await
            .unwrap();

        assert_eq!(
            upload_url.as_str(),
            &format!(
                "file://{}",
                std::fs::canonicalize(root.path())
                    .unwrap()
                    .join(parquet_file_path)
                    .display()
            )
        );
    }

    #[test]
    fn endpoint() {
        assert_eq!(
            Endpoint::from_str("http://localhost:8080")
                .unwrap()
                .to_string(),
            "http://localhost:8080",
        );
        assert_eq!(
            Endpoint::from_str("http://localhost:8080/")
                .unwrap()
                .to_string(),
            "http://localhost:8080",
        );
        assert_eq!(
            Endpoint::from_str("whatever.com").unwrap_err().to_string(),
            "relative URL without a base",
        );
    }

    impl AwsCredentialReloader {
        fn new_test(
            path: PathBuf,
            initial_test_credentials: AwsFileCredential,
            time_provider: Arc<dyn TimeProvider>,
        ) -> Self {
            let credential = initial_test_credentials.into();
            Self {
                path,
                current: Arc::new(RwLock::new(Arc::new(credential))),
                time_provider,
            }
        }
    }

    #[tokio::test]
    async fn validate_aws_credential_reloader() {
        let initial_time = Time::from_timestamp(60 * 60 * 24 * 365, 0).unwrap();
        let expiry = initial_time + Duration::from_secs(60 * 60);
        let next_expiry = expiry + Duration::from_secs(60 * 60);

        let mock_provider: Arc<MockProvider> = Arc::new(MockProvider::new(initial_time));
        let time_provider: Arc<dyn TimeProvider> = Arc::clone(&mock_provider) as _;

        let dir = TempDir::new().expect("must be able to create temp directory");
        let path = dir.path().join("credentials-file");

        let initial_file_credentials = AwsFileCredential {
            aws_access_key_id: String::from("access_key_1"),
            aws_secret_access_key: String::from("secret_key_1"),
            aws_session_token: None,
            expiry: Some(expiry.timestamp() as u64),
        };
        let initial_credentials: AwsCredential = initial_file_credentials.clone().into();
        let initial_file_credentials_serialized =
            serde_json::to_string(&initial_file_credentials).expect("must serialize");
        std::fs::write(&path, initial_file_credentials_serialized).expect("must succeed writing");

        let next_file_credentials = AwsFileCredential {
            aws_access_key_id: String::from("access_key_2"),
            aws_secret_access_key: String::from("secret_key_2"),
            aws_session_token: None,
            expiry: Some(next_expiry.timestamp() as u64),
        };
        let next_credentials: AwsCredential = next_file_credentials.clone().into();
        let next_file_credentials_serialized =
            serde_json::to_string(&next_file_credentials).expect("must serialize");

        let reloader =
            AwsCredentialReloader::new_test(path.clone(), initial_file_credentials, time_provider);

        // validate the current reloader credentials are still equal to the initial credentials
        assert_eq!(reloader.current.read().await.as_ref(), &initial_credentials);

        reloader.spawn_background_updates();

        // set the duration to five seconds after expiry and give the background task a short time
        // to reload the credentials
        mock_provider.set(expiry + Duration::from_secs(5));
        tokio::time::sleep(Duration::from_millis(50)).await;

        // validate the current reloader credentials haven't changed -- nothing was written to disk
        // yet so the initial credentials written to disk should be the same
        assert_eq!(reloader.current.read().await.as_ref(), &initial_credentials);

        // write to the credentials file
        std::fs::write(&path, next_file_credentials_serialized).expect("must succeed writing");

        // set the duration to five seconds past the default interval reloader interval so we
        // prompt another check
        mock_provider.set(next_expiry + Duration::from_secs(5));
        tokio::time::sleep(Duration::from_millis(50)).await;

        // verify the credentials have been updated in the reloader
        assert_eq!(reloader.current.read().await.as_ref(), &next_credentials);
    }

    #[derive(Debug)]
    struct TestReloadingObjectStoreInner {
        inner: Arc<dyn ObjectStore>,
        next_error: Arc<Mutex<Option<object_store::Error>>>,
    }

    impl TestReloadingObjectStoreInner {
        fn next_error(&self) -> Option<object_store::Error> {
            self.next_error.lock().unwrap().take()
        }
    }

    impl std::fmt::Display for TestReloadingObjectStoreInner {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            write!(f, "whatever")
        }
    }

    #[async_trait::async_trait]
    impl ObjectStore for TestReloadingObjectStoreInner {
        async fn copy(&self, from: &Path, to: &Path) -> object_store::Result<()> {
            if let Some(e) = self.next_error() {
                Err(e)
            } else {
                self.inner.as_ref().copy(from, to).await
            }
        }

        async fn copy_if_not_exists(&self, from: &Path, to: &Path) -> object_store::Result<()> {
            if let Some(e) = self.next_error() {
                Err(e)
            } else {
                self.inner.as_ref().copy_if_not_exists(from, to).await
            }
        }

        async fn delete(&self, location: &Path) -> object_store::Result<()> {
            if let Some(e) = self.next_error() {
                Err(e)
            } else {
                self.inner.as_ref().delete(location).await
            }
        }

        async fn get_opts(
            &self,
            location: &Path,
            options: GetOptions,
        ) -> object_store::Result<GetResult> {
            if let Some(e) = self.next_error() {
                Err(e)
            } else {
                self.inner.as_ref().get_opts(location, options).await
            }
        }

        fn list(
            &self,
            _prefix: Option<&Path>,
        ) -> BoxStream<'static, object_store::Result<ObjectMeta>> {
            unimplemented!()
        }

        async fn list_with_delimiter(
            &self,
            prefix: Option<&Path>,
        ) -> object_store::Result<ListResult> {
            if let Some(e) = self.next_error() {
                Err(e)
            } else {
                self.inner.as_ref().list_with_delimiter(prefix).await
            }
        }

        async fn put_multipart_opts(
            &self,
            location: &Path,
            opts: PutMultipartOpts,
        ) -> object_store::Result<Box<dyn MultipartUpload>> {
            if let Some(e) = self.next_error() {
                Err(e)
            } else {
                self.inner.as_ref().put_multipart_opts(location, opts).await
            }
        }

        async fn put_opts(
            &self,
            location: &Path,
            payload: PutPayload,
            options: PutOptions,
        ) -> object_store::Result<PutResult> {
            if let Some(e) = self.next_error() {
                Err(e)
            } else {
                self.inner
                    .as_ref()
                    .put_opts(location, payload, options)
                    .await
            }
        }
    }

    #[tokio::test]
    async fn validate_reauthing_object_store() {
        let initial_time = Time::from_timestamp(60 * 60 * 24 * 365, 0).unwrap();
        let expiry = initial_time + Duration::from_secs(60 * 60);

        let mock_provider: Arc<MockProvider> = Arc::new(MockProvider::new(initial_time));
        let time_provider: Arc<dyn TimeProvider> = Arc::clone(&mock_provider) as _;

        let dir = TempDir::new().expect("must be able to create temp directory");
        let path = dir.path().join("credentials-file");

        let initial_file_credentials = AwsFileCredential {
            aws_access_key_id: String::from("access_key_1"),
            aws_secret_access_key: String::from("secret_key_1"),
            aws_session_token: None,
            expiry: Some(expiry.timestamp() as u64),
        };
        let initial_credentials: AwsCredential = initial_file_credentials.clone().into();
        let initial_file_credentials_serialized =
            serde_json::to_string(&initial_file_credentials).expect("must serialize");
        std::fs::write(&path, initial_file_credentials_serialized.clone())
            .expect("must succeed writing");

        let next_file_credentials = AwsFileCredential {
            aws_access_key_id: String::from("access_key_2"),
            aws_secret_access_key: String::from("secret_key_2"),
            aws_session_token: None,
            expiry: Some(expiry.timestamp() as u64),
        };
        let next_credentials: AwsCredential = next_file_credentials.clone().into();
        let next_file_credentials_serialized =
            serde_json::to_string(&next_file_credentials).expect("must serialize");

        let reloader = Arc::new(AwsCredentialReloader::new_test(
            path.clone(),
            initial_file_credentials.clone(),
            time_provider,
        ));

        let reauthable_fn = || object_store::Error::Unauthenticated {
            path: "fake".to_string(),
            source: "fake error".into(),
        };
        let not_reauthable_fn = || object_store::Error::NotImplemented;

        let generic_expired_token_fn = || object_store::Error::Generic {
            store: "TestStore",
            source: "ExpiredToken: The provided token has expired".into(),
        };

        let generic_other_error_fn = || object_store::Error::Generic {
            store: "TestStore",
            source: "Some other generic error".into(),
        };

        let object_store = Arc::new(object_store::memory::InMemory::new()) as _;
        let test_inner = Arc::new(TestReloadingObjectStoreInner {
            inner: object_store,
            next_error: Arc::new(Mutex::new(None)),
        });
        let reloading_object_store =
            ReauthingObjectStore::new_arc(Arc::clone(&test_inner) as _, Arc::clone(&reloader));

        macro_rules! validate_endpoint {
            ($expression:expr) => {
                // validate expression works normally
                $expression
                    .await
                    .expect("must succeed");

                // ============================================================
                // Test re-authable error (Error::Unauthenticated)

                // set reauthable error on inner test object store
                test_inner
                    .as_ref()
                    .next_error
                    .lock()
                    .unwrap()
                    .replace(reauthable_fn());

                // verify the initial credentials are still set
                assert_eq!(reloader.current.read().await.as_ref(), &initial_credentials);

                // write to the credentials file so we can verify it gets updated
                std::fs::write(&path, next_file_credentials_serialized.clone()).expect("must succeed writing");

                // validate that the expression still transparently works even though there is an
                // initial authentication error
                $expression
                    .await
                    .expect("must succeed");

                // verify the next credentials have been loaded
                assert_eq!(reloader.current.read().await.as_ref(), &next_credentials);

                // ============================================================
                // Test non-re-authable error

                // write to the credentials file so we can verify it doesn't get updated
                std::fs::write(&path, initial_file_credentials_serialized.clone())
                    .expect("must succeed writing");

                // set non-reauthable error on inner test object store so we can validate no re-auth occurs
                test_inner
                    .as_ref()
                    .next_error
                    .lock()
                    .unwrap()
                    .replace(not_reauthable_fn());

                // verify the credentials are unchanged
                assert_eq!(reloader.current.read().await.as_ref(), &next_credentials);

                // validate that we get an error
                $expression
                    .await
                    .expect_err("must error");

                // reset the credentials to the initial state for the next test
                *reloader.current.write().await = Arc::new(initial_file_credentials.clone().into());

                // ============================================================
                // Test Generic error with ExpiredToken

                // Set generic expired token error
                test_inner
                    .next_error
                    .lock()
                    .unwrap()
                    .replace(generic_expired_token_fn());

                // Verify initial credentials are set
                assert_eq!(reloader.current.read().await.as_ref(), &initial_credentials);

                // Write new credentials to file
                std::fs::write(&path, next_file_credentials_serialized.clone())
                    .expect("must succeed writing");

                // This should trigger re-auth due to ExpiredToken in Generic error
                $expression
                    .await
                    .expect("must succeed after reauth");

                // Verify credentials were reloaded
                assert_eq!(reloader.current.read().await.as_ref(), &next_credentials);

                // ============================================================
                // Test Generic error without ExpiredToken (should return Err)

                // write to the credentials file so we can verify it doesn't get updated
                std::fs::write(&path, initial_file_credentials_serialized.clone())
                    .expect("must succeed writing");

                // set non-reauthable error on inner test object store so we can validate no re-auth occurs
                test_inner
                    .as_ref()
                    .next_error
                    .lock()
                    .unwrap()
                    .replace(generic_other_error_fn());

                // Write initial credentials to file (to see if they get loaded)
                std::fs::write(&path, initial_file_credentials_serialized.clone())
                    .expect("must succeed writing");

                // This should NOT trigger re-auth since no ExpiredToken in error
                $expression
                    .await
                    .expect_err("must return an error");

                // The error gets consumed by the first call, so we might get success or error
                assert_eq!(reloader.current.read().await.as_ref(), &next_credentials);

                // reset the credentials to the initial state for the next test
                *reloader.current.write().await = Arc::new(initial_file_credentials.clone().into());
            }
        }

        let obj_path = Path::from("whatever");
        validate_endpoint!(reloading_object_store.put(&obj_path, PutPayload::from("woof")));
        validate_endpoint!(reloading_object_store.put_opts(
            &obj_path,
            PutPayload::from("woof"),
            PutOptions::default()
        ));
        validate_endpoint!(
            reloading_object_store.put_multipart_opts(&obj_path, PutMultipartOpts::default())
        );
        validate_endpoint!(reloading_object_store.get(&obj_path));
        validate_endpoint!(reloading_object_store.get_opts(&obj_path, GetOptions::default()));
        validate_endpoint!(reloading_object_store.list_with_delimiter(Some(&obj_path)));

        let copy_path = Path::from("whatever2");
        validate_endpoint!(reloading_object_store.copy(&obj_path, &copy_path));

        validate_endpoint!(reloading_object_store.delete(&copy_path));

        // cannot validate copy_if_not_exists using the above macro since copies will fail if the
        // destination path already has content
        reloading_object_store
            .copy_if_not_exists(&obj_path, &copy_path)
            .await
            .expect("must copy");
        reloading_object_store
            .delete(&copy_path)
            .await
            .expect("must delete");

        // set reauthable error on inner test object store
        test_inner
            .as_ref()
            .next_error
            .lock()
            .unwrap()
            .replace(reauthable_fn());

        // verify the initial credentials are still set
        assert_eq!(reloader.current.read().await.as_ref(), &initial_credentials);

        // write to the credentials file so we can verify it gets updated
        std::fs::write(&path, next_file_credentials_serialized.clone())
            .expect("must succeed writing");

        // validate that copy still transparently works even though there is an initialy
        // authentication error
        reloading_object_store
            .copy_if_not_exists(&obj_path, &copy_path)
            .await
            .expect("must copy");
        // must always delete after copy_if_not_exists
        reloading_object_store
            .delete(&copy_path)
            .await
            .expect("must delete");

        // verify the next credentials have been loaded
        assert_eq!(reloader.current.read().await.as_ref(), &next_credentials);

        // write to the credentials file so we can verify it doesn't get updated
        std::fs::write(&path, initial_file_credentials_serialized.clone())
            .expect("must succeed writing");

        // set non-reauthable error on inner test object store so we can validate no re-auth occurs
        test_inner
            .as_ref()
            .next_error
            .lock()
            .unwrap()
            .replace(not_reauthable_fn());

        // verify the credentials are unchanged
        assert_eq!(reloader.current.read().await.as_ref(), &next_credentials);

        // validate that copy still transparently works
        reloading_object_store
            .copy_if_not_exists(&obj_path, &copy_path)
            .await
            .expect_err("must error");

        // reset the credentials to the initial state for the next test
        *reloader.current.write().await = Arc::new(initial_file_credentials.clone().into());
    }
}
