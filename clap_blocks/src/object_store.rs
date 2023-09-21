//! CLI handling for object store config (via CLI arguments and environment variables).

use futures::TryStreamExt;
use object_store::memory::InMemory;
use object_store::path::Path;
use object_store::throttle::ThrottledStore;
use object_store::{throttle::ThrottleConfig, DynObjectStore};
use observability_deps::tracing::{info, warn};
use snafu::{ResultExt, Snafu};
use std::sync::Arc;
use std::{fs, num::NonZeroUsize, path::PathBuf, time::Duration};
use uuid::Uuid;

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
}

/// The AWS region to use for Amazon S3 based object storage if none is
/// specified.
pub const FALLBACK_AWS_REGION: &str = "us-east-1";

/// CLI config for object stores.
#[derive(Debug, Clone, clap::Parser)]
pub struct ObjectStoreConfig {
    /// Which object storage to use. If not specified, defaults to memory.
    ///
    /// Possible values (case insensitive):
    ///
    /// * memory (default): Effectively no object persistence.
    /// * memorythrottled: Like `memory` but with latency and throughput that somewhat resamble a cloud
    ///    object store. Useful for testing and benchmarking.
    /// * file: Stores objects in the local filesystem. Must also set `--data-dir`.
    /// * s3: Amazon S3. Must also set `--bucket`, `--aws-access-key-id`, `--aws-secret-access-key`, and
    ///    possibly `--aws-default-region`.
    /// * google: Google Cloud Storage. Must also set `--bucket` and `--google-service-account`.
    /// * azure: Microsoft Azure blob storage. Must also set `--bucket`, `--azure-storage-account`,
    ///    and `--azure-storage-access-key`.
    #[clap(
        value_enum,
        long = "object-store",
        env = "INFLUXDB_IOX_OBJECT_STORE",
        ignore_case = true,
        action
    )]
    pub object_store: Option<ObjectStoreType>,

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
    #[clap(long = "bucket", env = "INFLUXDB_IOX_BUCKET", action)]
    pub bucket: Option<String>,

    /// The location InfluxDB IOx will use to store files locally.
    #[clap(long = "data-dir", env = "INFLUXDB_IOX_DB_DIR", action)]
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
    #[clap(long = "aws-access-key-id", env = "AWS_ACCESS_KEY_ID", action)]
    pub aws_access_key_id: Option<String>,

    /// When using Amazon S3 as the object store, set this to the secret access
    /// key that goes with the specified access key ID.
    ///
    /// Must also set `--object-store=s3`, `--bucket`, `--aws-access-key-id`.
    /// Can also set `--aws-default-region` if not using the fallback region.
    ///
    /// Prefer the environment variable over the command line flag in shared
    /// environments.
    #[clap(long = "aws-secret-access-key", env = "AWS_SECRET_ACCESS_KEY", action)]
    pub aws_secret_access_key: Option<String>,

    /// When using Amazon S3 as the object store, set this to the region
    /// that goes with the specified bucket if different from the fallback
    /// value.
    ///
    /// Must also set `--object-store=s3`, `--bucket`, `--aws-access-key-id`,
    /// and `--aws-secret-access-key`.
    #[clap(
        long = "aws-default-region",
        env = "AWS_DEFAULT_REGION",
        default_value = FALLBACK_AWS_REGION,
        action,
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
    #[clap(long = "aws-endpoint", env = "AWS_ENDPOINT", action)]
    pub aws_endpoint: Option<String>,

    /// When using Amazon S3 as an object store, set this to the session token. This is handy when using a federated
    /// login / SSO and you fetch credentials via the UI.
    ///
    /// Is it assumed that the session is valid as long as the IOx server is running.
    ///
    /// Prefer the environment variable over the command line flag in shared
    /// environments.
    #[clap(long = "aws-session-token", env = "AWS_SESSION_TOKEN", action)]
    pub aws_session_token: Option<String>,

    /// Allow unencrypted HTTP connection to AWS.
    #[clap(long = "aws-allow-http", env = "AWS_ALLOW_HTTP", action)]
    pub aws_allow_http: bool,

    /// When using Google Cloud Storage as the object store, set this to the
    /// path to the JSON file that contains the Google credentials.
    ///
    /// Must also set `--object-store=google` and `--bucket`.
    #[clap(
        long = "google-service-account",
        env = "GOOGLE_SERVICE_ACCOUNT",
        action
    )]
    pub google_service_account: Option<String>,

    /// When using Microsoft Azure as the object store, set this to the
    /// name you see when going to All Services > Storage accounts > `[name]`.
    ///
    /// Must also set `--object-store=azure`, `--bucket`, and
    /// `--azure-storage-access-key`.
    #[clap(long = "azure-storage-account", env = "AZURE_STORAGE_ACCOUNT", action)]
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
        long = "azure-storage-access-key",
        env = "AZURE_STORAGE_ACCESS_KEY",
        action
    )]
    pub azure_storage_access_key: Option<String>,

    /// When using a network-based object store, limit the number of connection to this value.
    #[clap(
        long = "object-store-connection-limit",
        env = "OBJECT_STORE_CONNECTION_LIMIT",
        default_value = "16",
        action
    )]
    pub object_store_connection_limit: NonZeroUsize,
}

impl ObjectStoreConfig {
    /// Create a new instance for all-in-one mode, only allowing some arguments.
    pub fn new(database_directory: Option<PathBuf>) -> Self {
        match &database_directory {
            Some(dir) => info!("Object store: File-based in `{}`", dir.display()),
            None => info!("Object store: In-memory"),
        }

        let object_store = database_directory.as_ref().map(|_| ObjectStoreType::File);

        Self {
            aws_access_key_id: Default::default(),
            aws_allow_http: Default::default(),
            aws_default_region: Default::default(),
            aws_endpoint: Default::default(),
            aws_secret_access_key: Default::default(),
            aws_session_token: Default::default(),
            azure_storage_access_key: Default::default(),
            azure_storage_account: Default::default(),
            bucket: Default::default(),
            database_directory,
            google_service_account: Default::default(),
            object_store,
            object_store_connection_limit: NonZeroUsize::new(16).unwrap(),
        }
    }
}

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

#[cfg(feature = "gcp")]
fn new_gcs(config: &ObjectStoreConfig) -> Result<Arc<DynObjectStore>, ParseError> {
    use object_store::gcp::GoogleCloudStorageBuilder;
    use object_store::limit::LimitStore;

    info!(bucket=?config.bucket, object_store_type="GCS", "Object Store");

    let mut builder = GoogleCloudStorageBuilder::new();

    if let Some(bucket) = &config.bucket {
        builder = builder.with_bucket_name(bucket);
    }
    if let Some(account) = &config.google_service_account {
        builder = builder.with_service_account_path(account);
    }

    Ok(Arc::new(LimitStore::new(
        builder.build().context(InvalidGCSConfigSnafu)?,
        config.object_store_connection_limit.get(),
    )))
}

#[cfg(not(feature = "gcp"))]
fn new_gcs(_: &ObjectStoreConfig) -> Result<Arc<DynObjectStore>, ParseError> {
    panic!("GCS support not enabled, recompile with the gcp feature enabled")
}

#[cfg(feature = "aws")]
fn new_s3(config: &ObjectStoreConfig) -> Result<Arc<DynObjectStore>, ParseError> {
    use object_store::aws::AmazonS3Builder;
    use object_store::limit::LimitStore;

    info!(bucket=?config.bucket, endpoint=?config.aws_endpoint, object_store_type="S3", "Object Store");

    let mut builder = AmazonS3Builder::new()
        .with_allow_http(config.aws_allow_http)
        .with_region(&config.aws_default_region)
        .with_imdsv1_fallback();

    if let Some(bucket) = &config.bucket {
        builder = builder.with_bucket_name(bucket);
    }
    if let Some(key_id) = &config.aws_access_key_id {
        builder = builder.with_access_key_id(key_id);
    }
    if let Some(token) = &config.aws_session_token {
        builder = builder.with_token(token);
    }
    if let Some(secret) = &config.aws_secret_access_key {
        builder = builder.with_secret_access_key(secret);
    }
    if let Some(endpoint) = &config.aws_endpoint {
        builder = builder.with_endpoint(endpoint);
    }

    Ok(Arc::new(LimitStore::new(
        builder.build().context(InvalidS3ConfigSnafu)?,
        config.object_store_connection_limit.get(),
    )))
}

#[cfg(not(feature = "aws"))]
fn new_s3(_: &ObjectStoreConfig) -> Result<Arc<DynObjectStore>, ParseError> {
    panic!("S3 support not enabled, recompile with the aws feature enabled")
}

#[cfg(feature = "azure")]
fn new_azure(config: &ObjectStoreConfig) -> Result<Arc<DynObjectStore>, ParseError> {
    use object_store::azure::MicrosoftAzureBuilder;
    use object_store::limit::LimitStore;

    info!(bucket=?config.bucket, account=?config.azure_storage_account,
          object_store_type="Azure", "Object Store");

    let mut builder = MicrosoftAzureBuilder::new();

    if let Some(bucket) = &config.bucket {
        builder = builder.with_container_name(bucket);
    }
    if let Some(account) = &config.azure_storage_account {
        builder = builder.with_account(account)
    }
    if let Some(key) = &config.azure_storage_access_key {
        builder = builder.with_access_key(key)
    }

    Ok(Arc::new(LimitStore::new(
        builder.build().context(InvalidAzureConfigSnafu)?,
        config.object_store_connection_limit.get(),
    )))
}

#[cfg(not(feature = "azure"))]
fn new_azure(_: &ObjectStoreConfig) -> Result<Arc<DynObjectStore>, ParseError> {
    panic!("Azure blob storage support not enabled, recompile with the azure feature enabled")
}

/// Create config-dependant object store.
pub fn make_object_store(config: &ObjectStoreConfig) -> Result<Arc<DynObjectStore>, ParseError> {
    if let Some(data_dir) = &config.database_directory {
        if !matches!(&config.object_store, Some(ObjectStoreType::File)) {
            warn!(?data_dir, object_store_type=?config.object_store,
                  "--data-dir / `INFLUXDB_IOX_DB_DIR` ignored. It only affects 'file' object stores");
        }
    }

    match &config.object_store {
        Some(ObjectStoreType::Memory) | None => {
            info!(object_store_type = "Memory", "Object Store");
            Ok(Arc::new(InMemory::new()))
        }
        Some(ObjectStoreType::MemoryThrottled) => {
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
            Ok(Arc::new(ThrottledStore::new(InMemory::new(), config)))
        }

        Some(ObjectStoreType::Google) => new_gcs(config),
        Some(ObjectStoreType::S3) => new_s3(config),
        Some(ObjectStoreType::Azure) => new_azure(config),
        Some(ObjectStoreType::File) => match config.database_directory.as_ref() {
            Some(db_dir) => {
                info!(?db_dir, object_store_type = "Directory", "Object Store");
                fs::create_dir_all(db_dir)
                    .context(CreatingDatabaseDirectorySnafu { path: db_dir })?;

                let store = object_store::local::LocalFileSystem::new_with_prefix(db_dir)
                    .context(CreateLocalFileSystemSnafu { path: db_dir })?;
                Ok(Arc::new(store))
            }
            None => MissingObjectStoreConfigSnafu {
                object_store: ObjectStoreType::File,
                missing: "data-dir",
            }
            .fail(),
        },
    }
}

#[derive(Debug, Snafu)]
#[allow(missing_docs)]
pub enum CheckError {
    #[snafu(display("Cannot read from object store: {}", source))]
    CannotReadObjectStore { source: object_store::Error },
}

/// Check if object store is properly configured and accepts writes and reads.
///
/// Note: This does NOT test if the object store is writable!
pub async fn check_object_store(object_store: &DynObjectStore) -> Result<(), CheckError> {
    // Use some prefix that will very likely end in an empty result, so we don't pull too much actual data here.
    let uuid = Uuid::new_v4().to_string();
    let prefix = Path::from_iter([uuid]);

    // create stream (this might fail if the store is not readable)
    let mut stream = object_store
        .list(Some(&prefix))
        .await
        .context(CannotReadObjectStoreSnafu)?;

    // ... but sometimes it fails only if we use the resulting stream, so try that once
    stream
        .try_next()
        .await
        .context(CannotReadObjectStoreSnafu)?;

    // store seems to be readable
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use clap::Parser;
    use std::env;
    use tempfile::TempDir;

    #[test]
    fn default_object_store_is_memory() {
        let config = ObjectStoreConfig::try_parse_from(["server"]).unwrap();

        let object_store = make_object_store(&config).unwrap();
        assert_eq!(&object_store.to_string(), "InMemory")
    }

    #[test]
    fn explicitly_set_object_store_to_memory() {
        let config =
            ObjectStoreConfig::try_parse_from(["server", "--object-store", "memory"]).unwrap();

        let object_store = make_object_store(&config).unwrap();
        assert_eq!(&object_store.to_string(), "InMemory")
    }

    #[test]
    #[cfg(feature = "aws")]
    fn valid_s3_config() {
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

        let object_store = make_object_store(&config).unwrap();
        assert_eq!(&object_store.to_string(), "AmazonS3(mybucket)")
    }

    #[test]
    #[cfg(feature = "aws")]
    fn s3_config_missing_params() {
        let mut config =
            ObjectStoreConfig::try_parse_from(["server", "--object-store", "s3"]).unwrap();

        // clean out eventual leaks via env variables
        config.bucket = None;

        let err = make_object_store(&config).unwrap_err().to_string();

        assert_eq!(
            err,
            "Specified S3 for the object store, required configuration missing for bucket"
        );
    }

    #[test]
    #[cfg(feature = "gcp")]
    fn valid_google_config() {
        let config = ObjectStoreConfig::try_parse_from([
            "server",
            "--object-store",
            "google",
            "--bucket",
            "mybucket",
            "--google-service-account",
            "~/Not/A/Real/path.json",
        ])
        .unwrap();

        let object_store = make_object_store(&config).unwrap();
        assert_eq!(&object_store.to_string(), "GoogleCloudStorage(mybucket)")
    }

    #[test]
    #[cfg(feature = "gcp")]
    fn google_config_missing_params() {
        let mut config =
            ObjectStoreConfig::try_parse_from(["server", "--object-store", "google"]).unwrap();

        // clean out eventual leaks via env variables
        config.bucket = None;

        let err = make_object_store(&config).unwrap_err().to_string();

        assert_eq!(
            err,
            "Specified Google for the object store, required configuration missing for \
            bucket, google-service-account"
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
            "NotARealKey",
        ])
        .unwrap();

        let object_store = make_object_store(&config).unwrap();
        assert_eq!(&object_store.to_string(), "MicrosoftAzure(mybucket)")
    }

    #[test]
    #[cfg(feature = "azure")]
    fn azure_config_missing_params() {
        let mut config =
            ObjectStoreConfig::try_parse_from(["server", "--object-store", "azure"]).unwrap();

        // clean out eventual leaks via env variables
        config.bucket = None;

        let err = make_object_store(&config).unwrap_err().to_string();

        assert_eq!(
            err,
            "Specified Azure for the object store, required configuration missing for \
            bucket, azure-storage-account, azure-storage-access-key"
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

        let object_store = make_object_store(&config).unwrap().to_string();
        assert!(
            object_store.starts_with("LocalFileSystem"),
            "{}",
            object_store
        )
    }

    #[test]
    fn file_config_missing_params() {
        // this test tests for failure to configure the object store because of data-dir configuration missing
        // if the INFLUXDB_IOX_DB_DIR env variable is set, the test fails because the configuration is
        // actually present.
        env::remove_var("INFLUXDB_IOX_DB_DIR");
        let config =
            ObjectStoreConfig::try_parse_from(["server", "--object-store", "file"]).unwrap();

        let err = make_object_store(&config).unwrap_err().to_string();

        assert_eq!(
            err,
            "Specified File for the object store, required configuration missing for \
            data-dir"
        );
    }
}
