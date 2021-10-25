//! CLI handling for object store config (via CLI arguments and environment variables).
use std::{convert::TryFrom, fs, path::PathBuf, time::Duration};

use clap::arg_enum;
use futures::TryStreamExt;
use object_store::{path::ObjectStorePath, ObjectStore, ObjectStoreApi, ThrottleConfig};
use observability_deps::tracing::{info, warn};
use snafu::{ResultExt, Snafu};
use structopt::StructOpt;
use uuid::Uuid;

#[derive(Debug, Snafu)]
pub enum ParseError {
    #[snafu(display("Unable to create database directory {:?}: {}", path, source))]
    CreatingDatabaseDirectory {
        path: PathBuf,
        source: std::io::Error,
    },

    #[snafu(display(
        "Specified {} for the object store, required configuration missing for {}",
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
#[derive(Debug, StructOpt, Clone)]
pub struct ObjectStoreConfig {
    #[structopt(
    long = "--object-store",
    env = "INFLUXDB_IOX_OBJECT_STORE",
    possible_values = &ObjectStoreType::variants(),
    case_insensitive = true,
    long_help = r#"Which object storage to use. If not specified, defaults to memory.

Possible values (case insensitive):

* memory (default): Effectively no object persistence.
* memorythrottled: Like `memory` but with latency and throughput that somewhat resamble a cloud
   object store. Useful for testing and benchmarking.
* file: Stores objects in the local filesystem. Must also set `--data-dir`.
* s3: Amazon S3. Must also set `--bucket`, `--aws-access-key-id`, `--aws-secret-access-key`, and
   possibly `--aws-default-region`.
* google: Google Cloud Storage. Must also set `--bucket` and `--google-service-account`.
* azure: Microsoft Azure blob storage. Must also set `--bucket`, `--azure-storage-account`,
   and `--azure-storage-access-key`.
        "#,
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
    #[structopt(long = "--bucket", env = "INFLUXDB_IOX_BUCKET")]
    pub bucket: Option<String>,

    /// The location InfluxDB IOx will use to store files locally.
    #[structopt(long = "--data-dir", env = "INFLUXDB_IOX_DB_DIR")]
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
    #[structopt(long = "--aws-access-key-id", env = "AWS_ACCESS_KEY_ID")]
    pub aws_access_key_id: Option<String>,

    /// When using Amazon S3 as the object store, set this to the secret access
    /// key that goes with the specified access key ID.
    ///
    /// Must also set `--object-store=s3`, `--bucket`, `--aws-access-key-id`.
    /// Can also set `--aws-default-region` if not using the fallback region.
    ///
    /// Prefer the environment variable over the command line flag in shared
    /// environments.
    #[structopt(long = "--aws-secret-access-key", env = "AWS_SECRET_ACCESS_KEY")]
    pub aws_secret_access_key: Option<String>,

    /// When using Amazon S3 as the object store, set this to the region
    /// that goes with the specified bucket if different from the fallback
    /// value.
    ///
    /// Must also set `--object-store=s3`, `--bucket`, `--aws-access-key-id`,
    /// and `--aws-secret-access-key`.
    #[structopt(
    long = "--aws-default-region",
    env = "AWS_DEFAULT_REGION",
    default_value = FALLBACK_AWS_REGION,
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
    #[structopt(long = "--aws-endpoint", env = "AWS_ENDPOINT")]
    pub aws_endpoint: Option<String>,

    /// When using Amazon S3 as an object store, set this to the session token. This is handy when using a federated
    /// login / SSO and you fetch credentials via the UI.
    ///
    /// Is it assumed that the session is valid as long as the IOx server is running.
    ///
    /// Prefer the environment variable over the command line flag in shared
    /// environments.
    #[structopt(long = "--aws-session-token", env = "AWS_SESSION_TOKEN")]
    pub aws_session_token: Option<String>,

    /// When using Google Cloud Storage as the object store, set this to the
    /// path to the JSON file that contains the Google credentials.
    ///
    /// Must also set `--object-store=google` and `--bucket`.
    #[structopt(long = "--google-service-account", env = "GOOGLE_SERVICE_ACCOUNT")]
    pub google_service_account: Option<String>,

    /// When using Microsoft Azure as the object store, set this to the
    /// name you see when going to All Services > Storage accounts > `[name]`.
    ///
    /// Must also set `--object-store=azure`, `--bucket`, and
    /// `--azure-storage-access-key`.
    #[structopt(long = "--azure-storage-account", env = "AZURE_STORAGE_ACCOUNT")]
    pub azure_storage_account: Option<String>,

    /// When using Microsoft Azure as the object store, set this to one of the
    /// Key values in the Storage account's Settings > Access keys.
    ///
    /// Must also set `--object-store=azure`, `--bucket`, and
    /// `--azure-storage-account`.
    ///
    /// Prefer the environment variable over the command line flag in shared
    /// environments.
    #[structopt(long = "--azure-storage-access-key", env = "AZURE_STORAGE_ACCESS_KEY")]
    pub azure_storage_access_key: Option<String>,
}

arg_enum! {
    #[derive(Debug, Copy, Clone, PartialEq)]
    pub enum ObjectStoreType {
        Memory,
        MemoryThrottled,
        File,
        S3,
        Google,
        Azure,
    }
}

pub fn warn_about_inmem_store(config: &ObjectStoreConfig) {
    match config.object_store {
        Some(ObjectStoreType::Memory) | None => {
            warn!("NO PERSISTENCE: using Memory for object storage");
        }
        Some(store) => {
            info!("Using {} for object storage", store);
        }
    }
}

impl TryFrom<&ObjectStoreConfig> for ObjectStore {
    type Error = ParseError;

    fn try_from(config: &ObjectStoreConfig) -> Result<Self, Self::Error> {
        match config.object_store {
            Some(ObjectStoreType::Memory) | None => Ok(Self::new_in_memory()),
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

                Ok(Self::new_in_memory_throttled(config))
            }

            Some(ObjectStoreType::Google) => {
                match (
                    config.bucket.as_ref(),
                    config.google_service_account.as_ref(),
                ) {
                    (Some(bucket), Some(service_account)) => {
                        Self::new_google_cloud_storage(service_account, bucket)
                            .context(InvalidGCSConfig)
                    }
                    (bucket, service_account) => {
                        let mut missing_args = vec![];

                        if bucket.is_none() {
                            missing_args.push("bucket");
                        }
                        if service_account.is_none() {
                            missing_args.push("google-service-account");
                        }
                        MissingObjectStoreConfig {
                            object_store: ObjectStoreType::Google,
                            missing: missing_args.join(", "),
                        }
                        .fail()
                    }
                }
            }

            Some(ObjectStoreType::S3) => {
                match (
                    config.bucket.as_ref(),
                    config.aws_access_key_id.as_ref(),
                    config.aws_secret_access_key.as_ref(),
                    config.aws_default_region.as_str(),
                    config.aws_endpoint.as_ref(),
                    config.aws_session_token.as_ref(),
                ) {
                    (Some(bucket), key_id, secret_key, region, endpoint, session_token) => {
                        Self::new_amazon_s3(
                            key_id,
                            secret_key,
                            region,
                            bucket,
                            endpoint,
                            session_token,
                        )
                        .context(InvalidS3Config)
                    }
                    (bucket, _, _, _, _, _) => {
                        let mut missing_args = vec![];

                        if bucket.is_none() {
                            missing_args.push("bucket");
                        }
                        MissingObjectStoreConfig {
                            object_store: ObjectStoreType::S3,
                            missing: missing_args.join(", "),
                        }
                        .fail()
                    }
                }
            }

            Some(ObjectStoreType::Azure) => {
                match (
                    config.bucket.as_ref(),
                    config.azure_storage_account.as_ref(),
                    config.azure_storage_access_key.as_ref(),
                ) {
                    (Some(bucket), Some(storage_account), Some(access_key)) => {
                        Self::new_microsoft_azure(storage_account, access_key, bucket)
                            .context(InvalidAzureConfig)
                    }
                    (bucket, storage_account, access_key) => {
                        let mut missing_args = vec![];

                        if bucket.is_none() {
                            missing_args.push("bucket");
                        }
                        if storage_account.is_none() {
                            missing_args.push("azure-storage-account");
                        }
                        if access_key.is_none() {
                            missing_args.push("azure-storage-access-key");
                        }

                        MissingObjectStoreConfig {
                            object_store: ObjectStoreType::Azure,
                            missing: missing_args.join(", "),
                        }
                        .fail()
                    }
                }
            }

            Some(ObjectStoreType::File) => match config.database_directory.as_ref() {
                Some(db_dir) => {
                    fs::create_dir_all(db_dir)
                        .context(CreatingDatabaseDirectory { path: db_dir })?;
                    Ok(Self::new_file(&db_dir))
                }
                None => MissingObjectStoreConfig {
                    object_store: ObjectStoreType::File,
                    missing: "data-dir",
                }
                .fail(),
            },
        }
    }
}

#[derive(Debug, Snafu)]
pub enum CheckError {
    #[snafu(display("Cannot read from object store: {}", source))]
    CannotReadObjectStore { source: object_store::Error },
}

/// Check if object store is properly configured and accepts writes and reads.
///
/// Note: This does NOT test if the object store is writable!
pub async fn check_object_store(object_store: &ObjectStore) -> Result<(), CheckError> {
    // Use some prefix that will very likely end in an empty result, so we don't pull too much actual data here.
    let uuid = Uuid::new_v4().to_string();
    let mut prefix = object_store.new_path();
    prefix.push_dir(&uuid);

    // create stream (this might fail if the store is not readable)
    let mut stream = object_store
        .list(Some(&prefix))
        .await
        .context(CannotReadObjectStore)?;

    // ... but sometimes it fails only if we use the resulting stream, so try that once
    stream.try_next().await.context(CannotReadObjectStore)?;

    // store seems to be readable
    Ok(())
}

#[cfg(test)]
mod tests {
    use object_store::ObjectStoreIntegration;
    use tempfile::TempDir;

    use super::*;

    #[test]
    fn default_object_store_is_memory() {
        let config = ObjectStoreConfig::from_iter_safe(&["server"]).unwrap();

        let object_store = ObjectStore::try_from(&config).unwrap();
        let ObjectStore { integration, .. } = object_store;

        assert!(matches!(integration, ObjectStoreIntegration::InMemory(_)));
    }

    #[test]
    fn explicitly_set_object_store_to_memory() {
        let config =
            ObjectStoreConfig::from_iter_safe(&["server", "--object-store", "memory"]).unwrap();

        let object_store = ObjectStore::try_from(&config).unwrap();
        let ObjectStore { integration, .. } = object_store;

        assert!(matches!(integration, ObjectStoreIntegration::InMemory(_)));
    }

    #[test]
    #[cfg(feature = "aws")]
    fn valid_s3_config() {
        let config = ObjectStoreConfig::from_iter_safe(&[
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

        let object_store = ObjectStore::try_from(&config).unwrap();
        let ObjectStore { integration, .. } = object_store;

        assert!(matches!(integration, ObjectStoreIntegration::AmazonS3(_)));
    }

    #[test]
    fn s3_config_missing_params() {
        let config =
            ObjectStoreConfig::from_iter_safe(&["server", "--object-store", "s3"]).unwrap();

        let err = ObjectStore::try_from(&config).unwrap_err().to_string();

        assert_eq!(
            err,
            "Specified S3 for the object store, required configuration missing for bucket"
        );
    }

    #[test]
    #[cfg(feature = "gcp")]
    fn valid_google_config() {
        let config = ObjectStoreConfig::from_iter_safe(&[
            "server",
            "--object-store",
            "google",
            "--bucket",
            "mybucket",
            "--google-service-account",
            "~/Not/A/Real/path.json",
        ])
        .unwrap();

        let object_store = ObjectStore::try_from(&config).unwrap();
        let ObjectStore { integration, .. } = object_store;

        assert!(matches!(
            integration,
            ObjectStoreIntegration::GoogleCloudStorage(_)
        ));
    }

    #[test]
    fn google_config_missing_params() {
        let config =
            ObjectStoreConfig::from_iter_safe(&["server", "--object-store", "google"]).unwrap();

        let err = ObjectStore::try_from(&config).unwrap_err().to_string();

        assert_eq!(
            err,
            "Specified Google for the object store, required configuration missing for \
            bucket, google-service-account"
        );
    }

    #[test]
    #[cfg(feature = "azure")]
    fn valid_azure_config() {
        let config = ObjectStoreConfig::from_iter_safe(&[
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

        let object_store = ObjectStore::try_from(&config).unwrap();
        let ObjectStore { integration, .. } = object_store;

        assert!(matches!(
            integration,
            ObjectStoreIntegration::MicrosoftAzure(_)
        ));
    }

    #[test]
    fn azure_config_missing_params() {
        let config =
            ObjectStoreConfig::from_iter_safe(&["server", "--object-store", "azure"]).unwrap();

        let err = ObjectStore::try_from(&config).unwrap_err().to_string();

        assert_eq!(
            err,
            "Specified Azure for the object store, required configuration missing for \
            bucket, azure-storage-account, azure-storage-access-key"
        );
    }

    #[test]
    fn valid_file_config() {
        let root = TempDir::new().unwrap();

        let config = ObjectStoreConfig::from_iter_safe(&[
            "server",
            "--object-store",
            "file",
            "--data-dir",
            root.path().to_str().unwrap(),
        ])
        .unwrap();

        let object_store = ObjectStore::try_from(&config).unwrap();
        let ObjectStore { integration, .. } = object_store;

        assert!(matches!(integration, ObjectStoreIntegration::File(_)));
    }

    #[test]
    fn file_config_missing_params() {
        let config =
            ObjectStoreConfig::from_iter_safe(&["server", "--object-store", "file"]).unwrap();

        let err = ObjectStore::try_from(&config).unwrap_err().to_string();

        assert_eq!(
            err,
            "Specified File for the object store, required configuration missing for \
            data-dir"
        );
    }
}
