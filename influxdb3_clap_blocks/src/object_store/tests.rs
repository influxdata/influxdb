use super::*;
use clap::Parser;
use iox_time::{MockProvider, Time};
use object_store::ObjectStore;
use std::{str::FromStr, sync::Mutex};
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
fn explicitly_set_object_store_to_memory() {
    let configs = vec![
        StoreConfigs::Base(
            ObjectStoreConfig::try_parse_from(["server", "--object-store", "memory"]).unwrap(),
        ),
        StoreConfigs::Source(
            SourceObjectStoreConfig::try_parse_from(["server", "--source-object-store", "memory"])
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
    let config = ObjectStoreConfig::try_parse_from(["server", "--object-store", "memory"]).unwrap();

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
            "LimitStore(64, AmazonS3(mybucket))"
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
    let result = ObjectStoreConfig::try_parse_from(["server", "--aws-endpoint", "whatever.com"]);
    assert!(result.is_err(), "{result:?}");
    let result = SourceObjectStoreConfig::try_parse_from([
        "server",
        "--source-aws-endpoint",
        "whatever.com",
    ]);
    assert!(result.is_err(), "{result:?}");
    let result =
        SinkObjectStoreConfig::try_parse_from(["server", "--sink-aws-endpoint", "whatever.com"]);
    assert!(result.is_err(), "{result:?}");
}

#[test]
#[cfg(feature = "aws")]
fn s3_config_missing_params() {
    let mut config = ObjectStoreConfig::try_parse_from(["server", "--object-store", "s3"]).unwrap();

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
    let config = ObjectStoreConfig::try_parse_from(["server", "--object-store", "memory"]).unwrap();

    let signer = make_presigned_url_signer(&config).unwrap();
    assert!(signer.is_none(), "Expected None, got {signer:?}");
}

#[test]
#[cfg(feature = "aws")]
fn s3_url_signer_config_missing_params() {
    let mut config = ObjectStoreConfig::try_parse_from(["server", "--object-store", "s3"]).unwrap();

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
            "LimitStore(64, GoogleCloudStorage(mybucket))"
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
        "LimitStore(64, MicrosoftAzure { account: NotARealStorageAccount, container: mybucket })"
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
            http::Method::PUT,
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

    fn list(&self, _prefix: Option<&Path>) -> BoxStream<'static, object_store::Result<ObjectMeta>> {
        unimplemented!()
    }

    async fn list_with_delimiter(&self, prefix: Option<&Path>) -> object_store::Result<ListResult> {
        if let Some(e) = self.next_error() {
            Err(e)
        } else {
            self.inner.as_ref().list_with_delimiter(prefix).await
        }
    }

    async fn put_multipart_opts(
        &self,
        location: &Path,
        opts: PutMultipartOptions,
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
        reloading_object_store.put_multipart_opts(&obj_path, PutMultipartOptions::default())
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
    std::fs::write(&path, next_file_credentials_serialized.clone()).expect("must succeed writing");

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

#[test]
fn test_tls_cli_arguments() {
    // Test parsing TLS allow insecure flag
    let config = ObjectStoreConfig::try_parse_from([
        "server",
        "--object-store",
        "s3",
        "--object-store-tls-allow-insecure",
    ])
    .unwrap();
    assert!(config.tls_allow_insecure);

    // Test parsing TLS CA path
    let config = ObjectStoreConfig::try_parse_from([
        "server",
        "--object-store",
        "s3",
        "--object-store-tls-ca",
        "/path/to/ca.pem",
    ])
    .unwrap();
    assert_eq!(config.tls_ca_path, Some(PathBuf::from("/path/to/ca.pem")));

    // Test both arguments together
    let config = ObjectStoreConfig::try_parse_from([
        "server",
        "--object-store",
        "s3",
        "--object-store-tls-allow-insecure",
        "--object-store-tls-ca",
        "/path/to/ca.pem",
    ])
    .unwrap();
    assert!(config.tls_allow_insecure);
    assert_eq!(config.tls_ca_path, Some(PathBuf::from("/path/to/ca.pem")));

    // Test default values
    let config = ObjectStoreConfig::try_parse_from(["server", "--object-store", "s3"]).unwrap();
    assert!(!config.tls_allow_insecure);
    assert!(config.tls_ca_path.is_none());
}

#[test]
fn test_tls_configuration_with_different_stores() {
    // Test TLS options with AWS S3
    let config = ObjectStoreConfig::try_parse_from([
        "server",
        "--object-store",
        "s3",
        "--bucket",
        "test-bucket",
        "--object-store-tls-allow-insecure",
    ])
    .unwrap();
    assert!(config.tls_allow_insecure);
    assert_eq!(config.bucket, Some("test-bucket".to_string()));

    // Test TLS options with Azure
    let config = ObjectStoreConfig::try_parse_from([
        "server",
        "--object-store",
        "azure",
        "--azure-storage-account",
        "testaccount",
        "--object-store-tls-ca",
        "/etc/ssl/custom-ca.pem",
    ])
    .unwrap();
    assert_eq!(
        config.tls_ca_path,
        Some(PathBuf::from("/etc/ssl/custom-ca.pem"))
    );
    assert_eq!(
        config.azure_storage_account,
        Some("testaccount".to_string())
    );

    // Test TLS options with Google Cloud Storage
    let config = ObjectStoreConfig::try_parse_from([
        "server",
        "--object-store",
        "google",
        "--bucket",
        "gcs-bucket",
        "--object-store-tls-allow-insecure",
        "--object-store-tls-ca",
        "/custom/ca.pem",
    ])
    .unwrap();
    assert!(config.tls_allow_insecure);
    assert_eq!(config.tls_ca_path, Some(PathBuf::from("/custom/ca.pem")));

    // Test that TLS options work with file store (should be ignored but not error)
    let config = ObjectStoreConfig::try_parse_from([
        "server",
        "--object-store",
        "file",
        "--data-dir",
        "/tmp/data",
        "--object-store-tls-allow-insecure",
    ])
    .unwrap();
    assert!(config.tls_allow_insecure);
    assert_eq!(config.database_directory, Some(PathBuf::from("/tmp/data")));

    // Test with memory store (TLS options should be accepted but have no effect)
    let config = ObjectStoreConfig::try_parse_from([
        "server",
        "--object-store",
        "memory",
        "--object-store-tls-ca",
        "/ignored/ca.pem",
    ])
    .unwrap();
    assert_eq!(config.tls_ca_path, Some(PathBuf::from("/ignored/ca.pem")));
}

#[test]
fn test_tls_environment_variables() {
    use std::env;

    unsafe {
        // Test environment variable for allow-insecure
        // Boolean flags need explicit true/false values when set via environment variable
        env::set_var("INFLUXDB3_OBJECT_STORE_TLS_ALLOW_INSECURE", "true");
        let config = ObjectStoreConfig::try_parse_from(["server", "--object-store", "s3"]).unwrap();
        assert!(config.tls_allow_insecure);
        env::remove_var("INFLUXDB3_OBJECT_STORE_TLS_ALLOW_INSECURE");

        // Test that the flag is false when env var is not set
        let config = ObjectStoreConfig::try_parse_from(["server", "--object-store", "s3"]).unwrap();
        assert!(!config.tls_allow_insecure);

        // Test environment variable for CA path
        env::set_var("INFLUXDB3_OBJECT_STORE_TLS_CA", "/env/ca.pem");
        let config = ObjectStoreConfig::try_parse_from(["server", "--object-store", "s3"]).unwrap();
        assert_eq!(config.tls_ca_path, Some(PathBuf::from("/env/ca.pem")));
        env::remove_var("INFLUXDB3_OBJECT_STORE_TLS_CA");

        // Test CLI args override environment variables
        env::set_var("INFLUXDB3_OBJECT_STORE_TLS_CA", "/env/ca.pem");
        let config = ObjectStoreConfig::try_parse_from([
            "server",
            "--object-store",
            "s3",
            "--object-store-tls-ca",
            "/cli/ca.pem",
        ])
        .unwrap();
        assert_eq!(config.tls_ca_path, Some(PathBuf::from("/cli/ca.pem")));
        env::remove_var("INFLUXDB3_OBJECT_STORE_TLS_CA");
    }
}
