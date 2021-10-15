//! Crate that mimics the interface of the the various object stores
//! but does nothing if they are not enabled.
use async_trait::async_trait;
use bytes::Bytes;
use snafu::Snafu;

use crate::{path::cloud::CloudPath, ObjectStoreApi};

/// A specialized `Error` for Azure object store-related errors
#[derive(Debug, Snafu, Clone)]
#[allow(missing_docs)]
pub enum Error {
    #[snafu(display(
        "'{}' not supported with this build. Hint: recompile with appropriate features",
        name
    ))]
    NotSupported { name: String },
}
/// Result for the dummy object store
pub type Result<T, E = Error> = std::result::Result<T, E>;

#[derive(Debug, Clone)]
/// An object store that always generates an error
pub struct DummyObjectStore {
    name: String,
}

/// If aws feature not available, use DummyObjectStore
pub type AmazonS3 = DummyObjectStore;

/// If azure feature not available, use DummyObjectStore
pub type MicrosoftAzure = DummyObjectStore;

/// If gcp feature not available, use DummyObjectStore
pub type GoogleCloudStorage = DummyObjectStore;

#[async_trait]
impl ObjectStoreApi for DummyObjectStore {
    type Path = CloudPath;
    type Error = Error;

    fn new_path(&self) -> Self::Path {
        CloudPath::default()
    }

    fn path_from_raw(&self, raw: &str) -> Self::Path {
        CloudPath::raw(raw)
    }

    async fn put(&self, _location: &Self::Path, _bytes: Bytes) -> crate::Result<(), Self::Error> {
        NotSupported { name: &self.name }.fail()
    }

    async fn get(
        &self,
        _location: &Self::Path,
    ) -> crate::Result<
        futures::stream::BoxStream<'static, crate::Result<bytes::Bytes, Self::Error>>,
        Self::Error,
    > {
        NotSupported { name: &self.name }.fail()
    }

    async fn delete(&self, _location: &Self::Path) -> crate::Result<(), Self::Error> {
        NotSupported { name: &self.name }.fail()
    }

    async fn list<'a>(
        &'a self,
        _prefix: Option<&'a Self::Path>,
    ) -> crate::Result<
        futures::stream::BoxStream<'a, crate::Result<Vec<Self::Path>, Self::Error>>,
        Self::Error,
    > {
        NotSupported { name: &self.name }.fail()
    }

    async fn list_with_delimiter(
        &self,
        _prefix: &Self::Path,
    ) -> crate::Result<crate::ListResult<Self::Path>, Self::Error> {
        NotSupported { name: &self.name }.fail()
    }
}

/// Stub when s3 is not configured
#[allow(dead_code)]
pub(crate) fn new_s3(
    _access_key_id: Option<impl Into<String>>,
    _secret_access_key: Option<impl Into<String>>,
    _region: impl Into<String>,
    _bucket_name: impl Into<String>,
    _endpoint: Option<impl Into<String>>,
    _session_token: Option<impl Into<String>>,
) -> Result<DummyObjectStore> {
    NotSupported { name: "aws" }.fail()
}

#[allow(dead_code)]
pub(crate) fn new_failing_s3() -> Result<AmazonS3> {
    Ok(DummyObjectStore { name: "aws".into() })
}

/// Stub when gcs is not configured
#[allow(dead_code)]
pub(crate) fn new_gcs(
    _service_account_path: impl AsRef<std::ffi::OsStr>,
    _bucket_name: impl Into<String>,
) -> Result<DummyObjectStore> {
    NotSupported { name: "gcs" }.fail()
}

/// Stub when azure is not configured
#[allow(dead_code)]
pub(crate) fn new_azure(
    _account: impl Into<String>,
    _access_key: impl Into<String>,
    _container_name: impl Into<String>,
) -> Result<DummyObjectStore> {
    NotSupported { name: "azure" }.fail()
}
