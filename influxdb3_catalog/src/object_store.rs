pub(crate) mod versions;

use anyhow::anyhow;
use object_store::{GetOptions, GetRange, ObjectStore};
use observability_deps::tracing::error;
pub use versions::v2::*;

#[derive(Debug, thiserror::Error)]
pub enum ObjectStoreCatalogError {
    #[error("object store error: {0:?}")]
    ObjectStore(#[from] object_store::Error),

    #[error("unexpected error: {0:?}")]
    Unexpected(#[from] anyhow::Error),

    #[error("upgraded log")]
    UpgradedLog,
}

impl ObjectStoreCatalogError {
    pub fn unexpected(message: impl Into<String>) -> Self {
        Self::Unexpected(anyhow!(message.into()))
    }
}

type Result<T, E = ObjectStoreCatalogError> = std::result::Result<T, E>;

#[derive(Debug, Copy, Clone)]
pub enum PersistCatalogResult {
    Success,
    AlreadyExists,
}

/// File extension for catalog files
pub const CATALOG_LOG_FILE_EXTENSION: &str = "catalog";

/// File extension for catalog files
pub const CATALOG_SNAPSHOT_FILE_EXTENSION: &str = "catalog.snapshot";

/// Gather diagnostics for a HEAD request that failed for a reason other than the object not
/// existing.
///
/// Error responses to HEAD requests carry no body, so the object store's error code (for
/// example S3's `ExpiredToken`) is absent from `head_error`. Error responses to GET requests
/// do carry it, so issue a single-byte ranged GET for the same path and log the outcome.
///
/// The diagnostic is also returned so callers can attach it to the propagated error: the
/// first catalog load runs before the tracing subscriber is installed, so on that path the
/// returned string is the only way the diagnostic reaches the operator.
pub(crate) async fn log_failed_head_diagnostics(
    store: &dyn ObjectStore,
    path: &object_store::path::Path,
    head_error: &object_store::Error,
) -> String {
    let options = GetOptions {
        range: Some(GetRange::Bounded(0..1)),
        ..Default::default()
    };
    let diagnostics = match store.get_opts(path, options).await {
        Err(get_error) => format!(
            "diagnostic ranged GET issued after the HEAD failure also failed, and its error may carry the object store's error body: {get_error:?}"
        ),
        Ok(_) => "diagnostic ranged GET issued after the HEAD failure succeeded; the failure appears specific to HEAD requests".to_string(),
    };
    error!(%path, head_error = ?head_error, "{diagnostics}");
    diagnostics
}
