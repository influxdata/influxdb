pub(crate) mod versions;

use anyhow::anyhow;
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
