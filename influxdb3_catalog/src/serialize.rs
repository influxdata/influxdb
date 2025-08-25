pub mod versions;
pub use versions::v2::*;

use crate::CatalogError;
use crate::snapshot::versions::{v1, v2, v3, v4};
use bytes::Bytes;

pub trait VersionedFileType {
    const VERSION_ID: [u8; 10];
}

#[derive(Clone, Copy, Debug)]
pub enum CatalogVersion {
    V1,
    V2,
}

/// Detect the catalog version.
pub fn detect_catalog_version(bytes: &Bytes) -> crate::Result<CatalogVersion> {
    let version_id: &[u8; 10] = bytes.first_chunk().ok_or(CatalogError::unexpected(
        "file must contain at least 10 bytes",
    ))?;

    // Check against known versions
    match *version_id {
        v1::CatalogSnapshot::VERSION_ID
        | v2::CatalogSnapshot::VERSION_ID
        | v3::CatalogSnapshot::VERSION_ID => Ok(CatalogVersion::V1),
        v4::CatalogSnapshot::VERSION_ID => Ok(CatalogVersion::V2),
        _ => Err(CatalogError::unexpected("unrecognized catalog file format")),
    }
}
