pub mod versions;

use crate::serialize::VersionedFileType;
use serde::{Deserialize, Serialize};
pub use versions::v4::*;

/// A log written when a catalog is upgraded to a new version.
///
/// This will always be the last sequence in an existing catalog log.
#[derive(Debug, Clone, Copy, Serialize, Deserialize)]
pub struct UpgradedLog;

impl VersionedFileType for UpgradedLog {
    const VERSION_ID: [u8; 10] = *b"idb3.EOF.l";
}
