//! Binary framing format for catalog persistence.
//!
//! This module defines the binary framing format used for catalog log and
//! snapshot files.
//!
//! # File Structure
//!
//! ## Log files
//!
//! ```text
//! +----------------+
//! |     Header     |  64 bytes
//! +----------------+
//! |    Record 1    |  16-byte header + variable data
//! +----------------+
//! |    Record 2    |
//! +----------------+
//! |      ...       |
//! +----------------+
//! ```
//!
//! ## Snapshot files
//!
//! Same payload shape as a log file — the header `SNAPSHOT` flag is the only
//! difference. Records appear in application order.
//!
//! ```text
//! +----------------+
//! |     Header     |  64 bytes (SNAPSHOT flag set)
//! +----------------+
//! |    Record 1    |  16-byte header + variable data
//! +----------------+
//! |      ...       |
//! +----------------+
//! ```
//!
//! # Forward Compatibility
//!
//! Records are feature-gated by default — they cannot be written until the
//! cluster's committed feature level includes them. Records marked with
//! `UPGRADE_SAFE` can be written during upgrade windows and skipped by
//! nodes that don't recognize them.

/// Asserts that a value's bitcode encoding matches the expected hex string.
///
/// These types are embedded in catalog records, so their serialized bytes
/// are part of the on-disk catalog format. The assertion catches accidental
/// reordering, insertion, or removal of enum variants and struct fields.
///
/// ```ignore
/// assert_encoding_stable!(NodeMode::Core, "00");
/// ```
#[cfg(test)]
macro_rules! assert_encoding_stable {
    ($value:expr, $expects:literal) => {{
        let value = $value;
        let actual = hex::encode(bitcode::encode(&value));
        let expected = $expects;
        assert_eq!(
            expected,
            actual,
            "⚠ Encoded bytes for {} changed ⚠\n
Expected bytes:\n
\"{expected}\"\n
Actual encoded bytes:\n
\"{actual}\"\n
This type is embedded in a catalog record, so its serialized bytes are part of the \
on-disk catalog format. Once shipped in a released version of the software, they cannot \
change. To introduce new functionality, add a new enum variant or a new type rather than \
altering an existing one.\n
If this is the first time you're adding this type, or are making modifications to it \
prior to releasing it, then you can update the expected literal passed to the \
assert_encoding_stable! macro by copying the string literal from Actual encoded bytes.\n",
            stringify!($value),
        );
    }};
}

pub mod apply;
pub mod feature_level;
mod header;
pub mod reader;
mod record;
mod record_id;
pub(crate) mod record_ids;
#[allow(dead_code)]
pub mod records;
mod registry;
pub mod view;

pub use apply::ApplyError;
pub use feature_level::{FeatureLevel, derive_feature_level};
pub use header::{Header, file_flags};
pub use reader::CatalogFile;
pub use record::{RECORD_HEADER_SIZE, Record, RecordBatch, RecordHeader};
pub use record_id::RecordId;
pub use registry::{
    CatalogRecord, MakeRecord, REGISTRY, RecordRegistry, RegisteredRecord, validate_record_flags,
};
pub use view::{HeaderView, RecordBodyView, RecordHeaderView, RecordTypeCount, RecordView};

/// Magic bytes identifying an InfluxDB 3 catalog file.
pub const MAGIC: [u8; 4] = *b"IDB3";

/// Encodes a catalog record body to bytes.
///
/// This trait decouples `CatalogRecord` from the specific encoding library.
/// The initial implementation delegates to `bitcode`, but the trait boundary
/// allows the encoding to be swapped without changing record type definitions.
pub trait Encode {
    fn encode(&self, buf: &mut Vec<u8>);
}

/// Decodes a catalog record body from bytes.
///
/// `Decode` is implemented on each record type separately and is not part of
/// the `CatalogRecord` trait bound to preserve dyn-safety (`Decode: Sized`).
pub trait Decode: Sized {
    fn decode(buf: &[u8]) -> Result<Self, FormatError>;
}

/// Current format version.
pub const FORMAT_VERSION: u32 = 1;

/// Flags for record metadata.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct RecordFlags(u16);

impl RecordFlags {
    /// No flags set. Record is feature-gated (default).
    pub const NONE: u16 = 0;
    /// Record can be written before the committed feature level includes it;
    /// readers skip if ID is unknown.
    pub const UPGRADE_SAFE: u16 = 0x0001;

    /// Create flags from raw u16.
    pub fn from_u16(value: u16) -> Self {
        Self(value)
    }

    /// Get raw u16 value.
    pub fn to_u16(self) -> u16 {
        self.0
    }

    /// Check if UPGRADE_SAFE flag is set.
    pub fn is_upgrade_safe(self) -> bool {
        self.0 & Self::UPGRADE_SAFE != 0
    }

    /// Create flags with no bits set (feature-gated).
    pub const fn none() -> Self {
        Self(Self::NONE)
    }

    /// Create flags with UPGRADE_SAFE set.
    pub const fn upgrade_safe() -> Self {
        Self(Self::UPGRADE_SAFE)
    }
}

/// Errors that can occur when working with the binary format.
#[derive(Debug, Clone, thiserror::Error)]
pub enum FormatError {
    /// A decoded record failed to apply to the catalog.
    #[error(transparent)]
    Apply(#[from] apply::ApplyError),

    /// Invalid magic bytes at start of file.
    #[error("invalid magic bytes: expected {expected:?}, got {actual:?}")]
    InvalidMagic { expected: [u8; 4], actual: [u8; 4] },

    /// Unsupported format version.
    #[error("unsupported format version: {version}")]
    UnsupportedVersion { version: u32 },

    /// Buffer too short for required data.
    #[error("buffer too short: expected at least {expected} bytes, got {actual}")]
    BufferTooShort { expected: usize, actual: usize },

    /// Header CRC32 checksum mismatch.
    #[error("header CRC32 mismatch: expected {expected:#010x}, actual {actual:#010x}")]
    HeaderCrc32Mismatch { expected: u32, actual: u32 },

    /// Payload CRC32 checksum mismatch.
    #[error("payload CRC32 mismatch: expected {expected:#010x}, computed {computed:#010x}")]
    Crc32Mismatch { expected: u32, computed: u32 },

    /// Unknown record type without UPGRADE_SAFE flag — hard error.
    #[error("unknown record id {record_id} without UPGRADE_SAFE flag")]
    UnknownNonUpgradeSafeRecord { record_id: u16 },

    /// Invalid record length.
    #[error("invalid record length: {length}")]
    InvalidRecordLength { length: u32 },

    /// Record data exceeds remaining file.
    #[error(
        "record data exceeds file bounds: offset {offset}, length {length}, file size {file_size}"
    )]
    RecordExceedsFile {
        offset: u64,
        length: u32,
        file_size: u64,
    },

    /// File header is internally inconsistent (e.g. a reserved field is
    /// nonzero).
    #[error("invalid header: {reason}")]
    InvalidHeader { reason: &'static str },

    /// A `RestoreCatalog` record was encountered during sync apply with an
    /// empty preload. Callers on a persistence path must first load the
    /// backup state off-lock via `preload_restore_for_records` /
    /// `preload_restore_for_file` and pass the resulting `RestorePreload`
    /// into the apply call.
    #[error(
        "RestoreCatalog record encountered without object-store access \
         (sync apply path): the catalog cannot reload backup state from \
         here — use the async apply path"
    )]
    RestoreRequiresStore,

    /// Loading the backup snapshot/log files referenced by a `RestoreCatalog`
    /// record failed.
    #[error("failed to load restore source: {0}")]
    RestoreLoadFailed(String),

    /// The checkpoint or log path in a `RestoreCatalog` record could not be
    /// parsed as an object-store path.
    #[error("invalid restore path: {0}")]
    InvalidRestorePath(String),

    /// The backup snapshot referenced by a `RestoreCatalog` record was not
    /// found at the recorded checkpoint path.
    #[error("restore checkpoint not found at {checkpoint_path}")]
    RestoreCheckpointMissing { checkpoint_path: String },

    /// A decoded record could not be serialized to a value for inspection.
    #[error("failed to serialize record id {record_id} to value: {reason}")]
    RecordToValue { record_id: u16, reason: String },
}

#[cfg(test)]
mod tests;
