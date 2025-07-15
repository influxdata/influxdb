//! # InfluxDB 3 Catalog
//!
//! The catalog is the metadata management system for InfluxDB 3. It maintains information about
//! databases, tables, columns, nodes, tokens, and caching configurations.
//!
//! ## Catalog Persistence and Versioning
//!
//! The catalog persists its state to object storage using two types of files:
//!
//! ### 1. Log Files
//!
//! Log files record incremental changes to the catalog state as operations are performed.
//!
//! - **Structure**: Each log file contains an `OrderedCatalogBatch` with one or more operations
//! - **Naming**: `{prefix}/catalogs/{sequence:020}.catalog` (e.g., `myhost/catalogs/00000000000000000001.catalog`)
//! - **Serialization**: Version-dependent (v1 uses bitcode, v2+ use serde_json)
//! - **Current Version**: v3 (`OrderedCatalogBatch` in `log::versions::v3`)
//!
//! ### 2. Snapshot Files
//!
//! Snapshots contain the complete catalog state at a specific sequence number, used for faster startup.
//!
//! - **Structure**: Complete `CatalogSnapshot` containing all databases, tables, tokens, etc.
//! - **Location**: `{prefix}/_catalog_checkpoint`
//! - **Serialization**: Version-dependent (v1 uses bitcode, v2+ use serde_json)
//! - **Current Version**: v3 (`CatalogSnapshot` in `snapshot::versions::v3`)
//! - **Checkpoint Interval**: Created every N log files (configurable)
//!
//! ## Version Management
//!
//! Each file type uses a 10-byte version identifier to enable backward-compatible migrations:
//!
//! - Log versions: `idb3.001.l`, `idb3.002.l`, `idb3.003.l`
//! - Snapshot versions: `idb3.001.s`, `idb3.002.s`, `idb3.003.s`
//!
//! ### Version History
//!
//! - **v1**: Initial version using bitcode serialization
//! - **v2**: Switched to serde_json for better compatibility, added token support
//! - **v3**: Added CREATE permission for wildcard database WRITE permissions
//!
//! ### Version Migration
//!
//! When loading files from object storage, older versions are automatically migrated:
//!
//! ```text
//! v1 file → deserialize → v1 types → convert to v2 → convert to v3 → in-memory catalog
//! ```
//!
//! The conversion chain ensures that all changes are preserved through each version.
//!
//! ## Adding a New Version
//!
//! When catalog changes require a new version, follow these steps for both log and snapshot files:
//!
//! ### For Log Files
//!
//! #### 1. Create New Version Module
//!
//! ```ignore
//! // In log/versions/v4.rs
//! pub struct OrderedCatalogBatch { /* new fields */ }
//! impl VersionedFileType for OrderedCatalogBatch {
//!     const VERSION_ID: [u8; 10] = *b"idb3.004.l";
//! }
//! ```
//!
//! #### 2. Implement Conversion
//!
//! ```ignore
//! // In log/versions/v3/conversion.rs
//! impl From<v3::OrderedCatalogBatch> for v4::OrderedCatalogBatch {
//!     fn from(value: v3::OrderedCatalogBatch) -> Self {
//!         // Convert v3 to v4, handling new fields appropriately
//!     }
//! }
//! ```
//!
//! #### 3. Update Deserialization
//!
//! ```ignore
//! // In serialize.rs
//! match *version_id {
//!     v1::OrderedCatalogBatch::VERSION_ID => { /* v1 → v2 → v3 → v4 */ }
//!     v2::OrderedCatalogBatch::VERSION_ID => { /* v2 → v3 → v4 */ }
//!     v3::OrderedCatalogBatch::VERSION_ID => { /* v3 → v4 */ }
//!     v4::OrderedCatalogBatch::VERSION_ID => { /* direct v4 */ }
//! }
//! ```
//!
//! #### 4. Update Module Re-exports
//!
//! ```ignore
//! // In log.rs
//! pub use versions::v4::*;  // Export v4 as the current version
//! ```
//!
//! ### For Snapshot Files
//!
//! #### 1. Create New Version Module
//!
//! ```ignore
//! // In snapshot/versions/v4.rs
//! pub struct CatalogSnapshot { /* new fields */ }
//! impl VersionedFileType for CatalogSnapshot {
//!     const VERSION_ID: [u8; 10] = *b"idb3.004.s";  // Note: .s for snapshot
//! }
//! ```
//!
//! #### 2. Implement Conversion
//!
//! ```ignore
//! // In snapshot/versions/v3/conversion.rs
//! impl From<v3::CatalogSnapshot> for v4::CatalogSnapshot {
//!     fn from(value: v3::CatalogSnapshot) -> Self {
//!         // Convert v3 to v4, handling new fields appropriately
//!     }
//! }
//! ```
//!
//! #### 3. Update Deserialization
//!
//! ```ignore
//! // In serialize.rs (verify_and_deserialize_catalog_checkpoint_file)
//! match *version_id {
//!     v1::CatalogSnapshot::VERSION_ID => { /* v1 → v2 → v3 → v4 */ }
//!     v2::CatalogSnapshot::VERSION_ID => { /* v2 → v3 → v4 */ }
//!     v3::CatalogSnapshot::VERSION_ID => { /* v3 → v4 */ }
//!     v4::CatalogSnapshot::VERSION_ID => { /* direct v4 */ }
//! }
//! ```
//!
//! #### 4. Update Module Re-exports
//!
//! ```ignore
//! // In snapshot.rs
//! pub(crate) use versions::v4::*;  // Export v4 as the current version
//! ```
//!
//! #### 5. Update the Snapshot Trait Implementation
//!
//! The `Snapshot` trait is only implemented for the latest version and handles conversion
//! between in-memory types (`InnerCatalog`) and serialized types (`CatalogSnapshot`):
//!
//! ```ignore
//! // In snapshot/versions/mod.rs
//! impl Snapshot for InnerCatalog {
//!     type Serialized = CatalogSnapshot;  // This is v4::CatalogSnapshot
//!
//!     fn snapshot(&self) -> Self::Serialized {
//!         // Convert in-memory representation to v4 snapshot
//!     }
//!
//!     fn from_snapshot(snap: Self::Serialized) -> Self {
//!         // Convert v4 snapshot to in-memory representation
//!         // Note: Version migration happens before this, in conversion functions
//!     }
//! }
//! ```
//!
//! ### Important Considerations
//!
//! 1. **Never modify existing version modules** - they must remain stable for deserialization
//! 2. **Always provide From implementations** - ensure smooth migration from all previous versions
//! 3. **Handle missing data gracefully** - use defaults or Option types for new fields
//! 4. **Test migrations thoroughly** - include tests that load files from all previous versions
//! 5. **Document version changes** - explain what changed and why in the version module
//! 6. **Version ID format** - log files use `.l` suffix, snapshot files use `.s` suffix
//! 7. **Snapshot trait** - only implement for the latest version; older versions rely on conversion chains
//!
//! ## Example: v3 Token Permission Migration
//!
//! Version 3 demonstrates a typical migration pattern:
//!
//! ```ignore
//! // When converting v2 → v3, wildcard database WRITE permissions get CREATE added
//! if permission.resource_type == "database"
//!     && permission.resource_identifier == "*"
//!     && permission.actions.contains("write") {
//!     permission.actions.push("create");
//! }
//! ```
//!
//! This ensures existing tokens maintain expected behavior after the upgrade.

pub mod catalog;
pub mod channel;
pub mod error;
pub mod id;
pub mod log;
pub mod object_store;
pub mod resource;
pub mod serialize;
pub mod snapshot;

pub use error::CatalogError;
pub(crate) type Result<T, E = CatalogError> = std::result::Result<T, E>;
