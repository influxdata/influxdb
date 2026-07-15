//! Catalog record type definitions.
//!
//! Each record type implements `CatalogRecord` + `Encode`/`Decode` and is
//! registered with the global registry via `inventory::submit!`.
//!
//! # Core Records (RecordId::core)
//!
//! - **Feature level (1)**: AdvanceFeatureLevel
//! - **Node (2-3)**: RegisterNode, StopNode
//! - **Database (4-5)**: CreateDatabase, SoftDeleteDatabase
//! - **Table (6-8)**: CreateTable, SoftDeleteTable, AddColumns
//! - **Cache (9-12)**: CreateDistinctCache, DeleteDistinctCache, CreateLastCache, DeleteLastCache
//! - **Trigger (13-16)**: CreateTrigger, DeleteTrigger, EnableTrigger, DisableTrigger
//! - **Retention (17-18)**: SetDbRetentionPeriod, ClearDbRetentionPeriod
//! - **Token (19-21)**: CreateAdminToken, RegenerateAdminToken, DeleteToken
//! - **Hard delete (22-23)**: HardDeleteDatabase, HardDeleteTable
//! - **Config (24-25)**: SetGenerationDuration, SetStorageMode
//! - **Repository (26)**: SetNextId
//! - **User (27-38)**: CreateUser, UpdateUserDisplayName, DeleteUser, RestoreUser,
//!   CreateLoginIdentityUsernamePassword, UpdateLoginIdentityPasswordHash,
//!   UpdateLoginIdentityRequiresPasswordReset, DeleteLoginIdentityUsernamePassword,
//!   CreateRefreshToken, RevokeRefreshToken, RevokeAllRefreshTokensForUser, UpdateUserRoles
//! - **Role (39-42)**: CreateRole, UpdateRolePermissions, UpdateRole, DeleteRole
//! - **Node lifecycle (43-46)**: RequestStopNode, AckStopNode, RemoveNode, UnregisterNode
//!
//! # Enterprise Records (RecordId::enterprise)
//!
//! - **Token (e1)**: CreateResourceScopedToken
//! - **Table retention (e2-e3)**: SetTableRetentionPeriod, ClearTableRetentionPeriod
//! - **OAuth login identity (e4-e5)**: CreateLoginIdentityOAuth, DeleteLoginIdentityOAuth
//! - **Restore (e6)**: RestoreCatalog
//! - **Query group (e7-e9)**: CreateQueryGroup, UpdateQueryGroup, DeleteQueryGroup

pub mod types;

mod cache;
pub(crate) mod conversions;
mod database;
mod feature_level;
mod generation;
mod node;
mod query_group;
mod repository;
mod restore;
mod retention;
pub mod role;
mod table;
mod token;
mod trigger;
pub mod user;

// Re-export all record types
pub use cache::{CreateDistinctCache, CreateLastCache, DeleteDistinctCache, DeleteLastCache};
pub use database::*;
pub use feature_level::AdvanceFeatureLevel;
pub use generation::{SetGenerationDuration, SetStorageMode};
pub use node::{AckStopNode, RegisterNode, RemoveNode, RequestStopNode, StopNode, UnregisterNode};
pub use query_group::{CreateQueryGroup, DeleteQueryGroup, UpdateQueryGroup};
pub use repository::{NextIdScope, SetNextId};
pub use restore::RestoreCatalog;
pub use retention::{ClearDbRetentionPeriod, SetDbRetentionPeriod};
pub use role::{CreateRole, DeleteRole, UpdateRole, UpdateRolePermissions};
pub use table::{AddColumns, CreateTable, HardDeleteTable, SoftDeleteTable};
pub use token::{CreateAdminToken, DeleteToken, RegenerateAdminToken};
pub use trigger::{CreateTrigger, DeleteTrigger, DisableTrigger, EnableTrigger};
pub use user::{
    CreateLoginIdentityUsernamePassword, CreateRefreshToken, CreateUser,
    DeleteLoginIdentityUsernamePassword, DeleteUser, RestoreUser, RevokeAllRefreshTokensForUser,
    RevokeRefreshToken, UpdateLoginIdentityPasswordHash, UpdateLoginIdentityRequiresPasswordReset,
    UpdateUserDisplayName, UpdateUserRoles,
};

/// Implements the `format::Encode` and `format::Decode` traits for a type
/// that derives `bitcode::Encode` and `bitcode::Decode`.
macro_rules! impl_bitcode_encoding {
    ($($ty:ty),+ $(,)?) => {
        $(
            impl $crate::format::Encode for $ty {
                fn encode(&self, buf: &mut Vec<u8>) {
                    buf.extend_from_slice(&bitcode::encode(self));
                }
            }

            impl $crate::format::Decode for $ty {
                fn decode(buf: &[u8]) -> Result<Self, $crate::format::FormatError> {
                    bitcode::decode(buf).map_err(|_| {
                        $crate::format::FormatError::InvalidRecordLength {
                            length: buf.len() as u32,
                        }
                    })
                }
            }
        )+
    };
}

pub(crate) use impl_bitcode_encoding;

/// Asserts that a value roundtrips through `Encode` and `Decode`, and
/// snapshots the encoded bytes to detect accidental encoding changes.
///
/// Record bodies are frozen once shipped — any change to the serialized
/// bytes would break forward compatibility. The snapshot catches this.
///
/// ```ignore
/// assert_roundtrip!(SetStorageMode { mode: StorageMode::Parquet }, "deadbeef");
/// ```
#[cfg(test)]
macro_rules! assert_roundtrip {
    ($value:expr, $expects:literal) => {{
        use $crate::format::Encode as _;
        fn record_name<T: $crate::format::CatalogRecord>(_: &T) -> &'static str {
            T::NAME
        }
        let original = $value;
        let name = record_name(&original);
        let mut buf = Vec::new();
        original.encode(&mut buf);
        let decoded = <_ as $crate::format::Decode>::decode(&buf).expect("decode failed");
        assert_eq!(original, decoded, "round-trip encode decode failed");
        let actual = hex::encode(&buf);
        let expected = $expects;
        assert_eq!(
            expected, actual,
            "⚠ Encoded catalog record ({name}) serialized bytes changed ⚠\n
Expected bytes:\n
\"{expected}\"\n
Actual encoded bytes:\n
\"{actual}\"\n
Once a CatalogRecord type has been shipped in a released version of the software, it \
cannot be modified. To introduce new functionality, you must introduce a new type \
that implements the CatalogRecord trait.\n
If this is the first time you're adding this catalog record type, or are making \
modifications to it prior to releasing it, then you can update the expected literal \
passed to the assert_roundtrip! macro by copying the string literal from Actual \
encoded bytes.\n",
        );
    }};
}

#[cfg(test)]
pub(crate) use assert_roundtrip;

#[cfg(test)]
mod tests;
