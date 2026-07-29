//! Bitcode-compatible wire types for catalog records.
//!
//! These types mirror the catalog schema types but use bitcode-compatible
//! representations (e.g., `String` instead of `Arc<str>`, `Vec<(K, V)>` instead
//! of `HashMap<K, V>`). They are embedded within record structs and serialized
//! as part of record bodies.
//!
//! # Stability rules
//!
//! Bitcode uses positional encoding — there are no field tags or self-describing
//! metadata. This means:
//!
//! ## Enums: do not reorder, insert, remove — or append across a bucket edge
//!
//! Existing variant discriminants are positional (0, 1, 2, ...) and must not
//! change.
//!
//! **Appending is not automatically safe.** bitcode picks the packing for
//! variant tags from the enum's *variant count* and never writes that choice
//! into the stream, so a variant may only be appended within a packing bucket.
//! The bucket ceilings are 1, 2, 3, 4, 6, 16, 256: an enum whose count is one
//! of those cannot grow without breaking already-written catalogs. Crossing an
//! edge can also misdecode silently rather than erroring.
//!
//! See [issue #4905](https://github.com/influxdata/influxdb_pro/issues/4905).
//!
//! ## Structs: frozen
//!
//! Struct fields cannot be added, removed, or reordered. Adding a field —
//! even an `Option<T>` — changes the encoded byte layout and breaks
//! decoding of previously persisted data.
//!
//! If a struct needs a new field, create a new record type that includes the
//! updated struct definition. The old record type is deprecated (kept in the
//! registry for decoding old data, but no longer produced on the write path).
//!
//! ## Snapshot tests
//!
//! All types have byte-stability snapshot tests. These catch accidental
//! reordering, insertion, or field changes at compile time. When adding a new
//! enum variant, add a corresponding snapshot assertion in the test file.
//!
//! `assert_encoding_stable!` on a single value does not detect a bucket
//! crossing above 1 -> 2. An enum stored as `Vec<T>` in a record also needs a
//! record-level `assert_roundtrip!` fixture holding two or more elements of
//! distinct variants — that is the assertion that changes at a bucket edge.

use bitcode::{Decode, Encode};
use schema::InfluxColumnType;

use crate::catalog::TIME_COLUMN_NAME;

/// Node operating mode.
#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize, Encode, Decode)]
pub enum NodeMode {
    Core,
    Query,
    Ingest,
    Compact,
    Process,
    All,
}

/// Specification for which nodes an operation applies to.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, Encode, Decode, Default)]
pub enum NodeSpec {
    /// Applies to all nodes.
    #[default]
    All,
    /// Applies only to specific nodes by their catalog IDs.
    Nodes(Vec<u32>),
}

/// Source of a cache - whether user-created or auto-generated.
#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize, Encode, Decode, Default)]
pub enum CacheSource {
    #[default]
    User,
    Auto,
}

/// Definition for last cache value columns.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, Encode, Decode, Default)]
pub enum LastCacheValueColumnsDef {
    /// Explicit list of column identifiers.
    Explicit(Vec<ColumnIdentifier>),
    /// Store all non-key columns.
    #[default]
    AllNonKeyColumns,
}

/// Identifier for a column in a table.
#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize, Encode, Decode)]
pub enum ColumnIdentifier {
    Timestamp,
    Tag(u16),
    Field(FieldIdentifier),
}

/// Identifier for a field column.
#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize, Encode, Decode)]
pub struct FieldIdentifier {
    pub family_id: u16,
    pub field_id: u16,
}

/// Definition for a column.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, Encode, Decode)]
pub enum ColumnDefinition {
    Timestamp(TimestampColumn),
    Tag(TagColumn),
    Field(FieldColumn),
}

impl ColumnDefinition {
    pub fn column_id(&self) -> Option<u16> {
        match self {
            ColumnDefinition::Timestamp(TimestampColumn { column_id, .. }) => *column_id,
            ColumnDefinition::Tag(TagColumn { column_id, .. }) => *column_id,
            ColumnDefinition::Field(FieldColumn { column_id, .. }) => *column_id,
        }
    }

    pub fn name(&self) -> &str {
        match self {
            ColumnDefinition::Timestamp(_) => TIME_COLUMN_NAME,
            ColumnDefinition::Tag(c) => &c.name,
            ColumnDefinition::Field(c) => &c.name,
        }
    }

    pub fn column_type(&self) -> InfluxColumnType {
        match self {
            ColumnDefinition::Timestamp(_) => InfluxColumnType::Timestamp,
            ColumnDefinition::Tag(_) => InfluxColumnType::Tag,
            ColumnDefinition::Field(f) => InfluxColumnType::Field(f.data_type.into()),
        }
    }
}

/// Timestamp column definition.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, Encode, Decode)]
pub struct TimestampColumn {
    /// Legacy ordinal column ID; `None` once the table's `u16` ordinal space
    /// is exhausted (PachaTree mode only).
    pub column_id: Option<u16>,
    pub name: String,
}

/// Tag column definition.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, Encode, Decode)]
pub struct TagColumn {
    pub id: u16,
    /// Legacy ordinal column ID; `None` once the table's `u16` ordinal space
    /// is exhausted (PachaTree mode only).
    pub column_id: Option<u16>,
    pub name: String,
}

/// Field column definition.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, Encode, Decode)]
pub struct FieldColumn {
    pub id: FieldIdentifier,
    /// Legacy ordinal column ID; `None` once the table's `u16` ordinal space
    /// is exhausted (PachaTree mode only).
    pub column_id: Option<u16>,
    pub name: String,
    pub data_type: FieldDataType,
}

/// Data type for field columns.
#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize, Encode, Decode)]
pub enum FieldDataType {
    String,
    Integer,
    UInteger,
    Float,
    Boolean,
    Binary,
}

/// Field family name.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, Encode, Decode)]
pub enum FieldFamilyName {
    /// User-defined field family name.
    User(String),
    /// Auto-generated field family (Nth).
    Auto(u16),
}

/// Field family definition.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, Encode, Decode)]
pub struct FieldFamilyDefinition {
    pub id: u16,
    pub name: FieldFamilyName,
}

/// Field family mode for a table.
#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize, Encode, Decode, Default)]
pub enum FieldFamilyMode {
    /// Fields must always be specified with a field family prefix.
    #[default]
    Aware,
    /// Fields are never interpreted with a field family prefix.
    Auto,
}

/// Trigger specification definition.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, Encode, Decode)]
pub enum TriggerSpec {
    SingleTableWalWrite { table_name: String },
    AllTablesWalWrite,
    Schedule { schedule: String },
    RequestPath { path: String },
    Every { duration_ns: u64 },
}

/// Trigger settings.
#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize, Encode, Decode, Default)]
pub struct TriggerSettings {
    pub run_async: bool,
    pub error_behavior: ErrorBehavior,
}

/// Error behavior for triggers.
#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize, Encode, Decode, Default)]
pub enum ErrorBehavior {
    #[default]
    Log,
    Retry,
    Disable,
}

/// Retention period configuration.
#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize, Encode, Decode)]
pub enum RetentionPeriod {
    Indefinite,
    Duration { duration_secs: u64 },
}

/// Storage mode configuration.
#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize, Encode, Decode, Default)]
pub enum StorageMode {
    #[default]
    Parquet,
    PachaTree,
    ParquetAndPachaTree,
}

/// Scope for hard deletion operations.
#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize, Encode, Decode, Default)]
pub enum DeletionScope {
    /// Remove data and all catalog resources.
    #[default]
    DataAndCatalog,
    /// Remove data and table-level catalog resources, keep database-level.
    DataOnlyRemoveTables,
    /// Remove data only, keep all catalog resources.
    DataOnlyKeepResources,
}

/// Resource type granted by a token permission.
#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize, Encode, Decode)]
pub enum ResourceType {
    Database,
    Token,
    System,
    Wildcard,
}

/// Identifies the resource instances a permission applies to.
///
/// Each variant carries raw IDs (not names); apply resolves them against
/// in-memory catalog state.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, Encode, Decode)]
pub enum ResourceIdentifier {
    /// Specific database IDs.
    Database(Vec<u32>),
    /// Specific token IDs.
    Token(Vec<u64>),
    /// Specific system resource IDs (raw `SystemResourceIdentifier` u16).
    System(Vec<u16>),
    /// All resources of the parent type.
    Wildcard,
}

/// Action set granted, encoded as a per-resource-type bitmap.
///
/// Bit layouts match `influxdb3_authz::DatabaseActions` / `CrudActions` /
/// `SystemActions`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize, Encode, Decode)]
pub enum Actions {
    Database(u16),
    Token(u16),
    System(u16),
    Wildcard,
}

/// A single permission grant attached to a resource-scoped token.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, Encode, Decode)]
pub struct Permission {
    pub resource_type: ResourceType,
    pub resource_identifier: ResourceIdentifier,
    pub actions: Actions,
    /// Preserve names of deleted resources for display purposes.
    pub resource_names: Vec<ResourceNameEntry>,
}

#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, Encode, Decode)]
pub struct ResourceNameEntry {
    pub resource_id: String,
    pub name: String,
    pub deleted: bool,
}

// ---------------------------------------------------------------------------
// Role permissions
//
// Wire mirror of `influxdb3_authz::role::Permission` and friends.
// The role permission model is owned by the authz crate, but record bodies
// are frozen once shipped, so we re-declare the encoding here so on-disk
// bytes do not depend on changes to the external types.
// ---------------------------------------------------------------------------

/// A single permission grant attached to a role.
///
/// Mirrors `influxdb3_authz::role::Permission`, minus `System`, which has no
/// persisted representation (#4905). At 6 variants — a bucket ceiling, so read
/// the module docs before adding one.
#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize, Encode, Decode)]
pub enum RolePermissionGrant {
    AccountAdminAll,
    Database(RoleDatabasePermission),
    Token(RoleTokenPermission),
    User(RoleUserPermission),
    Role(RoleRolePermission),
    AdminToken(RoleAdminTokenPermission),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize, Encode, Decode)]
pub struct RoleDatabasePermission {
    pub action: RoleDatabaseAction,
    pub resource: RoleDatabaseResource,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize, Encode, Decode)]
pub struct RoleTokenPermission {
    pub action: RoleTokenAction,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize, Encode, Decode)]
pub struct RoleUserPermission {
    pub action: RoleUserAction,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize, Encode, Decode)]
pub struct RoleRolePermission {
    pub action: RoleRoleAction,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize, Encode, Decode)]
pub struct RoleAdminTokenPermission {
    pub action: RoleAdminTokenAction,
}

/// Database action.
#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize, Encode, Decode)]
pub enum RoleDatabaseAction {
    Describe,
    Read,
    Write,
    Create,
    Delete,
    // Retained so older catalogs still decode. Do not remove.
    GrantUsage,
}

/// Token action.
#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize, Encode, Decode)]
pub enum RoleTokenAction {
    Read,
    Create,
    Delete,
    // Retained so older catalogs still decode. Do not remove.
    GrantUsage,
}

/// User action.
#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize, Encode, Decode)]
pub enum RoleUserAction {
    Read,
    Create,
    Update,
    Delete,
    // Retained so older catalogs still decode. Do not remove.
    GrantUsage,
}

/// Role action.
#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize, Encode, Decode)]
pub enum RoleRoleAction {
    Read,
    Create,
    Update,
    Delete,
}

/// Admin token action.
#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize, Encode, Decode)]
pub enum RoleAdminTokenAction {
    Create,
    Delete,
}

/// Database resource. `All` represents `ResourceIdentifier::All`, otherwise the
/// raw `DbId` value.
#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize, Encode, Decode)]
pub enum RoleDatabaseResource {
    All,
    Identifier(u32),
}

#[cfg(test)]
mod tests;
