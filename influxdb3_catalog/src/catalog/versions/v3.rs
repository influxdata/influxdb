//! Catalog v3 — binary framing format with version-gated records.
//!
//! This module is being built incrementally. The schema types define the
//! in-memory representation of catalog resources for v3.

use influxdb3_id::{CatalogId, FieldFamilyId};

pub mod backup;
pub mod catalog;
pub mod deletes;
pub mod events;
pub mod inner;
pub mod legacy;
pub mod ops;
pub mod schema;
pub mod transaction;
pub mod usage;

/// Soft cap on tag columns per table. The underlying `TagId` is `u16` so the structural
/// limit is 65535, but we cap to a sane value here to prevent runaway schemas. This cap
/// is independent of `NUM_COLUMNS_PER_TABLE_LIMIT`, which gates total column count
/// (tags + fields + timestamp); when both are configured, the lower one wins for tags.
pub(crate) const NUM_TAG_COLUMNS_LIMIT: usize = 4096;
/// Limit for the number of fields in a field family.
pub(crate) const NUM_FIELDS_PER_FAMILY_LIMIT: usize = 100;
/// Limit for the number of field families in a table.
pub(crate) const NUM_FIELD_FAMILIES_LIMIT: usize = FieldFamilyId::MAX.get() as usize;
