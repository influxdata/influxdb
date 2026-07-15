//! Record registry for catalog record types.
//!
//! This module provides compile-time registration of catalog record types using
//! the `inventory` crate. Each record type implements `CatalogRecord` and is
//! registered via `inventory::submit!`.
//!
//! The global `REGISTRY` provides lookup of record types by ID at runtime.

use std::collections::BTreeMap;
use std::sync::LazyLock;

use bytes::Bytes;

use crate::catalog::versions::v3::events::CatalogEvent;
use crate::catalog::versions::v3::inner::InnerCatalog;

use super::apply::ApplyError;
use super::{Decode, Encode, FormatError, Record, RecordFlags, RecordId};

/// Trait implemented by all catalog record types.
///
/// Records are the persistence layer — frozen once shipped. Each record type
/// has a unique ID, flags, and name, and produces a domain event describing
/// the state change.
pub trait CatalogRecord: Sized + Encode {
    /// Unique identifier for this record type.
    const ID: RecordId;

    /// Default flags for this record.
    const FLAGS: RecordFlags;

    /// Human-readable name for this record type.
    const NAME: &'static str;

    /// Apply this record to the catalog, mutating its state.
    ///
    /// Called during log replay to reconstruct the catalog from persisted
    /// records. Each record type implements the specific mutation logic.
    ///
    /// # Errors
    ///
    /// Returns an [`ApplyError`] if the record is inconsistent with the
    /// current catalog state. Since records were validated before
    /// persistence, this indicates data corruption or a bug in the write
    /// path.
    fn apply(&self, catalog: &mut InnerCatalog) -> Result<(), ApplyError>;

    /// Produce the domain event for this record.
    ///
    /// Called after the record is applied to the catalog. The event is
    /// broadcast to subscribers.
    fn event(&self) -> CatalogEvent;
}

/// Extension trait that encodes a [`CatalogRecord`] into a [`Record`].
///
/// This bridges the type-safe `CatalogRecord` trait (compile-time metadata)
/// with the format-level `Record` type (header + opaque bytes). The blanket
/// implementation encodes the record body via the [`Encode`] trait and
/// constructs a `Record` with the appropriate ID, flags, and sequence.
///
/// Used by [`RecordBatch::push`](super::RecordBatch::push) on the write path.
pub trait MakeRecord {
    /// Encode this record into a format-level [`Record`] stamped with `sequence`.
    fn make_record(&self, sequence: u64) -> Record;
}

impl<T> MakeRecord for T
where
    T: CatalogRecord,
{
    fn make_record(&self, sequence: u64) -> Record {
        let mut buf = Vec::new();
        self.encode(&mut buf);
        Record::new(T::ID.raw(), T::FLAGS, sequence, Bytes::from(buf))
    }
}

/// Type-erased function that decodes raw bytes, applies the record to
/// the catalog, and returns the domain event.
///
/// Panics on decode failure — persisted records must be valid.
pub(crate) type DecodeApplyAndEventFn =
    fn(&[u8], &mut InnerCatalog) -> Result<CatalogEvent, ApplyError>;

/// Type-erased function that decodes raw bytes and serializes the record to a
/// `serde_json::Value` for inspection tooling.
pub(crate) type DecodeToValueFn = fn(&[u8]) -> Result<serde_json::Value, FormatError>;

/// A registered record type with its metadata and type-erased apply function.
///
/// Created via `RegisteredRecord::new::<T>()` and collected by `inventory`.
#[derive(Clone, Copy)]
pub struct RegisteredRecord {
    /// Record type identifier.
    pub id: RecordId,
    /// Default flags.
    pub flags: RecordFlags,
    /// Human-readable name.
    pub name: &'static str,
    /// Decodes raw bytes, applies the record, and returns its event.
    pub decode_apply_and_event: DecodeApplyAndEventFn,
    /// Decodes raw bytes and serializes the record to a value for inspection.
    pub decode_to_value: DecodeToValueFn,
}

impl std::fmt::Debug for RegisteredRecord {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("RegisteredRecord")
            .field("id", &self.id)
            .field("flags", &self.flags)
            .field("name", &self.name)
            .finish()
    }
}

impl RegisteredRecord {
    /// Create a new registration entry for a record type.
    pub const fn new<T: CatalogRecord + Decode + serde::Serialize>() -> Self {
        Self {
            id: T::ID,
            flags: T::FLAGS,
            name: T::NAME,
            decode_apply_and_event: decode_apply_and_event_impl::<T>,
            decode_to_value: decode_to_value_impl::<T>,
        }
    }
}

fn decode_apply_and_event_impl<T: CatalogRecord + Decode>(
    buf: &[u8],
    catalog: &mut InnerCatalog,
) -> Result<CatalogEvent, ApplyError> {
    let record: T = T::decode(buf).unwrap_or_else(|e| {
        panic!("CatalogRecord {}: decode failed: {e}", T::NAME);
    });
    record.apply(catalog)?;
    Ok(record.event())
}

fn decode_to_value_impl<T: CatalogRecord + Decode + serde::Serialize>(
    buf: &[u8],
) -> Result<serde_json::Value, FormatError> {
    let record: T = T::decode(buf)?;
    serde_json::to_value(&record).map_err(|e| FormatError::RecordToValue {
        record_id: T::ID.raw(),
        reason: e.to_string(),
    })
}

inventory::collect!(RegisteredRecord);

/// Registry of all known record types, indexed by raw ID.
#[derive(Debug)]
pub struct RecordRegistry {
    ops: BTreeMap<RecordId, &'static RegisteredRecord>,
}

impl RecordRegistry {
    fn new() -> Self {
        let mut ops = BTreeMap::new();
        for op in inventory::iter::<RegisteredRecord> {
            let raw = op.id;
            if ops.insert(raw, op).is_some() {
                panic!("duplicate record id: {} ({})", raw, op.name);
            }
        }
        Self { ops }
    }

    /// Look up a registered record by raw ID.
    pub fn get(&self, record_id: RecordId) -> Option<&'static RegisteredRecord> {
        self.ops.get(&record_id).copied()
    }

    /// Check if a record ID is registered.
    pub fn contains(&self, record_id: RecordId) -> bool {
        self.ops.contains_key(&record_id)
    }

    /// Number of registered records.
    pub fn len(&self) -> usize {
        self.ops.len()
    }

    /// Returns true if no records are registered.
    pub fn is_empty(&self) -> bool {
        self.ops.is_empty()
    }

    /// Iterate over all registered records.
    pub fn all(&self) -> impl Iterator<Item = &'static RegisteredRecord> + '_ {
        self.ops.values().copied()
    }
}

/// Global registry of all record types.
///
/// This is populated at program startup from `inventory::submit!` registrations.
pub static REGISTRY: LazyLock<RecordRegistry> = LazyLock::new(RecordRegistry::new);

/// Validate that a record's flags are appropriate for the given record ID.
///
/// Returns an error if the record ID is unknown and the UPGRADE_SAFE flag is not set.
///
/// Returns `Ok(true)` if the record should be processed (known record).
/// Returns `Ok(false)` if the record should be skipped (unknown but upgrade-safe).
pub fn validate_record_flags(record_id: RecordId, flags: RecordFlags) -> Result<bool, FormatError> {
    if REGISTRY.contains(record_id) {
        Ok(true)
    } else if flags.is_upgrade_safe() {
        Ok(false)
    } else {
        Err(FormatError::UnknownNonUpgradeSafeRecord {
            record_id: record_id.raw(),
        })
    }
}

#[cfg(test)]
mod tests;
