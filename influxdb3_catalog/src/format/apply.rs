//! Applying catalog records to the in-memory catalog.
//!
//! - [`ApplyError`] is returned by `CatalogRecord::apply` when a persisted
//!   record is inconsistent with the current in-memory catalog; a
//!   `From<RepositoryError>` impl lets [`Repository`](crate::repository::Repository)
//!   operations propagate via `?`.
//! - [`serialize_log_file`] assembles a slice of [`Record`]s into a complete
//!   binary log file with header and CRC.
//! - [`apply_records`] applies a slice of [`Record`]s to an [`InnerCatalog`]
//!   via the global registry, collecting domain events.
//! - [`apply_catalog_file`] is a convenience wrapper that applies a parsed
//!   [`CatalogFile`] to a catalog.
//!
//! # Restore records and the preload pattern
//!
//! Most catalog records are applied purely against the in-memory state, so
//! [`apply_records`] / [`apply_catalog_file`] are synchronous.
//!
//! The [`RestoreCatalog`] record is
//! the exception: applying it requires loading a backup snapshot + log files
//! from object store, which is async — and apply runs under a sync
//! `parking_lot::RwLock` that cannot be held across `.await`.
//!
//! To keep apply synchronous, the async load is hoisted out of the lock.
//! Callers on a persistence read/write path first run
//! `preload_restore_for_records` / `preload_restore_for_file` (async,
//! off-lock) to produce a [`RestorePreload`], then pass it by `&mut` into the
//! sync apply call. When apply encounters the restore record it consumes the
//! pre-loaded [`InnerCatalog`] from the preload and swaps `*catalog` for it.
//! Apply with an empty preload (the default for batches that carry no restore
//! record) returns [`FormatError::RestoreRequiresStore`] if a restore record
//! is nonetheless encountered.

use std::{collections::BTreeSet, sync::Arc};

use bytes::Bytes;
use influxdb3_id::{CatalogId, DbId, TableId};
use thiserror::Error;
use uuid::Uuid;

use crate::{
    catalog::{
        CatalogSequenceNumber,
        versions::v3::{events::CatalogEvent, inner::InnerCatalog},
    },
    format::records::{HardDeleteDatabase, HardDeleteTable, RestoreCatalog},
    object_store::versions::v3::ObjectStoreCatalog,
    repository::RepositoryError,
};

use super::reader::LEGACY_GROUP_INDEX_ENTRY_SIZE;
use super::{
    CatalogFile, Decode, FormatError, Header, REGISTRY, Record, file_flags, record_ids,
    validate_record_flags,
};

/// Error returned when applying a catalog record fails.
///
/// An `ApplyError` indicates the persisted record is inconsistent with the
/// current in-memory catalog — typically data corruption or a bug in the
/// write path. The carried string identifies the record type and the
/// specific mismatch, intended to give operators actionable context in the
/// event of catalog corruption.
#[derive(Debug, Clone, Error)]
#[error("{0}")]
pub struct ApplyError(pub String);

impl<I: CatalogId> From<RepositoryError<I>> for ApplyError {
    fn from(e: RepositoryError<I>) -> Self {
        Self(e.to_string())
    }
}

/// Build a complete catalog file from a pre-encoded payload, header
/// counts, and flags. Shared by [`serialize_log_file`] and
/// [`serialize_snapshot_file`], which differ only in the header flags.
fn make_file_bytes(
    catalog_uuid: Uuid,
    sequence_number: u64,
    payload: Vec<u8>,
    record_count: u32,
    group_count: u32,
    flags: u16,
) -> Bytes {
    let payload_crc = crc32fast::hash(&payload);
    let header = Header {
        format_version: Header::CURRENT_VERSION,
        flags,
        catalog_uuid: catalog_uuid.as_u128(),
        sequence_number,
        record_count,
        group_count,
        payload_len: payload.len() as u64,
        payload_crc,
    };

    let mut file = Vec::with_capacity(Header::SIZE + payload.len());
    file.extend_from_slice(&header.to_bytes());
    file.extend_from_slice(&payload);
    Bytes::from(file)
}

fn serialize_records(records: &[Record]) -> Vec<u8> {
    let mut payload = Vec::new();
    for record in records {
        payload.extend_from_slice(&record.to_bytes());
    }
    payload
}

/// Serialize a log file which represents a single atomic change to the
/// catalog.
pub fn serialize_log_file(catalog_uuid: Uuid, sequence_number: u64, records: &[Record]) -> Bytes {
    make_file_bytes(
        catalog_uuid,
        sequence_number,
        serialize_records(records),
        u32::try_from(records.len()).expect("record count overflow"),
        0,
        file_flags::NONE,
    )
}

/// Serialize a snapshot file: header (SNAPSHOT flag set), one
/// backward-compatibility group-index entry, then all records concatenated
/// in slice (application) order.
///
/// The index entry declares a single Global group spanning every record.
/// Readers deployed before the flat layout require a snapshot's group index
/// to be present, start with a Global group, and account for every record;
/// this one entry keeps snapshots written here readable on those binaries —
/// and correctly ordered, since flattening a single group is the identity.
/// Current readers skip the entry (see `read_records`).
///
/// Each record is written verbatim, preserving its original per-record
/// sequence — snapshots collect records across many log sequences and that
/// provenance is load-bearing state that must not be rewritten.
pub fn serialize_snapshot_file(
    catalog_uuid: Uuid,
    sequence_number: u64,
    records: &[Record],
) -> Bytes {
    let record_bytes = serialize_records(records);
    let record_count = u32::try_from(records.len()).expect("record count overflow");
    let byte_length = u32::try_from(record_bytes.len()).expect("record bytes overflow");

    // The legacy `GroupIndexEntry` wire layout: group_type and group_key
    // (Global = 0, 0), record_count and byte_length as u32 LE, then
    // byte_offset as u64 LE.
    let mut payload = Vec::with_capacity(LEGACY_GROUP_INDEX_ENTRY_SIZE + record_bytes.len());
    payload.extend_from_slice(&0u32.to_le_bytes());
    payload.extend_from_slice(&0u32.to_le_bytes());
    payload.extend_from_slice(&record_count.to_le_bytes());
    payload.extend_from_slice(&byte_length.to_le_bytes());
    payload.extend_from_slice(&0u64.to_le_bytes());
    payload.extend_from_slice(&record_bytes);

    make_file_bytes(
        catalog_uuid,
        sequence_number,
        payload,
        record_count,
        1,
        file_flags::SNAPSHOT,
    )
}

/// Pre-loaded backup state for a `RestoreCatalog` record in a batch.
///
/// Applying a restore record requires loading a backup snapshot + log files
/// from object store, which is async. Catalog apply runs under a sync write
/// lock (`parking_lot::RwLock`) that cannot be held across `.await`, so the
/// async load happens here, outside the lock, and the sync [`apply_records`]
/// path consumes the pre-loaded [`InnerCatalog`] when it encounters the
/// restore record.
///
/// A batch carries at most one restore record by construction; this holds a
/// single optional preload.
#[derive(Debug, Default)]
pub struct RestorePreload {
    loaded: Option<InnerCatalog>,
}

impl RestorePreload {
    /// An empty preload — sufficient for batches that do not contain a
    /// restore record. Passing this to [`apply_records`] errors on the
    /// first restore record encountered.
    pub fn empty() -> Self {
        Self { loaded: None }
    }

    fn from_loaded(loaded: InnerCatalog) -> Self {
        Self {
            loaded: Some(loaded),
        }
    }

    fn take(&mut self) -> Option<InnerCatalog> {
        self.loaded.take()
    }
}

/// Pre-load any `RestoreCatalog` record's backup state from object store.
///
/// Async; runs outside the catalog's write lock. The returned
/// [`RestorePreload`] is fed to [`apply_records`] (sync) which consumes the
/// loaded `InnerCatalog` when it encounters the restore record.
///
/// Returns an empty preload when the batch contains no restore records.
/// Returns an error if multiple restore records appear in one batch
/// (`RestoreOp` produces exactly one record per batch).
pub(crate) async fn preload_restore_for_records(
    records: &[Record],
    store: &ObjectStoreCatalog,
    committed_feature_level: super::FeatureLevel,
) -> Result<RestorePreload, FormatError> {
    let mut found: Option<&Record> = None;
    for r in records {
        if r.id() == record_ids::RESTORE_CATALOG {
            if found.is_some() {
                return Err(FormatError::RestoreLoadFailed(
                    "batch contains more than one RestoreCatalog record".to_string(),
                ));
            }
            found = Some(r);
        }
    }
    let Some(r) = found else {
        return Ok(RestorePreload::empty());
    };
    let restore = RestoreCatalog::decode(&r.data)?;
    let loaded = load_restore_state(&restore, store, committed_feature_level).await?;
    Ok(RestorePreload::from_loaded(loaded))
}

/// Pre-load any restore state for a parsed [`CatalogFile`]. Convenience
/// wrapper over [`preload_restore_for_records`] that handles both log and
/// snapshot files.
///
/// Returned as a boxed future to break the mutual recursion with
/// [`ObjectStoreCatalog::load_catalog_from_paths`], which calls back into
/// this preload path when replaying restored log files.
pub(crate) fn preload_restore_for_file<'a>(
    file: &'a CatalogFile,
    store: &'a ObjectStoreCatalog,
    committed_feature_level: super::FeatureLevel,
) -> std::pin::Pin<
    Box<dyn std::future::Future<Output = Result<RestorePreload, FormatError>> + Send + 'a>,
> {
    Box::pin(async move {
        preload_restore_for_records(&file.records, store, committed_feature_level).await
    })
}

/// Load the backup state referenced by a `RestoreCatalog` record.
///
/// Preserves the live cluster's committed feature level if it is ahead of
/// the backup's — rolling back a committed level would hide records still
/// in flight elsewhere.
async fn load_restore_state(
    restore: &RestoreCatalog,
    store: &ObjectStoreCatalog,
    committed_feature_level: super::FeatureLevel,
) -> Result<InnerCatalog, FormatError> {
    use object_store::path::Path as ObjPath;

    let checkpoint_path = ObjPath::parse(&restore.checkpoint_path).map_err(|e| {
        FormatError::InvalidRestorePath(format!(
            "checkpoint_path '{}': {e}",
            restore.checkpoint_path
        ))
    })?;
    let log_paths: Vec<ObjPath> = restore
        .log_paths
        .iter()
        .map(|p| {
            ObjPath::parse(p).map_err(|e| FormatError::InvalidRestorePath(format!("'{p}': {e}")))
        })
        .collect::<Result<_, _>>()?;

    let mut loaded = store
        .load_catalog_from_paths(&checkpoint_path, &log_paths)
        .await
        .map_err(|e| FormatError::RestoreLoadFailed(e.to_string()))?
        .ok_or_else(|| FormatError::RestoreCheckpointMissing {
            checkpoint_path: restore.checkpoint_path.clone(),
        })?;

    loaded.committed_feature_level.core = loaded
        .committed_feature_level
        .core
        .max(committed_feature_level.core);
    loaded.committed_feature_level.enterprise = loaded
        .committed_feature_level
        .enterprise
        .max(committed_feature_level.enterprise);
    Ok(loaded)
}

/// Apply a slice of records to the catalog via the global registry.
///
/// Each known record is decoded, applied, and its event collected. Unknown
/// records with the `UPGRADE_SAFE` flag are skipped. Unknown records without
/// the flag return an error.
///
/// Returns the domain events in application order.
///
/// # Restore records
///
/// If the batch contains a [`RestoreCatalog`] record, the caller must pre-load
/// its backup state via `preload_restore_for_records` and pass it in `preload`.
/// The sync apply path replaces `*catalog` with the pre-loaded state when the
/// restore record is encountered. The restore record is **not** retained in
/// `ordered_records` — the next snapshot captures the restored state without
/// the meta-record.
///
/// Encountering a restore record with no pre-loaded state returns
/// [`FormatError::RestoreRequiresStore`].
#[cfg_attr(
    feature = "true_deletion",
    doc = "
# Hard deletes

When the records being applied contain ids of [`record_ids::DELETE_TABLE`] or
[`record_ids::DELETE_DATABASE`], this function goes through all currently-stored records and
removes all that contain data exclusive to the database or table referenced by the delete
record(s).

This record removal described above is best-effort. We will go through all records and if we
can't decode one of them, we simply store that error and move to the next record to continue the
process. The removal function ([`hard_delete_records_for`]) will return an error if any of the
records fail to decode, but that error is not bubbled up out of this function - it's simply
logged. This is because a failure out of this function would imply that record application
failed, when a failure out of the removal function doesn't mean the same thing.

[`hard_delete_records_for`]: crate::format::records::hard_delete_records_for
"
)]
pub fn apply_records(
    records: &[Record],
    catalog: &mut InnerCatalog,
    sequence: CatalogSequenceNumber,
    preload: &mut RestorePreload,
) -> Result<Vec<CatalogEvent>, FormatError> {
    let mut events = Vec::with_capacity(records.len());
    let mut table_ids_to_clear = BTreeSet::<TableId>::new();
    let mut db_ids_to_clear = BTreeSet::<DbId>::new();

    for record in records {
        match record.id() {
            record_ids::RESTORE_CATALOG => {
                let restore = RestoreCatalog::decode(&record.data)?;
                let loaded = preload.take().ok_or(FormatError::RestoreRequiresStore)?;
                // Preserve the live cluster's catalog identity across the swap. The
                // backup snapshot carries its own `catalog_uuid`, but the outer
                // `Catalog.catalog_uuid` used for every subsequent `serialize_log_file`
                // is fixed at construction and never reassigned. Adopting the backup's
                // uuid here would split identity between the post-restore snapshot
                // (stamped from `inner.catalog_uuid`) and the live logs, which the
                // cold-start load applies without ever reconciling. A restore replaces
                // the catalog's *data*, not the cluster's identity — which also feeds
                // licensing — so keep the live uuid.
                let live_uuid = catalog.catalog_uuid;
                *catalog = loaded;
                catalog.catalog_uuid = live_uuid;
                events.push(CatalogEvent::CatalogFullyRestored {
                    restore_id: Arc::from(restore.restore_id.as_str()),
                });
                // Skip the `ordered_records.push` below — the restore record is
                // replaced by the loaded state and should not survive into the
                // next snapshot.
                continue;
            }
            // we don't check for SetNextId here 'cause we transform them so that the HardDelete
            // ones are never persisted after we start clearing the obsolete records
            record_ids::DELETE_TABLE => {
                let hard_delete = HardDeleteTable::decode(&record.data)?;
                table_ids_to_clear.insert(TableId::new(hard_delete.table_id));
            }
            record_ids::DELETE_DATABASE => {
                let hard_delete = HardDeleteDatabase::decode(&record.data)?;
                db_ids_to_clear.insert(DbId::new(hard_delete.db_id));
            }
            _ => (),
        };
        if validate_record_flags(record.id(), record.header.flags)? {
            let entry = REGISTRY.get(record.id()).unwrap();
            events.push((entry.decode_apply_and_event)(&record.data, catalog)?);
        }
        // Retain every validated record, including unknown UPGRADE_SAFE
        // records this process did not apply. They're opaque bytes that a
        // later node may understand and must survive into the next snapshot.
        // Records that fail validation or fail to apply have already
        // short-circuited above and are not retained.
        catalog.ordered_records.push(record.clone());
    }

    #[cfg(feature = "true_deletion")]
    {
        if !db_ids_to_clear.is_empty() || !table_ids_to_clear.is_empty() {
            // We're not failing here because that would imply that applying the records failed. It
            // didn't; we just failed to clean up artifacts that should be gone now. So we warn it to
            // let us know, but we don't want to stop this whole process, since the record application
            // actually did happen and they're in `catalog.ordered_records` now.
            let res = crate::format::record::hard_delete_records_for(
                &mut catalog.ordered_records,
                &db_ids_to_clear,
                &table_ids_to_clear,
            );
            if let Err(e) = res {
                observability_deps::tracing::warn!(
                    "Couldn't fully apply hard delete due to decoding error: {e}"
                );
            }
        }
    }

    catalog.sequence = sequence;
    Ok(events)
}

/// Apply all records from a parsed [`CatalogFile`] to the catalog.
///
/// Convenience wrapper over [`apply_records`] that extracts the sequence
/// number from the file header. See [`apply_records`] for the restore-record
/// caveat.
pub fn apply_catalog_file(
    file: &CatalogFile,
    catalog: &mut InnerCatalog,
    preload: &mut RestorePreload,
) -> Result<Vec<CatalogEvent>, FormatError> {
    let sequence = CatalogSequenceNumber::new(file.sequence_number());
    apply_records(&file.records, catalog, sequence, preload)
}

#[cfg(test)]
mod tests;
