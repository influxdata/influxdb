//! Catalog restore (record_id 27).
//!
//! A restore replaces the entire in-memory catalog with state loaded from
//! a backup. Unlike every other record, this one does not mutate part of
//! `InnerCatalog`: it wholesale swaps it. The body carries only the
//! object-store paths to the backup snapshot and its log files, plus a
//! `restore_id` for operator-visible correlation.
//!
//! Because the apply step requires async I/O against the object store,
//! `apply` here is `unreachable!()` — the apply driver
//! ([`crate::format::apply::apply_records`]) intercepts records with
//! [`record_ids::RESTORE_CATALOG`] and handles the load + swap directly.

use std::sync::Arc;

use super::impl_bitcode_encoding;
use crate::catalog::versions::v3::events::CatalogEvent;
use crate::catalog::versions::v3::inner::InnerCatalog;
use crate::format::apply::ApplyError;
use crate::format::{CatalogRecord, RecordFlags, RecordId, RegisteredRecord, record_ids};

/// Replace the in-memory catalog with state loaded from a backup snapshot
/// and replay log files.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, bitcode::Encode, bitcode::Decode)]
pub struct RestoreCatalog {
    /// Wall-clock time the restore was initiated.
    pub time_ns: i64,
    /// Operator-supplied identifier for this restore; surfaces in events
    /// and progress logs.
    pub restore_id: String,
    /// Object-store path to the backup checkpoint (snapshot) file.
    pub checkpoint_path: String,
    /// Object-store paths to the backup log files, applied in sequence
    /// order after the checkpoint.
    pub log_paths: Vec<String>,
}

impl CatalogRecord for RestoreCatalog {
    const ID: RecordId = record_ids::RESTORE_CATALOG;
    // Not UPGRADE_SAFE: an older node that does not understand restore must
    // hard-fail rather than silently ignore the state replacement.
    const FLAGS: RecordFlags = RecordFlags::none();
    const NAME: &'static str = "RestoreCatalog";

    fn apply(&self, _catalog: &mut InnerCatalog) -> Result<(), ApplyError> {
        // Intercepted by `apply_records` before dispatch — see
        // `format::apply::apply_records`. Reaching this path means the
        // driver is missing the special case.
        unreachable!(
            "RestoreCatalog must be intercepted by the apply driver \
             (format::apply::apply_records) — sync apply cannot perform \
             the required async object-store load",
        )
    }

    fn event(&self) -> CatalogEvent {
        CatalogEvent::CatalogFullyRestored {
            restore_id: Arc::from(self.restore_id.as_str()),
        }
    }
}

inventory::submit! {
    RegisteredRecord::new::<RestoreCatalog>()
}

impl_bitcode_encoding!(RestoreCatalog);

#[cfg(test)]
mod tests;
