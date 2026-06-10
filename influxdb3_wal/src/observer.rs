//! Observer trait for WAL catalog-snapshot persistence events.
//!
//! Minimal subset shared with the catalog crate; the broader Service-Level
//! Logging observer set is provided by the enterprise WAL crate.

use std::fmt::Debug;

/// Subscriber for catalog-snapshot persistence events.
///
/// Success fires when a catalog snapshot file is loaded at startup, when this
/// node persisted the initial snapshot, or when it persisted a periodic
/// background checkpoint. Error fires on real load/decode/apply/persist
/// failures. Node-scoped.
pub trait CatalogSnapshotObserver: Send + Sync + Debug {
    fn on_catalog_snapshot_success(&self, sequence_number: u64, size_bytes: u64);
    fn on_catalog_snapshot_error(&self, error_code: &'static str);
}

/// No-op observer used when no subscriber is wired in.
#[derive(Debug, Default, Clone, Copy)]
pub struct NoopCatalogSnapshotObserver;

impl CatalogSnapshotObserver for NoopCatalogSnapshotObserver {
    fn on_catalog_snapshot_success(&self, _: u64, _: u64) {}
    fn on_catalog_snapshot_error(&self, _: &'static str) {}
}
