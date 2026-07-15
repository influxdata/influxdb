//! Cross-crate hook for Service-Level Logging (SLL) of background subsystem
//! events. The SLL implementation lives in `ent/influxdb3_service_log/`, but
//! some emission sites are in OSS crates and cannot depend on `ent/`.
//!
//! OSS subsystems hold an `Arc<dyn SllSink>` and call [`SllSink::emit`] with a
//! [`SystemEvent`] variant. The enterprise binary wires in an impl that
//! translates the event into a JSONL service log entry.
//!
//! `ent/influxdb3_internal_api` re-exports this module so ent crates that
//! depend on the ent internal-api see the same [`SllSink`] / [`SystemEvent`]
//! types as OSS crates — emitters and sink agree on one identity.

use std::fmt::Debug;
use std::sync::Arc;

/// Sink for system state-change events.
pub trait SllSink: Send + Sync + Debug {
    fn emit(&self, event: SystemEvent);
}

/// A no-op sink for builds/tests where service logging is disabled.
#[derive(Debug, Default, Clone, Copy)]
pub struct NoopSllSink;

impl SllSink for NoopSllSink {
    fn emit(&self, _event: SystemEvent) {}
}

/// A background subsystem state-change event.
///
/// Each variant carries exactly the fields its emission site exposes — the
/// compiler enforces that an emitter cannot accidentally omit a required
/// field, and that fields belonging to a different event cannot leak in.
#[derive(Debug, Clone)]
pub enum SystemEvent {
    // ── wal_flush ──────────────────────────────────────────────────────
    /// A WAL buffer flush cycle completed successfully.
    WalFlushSuccess {
        /// Sequence number of the persisted WAL file.
        wal_file_number: u64,
        /// Number of `WriteBatch` ops included in this flush.
        write_batches_count: u64,
        /// Total bytes written to the persisted WAL file.
        bytes_flushed: u64,
        /// Wall-clock duration of the flush in milliseconds.
        duration_ms: u64,
    },
    /// A WAL flush attempt failed.
    WalFlushError {
        /// Error category code (never a full message).
        error_code: &'static str,
        /// Wall-clock duration of the flush in milliseconds.
        duration_ms: u64,
    },

    // ── snapshot_created ───────────────────────────────────────────────
    /// A buffer snapshot was persisted to the object store. A snapshot is
    /// **node-scoped** — it may contain data from multiple databases — and
    /// the counts here are aggregates across the whole snapshot.
    SnapshotCreatedSuccess {
        /// Number of parquet files created during persistence.
        parquet_file_count: u64,
        /// Total parquet bytes persisted in this snapshot. The snapshot
        /// manifest itself (a few KB) is excluded; for a removed-files-only
        /// snapshot this can legitimately be `0`.
        total_bytes_persisted: u64,
        /// Sequence number of the most recent WAL file included.
        wal_file_sequence_number: u64,
        /// Wall-clock duration of the flush in milliseconds.
        duration_ms: u64,
    },
    /// A snapshot persistence attempt failed.
    SnapshotCreatedError {
        error_code: &'static str,
        /// Wall-clock duration of the flush in milliseconds.
        duration_ms: u64,
    },

    // ── snapshot_checkpoint ────────────────────────────────────────────
    /// A snapshot checkpoint was written. The checkpoint records the
    /// snapshot high-watermark so a restart can resume from there.
    SnapshotCheckpointSuccess {
        /// Snapshot sequence number of the most recent snapshot included
        /// in this checkpoint — the high-watermark a restart resumes from.
        last_snapshot_sequence_number: u64,
        /// The year-month this checkpoint covers, e.g. `"2026-05"`. One
        /// checkpoint per month; startup-recovery writes a burst of these
        /// with different `year_month` values when many months rebuild.
        year_month: String,
        /// WAL file sequence number of the most recent snapshot included.
        /// Mirrors the `wal_file_sequence_number` on `snapshot_created`
        /// entries so operators can correlate the two events.
        wal_file_sequence_number: u64,
        /// Wall-clock duration of the flush in milliseconds.
        duration_ms: u64,
    },
    SnapshotCheckpointError {
        error_code: &'static str,
        /// Wall-clock duration of the flush in milliseconds.
        duration_ms: u64,
    },

    // ── retention_cleanup ─────────────────────────────────────────────
    /// A retention sweep removed expired data from a database.
    RetentionCleanupSuccess {
        /// Database whose retention policy fired.
        database_id: u32,
        /// Number of files removed across this database's tables.
        files_deleted: u64,
        /// Total bytes freed across all deleted files.
        bytes_reclaimed: u64,
        /// Wall-clock duration of the per-database sweep in milliseconds.
        duration_ms: u64,
    },
    /// A retention sweep failed mid-way for a database. `files_deleted` /
    /// `bytes_reclaimed` count only tables in this database whose purge
    /// completed fully; a table that errored contributes 0 even if some
    /// of its files were deleted before the failure.
    RetentionCleanupError {
        database_id: u32,
        files_deleted: u64,
        bytes_reclaimed: u64,
        error_code: &'static str,
        duration_ms: u64,
    },

    // ── gen1_cleanup ──────────────────────────────────────────────────
    /// A gen1 cleanup cycle removed compacted parquet files. Node-scoped:
    /// counts aggregate across every snapshot and table the cycle touched.
    Gen1CleanupSuccess {
        files_deleted: u64,
        bytes_reclaimed: u64,
        duration_ms: u64,
    },
    /// A gen1 cleanup cycle errored. Counts reflect what was reclaimed
    /// before the failure; object-store deletes happen before in-memory
    /// index updates, so files may be gone even if these counts don't
    /// reflect it.
    Gen1CleanupError {
        files_deleted: u64,
        bytes_reclaimed: u64,
        error_code: &'static str,
        duration_ms: u64,
    },

    // ── compaction_planned ────────────────────────────────────────────
    /// The compactor produced one or more plans for a database in this
    /// planning cycle. One emission per (cycle, database).
    ///
    /// `groups_planned` counts how many plan groups (each a planner
    /// stage like deletions / recompactions / per-level) included this
    /// database — not how many tables were touched. `files_to_compact`
    /// is the total input files across every plan for this database.
    CompactionPlannedSuccess {
        database_id: u32,
        groups_planned: u64,
        files_to_compact: u64,
        /// Plans skipped this cycle because their input file count exceeded the
        /// compactor's per-plan file limit. Zero on a healthy cycle; the log
        /// entry omits the field in that case.
        plans_skipped_file_limit: u64,
    },
    /// Cycle-wide setup failed before any per-database plans existed
    /// (e.g. loading snapshots errored). Node-scoped — no `database_id`
    /// because no plans have been associated with one yet.
    CompactionPlannedError { error_code: &'static str },

    // ── compaction_completed ──────────────────────────────────────────
    /// All compaction plans for a database in this cycle finished. Counts
    /// aggregate across plans; `duration_ms` is the sum of per-plan
    /// execution time for this database.
    ///
    /// `bytes_read` / `rows_deleted_by_retention` / `rows_deleted_by_predicate`
    /// from #2596 are intentionally absent: `compact_files` doesn't track
    /// them today. They'll appear here when instrumentation lands.
    CompactionCompletedSuccess {
        database_id: u32,
        files_compacted: u64,
        files_produced: u64,
        bytes_written: u64,
        duration_ms: u64,
    },
    /// A compaction cycle for a database failed mid-way. Counts reflect
    /// what completed before the failure.
    CompactionCompletedError {
        database_id: u32,
        files_compacted: u64,
        files_produced: u64,
        bytes_written: u64,
        error_code: &'static str,
        duration_ms: u64,
    },

    // ── memory_pressure ───────────────────────────────────────────────
    /// The in-memory write buffer crossed its configured threshold during
    /// a periodic check. Node-scoped — the buffer is a single allocation
    /// across every database on this node. Fires every check tick the
    /// buffer remains over threshold (typically 1-2 ticks per incident
    /// since the check triggers a forced snapshot that drains the buffer).
    MemoryPressureSuccess {
        current_buffer_size_bytes: u64,
        memory_threshold_bytes: u64,
    },

    // ── catalog_snapshot ──────────────────────────────────────────────
    /// The catalog snapshot file was observed in object store. Node-scoped.
    /// Fires at three moments: (a) on every successful startup load of an
    /// existing snapshot — common case, one per node boot, with the
    /// snapshot's actual on-disk `sequence_number` and `size_bytes`;
    /// (b) when this node persisted the initial snapshot itself
    /// (`sequence_number` 0, `size_bytes` of the bytes written); (c) when
    /// this node persisted a periodic `write_checkpoint` (real sequence
    /// and bytes written).
    CatalogSnapshotSuccess {
        /// Catalog sequence number this snapshot consolidates.
        sequence_number: u64,
        /// Serialized snapshot size in bytes.
        size_bytes: u64,
    },
    /// A catalog snapshot observation failed — covers load/decode/apply
    /// failures on the startup path and persist failures on the
    /// initialize / write-checkpoint paths.
    CatalogSnapshotError { error_code: &'static str },

    // ── startup_phase ─────────────────────────────────────────────────
    /// A major startup phase completed successfully. One emission per
    /// (phase, node-boot). `phase` is the discriminator (`catalog_load`,
    /// `snapshot_restore`, `wal_replay`, `cache_warm`, `node_registration`,
    /// `ready`); `detail` is a per-phase machine-readable summary with
    /// no PII. Node-scoped.
    StartupPhaseSuccess {
        phase: &'static str,
        duration_ms: u64,
        detail: String,
    },
    /// A major startup phase failed. The failure typically aborts the
    /// boot — operators see one error event for the failing phase plus
    /// success events for any phases that completed before it.
    StartupPhaseError {
        phase: &'static str,
        error_code: &'static str,
        duration_ms: u64,
    },

    // ── replica_wal_skipped / replica_snapshot_skipped ────────────────
    /// A replica node skipped a WAL file received from a peer ingester
    /// because it could not be deserialized. `from_node_id` identifies
    /// the peer whose file was skipped. Node-scoped.
    ReplicaWalSkipped {
        /// Peer ingester whose WAL file was skipped.
        from_node_id: Arc<str>,
        /// WAL file sequence number that was skipped.
        wal_file_sequence_number: u64,
        /// Error category code (never a full message).
        error_code: &'static str,
    },
    /// A replica node skipped a snapshot received from a peer ingester
    /// because it could not be deserialized. `from_node_id` identifies
    /// the peer whose snapshot was skipped. Node-scoped.
    ReplicaSnapshotSkipped {
        /// Peer ingester whose snapshot was skipped.
        from_node_id: Arc<str>,
        /// Snapshot sequence number that was skipped.
        snapshot_sequence_number: u64,
        /// Error category code (never a full message).
        error_code: &'static str,
    },
}

#[cfg(test)]
mod tests;
