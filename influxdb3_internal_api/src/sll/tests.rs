use super::*;

#[test]
fn wal_flush_success_variant_holds_its_fields() {
    let e = SystemEvent::WalFlushSuccess {
        wal_file_number: 1234,
        write_batches_count: 12,
        bytes_flushed: 65_536,
        duration_ms: 45,
    };
    match e {
        SystemEvent::WalFlushSuccess {
            wal_file_number,
            write_batches_count,
            bytes_flushed,
            duration_ms,
        } => {
            assert_eq!(wal_file_number, 1234);
            assert_eq!(write_batches_count, 12);
            assert_eq!(bytes_flushed, 65_536);
            assert_eq!(duration_ms, 45);
        }
        _ => panic!("wrong variant"),
    }
}

#[test]
fn error_variants_carry_error_code() {
    let e = SystemEvent::WalFlushError {
        error_code: "object_store_unavailable",
        duration_ms: 100,
    };
    match e {
        SystemEvent::WalFlushError {
            error_code,
            duration_ms,
        } => {
            assert_eq!(error_code, "object_store_unavailable");
            assert_eq!(duration_ms, 100);
        }
        _ => panic!("wrong variant"),
    }
}

#[test]
fn noop_sink_accepts_any_event() {
    let sink = NoopSllSink;
    sink.emit(SystemEvent::WalFlushSuccess {
        wal_file_number: 1,
        write_batches_count: 1,
        bytes_flushed: 1,
        duration_ms: 1,
    });
    sink.emit(SystemEvent::SnapshotCheckpointSuccess {
        last_snapshot_sequence_number: 1,
        year_month: "2026-05".into(),
        wal_file_sequence_number: 1,
        duration_ms: 1,
    });
    sink.emit(SystemEvent::RetentionCleanupSuccess {
        database_id: 1,
        files_deleted: 1,
        bytes_reclaimed: 1,
        duration_ms: 1,
    });
    sink.emit(SystemEvent::Gen1CleanupSuccess {
        files_deleted: 1,
        bytes_reclaimed: 1,
        duration_ms: 1,
    });
    sink.emit(SystemEvent::CompactionPlannedSuccess {
        database_id: 1,
        groups_planned: 1,
        plans_skipped_file_limit: 0,
        files_to_compact: 1,
    });
    sink.emit(SystemEvent::CompactionCompletedSuccess {
        database_id: 1,
        files_compacted: 1,
        files_produced: 1,
        bytes_written: 1,
        duration_ms: 1,
    });
    sink.emit(SystemEvent::MemoryPressureSuccess {
        current_buffer_size_bytes: 1,
        memory_threshold_bytes: 1,
    });
    sink.emit(SystemEvent::CatalogSnapshotSuccess {
        sequence_number: 1,
        size_bytes: 1,
    });
    sink.emit(SystemEvent::StartupPhaseSuccess {
        phase: "ready",
        duration_ms: 1,
        detail: "listening_0.0.0.0:8181".into(),
    });
}
