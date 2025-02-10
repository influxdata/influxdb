//! A set of helper methods for creating WAL operations in tests.
use crate::*;

/// Create a new [`WalContents`] with the provided arguments that will have a `persisted_timestamp_ns`
/// of `0`.
pub fn wal_contents(
    (min_timestamp_ns, max_timestamp_ns, wal_file_number): (i64, i64, u64),
    ops: impl IntoIterator<Item = WalOp>,
) -> WalContents {
    WalContents {
        persist_timestamp_ms: 0,
        min_timestamp_ns,
        max_timestamp_ns,
        wal_file_number: WalFileSequenceNumber::new(wal_file_number),
        ops: ops.into_iter().collect(),
        snapshot: None,
    }
}

/// Create a new [`WalContents`] with the provided arguments that will have a `persisted_timestamp_ns`
/// of `0`.
pub fn wal_contents_with_snapshot(
    (min_timestamp_ns, max_timestamp_ns, wal_file_number): (i64, i64, u64),
    ops: impl IntoIterator<Item = WalOp>,
    snapshot: SnapshotDetails,
) -> WalContents {
    WalContents {
        persist_timestamp_ms: 0,
        min_timestamp_ns,
        max_timestamp_ns,
        wal_file_number: WalFileSequenceNumber::new(wal_file_number),
        ops: ops.into_iter().collect(),
        snapshot: Some(snapshot),
    }
}

pub fn write_batch_op(write_batch: WriteBatch) -> WalOp {
    WalOp::Write(write_batch)
}
