use std::fmt::Debug;

use async_trait::async_trait;
use data_types::sequence_number_set::SequenceNumberSet;
use tokio::sync::watch::Receiver;
use wal::WriteResult;

use crate::dml_payload::IngestOp;

/// An abstraction over a write-ahead log, decoupling the write path from the
/// underlying implementation.
pub(super) trait WalAppender: Send + Sync + Debug {
    /// Add `op` to the write-head log, returning once `op` is durable.
    fn append(&self, op: &IngestOp) -> Receiver<Option<WriteResult>>;
}

/// An abstraction over a sequence number tracker for a write-ahead log,
/// decoupling the write path from the underlying implementation.
#[async_trait]
pub(super) trait UnbufferedWriteNotifier: Send + Sync + Debug {
    /// Notifies the receiver that a write with the given [`SequenceNumberSet`]
    /// failed to buffer.
    async fn notify_failed_write_buffer(&self, set: SequenceNumberSet);
}
