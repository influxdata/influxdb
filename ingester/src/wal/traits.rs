use std::fmt::Debug;

use tokio::sync::watch::Receiver;
use wal::WriteResult;

use crate::dml_payload::IngestOp;

/// An abstraction over a write-ahead log, decoupling the write path from the
/// underlying implementation.
pub(super) trait WalAppender: Send + Sync + Debug {
    /// Add `op` to the write-head log, returning once `op` is durable.
    fn append(&self, op: &IngestOp) -> Receiver<Option<WriteResult>>;
}
