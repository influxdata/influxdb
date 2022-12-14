use std::fmt::Debug;

use dml::DmlOperation;
use tokio::sync::watch::Receiver;
use wal::WriteResult;

/// An abstraction over a write-ahead log, decoupling the write path from the
/// underlying implementation.
pub(super) trait WalAppender: Send + Sync + Debug {
    /// Add `op` to the write-head log, returning once `op` is durable.
    fn append(&self, op: &DmlOperation) -> Receiver<Option<WriteResult>>;
}
