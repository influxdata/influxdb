use std::fmt::Debug;

use async_trait::async_trait;
use dml::DmlOperation;

/// An abstraction over a write-ahead log, decoupling the write path from the
/// underlying implementation.
#[async_trait]
pub(super) trait WalAppender: Send + Sync + Debug {
    /// Add `op` to the write-head log, returning once `op` is durable.
    async fn append(&self, op: &DmlOperation) -> Result<(), wal::Error>;
}
