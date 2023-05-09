use std::{fmt::Debug, sync::Arc};

use async_trait::async_trait;
use wal::SegmentId;

/// An abstraction defining the ability of an implementer to delete WAL segment
/// files by ID.
#[async_trait]
pub(crate) trait WalFileDeleter: Debug + Send + Sync + 'static {
    /// Delete the WAL segment with the specified [`SegmentId`], or panic if
    /// deletion fails.
    async fn delete_file(&self, id: SegmentId);
}

#[async_trait]
impl WalFileDeleter for Arc<wal::Wal> {
    async fn delete_file(&self, id: SegmentId) {
        self.delete(id).await.expect("failed to drop wal segment");
    }
}
