use std::{
    fmt::{Debug, Display},
    sync::Arc,
};

use async_trait::async_trait;
use data_types::{CompactionLevel, ParquetFile, ParquetFileId, ParquetFileParams, PartitionId};

pub(crate) mod logging;
pub(crate) mod metrics;
pub(crate) mod mock;

/// Error returned by [`Commit`] implementations.
#[derive(Debug, thiserror::Error)]
pub enum Error {
    /// Commit request was malformed
    #[error("Bad commit request: {0}")]
    BadRequest(String),

    /// Commit succeeded, but catalog returned an invalid result
    #[error("Result from catalog is invalid: {0}")]
    InvalidCatalogResult(String),

    /// Commit failed because of an error in the throttler
    #[error("Failure in throttler: {0}")]
    ThrottlerError(#[from] crate::ThrottleError),
}

/// Ensures that the file change (i.e. deletion and creation) are committed to the catalog.
#[async_trait]
pub trait Commit: Debug + Display + Send + Sync {
    /// Commmit deletions, upgrades and creations in a single transaction.
    ///
    /// Returns the IDs for the created files.
    ///
    /// This method retries. During the retries, no intermediate states (i.e. some files deleted, some created) will be
    /// visible. Commits are always all-or-nothing.
    async fn commit(
        &self,
        partition_id: PartitionId,
        delete: &[ParquetFile],
        upgrade: &[ParquetFile],
        create: &[ParquetFileParams],
        target_level: CompactionLevel,
    ) -> Result<Vec<ParquetFileId>, crate::commit::Error>;
}

/// Something that can wrap `Commit` instances
///
/// Use to install test observation
pub trait CommitWrapper: Debug + Send + Sync {
    /// a function to call that wraps a [`Commit`] instance
    fn wrap(&self, commit: Arc<dyn Commit>) -> Arc<dyn Commit>;
}

#[async_trait]
impl<T> Commit for Arc<T>
where
    T: Commit + ?Sized,
{
    async fn commit(
        &self,
        partition_id: PartitionId,
        delete: &[ParquetFile],
        upgrade: &[ParquetFile],
        create: &[ParquetFileParams],
        target_level: CompactionLevel,
    ) -> Result<Vec<ParquetFileId>, crate::commit::Error> {
        self.as_ref()
            .commit(partition_id, delete, upgrade, create, target_level)
            .await
    }
}
