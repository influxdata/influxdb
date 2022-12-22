use std::{error::Error, fmt::Debug, ops::Deref, sync::Arc};

use async_trait::async_trait;
use dml::DmlOperation;
use thiserror::Error;

/// Errors returned due from calls to [`DmlSink::apply()`].
#[derive(Debug, Error)]
pub enum DmlError {
    /// An error applying a [`DmlOperation`] to a [`BufferTree`].
    ///
    /// [`BufferTree`]: crate::buffer_tree::BufferTree
    #[error("failed to buffer op: {0}")]
    Buffer(#[from] mutable_batch::Error),

    /// An error appending the [`DmlOperation`] to the write-ahead log.
    #[error("wal commit failure: {0}")]
    Wal(String),
}

/// A [`DmlSink`] handles [`DmlOperation`] instances in some abstract way.
#[async_trait]
pub trait DmlSink: Debug + Send + Sync {
    /// The concrete error type returned by a [`DmlSink`] implementation,
    /// convertible to a [`DmlError`].
    type Error: Error + Into<DmlError> + Send;

    /// Apply `op` to the implementer's state.
    async fn apply(&self, op: DmlOperation) -> Result<(), Self::Error>;
}

#[async_trait]
impl<T> DmlSink for Arc<T>
where
    T: DmlSink,
{
    type Error = T::Error;
    async fn apply(&self, op: DmlOperation) -> Result<(), Self::Error> {
        self.deref().apply(op).await
    }
}
