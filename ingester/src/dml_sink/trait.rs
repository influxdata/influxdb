use std::{error::Error, fmt::Debug, ops::Deref, sync::Arc};

use crate::dml_payload::IngestOp;
use async_trait::async_trait;
use thiserror::Error;

/// Errors returned due from calls to [`DmlSink::apply()`].
#[derive(Debug, Error)]
pub enum DmlError {
    /// An error applying a [`IngestOp`] to a [`BufferTree`].
    ///
    /// [`BufferTree`]: crate::buffer_tree::BufferTree
    #[error("failed to buffer op: {0}")]
    Buffer(#[from] mutable_batch::Error),

    /// An error appending the [`IngestOp`] to the write-ahead log.
    #[error("wal commit failure: {0}")]
    Wal(String),

    /// The write has hit an internal timeout designed to prevent writes from
    /// retrying indefinitely.
    #[error("buffer apply request timeout")]
    ApplyTimeout,
}

/// A [`DmlSink`] handles [`IngestOp`] instances in some abstract way.
#[async_trait]
pub trait DmlSink: Debug + Send + Sync {
    /// The concrete error type returned by a [`DmlSink`] implementation,
    /// convertible to a [`DmlError`].
    type Error: Error + Into<DmlError> + Send;

    /// Apply `op` to the implementer's state.
    async fn apply(&self, op: IngestOp) -> Result<(), Self::Error>;
}

#[async_trait]
impl<T> DmlSink for Arc<T>
where
    T: DmlSink,
{
    type Error = T::Error;
    async fn apply(&self, op: IngestOp) -> Result<(), Self::Error> {
        self.deref().apply(op).await
    }
}
