use std::{error::Error, fmt::Debug, ops::Deref, sync::Arc};

use async_trait::async_trait;
use dml::DmlOperation;
use thiserror::Error;

#[derive(Debug, Error)]
pub(crate) enum DmlError {
    /// An error applying a [`DmlOperation`] to a [`BufferTree`].
    ///
    /// [`BufferTree`]: crate::buffer_tree::BufferTree
    #[error("failed to buffer op: {0}")]
    Buffer(#[from] mutable_batch::Error),
}

/// A [`DmlSink`] handles [`DmlOperation`] instances in some abstract way.
#[async_trait]
pub(crate) trait DmlSink: Debug + Send + Sync {
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
