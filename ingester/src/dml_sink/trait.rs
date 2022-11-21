use std::{error::Error, fmt::Debug, ops::Deref, sync::Arc};

use async_trait::async_trait;
use dml::DmlOperation;
use thiserror::Error;

use crate::data::DmlApplyAction;

#[derive(Debug, Error)]
pub(crate) enum DmlError {
    /// An error applying a [`DmlOperation`].
    #[error(transparent)]
    Data(#[from] crate::data::Error),
}

/// A [`DmlSink`] handles [`DmlOperation`] instances read from a shard.
#[async_trait]
pub(crate) trait DmlSink: Debug + Send + Sync {
    type Error: Error + Into<DmlError> + Send;

    /// Apply `op` read from a shard, returning `Ok(DmlApplyAction::Applied(bool))`, the bool indicating if the
    /// ingest should be paused. Returns `Ok(DmlApplyAction::Skipped)` if the operation has been
    /// applied previously and was skipped.
    async fn apply(&self, op: DmlOperation) -> Result<DmlApplyAction, Self::Error>;
}

#[async_trait]
impl<T> DmlSink for Arc<T>
where
    T: DmlSink,
{
    type Error = T::Error;
    async fn apply(&self, op: DmlOperation) -> Result<DmlApplyAction, Self::Error> {
        self.deref().apply(op).await
    }
}
