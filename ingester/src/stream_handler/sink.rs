use std::{fmt::Debug, ops::Deref, sync::Arc};

use async_trait::async_trait;
use dml::DmlOperation;

use crate::data::DmlApplyAction;

/// A [`DmlSink`] handles [`DmlOperation`] instances read from a shard.
#[async_trait]
pub(crate) trait DmlSink: Debug + Send + Sync {
    /// Apply `op` read from a shard, returning `Ok(DmlApplyAction::Applied(bool))`, the bool indicating if the
    /// ingest should be paused. Returns `Ok(DmlApplyAction::Skipped)` if the operation has been
    /// applied previously and was skipped.
    async fn apply(&self, op: DmlOperation) -> Result<DmlApplyAction, crate::data::Error>;
}

#[async_trait]
impl<T> DmlSink for Arc<T>
where
    T: DmlSink,
{
    async fn apply(&self, op: DmlOperation) -> Result<DmlApplyAction, crate::data::Error> {
        self.deref().apply(op).await
    }
}
