use std::{fmt::Debug, ops::Deref, sync::Arc};

use async_trait::async_trait;
use dml::DmlOperation;

/// A [`DmlSink`] handles [`DmlOperation`] instances read from a sequencer.
#[async_trait]
pub trait DmlSink: Debug + Send + Sync {
    /// Apply `op` read from a sequencer, returning `Ok(true)` if ingest should
    /// be paused.
    async fn apply(&self, op: DmlOperation) -> Result<bool, crate::data::Error>;
}

#[async_trait]
impl<T> DmlSink for Arc<T>
where
    T: DmlSink,
{
    async fn apply(&self, op: DmlOperation) -> Result<bool, crate::data::Error> {
        self.deref().apply(op).await
    }
}
