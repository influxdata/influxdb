//! Compatibility layer providing a [`DmlSink`] impl for [`IngesterData`].

use std::sync::Arc;

use async_trait::async_trait;
use data_types::ShardId;
use dml::DmlOperation;

use super::DmlSink;
use crate::{
    data::{DmlApplyAction, IngesterData},
    lifecycle::LifecycleHandleImpl,
};

/// Provides a [`DmlSink`] implementation for a [`IngesterData`] instance.
#[derive(Debug)]
pub(crate) struct IngestSinkAdaptor {
    ingest_data: Arc<IngesterData>,
    lifecycle_handle: LifecycleHandleImpl,
    shard_id: ShardId,
}

impl IngestSinkAdaptor {
    /// Wrap an [`IngesterData`] in an adaptor layer to provide a [`DmlSink`]
    /// implementation.
    pub(crate) fn new(
        ingest_data: Arc<IngesterData>,
        lifecycle_handle: LifecycleHandleImpl,
        shard_id: ShardId,
    ) -> Self {
        Self {
            ingest_data,
            lifecycle_handle,
            shard_id,
        }
    }
}

#[async_trait]
impl DmlSink for IngestSinkAdaptor {
    async fn apply(&self, op: DmlOperation) -> Result<DmlApplyAction, crate::data::Error> {
        self.ingest_data
            .buffer_operation(self.shard_id, op, &self.lifecycle_handle)
            .await
    }
}
