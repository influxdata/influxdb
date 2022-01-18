//! A NOP implementation of [`DmlHandler`].

use async_trait::async_trait;
use data_types::{delete_predicate::DeletePredicate, DatabaseName};

use hashbrown::HashMap;
use mutable_batch::MutableBatch;
use mutable_batch_lp::PayloadStatistics;
use observability_deps::tracing::*;
use trace::ctx::SpanContext;

use super::{DmlError, DmlHandler};

/// A [`DmlHandler`] implementation that does nothing.
#[derive(Debug, Default)]
pub struct NopDmlHandler;

#[async_trait]
impl DmlHandler for NopDmlHandler {
    async fn write(
        &self,
        namespace: DatabaseName<'static>,
        batches: HashMap<String, MutableBatch>,
        _payload_stats: PayloadStatistics,
        _body_len: usize,
        _span_ctx: Option<SpanContext>,
    ) -> Result<(), DmlError> {
        info!(%namespace, ?batches, "dropping write operation");
        Ok(())
    }

    async fn delete<'a>(
        &self,
        namespace: DatabaseName<'static>,
        table: impl Into<String> + Send + Sync + 'a,
        predicate: DeletePredicate,
        _span_ctx: Option<SpanContext>,
    ) -> Result<(), DmlError> {
        let table = table.into();
        info!(%namespace, %table, ?predicate, "dropping delete operation");
        Ok(())
    }
}
