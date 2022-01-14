//! A NOP implementation of [`DmlHandler`].

use async_trait::async_trait;
use data_types::DatabaseName;

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
    async fn write<'a>(
        &'a self,
        db_name: DatabaseName<'_>,
        batches: HashMap<String, MutableBatch>,
        _payload_stats: PayloadStatistics,
        _body_len: usize,
        _span_ctx: Option<SpanContext>,
    ) -> Result<(), DmlError> {
        info!(%db_name, ?batches, "dropping write operation");
        Ok(())
    }
}
