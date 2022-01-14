//! A NOP implementation of [`DmlHandler`].

use async_trait::async_trait;
use dml::DmlOperation;
use mutable_batch_lp::PayloadStatistics;
use observability_deps::tracing::*;

use super::{DmlError, DmlHandler};

/// A [`DmlHandler`] implementation that does nothing.
#[derive(Debug, Default)]
pub struct NopDmlHandler;

#[async_trait]
impl DmlHandler for NopDmlHandler {
    async fn dispatch<'a>(
        &'a self,
        db_name: impl Into<String> + Send + Sync + 'a,
        op: impl Into<DmlOperation> + Send + Sync + 'a,
        _payload_stats: PayloadStatistics,
        _body_len: usize,
    ) -> Result<(), DmlError> {
        let db_name = db_name.into();
        let op = op.into();
        info!(%db_name, ?op, "dropping dml operation");
        Ok(())
    }
}
