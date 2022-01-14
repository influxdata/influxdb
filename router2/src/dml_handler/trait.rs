use std::fmt::Debug;

use async_trait::async_trait;
use data_types::DatabaseName;

use hashbrown::HashMap;
use mutable_batch::MutableBatch;
use mutable_batch_lp::PayloadStatistics;
use predicate::delete_predicate::HttpDeleteRequest;
use thiserror::Error;
use trace::ctx::SpanContext;

/// Errors emitted by a [`DmlHandler`] implementation during DML request
/// processing.
#[derive(Debug, Error)]
pub enum DmlError {
    /// The database specified by the caller does not exist.
    #[error("database {0} does not exist")]
    DatabaseNotFound(String),

    /// An unknown error occured while processing the DML request.
    #[error("internal dml handler error: {0}")]
    Internal(Box<dyn std::error::Error + Send + Sync>),
}

/// An abstract handler of DML requests.
#[async_trait]
pub trait DmlHandler: Debug + Send + Sync {
    /// Write `batches` to `namespace`.
    async fn write(
        &self,
        namespace: DatabaseName<'_>,
        batches: HashMap<String, MutableBatch>,
        payload_stats: PayloadStatistics,
        body_len: usize,
        span_ctx: Option<SpanContext>,
    ) -> Result<(), DmlError>;

    /// Delete the data specified in `delete`.
    async fn delete(
        &self,
        delete: HttpDeleteRequest,
        span_ctx: Option<SpanContext>,
    ) -> Result<(), DmlError>;
}
