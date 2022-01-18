use std::fmt::Debug;

use async_trait::async_trait;
use data_types::{delete_predicate::DeletePredicate, DatabaseName};

use hashbrown::HashMap;
use mutable_batch::MutableBatch;
use mutable_batch_lp::PayloadStatistics;
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
        namespace: DatabaseName<'static>,
        batches: HashMap<String, MutableBatch>,
        payload_stats: PayloadStatistics,
        body_len: usize,
        span_ctx: Option<SpanContext>,
    ) -> Result<(), DmlError>;

    /// Delete the data specified in `delete`.
    async fn delete<'a>(
        &self,
        namespace: DatabaseName<'static>,
        table_name: impl Into<String> + Send + Sync + 'a,
        predicate: DeletePredicate,
        span_ctx: Option<SpanContext>,
    ) -> Result<(), DmlError>;
}
