use std::{error::Error, fmt::Debug};

use async_trait::async_trait;
use data_types::{delete_predicate::DeletePredicate, DatabaseName};

use hashbrown::HashMap;
use mutable_batch::MutableBatch;
use thiserror::Error;
use trace::ctx::SpanContext;

use super::{NamespaceCreationError, SchemaError, ShardError};

/// Errors emitted by a [`DmlHandler`] implementation during DML request
/// processing.
#[derive(Debug, Error)]
pub enum DmlError {
    /// The database specified by the caller does not exist.
    #[error("database {0} does not exist")]
    DatabaseNotFound(String),

    /// An error sharding the writes and pushing them to the write buffer.
    #[error(transparent)]
    WriteBuffer(#[from] ShardError),

    /// A schema validation failure.
    #[error(transparent)]
    Schema(#[from] SchemaError),

    /// Failed to create the request namespace.
    #[error(transparent)]
    NamespaceCreation(#[from] NamespaceCreationError),

    /// An unknown error occured while processing the DML request.
    #[error("internal dml handler error: {0}")]
    Internal(Box<dyn Error + Send + Sync>),
}

/// A composable, abstract handler of DML requests.
#[async_trait]
pub trait DmlHandler: Debug + Send + Sync {
    /// The type of error a [`DmlHandler`] implementation produces for write
    /// requests.
    ///
    /// All errors must be mappable into the concrete [`DmlError`] type.
    type WriteError: Error + Into<DmlError> + Send;
    /// The error type of the delete handler.
    type DeleteError: Error + Into<DmlError> + Send;

    /// Write `batches` to `namespace`.
    async fn write(
        &self,
        namespace: DatabaseName<'static>,
        batches: HashMap<String, MutableBatch>,
        span_ctx: Option<SpanContext>,
    ) -> Result<(), Self::WriteError>;

    /// Delete the data specified in `delete`.
    async fn delete<'a>(
        &self,
        namespace: DatabaseName<'static>,
        table_name: impl Into<String> + Send + Sync + 'a,
        predicate: DeletePredicate,
        span_ctx: Option<SpanContext>,
    ) -> Result<(), Self::DeleteError>;
}
