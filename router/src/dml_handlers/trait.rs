use std::{error::Error, fmt::Debug, sync::Arc};

use async_trait::async_trait;
use data_types::{DatabaseName, DeletePredicate, NamespaceId};
use thiserror::Error;
use trace::ctx::SpanContext;

use super::{partitioner::PartitionError, SchemaError, ShardError};

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

    /// An error partitioning the request.
    #[error(transparent)]
    Partition(#[from] PartitionError),

    /// An unknown error occured while processing the DML request.
    #[error("internal dml handler error: {0}")]
    Internal(Box<dyn Error + Send + Sync>),
}

/// A composable, abstract handler of DML requests.
#[async_trait]
pub trait DmlHandler: Debug + Send + Sync {
    /// The input type this handler expects for a DML write.
    ///
    /// By allowing handlers to vary their input type, it is possible to
    /// construct a chain of [`DmlHandler`] implementations that transform the
    /// input request as it progresses through the handler pipeline.
    type WriteInput: Debug + Send + Sync;

    /// The (possibly transformed) output type returned by this handler after
    /// processing a write.
    type WriteOutput: Debug + Send + Sync;

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
        namespace: &DatabaseName<'static>,
        namespace_id: NamespaceId,
        input: Self::WriteInput,
        span_ctx: Option<SpanContext>,
    ) -> Result<Self::WriteOutput, Self::WriteError>;

    /// Delete the data specified in `delete`.
    async fn delete(
        &self,
        namespace: &DatabaseName<'static>,
        namespace_id: NamespaceId,
        table_name: &str,
        predicate: &DeletePredicate,
        span_ctx: Option<SpanContext>,
    ) -> Result<(), Self::DeleteError>;
}

#[async_trait]
impl<T> DmlHandler for Arc<T>
where
    T: DmlHandler,
{
    type WriteInput = T::WriteInput;
    type WriteOutput = T::WriteOutput;
    type WriteError = T::WriteError;
    type DeleteError = T::DeleteError;

    async fn write(
        &self,
        namespace: &DatabaseName<'static>,
        namespace_id: NamespaceId,
        input: Self::WriteInput,
        span_ctx: Option<SpanContext>,
    ) -> Result<Self::WriteOutput, Self::WriteError> {
        (**self)
            .write(namespace, namespace_id, input, span_ctx)
            .await
    }

    /// Delete the data specified in `delete`.
    async fn delete(
        &self,
        namespace: &DatabaseName<'static>,
        namespace_id: NamespaceId,
        table_name: &str,
        predicate: &DeletePredicate,
        span_ctx: Option<SpanContext>,
    ) -> Result<(), Self::DeleteError> {
        (**self)
            .delete(namespace, namespace_id, table_name, predicate, span_ctx)
            .await
    }
}
