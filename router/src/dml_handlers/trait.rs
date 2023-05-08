use super::{
    partitioner::PartitionError, retention_validation::RetentionError, RpcWriteError, SchemaError,
};
use async_trait::async_trait;
use data_types::{NamespaceName, NamespaceSchema};
use std::{error::Error, fmt::Debug, sync::Arc};
use thiserror::Error;
use trace::ctx::SpanContext;

/// Errors emitted by a [`DmlHandler`] implementation during DML request
/// processing.
#[derive(Debug, Error)]
pub enum DmlError {
    /// The namespace specified by the caller does not exist.
    #[error("namespace {0} does not exist")]
    NamespaceNotFound(String),

    /// An error pushing the request to a downstream ingester via a direct RPC
    /// call.
    #[error(transparent)]
    RpcWrite(#[from] RpcWriteError),

    /// A schema validation failure.
    #[error(transparent)]
    Schema(#[from] SchemaError),

    /// An error partitioning the request.
    #[error(transparent)]
    Partition(#[from] PartitionError),

    /// An error validate retention period
    #[error(transparent)]
    Retention(#[from] RetentionError),

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

    /// Write `batches` to `namespace`.
    async fn write(
        &self,
        namespace: &NamespaceName<'static>,
        namespace_schema: Arc<NamespaceSchema>,
        input: Self::WriteInput,
        span_ctx: Option<SpanContext>,
    ) -> Result<Self::WriteOutput, Self::WriteError>;
}

#[async_trait]
impl<T> DmlHandler for Arc<T>
where
    T: DmlHandler,
{
    type WriteInput = T::WriteInput;
    type WriteOutput = T::WriteOutput;
    type WriteError = T::WriteError;

    async fn write(
        &self,
        namespace: &NamespaceName<'static>,
        namespace_schema: Arc<NamespaceSchema>,
        input: Self::WriteInput,
        span_ctx: Option<SpanContext>,
    ) -> Result<Self::WriteOutput, Self::WriteError> {
        (**self)
            .write(namespace, namespace_schema, input, span_ctx)
            .await
    }
}
