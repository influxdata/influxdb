use async_trait::async_trait;
use data_types::{DeletePredicate, NamespaceId, NamespaceName};
use trace::ctx::SpanContext;

use super::{DmlError, DmlHandler};

/// An extension trait to chain together the execution of a pair of
/// [`DmlHandler`] implementations.
pub trait DmlHandlerChainExt: DmlHandler + Sized {
    /// Chain `next` onto `self`, calling `next` if executing `self` DML
    /// handler is `Ok`.
    ///
    /// If `self` returns an error when executing the DML handler, `next` is not
    /// called and the error is returned.
    fn and_then<T>(self, next: T) -> Chain<Self, T>
    where
        T: DmlHandler,
    {
        Chain::new(self, next)
    }
}

impl<T> DmlHandlerChainExt for T where T: DmlHandler {}

/// A combinator type that calls `T` and if successful, calls `U` with the
/// (potentially transformed) DML operation.
///
/// This type is constructed by calling [`DmlHandlerChainExt::and_then()`] on a
/// [`DmlHandler`] implementation.
#[derive(Debug)]
pub struct Chain<T, U> {
    first: T,
    second: U,
}

impl<T, U> Chain<T, U> {
    fn new(first: T, second: U) -> Self {
        Self { first, second }
    }
}

#[async_trait]
impl<T, U> DmlHandler for Chain<T, U>
where
    T: DmlHandler,
    U: DmlHandler<WriteInput = T::WriteOutput>,
{
    // Take the first input type, and return the possibly transformed second
    // handler's output type.
    type WriteInput = T::WriteInput;
    type WriteOutput = U::WriteOutput;

    // All errors are converted into DML errors before returning to the caller
    // in order to present a consistent error type for chained handlers.
    type WriteError = DmlError;
    type DeleteError = DmlError;

    /// Write `batches` to `namespace`.
    async fn write(
        &self,
        namespace: &NamespaceName<'static>,
        namespace_id: NamespaceId,
        input: Self::WriteInput,
        span_ctx: Option<SpanContext>,
    ) -> Result<Self::WriteOutput, Self::WriteError> {
        let output = self
            .first
            .write(namespace, namespace_id, input, span_ctx.clone())
            .await
            .map_err(Into::into)?;

        self.second
            .write(namespace, namespace_id, output, span_ctx)
            .await
            .map_err(Into::into)
    }

    /// Delete the data specified in `delete`.
    async fn delete(
        &self,
        namespace: &NamespaceName<'static>,
        namespace_id: NamespaceId,
        table_name: &str,
        predicate: &DeletePredicate,
        span_ctx: Option<SpanContext>,
    ) -> Result<(), Self::DeleteError> {
        self.first
            .delete(
                namespace,
                namespace_id,
                table_name,
                predicate,
                span_ctx.clone(),
            )
            .await
            .map_err(Into::into)?;

        self.second
            .delete(namespace, namespace_id, table_name, predicate, span_ctx)
            .await
            .map_err(Into::into)
    }
}
