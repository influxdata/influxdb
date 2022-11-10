use std::{fmt::Debug, marker::PhantomData};

use async_trait::async_trait;
use data_types::{DeletePredicate, NamespaceId, NamespaceName};
use futures::{stream::FuturesUnordered, TryStreamExt};
use trace::ctx::SpanContext;

use super::DmlHandler;

/// A [`FanOutAdaptor`] takes an iterator of DML write operation inputs and
/// executes them concurrently against the inner handler, returning once all
/// operations are complete.
///
/// If handling an operation produces an error the remaining in-flight writes
/// are aborted and the error is immediately returned.
///
/// Deletes are passed through to the inner handler unmodified.
#[derive(Debug, Default)]
pub struct FanOutAdaptor<T, I> {
    inner: T,
    _iter: PhantomData<I>,
}

impl<T, I> FanOutAdaptor<T, I> {
    /// Construct a [`FanOutAdaptor`] that passes DML operations to `inner`
    /// concurrently.
    pub fn new(inner: T) -> Self {
        Self {
            inner,
            _iter: Default::default(),
        }
    }
}

#[async_trait]
impl<T, I, U> DmlHandler for FanOutAdaptor<T, I>
where
    T: DmlHandler,
    I: IntoIterator<IntoIter = U> + Debug + Send + Sync,
    U: Iterator<Item = T::WriteInput> + Send + Sync,
{
    type WriteInput = I;
    type WriteOutput = Vec<T::WriteOutput>;
    type WriteError = T::WriteError;
    type DeleteError = T::DeleteError;

    /// Concurrently execute the write inputs in `input` against the inner
    /// handler, returning early and aborting in-flight writes if an error
    /// occurs.
    async fn write(
        &self,
        namespace: &NamespaceName<'static>,
        namespace_id: NamespaceId,
        input: Self::WriteInput,
        span_ctx: Option<SpanContext>,
    ) -> Result<Self::WriteOutput, Self::WriteError> {
        let results = input
            .into_iter()
            .map(|v| {
                let namespace = namespace.clone();
                let span_ctx = span_ctx.clone();
                async move {
                    self.inner
                        .write(&namespace, namespace_id, v, span_ctx)
                        .await
                }
            })
            .collect::<FuturesUnordered<_>>()
            .try_collect::<Vec<_>>()
            .await?;
        Ok(results)
    }

    /// Pass the delete through to the inner handler.
    async fn delete(
        &self,
        namespace: &NamespaceName<'static>,
        namespace_id: NamespaceId,
        table_name: &str,
        predicate: &DeletePredicate,
        span_ctx: Option<SpanContext>,
    ) -> Result<(), Self::DeleteError> {
        self.inner
            .delete(namespace, namespace_id, table_name, predicate, span_ctx)
            .await
    }
}
