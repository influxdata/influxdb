use std::{fmt::Debug, marker::PhantomData, sync::Arc};

use async_trait::async_trait;
use data_types::{NamespaceName, NamespaceSchema};
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
    type WriteOutput = ();
    type WriteError = T::WriteError;

    /// Concurrently execute the write inputs in `input` against the inner
    /// handler, returning early and aborting in-flight writes if an error
    /// occurs.
    async fn write(
        &self,
        namespace: &NamespaceName<'static>,
        namespace_schema: Arc<NamespaceSchema>,
        input: Self::WriteInput,
        span_ctx: Option<SpanContext>,
    ) -> Result<Self::WriteOutput, Self::WriteError> {
        input
            .into_iter()
            .map(|v| {
                let namespace = namespace.clone();
                let namespace_schema = Arc::clone(&namespace_schema);
                let span_ctx = span_ctx.clone();
                async move {
                    self.inner
                        .write(&namespace, namespace_schema, v, span_ctx)
                        .await
                }
            })
            .collect::<FuturesUnordered<_>>()
            .try_collect::<Vec<_>>()
            .await?;
        Ok(())
    }
}
