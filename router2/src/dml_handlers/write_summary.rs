use super::DmlHandler;
use async_trait::async_trait;
use data_types2::{DatabaseName, DeletePredicate};
use dml::DmlMeta;
use std::fmt::Debug;
use trace::ctx::SpanContext;
use write_summary::WriteSummary;

/// A [`WriteSummaryAdapter`] wraps DML Handler that produces
///  `Vec<Vec<DmlMeta>>` for each write, and produces a WriteSummary,
///  suitable for
/// sending back to a client
#[derive(Debug, Default)]
pub struct WriteSummaryAdapter<T> {
    inner: T,
}

impl<T> WriteSummaryAdapter<T> {
    /// Construct a [`WriteSummaryAdapter`] that passes DML operations to `inner`
    /// concurrently.
    pub fn new(inner: T) -> Self {
        Self { inner }
    }
}

#[async_trait]
impl<T> DmlHandler for WriteSummaryAdapter<T>
where
    T: DmlHandler<WriteOutput = Vec<Vec<DmlMeta>>>,
{
    type WriteInput = T::WriteInput;
    type WriteOutput = WriteSummary;
    type WriteError = T::WriteError;
    type DeleteError = T::DeleteError;

    /// Sends `input` to the inner handler, which returns a
    /// `Vec<Vec<DmlMeta>>`, creating a `WriteSummary`
    async fn write(
        &self,
        namespace: &DatabaseName<'static>,
        input: Self::WriteInput,
        span_ctx: Option<SpanContext>,
    ) -> Result<Self::WriteOutput, Self::WriteError> {
        let metas = self.inner.write(namespace, input, span_ctx).await?;
        Ok(WriteSummary::new(metas))
    }

    /// Pass the delete through to the inner handler.
    async fn delete(
        &self,
        namespace: &DatabaseName<'static>,
        table_name: &str,
        predicate: &DeletePredicate,
        span_ctx: Option<SpanContext>,
    ) -> Result<(), Self::DeleteError> {
        self.inner
            .delete(namespace, table_name, predicate, span_ctx)
            .await
    }
}
