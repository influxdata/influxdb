//! A NOP implementation of [`DmlHandler`].

use std::{fmt::Debug, marker::PhantomData, sync::Arc};

use async_trait::async_trait;
use data_types::{NamespaceName, NamespaceSchema};
use observability_deps::tracing::*;
use trace::ctx::SpanContext;

use super::{DmlError, DmlHandler};

/// A [`DmlHandler`] implementation that does nothing.
#[derive(Debug)]
pub struct NopDmlHandler<T>(PhantomData<T>);

impl<T> Default for NopDmlHandler<T> {
    fn default() -> Self {
        Self(Default::default())
    }
}

#[async_trait]
impl<T> DmlHandler for NopDmlHandler<T>
where
    T: Debug + Send + Sync,
{
    type WriteError = DmlError;
    type WriteInput = T;
    type WriteOutput = T;

    async fn write(
        &self,
        namespace: &NamespaceName<'static>,
        namespace_schema: Arc<NamespaceSchema>,
        batches: Self::WriteInput,
        _span_ctx: Option<SpanContext>,
    ) -> Result<Self::WriteOutput, Self::WriteError> {
        info!(%namespace, %namespace_schema.id, ?batches, "dropping write operation");
        Ok(batches)
    }
}
