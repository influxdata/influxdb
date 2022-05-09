//! A NOP implementation of [`DmlHandler`].

use super::{DmlError, DmlHandler};
use async_trait::async_trait;
use data_types::{DatabaseName, DeletePredicate};
use observability_deps::tracing::*;
use std::{fmt::Debug, marker::PhantomData};
use trace::ctx::SpanContext;

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
    type DeleteError = DmlError;
    type WriteInput = T;
    type WriteOutput = T;

    async fn write(
        &self,
        namespace: &DatabaseName<'static>,
        batches: Self::WriteInput,
        _span_ctx: Option<SpanContext>,
    ) -> Result<Self::WriteOutput, Self::WriteError> {
        info!(%namespace, ?batches, "dropping write operation");
        Ok(batches)
    }

    async fn delete(
        &self,
        namespace: &DatabaseName<'static>,
        table_name: &str,
        predicate: &DeletePredicate,
        _span_ctx: Option<SpanContext>,
    ) -> Result<(), Self::DeleteError> {
        info!(%namespace, %table_name, ?predicate, "dropping delete operation");
        Ok(())
    }
}
