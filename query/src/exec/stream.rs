//! Common utilities for implement SendableRecordBatchStreams

use std::sync::Arc;

use datafusion::physical_plan::{RecordBatchStream, SendableRecordBatchStream};
use futures::{Stream, StreamExt};
use tokio::sync::mpsc::Receiver;
use tokio_stream::wrappers::ReceiverStream;

use arrow::{datatypes::SchemaRef, error::Result as ArrowResult, record_batch::RecordBatch};

#[derive(Debug)]
/// Implements a [`SendableRecordBatchStream`] to help create DataFusion outputs from tokio channels.
///
/// It sends streams of RecordBatches from a tokio channel *and*
/// crucially knows up front the schema each batch will have be used.
pub struct AdapterStream {
    /// Schema
    schema: SchemaRef,
    /// channel for getting deduplicated batches
    inner: ReceiverStream<ArrowResult<RecordBatch>>,
}

impl AdapterStream {
    /// Create a new stream which wraps the `inner` channel which
    /// produces [`RecordBatch`]es that each have the specified schama
    ///
    /// Not called `new` because it returns a pinned reference rather than the object itself
    pub fn adapt(
        schema: SchemaRef,
        rx: Receiver<ArrowResult<RecordBatch>>,
    ) -> SendableRecordBatchStream {
        let inner = ReceiverStream::new(rx);
        Box::pin(Self { schema, inner })
    }
}

impl Stream for AdapterStream {
    type Item = ArrowResult<RecordBatch>;

    fn poll_next(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Option<Self::Item>> {
        self.inner.poll_next_unpin(cx)
    }
}

impl RecordBatchStream for AdapterStream {
    fn schema(&self) -> SchemaRef {
        Arc::clone(&self.schema)
    }
}
