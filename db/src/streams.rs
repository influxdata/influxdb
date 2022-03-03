//! Adapter streams for different Chunk types that implement the interface
//! needed by DataFusion
use arrow::{datatypes::SchemaRef, error::Result as ArrowResult, record_batch::RecordBatch};
use datafusion::physical_plan::RecordBatchStream;
use query::exec::IOxExecutionContext;
use read_buffer::ReadFilterResults;

use std::{
    sync::Arc,
    task::{Context, Poll},
};

/// Adapter which will take a ReadFilterResults and make it an async stream
pub struct ReadFilterResultsStream {
    read_results: ReadFilterResults,
    schema: SchemaRef,
    ctx: IOxExecutionContext,
}

impl ReadFilterResultsStream {
    pub fn new(
        ctx: IOxExecutionContext,
        read_results: ReadFilterResults,
        schema: SchemaRef,
    ) -> Self {
        Self {
            ctx,
            read_results,
            schema,
        }
    }
}

impl RecordBatchStream for ReadFilterResultsStream {
    fn schema(&self) -> SchemaRef {
        Arc::clone(&self.schema)
    }
}

impl futures::Stream for ReadFilterResultsStream {
    type Item = ArrowResult<RecordBatch>;

    fn poll_next(
        mut self: std::pin::Pin<&mut Self>,
        _: &mut Context<'_>,
    ) -> Poll<Option<Self::Item>> {
        let mut ctx = self.ctx.child_ctx("next_row_group");
        let rb = self.read_results.next();
        if let Some(rb) = &rb {
            ctx.set_metadata("output_rows", rb.num_rows() as i64);
        }

        Poll::Ready(Ok(rb).transpose())
    }

    // TODO is there a useful size_hint to pass?
}
