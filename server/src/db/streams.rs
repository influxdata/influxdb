//! Adapter streams for different Chunk types that implement the interface
//! needed by DataFusion
use arrow_deps::{
    arrow::{
        datatypes::SchemaRef,
        error::{ArrowError, Result as ArrowResult},
        record_batch::RecordBatch,
    },
    datafusion::physical_plan::RecordBatchStream,
};
use internal_types::selection::Selection;
use mutable_buffer::chunk::Chunk as MBChunk;
use read_buffer::ReadFilterResults;

use std::{
    sync::Arc,
    task::{Context, Poll},
};

use snafu::{ResultExt, Snafu};

#[derive(Debug, Snafu)]
pub enum Error {
    #[snafu(display(
        "Error getting data for table '{}' chunk {}: {}",
        table_name,
        chunk_id,
        source
    ))]
    GettingTableData {
        table_name: String,
        chunk_id: u32,
        source: mutable_buffer::chunk::Error,
    },
}

/// Adapter which will produce record batches from a mutable buffer
/// chunk on demand
pub(crate) struct MutableBufferChunkStream {
    /// Requested output schema (includes selection)
    schema: SchemaRef,
    chunk: Arc<MBChunk>,
    table_name: Arc<String>,

    /// Vector of record batches to send in reverse order (send data[len-1]
    /// next) Is None until the first call to poll_next
    data: Option<Vec<RecordBatch>>,
}

impl MutableBufferChunkStream {
    pub fn new(chunk: Arc<MBChunk>, schema: SchemaRef, table_name: impl Into<String>) -> Self {
        Self {
            chunk,
            schema,
            table_name: Arc::new(table_name.into()),
            data: None,
        }
    }

    // gets the next batch, as needed
    fn next_batch(&mut self) -> ArrowResult<Option<RecordBatch>> {
        if self.data.is_none() {
            // Want all the columns in the schema. Note we don't
            // use `Selection::All` here because the mutable buffer chunk would interpret it
            // as "all columns in the table in that chunk" rather than
            // all columns this query needs
            let selected_cols = self
                .schema
                .fields()
                .iter()
                .map(|f| f.name() as &str)
                .collect::<Vec<_>>();
            let selection = Selection::Some(&selected_cols);

            let mut data = Vec::new();
            self.chunk
                .table_to_arrow(&mut data, self.table_name.as_ref(), selection)
                .context(GettingTableData {
                    table_name: self.table_name.as_ref(),
                    chunk_id: self.chunk.id(),
                })
                .map_err(|e| ArrowError::ExternalError(Box::new(e)))?;

            // reverse the array so we can pop off the back
            data.reverse();
            self.data = Some(data);
        }

        // self.data was set to Some above
        Ok(self.data.as_mut().unwrap().pop())
    }
}

impl RecordBatchStream for MutableBufferChunkStream {
    fn schema(&self) -> SchemaRef {
        Arc::clone(&self.schema)
    }
}

impl futures::Stream for MutableBufferChunkStream {
    type Item = ArrowResult<RecordBatch>;

    fn poll_next(
        mut self: std::pin::Pin<&mut Self>,
        _: &mut Context<'_>,
    ) -> Poll<Option<Self::Item>> {
        Poll::Ready(self.next_batch().transpose())
    }

    // TODO is there a useful size_hint to pass?
}

/// Adapter which will take a ReadFilterResults and make it an async stream
pub struct ReadFilterResultsStream {
    read_results: ReadFilterResults,
    schema: SchemaRef,
}

impl ReadFilterResultsStream {
    pub fn new(read_results: ReadFilterResults, schema: SchemaRef) -> Self {
        Self {
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
        Poll::Ready(Ok(self.read_results.next()).transpose())
    }

    // TODO is there a useful size_hint to pass?
}
