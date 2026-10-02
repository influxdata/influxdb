//! One sorted input stream, with the batch the merge has reached within it.

use std::{
    sync::Arc,
    task::{Context, Poll},
};

use arrow::{
    array::{ArrayRef, RecordBatch},
    datatypes::SchemaRef,
};
use datafusion::{
    common::internal_err, error::Result, execution::SendableRecordBatchStream,
    physical_expr::LexOrdering, physical_plan::stream::RecordBatchStreamAdapter,
};
use futures::{StreamExt, stream};

use super::sort_key::evaluate_sort_keys;

/// The batch an input has reached, together with its evaluated sort key.
struct HeadBatch {
    batch: RecordBatch,

    /// One array per sort expression, in row order.
    keys: Vec<ArrayRef>,

    /// The number of bytes charged to the merge's reservation for this batch.
    bytes: usize,
}

/// A sorted stream, buffered one batch at a time.
///
/// The merge needs the next row of every input before it can play a round, so
/// exactly one batch per input is held. Rows are consumed from the front of
/// that batch; when it runs out the input is refilled from the stream, which is
/// the only point at which the merge can return [`Poll::Pending`].
pub(super) struct MergeInput {
    /// The stream, dropped once it has ended.
    stream: Option<SendableRecordBatchStream>,

    /// The buffered batch, absent before the first fill and after the input is
    /// exhausted.
    head: Option<HeadBatch>,

    /// The row of the buffered batch the merge has reached.
    offset: usize,
}

impl MergeInput {
    pub(super) fn new(stream: SendableRecordBatchStream) -> Self {
        Self {
            stream: Some(stream),
            head: None,
            offset: 0,
        }
    }

    /// True when the input has no buffered batch but its stream may yet
    /// produce one.
    pub(super) fn needs_batch(&self) -> bool {
        self.head.is_none() && self.stream.is_some()
    }

    /// True when every row of the input has been consumed.
    pub(super) fn is_exhausted(&self) -> bool {
        self.head.is_none() && self.stream.is_none()
    }

    /// Buffer the next non-empty batch, returning the bytes it occupies.
    ///
    /// An exhausted input returns zero, so the caller can treat a ready poll as
    /// "this input is now either loaded or finished".
    pub(super) fn poll_fill(
        &mut self,
        cx: &mut Context<'_>,
        expr: &LexOrdering,
    ) -> Poll<Result<usize>> {
        loop {
            let Some(stream) = self.stream.as_mut() else {
                return Poll::Ready(Ok(0));
            };
            match futures::ready!(stream.poll_next_unpin(cx)) {
                Some(Ok(batch)) => {
                    if batch.num_rows() == 0 {
                        // An empty batch has no next row to play with.
                        continue;
                    }
                    let keys = evaluate_sort_keys(expr, &batch)?;
                    let bytes = batch.get_array_memory_size() + keys.extra_bytes;
                    self.offset = 0;
                    self.head = Some(HeadBatch {
                        batch,
                        keys: keys.arrays,
                        bytes,
                    });
                    return Poll::Ready(Ok(bytes));
                }
                Some(Err(e)) => return Poll::Ready(Err(e)),
                None => {
                    self.stream = None;
                    return Poll::Ready(Ok(0));
                }
            }
        }
    }

    /// The sort-key arrays of the buffered batch, with the row the input has
    /// reached within them.
    pub(super) fn head(&self) -> Option<(&[ArrayRef], usize)> {
        self.head.as_ref().map(|h| (h.keys.as_slice(), self.offset))
    }

    /// The number of unconsumed rows in the buffered batch.
    pub(super) fn block_len(&self) -> usize {
        self.head
            .as_ref()
            .map_or(0, |h| h.batch.num_rows() - self.offset)
    }

    /// Detach the next `len` rows, returning them along with the number of
    /// bytes released if that consumed the buffered batch.
    ///
    /// The returned batch is a zero-copy slice, so the bytes are not actually
    /// released until whoever receives it lets go.
    pub(super) fn take_block(&mut self, len: usize) -> Result<(RecordBatch, usize)> {
        let Some(head) = self.head.as_ref() else {
            return internal_err!("run-optimized merge took a block from an exhausted input");
        };
        let block = head.batch.slice(self.offset, len);
        self.offset += len;
        if self.offset < head.batch.num_rows() {
            return Ok((block, 0));
        }
        let bytes = head.bytes;
        self.head = None;
        self.offset = 0;
        Ok((block, bytes))
    }

    /// Turn what is left of the input back into a stream, or `None` when it is
    /// exhausted.
    ///
    /// The buffered batch is put back in front of the rest of the stream, so
    /// the result is the exact remainder of the input in its original order.
    pub(super) fn into_stream(self, schema: &SchemaRef) -> Option<SendableRecordBatchStream> {
        let offset = self.offset;
        let remainder = self
            .head
            .map(|h| h.batch.slice(offset, h.batch.num_rows() - offset));
        match (remainder, self.stream) {
            (None, stream) => stream,
            (Some(batch), None) => Some(Box::pin(RecordBatchStreamAdapter::new(
                Arc::clone(schema),
                stream::iter([Ok(batch)]),
            ))),
            (Some(batch), Some(rest)) => Some(Box::pin(RecordBatchStreamAdapter::new(
                Arc::clone(schema),
                stream::iter([Ok(batch)]).chain(rest),
            ))),
        }
    }
}
