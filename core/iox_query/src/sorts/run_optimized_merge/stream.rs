//! The stream that drives the block merge and its fallback.

use std::{
    pin::Pin,
    sync::Arc,
    task::{Context, Poll},
};

use arrow::{array::RecordBatch, compute::BatchCoalescer, datatypes::SchemaRef};
use datafusion::{
    common::internal_err,
    error::{DataFusionError, Result},
    execution::{RecordBatchStream, SendableRecordBatchStream},
    physical_expr::LexOrdering,
    physical_plan::{
        metrics::{BaselineMetrics, ExecutionPlanMetricsSet},
        sorts::streaming_merge::StreamingMergeBuilder,
    },
};
use futures::{Stream, StreamExt, ready};

use super::{merge::BlockMerger, metrics::RunOptimizedMergeMetrics};

/// The stage the stream has reached.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum State {
    /// Producing output from the inputs.
    Merge,
    /// Flushing the last partial batch out of the coalescer.
    Drain,
    /// All output produced.
    Done,
}

/// The outcome of one step of the merge.
enum Step {
    /// A batch to return to the consumer.
    Yield(RecordBatch),
    /// Progress was made; look for more output.
    Continue,
    /// No more output will be produced.
    Finished,
}

pub(super) struct RunOptimizedMergeStream {
    schema: SchemaRef,

    /// Kept for the fallback, which needs to build its own comparators.
    expr: LexOrdering,

    baseline: BaselineMetrics,
    metrics: RunOptimizedMergeMetrics,

    coalescer: BatchCoalescer,
    batch_size: usize,

    /// The maximum number of rows to produce, if any.
    fetch: Option<usize>,

    /// The number of rows handed on so far, needed only to honour `fetch`.
    emitted: usize,

    /// The mean block length below which the merge hands over.
    degradation_threshold: usize,

    state: State,

    /// The block merge, taken when it hands over.
    merger: Option<BlockMerger>,

    /// DataFusion's row-at-a-time merge, once the block merge has given up.
    fallback: Option<SendableRecordBatchStream>,
}

impl RunOptimizedMergeStream {
    pub(super) fn new(
        merger: BlockMerger,
        schema: SchemaRef,
        expr: LexOrdering,
        baseline: BaselineMetrics,
        metrics: RunOptimizedMergeMetrics,
        batch_size: usize,
        fetch: Option<usize>,
        degradation_threshold: usize,
    ) -> Self {
        Self {
            coalescer: BatchCoalescer::new(Arc::clone(&schema), batch_size),
            schema,
            expr,
            baseline,
            metrics,
            batch_size,
            fetch,
            emitted: 0,
            degradation_threshold,
            state: State::Merge,
            merger: Some(merger),
            fallback: None,
        }
    }

    /// The number of rows still wanted, or `usize::MAX` when unlimited.
    fn remaining(&self) -> usize {
        self.fetch
            .map_or(usize::MAX, |fetch| fetch.saturating_sub(self.emitted))
    }

    /// Hand the unconsumed remainder of the inputs to DataFusion's
    /// row-at-a-time merge.
    ///
    /// The merge's reservation is released along with the merge itself; the
    /// fallback gets a fresh one on the same consumer and grows it as it reads,
    /// so the high-water mark is still one batch per input.
    fn fall_back(&mut self) -> Result<()> {
        let Some(merger) = self.merger.take() else {
            return internal_err!("run-optimized merge fell back outside of a block merge");
        };
        self.metrics.fell_back_to_streaming_merge.add(1);

        // Rows already in the coalescer precede everything the merge will
        // produce, so they have to be flushed before the switch.
        self.coalescer.finish_buffered_batch()?;

        let reservation = merger.new_empty_reservation();
        let streams = merger.into_streams(&self.schema);
        if streams.is_empty() {
            self.state = State::Drain;
            return Ok(());
        }
        let fetch = self.fetch.map(|fetch| fetch.saturating_sub(self.emitted));
        self.fallback = Some(
            StreamingMergeBuilder::new()
                .with_streams(streams)
                .with_schema(Arc::clone(&self.schema))
                .with_expressions(&self.expr)
                // Output rows are recorded by this stream, not the inner merge.
                .with_metrics(BaselineMetrics::new(&ExecutionPlanMetricsSet::new(), 0))
                .with_batch_size(self.batch_size)
                .with_fetch(fetch)
                .with_reservation(reservation)
                .build()?,
        );
        Ok(())
    }

    fn merge_step(&mut self, cx: &mut Context<'_>) -> Poll<Result<Step>> {
        if let Some(stream) = self.fallback.as_mut() {
            // The remainders handed to the fallback are in memory, so this
            // covers work rather than waiting.
            let compute = self.baseline.elapsed_compute().clone();
            let _compute = compute.timer();
            return match ready!(stream.poll_next_unpin(cx)) {
                Some(Ok(batch)) => {
                    self.emitted += batch.num_rows();
                    Poll::Ready(Ok(Step::Yield(batch)))
                }
                Some(Err(e)) => Poll::Ready(Err(e)),
                None => Poll::Ready(Ok(Step::Finished)),
            };
        }

        let remaining = self.remaining();
        let compute = self.baseline.elapsed_compute().clone();
        let Some(merger) = self.merger.as_mut() else {
            return Poll::Ready(Ok(Step::Finished));
        };
        // Refilling an input is the only part of a round that can block, and
        // the only part that is someone else's work, so the compute timer
        // starts after it.
        ready!(merger.poll_fill(cx))?;

        let _compute = compute.timer();

        let block = {
            let merge_time = self.metrics.merge_time.clone();
            let _timer = merge_time.timer();
            merger.next(remaining)?
        };
        let Some(block) = block else {
            return Poll::Ready(Ok(Step::Finished));
        };
        let degraded = merger.is_degraded(self.degradation_threshold);

        self.emitted += block.num_rows();
        {
            let _timer = self.metrics.coalesce_time.timer();
            self.coalescer.push_batch(block)?;
        }
        if degraded {
            self.fall_back()?;
        }
        Poll::Ready(Ok(Step::Continue))
    }

    fn output(&self, batch: RecordBatch) -> Poll<Option<Result<RecordBatch>>> {
        self.baseline.record_output(batch.num_rows());
        Poll::Ready(Some(Ok(batch)))
    }
}

impl Stream for RunOptimizedMergeStream {
    type Item = Result<RecordBatch>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        loop {
            if let Some(batch) = self.coalescer.next_completed_batch() {
                return self.output(batch);
            }

            match self.state {
                State::Merge => match ready!(self.merge_step(cx)) {
                    Ok(Step::Yield(batch)) => return self.output(batch),
                    Ok(Step::Continue) => {}
                    Ok(Step::Finished) => {
                        self.merger = None;
                        self.fallback = None;
                        self.state = State::Drain;
                    }
                    Err(e) => {
                        self.state = State::Done;
                        return Poll::Ready(Some(Err(e)));
                    }
                },
                State::Drain => {
                    self.state = State::Done;
                    if let Err(e) = self.coalescer.finish_buffered_batch() {
                        return Poll::Ready(Some(Err(DataFusionError::from(e))));
                    }
                }
                State::Done => {
                    self.merger = None;
                    self.fallback = None;
                    self.baseline.done();
                    return Poll::Ready(None);
                }
            }
        }
    }
}

impl RecordBatchStream for RunOptimizedMergeStream {
    fn schema(&self) -> SchemaRef {
        Arc::clone(&self.schema)
    }
}
