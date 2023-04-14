//! Implementation of [Stream] that performs gap-filling on tables.
use std::{
    pin::Pin,
    sync::Arc,
    task::{Context, Poll},
};

use arrow::{
    array::{ArrayRef, TimestampNanosecondArray},
    datatypes::SchemaRef,
    record_batch::RecordBatch,
};
use arrow_util::optimize::optimize_dictionaries;
use datafusion::{
    error::{DataFusionError, Result},
    execution::memory_pool::MemoryReservation,
    physical_plan::{
        expressions::Column,
        metrics::{BaselineMetrics, RecordOutput},
        ExecutionPlan, PhysicalExpr, RecordBatchStream, SendableRecordBatchStream,
    },
};
use futures::{ready, Stream, StreamExt};

use super::{algo::GapFiller, buffered_input::BufferedInput, params::GapFillParams, GapFillExec};

/// An implementation of a gap-filling operator that uses the [Stream] trait.
///
/// This type takes responsibility for:
/// - Reading input record batches
/// - Accounting for memory
/// - Extracting arrays for processing by [`GapFiller`]
/// - Recording metrics
/// - Sending record batches to next operator (by implementing [`Self::poll_next'])
#[allow(dead_code)]
pub(super) struct GapFillStream {
    /// The schema of the input and output.
    schema: SchemaRef,
    /// The column from the input that contains the timestamps for each row.
    /// This column has already had `date_bin` applied to it by a previous `Aggregate`
    /// operator.
    time_expr: Arc<dyn PhysicalExpr>,
    /// The other columns from the input that appeared in the GROUP BY clause of the
    /// original query.
    group_expr: Vec<Arc<dyn PhysicalExpr>>,
    /// The aggregate columns from the select list of the original query.
    aggr_expr: Vec<Arc<dyn PhysicalExpr>>,
    /// The producer of the input record batches.
    input: SendableRecordBatchStream,
    /// Input that has been read from the iput stream.
    buffered_input: BufferedInput,
    /// The thing that does the gap filling.
    gap_filler: GapFiller,
    /// This is true as long as there are more input record batches to read from `input`.
    more_input: bool,
    /// For tracking memory.
    reservation: MemoryReservation,
    /// Baseline metrics.
    baseline_metrics: BaselineMetrics,
}

impl GapFillStream {
    /// Creates a new GapFillStream.
    pub fn try_new(
        exec: &GapFillExec,
        batch_size: usize,
        input: SendableRecordBatchStream,
        reservation: MemoryReservation,
        metrics: BaselineMetrics,
    ) -> Result<Self> {
        let schema = exec.schema();
        let GapFillExec {
            sort_expr,
            aggr_expr,
            params,
            ..
        } = exec;

        if sort_expr.is_empty() {
            return Err(DataFusionError::Internal(
                "empty sort_expr vector for gap filling; should have at least a time expression"
                    .to_string(),
            ));
        }
        let mut group_expr = sort_expr
            .iter()
            .map(|se| Arc::clone(&se.expr))
            .collect::<Vec<_>>();
        let aggr_expr = aggr_expr.to_owned();
        let time_expr = group_expr.split_off(group_expr.len() - 1).pop().unwrap();

        let group_cols = group_expr.iter().map(expr_to_index).collect::<Vec<_>>();
        let params = GapFillParams::try_new(Arc::clone(&schema), params)?;
        let buffered_input = BufferedInput::new(&params, group_cols);

        let gap_filler = GapFiller::new(params, batch_size);
        Ok(Self {
            schema,
            time_expr,
            group_expr,
            aggr_expr,
            input,
            buffered_input,
            gap_filler,
            more_input: true,
            reservation,
            baseline_metrics: metrics,
        })
    }
}

impl RecordBatchStream for GapFillStream {
    fn schema(&self) -> arrow::datatypes::SchemaRef {
        Arc::clone(&self.schema)
    }
}

impl Stream for GapFillStream {
    type Item = Result<RecordBatch>;

    /// Produces a gap-filled record batch from its input stream.
    ///
    /// For details on implementation, see [`GapFiller`].
    fn poll_next(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<Option<Result<RecordBatch>>> {
        let last_output_row_offset = self.gap_filler.last_output_row_offset();
        while self.more_input && self.buffered_input.need_more(last_output_row_offset)? {
            match ready!(self.input.poll_next_unpin(cx)) {
                Some(Ok(batch)) => {
                    self.reservation.try_grow(batch.get_array_memory_size())?;
                    self.buffered_input.push(batch);
                }
                Some(Err(e)) => {
                    return Poll::Ready(Some(Err(e)));
                }
                None => {
                    self.more_input = false;
                }
            }
        }

        let input_batch = match self.take_buffered_input() {
            Ok(None) => return Poll::Ready(None),
            Ok(Some(input_batch)) => {
                // If we have consumed all of our input, and there is no more work
                if self.gap_filler.done(input_batch.num_rows()) {
                    // leave the input batch taken so that its reference
                    // count goes to zero.
                    self.reservation.shrink(input_batch.get_array_memory_size());
                    return Poll::Ready(None);
                }

                input_batch
            }
            Err(e) => return Poll::Ready(Some(Err(e))),
        };

        match self.process(input_batch) {
            Ok((output_batch, remaining_input_batch)) => {
                self.buffered_input.push(remaining_input_batch);

                self.reservation
                    .shrink(output_batch.get_array_memory_size());
                Poll::Ready(Some(Ok(output_batch)))
            }
            Err(e) => Poll::Ready(Some(Err(e))),
        }
    }
}

impl GapFillStream {
    /// If any buffered input batches are present, concatenates it all together
    /// and returns an owned batch to the caller, leaving `self.buffered_input_batches` empty.
    fn take_buffered_input(&mut self) -> Result<Option<RecordBatch>> {
        let batches = self.buffered_input.take();
        if batches.is_empty() {
            return Ok(None);
        }

        let old_size = batches.iter().map(|rb| rb.get_array_memory_size()).sum();

        let mut batch = arrow::compute::concat_batches(&self.schema, &batches)
            .map_err(DataFusionError::ArrowError)?;
        self.reservation.try_grow(batch.get_array_memory_size())?;

        if batches.len() > 1 {
            // Optimize the dictionaries. The output of this operator uses the take kernel to produce
            // its output. Since the input batches will usually be smaller than the output, it should
            // be less work to optimize here vs optimizing the output.
            batch = optimize_dictionaries(&batch).map_err(DataFusionError::ArrowError)?;
        }

        self.reservation.shrink(old_size);
        Ok(Some(batch))
    }

    /// Produces a 2-tuple of [RecordBatch]es:
    /// - The gap-filled output
    /// - Remaining buffered input
    fn process(&mut self, mut input_batch: RecordBatch) -> Result<(RecordBatch, RecordBatch)> {
        let elapsed_compute = self.baseline_metrics.elapsed_compute().clone();

        let input_time_array = self
            .time_expr
            .evaluate(&input_batch)?
            .into_array(input_batch.num_rows());
        let input_time_array: &TimestampNanosecondArray = input_time_array
            .as_any()
            .downcast_ref()
            .ok_or(DataFusionError::Internal(
                "time array must be a TimestampNanosecondArray".to_string(),
            ))?;
        let input_time_array = (expr_to_index(&self.time_expr), input_time_array);

        let group_arrays = self.group_arrays(&input_batch)?;
        let aggr_arrays = self.aggr_arrays(&input_batch)?;

        let timer = elapsed_compute.timer();
        let output_batch = self
            .gap_filler
            .build_gapfilled_output(
                Arc::clone(&self.schema),
                input_time_array,
                &group_arrays,
                &aggr_arrays,
            )
            .record_output(&self.baseline_metrics)?;
        timer.done();

        self.reservation
            .try_grow(output_batch.get_array_memory_size())?;

        // Slice the input to just what is needed moving forward, with one context
        // row before the next input offset.
        input_batch = self.gap_filler.slice_input_batch(input_batch)?;

        Ok((output_batch, input_batch))
    }

    /// Produces the arrays for the group columns in the input.
    /// The first item in the 2-tuple is the arrays offset in the schema.
    fn group_arrays(&self, input_batch: &RecordBatch) -> Result<Vec<(usize, ArrayRef)>> {
        self.group_expr
            .iter()
            .map(|e| {
                Ok((
                    expr_to_index(e),
                    e.evaluate(input_batch)?.into_array(input_batch.num_rows()),
                ))
            })
            .collect::<Result<Vec<_>>>()
    }

    /// Produces the arrays for the aggregate columns in the input.
    /// The first item in the 2-tuple is the arrays offset in the schema.
    fn aggr_arrays(&self, input_batch: &RecordBatch) -> Result<Vec<(usize, ArrayRef)>> {
        self.aggr_expr
            .iter()
            .map(|e| {
                Ok((
                    expr_to_index(e),
                    e.evaluate(input_batch)?.into_array(input_batch.num_rows()),
                ))
            })
            .collect::<Result<Vec<_>>>()
    }
}

/// Returns the index of the given expression in the schema,
/// assuming that it is a column.
///
/// # Panic
/// Panics if the expression is not a column.
fn expr_to_index(expr: &Arc<dyn PhysicalExpr>) -> usize {
    expr.as_any()
        .downcast_ref::<Column>()
        .expect("all exprs should be columns")
        .index()
}
