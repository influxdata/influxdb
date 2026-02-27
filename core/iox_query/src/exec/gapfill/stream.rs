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
use datafusion::config::ConfigOptions;
use datafusion::{
    error::{DataFusionError, Result},
    execution::memory_pool::MemoryReservation,
    physical_expr::ScalarFunctionExpr,
    physical_plan::{
        ExecutionPlan, PhysicalExpr, RecordBatchStream, SendableRecordBatchStream,
        expressions::Column,
        metrics::{BaselineMetrics, RecordOutput},
    },
};
use futures::{Stream, StreamExt, ready};

use super::{GapFillExec, algo::GapFiller, buffered_input::BufferedInput, params::GapFillParams};

/// An implementation of a gap-filling operator that uses the [Stream] trait.
///
/// This type takes responsibility for:
/// - Reading input record batches
/// - Accounting for memory
/// - Extracting arrays for processing by [`GapFiller`]
/// - Recording metrics
/// - Sending record batches to next operator (by implementing [`Self::poll_next`])
pub(super) struct GapFillStream {
    /// The schema of the input and output.
    schema: SchemaRef,
    /// The columns that define the time series that a value belongs to.
    series_expr: Vec<Arc<dyn PhysicalExpr>>,
    /// The column from the input that contains the timestamps for each row.
    /// This column has already had `date_bin` applied to it by a previous `Aggregate`
    /// operator.
    time_expr: Arc<dyn PhysicalExpr>,
    /// The aggregate columns from the select list of the original query.
    fill_expr: Vec<Arc<dyn PhysicalExpr>>,
    /// The producer of the input record batches.
    input: SendableRecordBatchStream,
    /// Input that has been read from the input stream.
    buffered_input: BufferedInput,
    /// The thing that does the gap filling.
    gap_filler: GapFiller,
    /// This is true as long as there are more input record batches to read from `input`.
    more_input: bool,
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
        config_options: Arc<ConfigOptions>,
    ) -> Result<Self> {
        let schema = exec.schema();
        let GapFillExec {
            series_expr,
            time_expr,
            fill_expr,
            time_range,
            ..
        } = exec;

        let series_cols = series_expr.iter().map(expr_to_index).collect::<Vec<_>>();
        let params = GapFillParams::try_new(
            Arc::clone(&schema),
            time_expr,
            fill_expr,
            time_range,
            config_options,
        )?;
        let buffered_input = BufferedInput::new(&params, series_cols, reservation);

        let time_expr = if let Some(func) = time_expr.as_any().downcast_ref::<ScalarFunctionExpr>()
        {
            // The time_expr has already been determined to be a
            // date_bin call. Thie input time column is the second
            // argument.
            Arc::clone(&func.args()[1])
        } else {
            return Err(DataFusionError::Internal(
                "time_expr must be a ScalarFunctionExpr".to_string(),
            ));
        };

        let fill_expr = fill_expr
            .iter()
            .map(|pfe| Arc::clone(&pfe.expr))
            .collect::<Vec<_>>();

        let gap_filler = GapFiller::new(params, batch_size);
        Ok(Self {
            schema,
            series_expr: series_expr.clone(),
            time_expr,
            fill_expr,
            input,
            buffered_input,
            gap_filler,
            more_input: true,
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
                    self.buffered_input.try_push(batch)?;
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
                // If there are no more output rows to produce for the current buffered input
                if self.gap_filler.done(input_batch.num_rows()) {
                    return Poll::Ready(None);
                }
                input_batch
            }
            Err(e) => return Poll::Ready(Some(Err(e))),
        };

        match self.process(input_batch) {
            Ok((output_batch, remaining_input_batch)) => {
                self.buffered_input.try_push(remaining_input_batch)?;
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

        let mut batch = arrow::compute::concat_batches(&batches[0].schema(), &batches)
            .map_err(|err| DataFusionError::ArrowError(Box::new(err), None))?;

        if batches.len() > 1 {
            // Optimize the dictionaries. The output of this operator uses the take kernel to produce
            // its output. Since the input batches will usually be smaller than the output, it should
            // be less work to optimize here vs optimizing the output.
            batch = optimize_dictionaries(&batch)
                .map_err(|err| DataFusionError::ArrowError(Box::new(err), None))?;
        }

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
            .into_array(input_batch.num_rows())?;
        let input_time_array: &TimestampNanosecondArray = input_time_array
            .as_any()
            .downcast_ref()
            .ok_or(DataFusionError::Internal(
                "time array must be a TimestampNanosecondArray".to_string(),
            ))?;

        let series_arrays = self.series_arrays(&input_batch)?;
        let fill_arrays = self.fill_arrays(&input_batch)?;

        let timer = elapsed_compute.timer();
        let output_batch = self
            .gap_filler
            .build_gapfilled_output(
                Arc::clone(&self.schema),
                input_time_array,
                &series_arrays,
                &fill_arrays,
            )
            .record_output(&self.baseline_metrics)?;
        timer.done();

        // Slice the input to just what is needed moving forward, with one context
        // row before the next input offset.
        input_batch = self.gap_filler.slice_input_batch(input_batch)?;

        Ok((output_batch, input_batch))
    }

    /// Produces the arrays for the group columns in the input.
    /// The first item in the 2-tuple is the arrays offset in the schema.
    fn series_arrays(&self, input_batch: &RecordBatch) -> Result<Vec<ArrayRef>> {
        self.series_expr
            .iter()
            .map(|e| e.evaluate(input_batch)?.into_array(input_batch.num_rows()))
            .collect::<Result<Vec<_>>>()
    }

    /// Produces the arrays for the aggregate columns in the input.
    /// The first item in the 2-tuple is the arrays offset in the schema.
    fn fill_arrays(&self, input_batch: &RecordBatch) -> Result<Vec<(usize, ArrayRef)>> {
        self.fill_expr
            .iter()
            .map(|e| {
                Ok((
                    expr_to_index(e),
                    e.evaluate(input_batch)?
                        .into_array(input_batch.num_rows())?,
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
