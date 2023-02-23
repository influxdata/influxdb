//! Implementation of [Stream] that performs gap-filling on tables.
use std::{
    ops::Range,
    pin::Pin,
    sync::Arc,
    task::{Context, Poll},
};

use arrow::{
    array::{ArrayRef, TimestampNanosecondArray},
    compute::SortColumn,
    datatypes::SchemaRef,
    record_batch::RecordBatch,
};
use datafusion::{
    error::{DataFusionError, Result},
    physical_expr::PhysicalSortExpr,
    physical_plan::{
        expressions::Column,
        metrics::{BaselineMetrics, RecordOutput},
        PhysicalExpr, RecordBatchStream, SendableRecordBatchStream,
    },
};
use futures::{ready, Stream, StreamExt};

use super::{
    builder::{build_output, OutputState},
    params::GapFillParams,
    series::{SeriesAppender, SeriesState},
    GapFillExecParams,
};

/// An implementation of a gap-filling operator that uses the [Stream] trait.
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
    /// The parameters of gap-filling: time range start, end and the stride.
    params: GapFillParams,
    /// The number of rows to produce in each output batch.
    batch_size: usize,
    /// The producer of the input record batches.
    input: SendableRecordBatchStream,
    /// Input that has been read from the iput stream.
    buffered_input_batches: Vec<RecordBatch>,
    /// The current state of gap-filling, including the offset of the next row
    /// to read from the input and the next timestamp to produce.
    series_state: SeriesState,
    /// This bit is set when we have consumed all of the input for a series in
    /// a previous output batch, but there are still trailing gap rows to produce.
    trailing_gaps: bool,
    /// This is true as long as there are more input record batches to read from `input`.
    more_input: bool,
    /// Baseline metrics.
    baseline_metrics: BaselineMetrics,
}

impl GapFillStream {
    /// Creates a new GapFillStream.
    pub fn try_new(
        schema: SchemaRef,
        sort_expr: &[PhysicalSortExpr],
        aggr_expr: &[Arc<dyn PhysicalExpr>],
        exec_params: &GapFillExecParams,
        batch_size: usize,
        input: SendableRecordBatchStream,
        metrics: BaselineMetrics,
    ) -> Result<Self> {
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
        let params = GapFillParams::try_new(Arc::clone(&schema), exec_params)?;
        let series_state = SeriesState::new(&params, batch_size);
        Ok(Self {
            schema: Arc::clone(&schema),
            time_expr,
            group_expr,
            aggr_expr,
            params,
            batch_size,
            input,
            buffered_input_batches: vec![],
            series_state,
            trailing_gaps: false,
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
    /// This method starts off by reading input until it has buffered `batch_size` + 2 rows,
    /// or until there is no more input. Having at least `batch_size` rows ensures that we
    /// can produce at least one full output batch. We need two additional rows so that we have
    ///  1) an input row that corresponds to the row before the current output batch. This is
    ///     needed for the case where we are producing trailing gaps, and we need to use the
    ///     `take` kernel to build the group columns. There must be at least one row from the
    ///     corresponding series in the input to take from.
    ///  2) an input row that corresponds to the next input row that will be read after the
    ///     current output batch. This tells us if we have processed all of our input for a series
    ///     but may be in "trailing gaps" mode.
    ///
    /// Once input rows have been buffered, it will produce a gap-filled [RecordBatch] with `self.batch_size`
    /// rows (or less, if there is no more input).
    fn poll_next(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<Option<Result<RecordBatch>>> {
        while self.more_input && self.buffered_input_row_count() < self.batch_size + 2 {
            match ready!(self.input.poll_next_unpin(cx)) {
                Some(Ok(batch)) => self.buffered_input_batches.push(batch),
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
                // If we have consumed all of our input, and there are no trailing gaps to produce
                if self.series_state.next_input_offset == input_batch.num_rows()
                    && !self.trailing_gaps
                {
                    // leave the input batch taken so that its reference
                    // count goes to zero.
                    return Poll::Ready(None);
                }

                input_batch
            }
            Err(e) => return Poll::Ready(Some(Err(e))),
        };

        match self.process(input_batch) {
            Ok((output_batch, remaining_input_batch)) => {
                self.buffered_input_batches.push(remaining_input_batch);
                assert_eq!(1, self.buffered_input_batches.len());
                Poll::Ready(Some(Ok(output_batch)))
            }
            Err(e) => Poll::Ready(Some(Err(e))),
        }
    }
}

impl GapFillStream {
    /// Count of input rows that are currently buffered.
    fn buffered_input_row_count(&self) -> usize {
        self.buffered_input_batches
            .iter()
            .map(|rb| rb.num_rows())
            .sum()
    }

    /// If any buffered input batches are present, concatenates it all together
    /// and returns an owned batch to the caller, leaving `self.buffered_input_batches` empty.
    fn take_buffered_input(&mut self) -> Result<Option<RecordBatch>> {
        if self.buffered_input_batches.is_empty() {
            return Ok(None);
        }

        let mut v = vec![];
        std::mem::swap(&mut v, &mut self.buffered_input_batches);

        Ok(Some(
            arrow::compute::concat_batches(&self.schema, &v)
                .map_err(DataFusionError::ArrowError)?,
        ))
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
        let series_appenders = self.series_appenders(input_time_array.1, &group_arrays)?;

        let timer = elapsed_compute.timer();
        let OutputState {
            output_batch,
            new_series_state,
        } = build_output(
            self.series_state.clone(),
            Arc::clone(&self.schema),
            &self.params,
            input_time_array,
            &group_arrays,
            &aggr_arrays,
            &series_appenders,
        )?;
        timer.done();
        let output_batch = output_batch.record_output(&self.baseline_metrics);
        self.series_state = new_series_state;

        let last_series = series_appenders
            .last()
            .expect("there is at least one series");

        // there are three possible states at this point:
        // 1. We ended output in the middle of input for a series. In this case
        //     series_state.next_input_offset < last_series.input_end
        // 2. We processed all the input for a series, but there are still more
        //     gaps to fill at the end. In this case series_is_done will return false.
        // 3. We ended exactly at the end of a series, and filled all the gaps.
        //     In this case series_is_done will return true.

        // No special action needed for possible state 1.
        if self.series_state.next_input_offset == last_series.input_end {
            if !self.series_state.series_is_done(&self.params, last_series) {
                // Possible state 2:
                // Set this bit so that the next invocation of poll_next generates
                // the trailing gaps.
                self.trailing_gaps = true;
            } else {
                // Possible state 3
                self.series_state.fresh_series(&self.params);
            }
        }

        // Slice the input so array data that is no longer referenced can be dropped.
        input_batch = self.series_state.slice_input_batch(input_batch)?;

        self.series_state.fresh_output_batch(self.batch_size);

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

    /// Produces the [SeriesAppender] structs for each series that can at least
    /// be started in the next output batch.
    fn series_appenders(
        &mut self,
        input_time_array: &TimestampNanosecondArray,
        group_arr: &[(usize, ArrayRef)],
    ) -> Result<Vec<SeriesAppender>> {
        if group_arr.is_empty() {
            self.trailing_gaps = false;
            // there are no group columns, so the output
            // will be just one big series.
            return Ok(vec![SeriesAppender::new_with_input_end_offset(
                input_time_array.len(),
            )]);
        }

        let sort_columns = group_arr
            .iter()
            .map(|(_, arr)| SortColumn {
                values: Arc::clone(arr),
                options: None,
            })
            .collect::<Vec<_>>();
        let mut ranges = arrow::compute::lexicographical_partition_ranges(&sort_columns)
            .map_err(DataFusionError::ArrowError)?;

        let mut appenders = vec![];
        let mut series_state = self.series_state.clone();
        let mut output_row_count = 0;

        let start_offset = series_state.next_input_offset;
        assert!(start_offset <= 1, "input is sliced after it is consumed");
        while output_row_count < self.batch_size {
            match ranges.next() {
                Some(Range { end, .. }) => {
                    assert!(
                        end > 0,
                        "each lexicographical partition will have at least one row"
                    );
                    if end == start_offset && !self.trailing_gaps {
                        // This represents a partition that ends at our current input offset,
                        // but since trailing_gaps is not set, there is nothing to do.
                        continue;
                    }

                    let sa = SeriesAppender::new_with_input_end_offset(end);
                    let (nrows, ss) =
                        sa.remaining_output_row_count(&self.params, input_time_array, series_state);
                    series_state = ss;
                    output_row_count += nrows;
                    appenders.push(sa);
                }
                None => break,
            }
        }
        self.trailing_gaps = false;

        Ok(appenders)
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
