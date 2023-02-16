use std::{collections::VecDeque, ops::Range, sync::Arc};

use arrow::{
    array::Array,
    array::TimestampNanosecondArray,
    compute::{lexicographical_partition_ranges, SortColumn},
    record_batch::RecordBatch,
};
use datafusion::{
    error::{DataFusionError, Result},
    physical_expr::PhysicalSortExpr,
    physical_plan::{
        expressions::Column,
        metrics::{BaselineMetrics, RecordOutput},
        PhysicalExpr, SendableRecordBatchStream,
    },
};
use tokio::sync::mpsc;
use tokio_stream::StreamExt;

use crate::exec::gapfill::builder::build_output;
use crate::exec::gapfill::series::SeriesAppender;

use super::{builder::OutputState, params::GapFillParams, series::SeriesState, GapFillExecParams};

/// Fill in the gaps in a stream of records that represent
/// one or more time series.
///
/// # Arguments
///
/// * `output_batch_size`
/// * `input_stream`
/// * `sort_expr` - The incoming records will be sorted by these
///        expressions. They will all be simple column references,
///        with the last one being the timestamp value for each row.
///        The last column will already have been normalized by a previous
///        call to DATE_BIN.
/// * `aggr_expr` - A set of column expressions that are the aggregate values
///        computed by an upstream Aggregate node.
/// * `params` - The parameters for gap filling, including the stride and the
///        start and end of the time range for this operation.
/// * `tx` - The transmit end of the channel for output.
/// * `baseline_metrics`
pub(super) async fn fill_gaps(
    output_batch_size: usize,
    mut input_stream: SendableRecordBatchStream,
    sort_expr: Vec<PhysicalSortExpr>,
    aggr_expr: Vec<Arc<dyn PhysicalExpr>>,
    exec_params: GapFillExecParams,
    tx: mpsc::Sender<Result<RecordBatch>>,
    baseline_metrics: BaselineMetrics,
) -> Result<()> {
    let time_expr = sort_expr.last().ok_or_else(|| {
        DataFusionError::Internal("should be at least time column in sort exprs".into())
    })?;
    let time_expr = &time_expr.expr;

    let mut params: Option<GapFillParams> = None;
    let mut last_input_batch: Option<RecordBatch> = None;

    while let Some(input_batch) = input_stream.next().await {
        let input_batch = input_batch?;
        if input_batch.num_rows() == 0 {
            // this is apparently possible...
            continue;
        }

        if last_input_batch.is_some() {
            return Err(DataFusionError::NotImplemented(
                "gap filling with multiple input batches".to_string(),
            ));
        }

        if params.is_none() {
            params = Some(GapFillParams::try_new(input_batch.schema(), &exec_params)?);
        }
        let params = params.as_ref().unwrap();

        // Produce the set of arrays from the input

        let input_time_array = time_expr
            .evaluate(&input_batch)?
            .into_array(input_batch.num_rows());
        let input_time_array: &TimestampNanosecondArray =
            input_time_array.as_any().downcast_ref().unwrap();
        let input_time_array = (expr_to_index(time_expr), input_time_array);

        let aggr_arr = aggr_expr
            .iter()
            .map(|e| {
                Ok((
                    expr_to_index(e),
                    e.evaluate(&input_batch)?.into_array(input_batch.num_rows()),
                ))
            })
            .collect::<Result<Vec<_>>>()?;

        let group_arr = sort_expr
            .iter()
            .take(sort_expr.len() - 1)
            .map(|se| {
                Ok((
                    expr_to_index(&se.expr),
                    se.expr
                        .evaluate(&input_batch)?
                        .into_array(input_batch.num_rows()),
                ))
            })
            .collect::<Result<Vec<_>>>()?;

        // Make a set of sort columns to find out when the group columns
        // change.
        let sort_columns = group_arr
            .iter()
            .map(|(_, arr)| SortColumn {
                values: Arc::clone(arr),
                options: None,
            })
            .collect::<Vec<_>>();

        // Each item produced by this iterator is a range of row offsets [start, end) for when
        // the group columns change.
        let input_range_iter = if !sort_columns.is_empty() {
            Box::new(lexicographical_partition_ranges(&sort_columns)?)
        } else {
            // lexicographical_partition_ranges will refuse to work if there are no
            // sort columns. All the rows are in one big partition for this case.
            Box::new(vec![(0..input_batch.num_rows())].into_iter())
                as Box<dyn Iterator<Item = Range<usize>> + Send>
        };

        let mut series_queue = VecDeque::new();
        for range in input_range_iter {
            series_queue.push_back(SeriesAppender::new_with_input_end_offset(range.end));
        }

        // Send off one output batch for every iteration of this loop
        let mut series_state = SeriesState::new(params, output_batch_size);
        while !series_queue.is_empty() {
            // take the next set of series that will fit into the next record batch to
            // be produced. The last series may only be partially output.
            let mut next_batch_series = Vec::new();
            let mut output_rows = 0;
            let mut batching_series_state = series_state.clone();
            while output_rows < output_batch_size && !series_queue.is_empty() {
                let series = series_queue.pop_front().unwrap();
                let (series_row_count, ss) = series.remaining_output_row_count(
                    params,
                    input_time_array.1,
                    batching_series_state,
                );
                output_rows += series_row_count;
                batching_series_state = ss;
                next_batch_series.push(series);
            }

            // produce a new record batch for the series that can at least be started
            // in this batch.
            let OutputState {
                output_batch,
                new_series_state,
            } = build_output(
                series_state,
                input_stream.schema(),
                params,
                input_time_array,
                &group_arr,
                &aggr_arr,
                &next_batch_series,
            )?;
            let output_batch = output_batch.record_output(&baseline_metrics);
            tx.send(Ok(output_batch))
                .await
                .map_err(|e| DataFusionError::External(Box::new(e)))?;

            // update the state to reflect where we are in the input
            // and what the next timestamp should be.
            series_state = new_series_state;

            let last_series = next_batch_series
                .last()
                .expect("there is at least one series");
            if series_state.series_is_done(params, last_series) {
                series_state.fresh_series(params);
            } else {
                // There was only room for part of the last series, so
                // push it back onto the queue.
                series_queue.push_front(last_series.clone());
            }

            // get ready to produce a new record batch on the next iteration
            // of this loop.
            series_state.fresh_output_batch(output_batch_size);
        }
        last_input_batch = Some(input_batch);
    }
    Ok(())
}

fn expr_to_index(expr: &Arc<dyn PhysicalExpr>) -> usize {
    expr.as_any()
        .downcast_ref::<Column>()
        .expect("all exprs should be columns")
        .index()
}
