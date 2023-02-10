use std::{
    collections::VecDeque,
    ops::{Bound, Range, RangeBounds},
    sync::Arc,
};

use arrow::{
    array::Array,
    array::TimestampNanosecondArray,
    compute::{lexicographical_partition_ranges, SortColumn},
    datatypes::IntervalDayTimeType,
    record_batch::RecordBatch,
};
use chrono::Duration;
use datafusion::{
    error::{DataFusionError, Result},
    physical_expr::{datetime_expressions::date_bin, PhysicalSortExpr},
    physical_plan::{
        expressions::Column,
        metrics::{BaselineMetrics, RecordOutput},
        ColumnarValue, PhysicalExpr, SendableRecordBatchStream,
    },
    scalar::ScalarValue,
};
use tokio::sync::mpsc;
use tokio_stream::StreamExt;

use crate::exec::gapfill::builder::build_output;
use crate::exec::gapfill::series::Series;

use super::{try_map_bound, try_map_range, GapFillExecParams};

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
            params = Some(evaluate_params(&input_batch, &exec_params)?);
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
        let range_iter = if !sort_columns.is_empty() {
            Box::new(lexicographical_partition_ranges(&sort_columns)?)
        } else {
            // lexicographical_partition_ranges will refuse to work if there are no
            // sort columns. All the rows are in one big partition for this case.
            Box::new(vec![(0..input_batch.num_rows())].into_iter())
                as Box<dyn Iterator<Item = Range<usize>> + Send>
        };

        let mut series_queue = VecDeque::new();
        for range in range_iter {
            series_queue.push_back(Series::new(range, input_time_array.1));
        }

        // Send off one output batch for every iteration of this loop
        // (currently only one output batch is supported)
        while !series_queue.is_empty() {
            let mut next_batch = Vec::new();
            let mut remaining = output_batch_size;
            loop {
                if remaining == 0 {
                    break;
                }

                let series = match series_queue.pop_front() {
                    Some(series) => series,
                    None => break,
                };

                let series_row_count = series.total_output_row_count(params);
                if series_row_count <= remaining {
                    next_batch.push(series);
                    remaining -= series_row_count;
                } else {
                    // TODO: break up the series here
                    return Err(DataFusionError::NotImplemented(
                        "gap fill output spanning multiple batches".to_string(),
                    ));
                }
            }
            let output_batch = build_output(
                input_stream.schema(),
                params,
                input_time_array,
                &group_arr,
                &aggr_arr,
                &next_batch,
            )?
            .record_output(&baseline_metrics);
            tx.send(Ok(output_batch))
                .await
                .map_err(|e| DataFusionError::External(Box::new(e)))?;
        }
        last_input_batch = Some(input_batch);
    }
    Ok(())
}

#[derive(Debug, PartialEq)]
pub(super) struct GapFillParams {
    pub stride: i64,
    pub first_ts: i64,
    pub last_ts: i64, // inclusive!
}

impl GapFillParams {
    pub fn valid_row_count<RB: RangeBounds<i64>>(&self, rb: RB) -> usize {
        let first_ts = match rb.start_bound() {
            Bound::Unbounded => self.first_ts,
            Bound::Included(first_ts) => *first_ts,
            Bound::Excluded(first_ts) => *first_ts + self.stride,
        };
        let last_ts = match rb.end_bound() {
            Bound::Unbounded => self.last_ts,
            Bound::Included(last_ts) => *last_ts,
            Bound::Excluded(last_ts) => *last_ts - self.stride,
        };
        if last_ts >= first_ts {
            ((last_ts - first_ts) / self.stride + 1) as usize
        } else {
            0
        }
    }

    pub fn iter_times(&self) -> impl Iterator<Item = i64> {
        (self.first_ts..=self.last_ts).step_by(self.stride as usize)
    }
}

/// Figure out the actual values (as native i64) for the stride,
/// first and last timestamp for gap filling.
fn evaluate_params(
    batch: &RecordBatch,
    params: &super::GapFillExecParams,
) -> Result<GapFillParams> {
    let stride = params.stride.evaluate(batch)?;
    let origin = params.origin.evaluate(batch)?;

    // Evaluate the upper and lower bounds of the time range
    let range = try_map_range(&params.time_range, |b| {
        try_map_bound(b.as_ref(), |pe| {
            extract_timestamp_nanos(&pe.evaluate(batch)?)
        })
    })?;

    // Find the smallest timestamp that might appear in the
    // range
    let first_ts = match range.start {
        Bound::Included(v) => v,
        Bound::Excluded(v) => v + 1,
        Bound::Unbounded => {
            return Err(DataFusionError::Execution(
                "missing lower time bound for gap filling".to_string(),
            ))
        }
    };

    // Find the largest timestamp that might appear in the
    // range
    let last_ts = match range.end {
        Bound::Included(v) => v,
        Bound::Excluded(v) => v - 1,
        Bound::Unbounded => {
            return Err(DataFusionError::Execution(
                "missing upper time bound for gap filling".to_string(),
            ))
        }
    };

    // Call date_bin on the timestamps to find the first and last time bins
    // for each series
    let mut args = vec![stride, i64_to_columnar_ts(first_ts), origin];
    let first_ts = extract_timestamp_nanos(&date_bin(&args)?)?;
    args[1] = i64_to_columnar_ts(last_ts);
    let last_ts = extract_timestamp_nanos(&date_bin(&args)?)?;

    Ok(GapFillParams {
        stride: extract_interval_nanos(&args[0])?,
        first_ts,
        last_ts,
    })
}

fn i64_to_columnar_ts(i: i64) -> ColumnarValue {
    ColumnarValue::Scalar(ScalarValue::TimestampNanosecond(Some(i), None))
}

fn extract_timestamp_nanos(cv: &ColumnarValue) -> Result<i64> {
    Ok(match cv {
        ColumnarValue::Scalar(ScalarValue::TimestampNanosecond(Some(v), _)) => *v,
        _ => {
            return Err(DataFusionError::Execution(
                "gap filling argument must be a scalar timestamp".to_string(),
            ))
        }
    })
}

fn extract_interval_nanos(cv: &ColumnarValue) -> Result<i64> {
    match cv {
        ColumnarValue::Scalar(ScalarValue::IntervalDayTime(Some(v))) => {
            let (days, ms) = IntervalDayTimeType::to_parts(*v);
            let nanos =
                (Duration::days(days as i64) + Duration::milliseconds(ms as i64)).num_nanoseconds();
            nanos.ok_or_else(|| {
                DataFusionError::Execution("gap filling argument is too large".to_string())
            })
        }
        _ => Err(DataFusionError::Execution(
            "gap filling expects a stride parameter to be a scalar interval".to_string(),
        )),
    }
}

fn expr_to_index(expr: &Arc<dyn PhysicalExpr>) -> usize {
    expr.as_any()
        .downcast_ref::<Column>()
        .expect("all exprs should be columns")
        .index()
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use arrow::{
        array::{ArrayRef, Float64Array, StringArray, TimestampNanosecondArray},
        datatypes::{DataType, Field, Schema, TimeUnit},
        error::Result as ArrowResult,
        record_batch::RecordBatch,
    };
    use datafusion::{datasource::empty::EmptyTable, error::Result, from_slice::FromSlice};

    use crate::exec::{gapfill::GapFillExec, Executor, ExecutorType};

    use super::GapFillParams;

    #[test]
    #[allow(clippy::reversed_empty_ranges)]
    fn test_params_row_count() -> Result<()> {
        test_helpers::maybe_start_logging();
        let params = GapFillParams {
            stride: 10,
            first_ts: 1000,
            last_ts: 1050,
        };

        assert_eq!(6, params.valid_row_count(..));
        assert_eq!(5, params.valid_row_count(1010..));
        assert_eq!(2, params.valid_row_count(..1020)); // exclusive end
        assert_eq!(1, params.valid_row_count(1010..1020)); // exclusive end
        assert_eq!(3, params.valid_row_count(..=1020)); // inclusive end
        assert_eq!(2, params.valid_row_count(1010..=1020)); // inclusive end
        assert_eq!(0, params.valid_row_count(1020..1010)); // start > end
        Ok(())
    }

    fn schema() -> Schema {
        Schema::new(vec![
            Field::new(
                "time",
                DataType::Timestamp(TimeUnit::Nanosecond, None),
                false,
            ),
            Field::new(
                "other_time",
                DataType::Timestamp(TimeUnit::Nanosecond, None),
                false,
            ),
            Field::new("loc", DataType::Utf8, false),
            Field::new("temp", DataType::Float64, false),
        ])
    }

    fn record_batch() -> ArrowResult<RecordBatch> {
        let columns: Vec<ArrayRef> = vec![
            Arc::new(TimestampNanosecondArray::from_slice([1000])),
            Arc::new(TimestampNanosecondArray::from_slice([2000])),
            Arc::new(StringArray::from_slice(["kitchen"])),
            Arc::new(Float64Array::from_slice([27.1])),
        ];
        RecordBatch::try_new(Arc::new(schema()), columns)
    }

    async fn plan_statement_and_get_params(sql: &str) -> Result<GapFillParams> {
        let executor = Executor::new_testing();
        let context = executor.new_context(ExecutorType::Query);
        context
            .inner()
            .register_table("t", Arc::new(EmptyTable::new(Arc::new(schema()))))?;
        let physical_plan = context.prepare_sql(sql).await?;
        let gapfill_node = &physical_plan.children()[0];
        let gapfill_node = gapfill_node.as_any().downcast_ref::<GapFillExec>().unwrap();
        let exec_params = &gapfill_node.params;
        super::evaluate_params(&record_batch()?, exec_params)
    }

    #[tokio::test]
    async fn test_evaluate_params() -> Result<()> {
        test_helpers::maybe_start_logging();
        let actual = plan_statement_and_get_params(
                "select\
               \n    date_bin_gapfill(interval '1 minute', time, timestamp '1970-01-01T00:00:00Z') minute\
               \nfrom t\
               \nwhere time >= timestamp '1984-01-01T16:00:00Z' - interval '5 minutes'\
               \n    and time <= timestamp '1984-01-01T16:00:00Z'\
               \ngroup by minute",
            ).await?;
        let expected = GapFillParams {
            stride: 60_000_000_000,            // 1 minute
            first_ts: 441_820_500_000_000_000, // Sunday, January 1, 1984 3:55:00 PM
            last_ts: 441_820_800_000_000_000,  // Sunday, January 1, 1984 3:59:00 PM
        };
        assert_eq!(expected, actual);
        Ok(())
    }

    #[tokio::test]
    async fn test_evaluate_params_exclude_end() -> Result<()> {
        test_helpers::maybe_start_logging();
        let actual = plan_statement_and_get_params(
                "select\
               \n    date_bin_gapfill(interval '1 minute', time, timestamp '1970-01-01T00:00:00Z') minute\
               \nfrom t\
               \nwhere time >= timestamp '1984-01-01T16:00:00Z' - interval '5 minutes'\
               \n    and time < timestamp '1984-01-01T16:00:00Z'\
               \ngroup by minute",
            ).await?;
        let expected = GapFillParams {
            stride: 60_000_000_000,            // 1 minute
            first_ts: 441_820_500_000_000_000, // Sunday, January 1, 1984 3:55:00 PM
            // Last bin at 16:00 is excluded
            last_ts: 441_820_740_000_000_000, // Sunday, January 1, 1984 3:59:00 PM
        };
        assert_eq!(expected, actual);
        Ok(())
    }

    #[tokio::test]
    async fn test_evaluate_params_exclude_start() -> Result<()> {
        test_helpers::maybe_start_logging();
        let actual = plan_statement_and_get_params(
                "select\
               \n    date_bin_gapfill(interval '1 minute', time, timestamp '1970-01-01T00:00:00Z') minute\
               \nfrom t\
               \nwhere time > timestamp '1984-01-01T16:00:00Z' - interval '5 minutes'\
               \n    and time <= timestamp '1984-01-01T16:00:00Z'\
               \ngroup by minute",
            ).await?;
        let expected = GapFillParams {
            stride: 60_000_000_000, // 1 minute
            // First bin not exluded since it truncates to 15:55:00
            first_ts: 441_820_500_000_000_000, // Sunday, January 1, 1984 3:55:00 PM
            last_ts: 441_820_800_000_000_000,  // Sunday, January 1, 1984 3:59:00 PM
        };
        assert_eq!(expected, actual);
        Ok(())
    }

    #[tokio::test]
    async fn test_evaluate_params_origin() -> Result<()> {
        test_helpers::maybe_start_logging();
        let actual = plan_statement_and_get_params(
            // origin is 9s after the epoch
                "select\
               \n    date_bin_gapfill(interval '1 minute', time, timestamp '1970-01-01T00:00:09Z') minute\
               \nfrom t\
               \nwhere time >= timestamp '1984-01-01T16:00:00Z' - interval '5 minutes'\
               \n    and time <= timestamp '1984-01-01T16:00:00Z'\
               \ngroup by minute",
            ).await?;
        let expected = GapFillParams {
            stride: 60_000_000_000,            // 1 minute
            first_ts: 441_820_449_000_000_000, // Sunday, January 1, 1984 3:54:09 PM
            last_ts: 441_820_749_000_000_000,  // Sunday, January 1, 1984 3:59:09 PM
        };
        assert_eq!(expected, actual);
        Ok(())
    }
}
