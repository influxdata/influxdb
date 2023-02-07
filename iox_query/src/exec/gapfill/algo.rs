use std::{ops::Bound, sync::Arc};

use arrow::{datatypes::IntervalDayTimeType, record_batch::RecordBatch};
use chrono::Duration;
use datafusion::{
    error::DataFusionError,
    error::Result,
    physical_expr::{datetime_expressions::date_bin, PhysicalSortExpr},
    physical_plan::{
        metrics::BaselineMetrics, ColumnarValue, PhysicalExpr, SendableRecordBatchStream,
    },
    scalar::ScalarValue,
};
use tokio::sync::mpsc;
use tokio_stream::StreamExt;

use super::{try_map_bound, try_map_range, GapFillExecParams};

/// Fill in the gaps in a stream of records that represent
/// one or more time series.
///
/// # Arguments
///
/// * `output_batch_size`
/// * `input_stream`
/// * `_sort_expr` - The incoming records will be sorted by these
///        expressions. They will all be simple column references,
///        with the last one being the timestamp value for each row.
///        The last column will already have been normalized by a previous
///        call to DATE_BIN.
/// * `_aggr_expr` - A set of column expressions that are the aggregate values
///        computed by an upstream Aggregate node.
/// * `params` - The parameters for gap filling, including the stride and the
///        start and end of the time range for this operation.
/// * `_tx` - The transmit end of the channel for output.
/// * `_baseline_metrics`
pub(super) async fn fill_gaps(
    _output_batch_size: usize,
    mut input_stream: SendableRecordBatchStream,
    _sort_expr: Vec<PhysicalSortExpr>,
    _aggr_expr: Vec<Arc<dyn PhysicalExpr>>,
    params: GapFillExecParams,
    _tx: mpsc::Sender<Result<RecordBatch>>,
    _baseline_metrics: BaselineMetrics,
) -> Result<()> {
    while let Some(batch) = input_stream.next().await {
        let batch = batch?;
        let _params = evaluate_params(&batch, &params);
    }
    Err(DataFusionError::NotImplemented("gap_filling".to_string()))
}

#[derive(Debug, PartialEq)]
struct GapFillParams {
    #[allow(unused)]
    pub stride: i64,
    #[allow(unused)]
    pub first_ts: i64,
    #[allow(unused)]
    pub last_ts: i64,
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

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use arrow::{
        array::{ArrayRef, Float64Array, StringArray, TimestampNanosecondArray},
        datatypes::{DataType, Field, Schema, TimeUnit},
        error::Result as ArrowResult,
        record_batch::RecordBatch,
    };
    use datafusion::{
        datasource::empty::EmptyTable, error::Result, from_slice::FromSlice, sql::TableReference,
    };

    use crate::exec::{gapfill::GapFillExec, Executor, ExecutorType};

    use super::GapFillParams;

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
        context.inner().register_table(
            TableReference::Bare { table: "t" },
            Arc::new(EmptyTable::new(Arc::new(schema()))),
        )?;
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
