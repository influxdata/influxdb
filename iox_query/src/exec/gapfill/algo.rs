//! Contains the [GapFiller] type which does the
//! actual gap filling of record batches.

use std::{ops::Range, sync::Arc};

use arrow::{
    array::{Array, ArrayRef, TimestampNanosecondArray, UInt64Array},
    compute::{kernels::take, SortColumn},
    datatypes::SchemaRef,
    record_batch::RecordBatch,
};
use datafusion::error::{DataFusionError, Result};

use super::params::GapFillParams;

/// Provides methods to the [`GapFillStream`](super::stream::GapFillStream)
/// module that fill gaps in buffered input.
///
/// [GapFiller] assumes that there will be at least `output_batch_size + 2`
/// input records buffered when [`build_gapfilled_output`](GapFiller::build_gapfilled_output)
/// is invoked, provided there is enough data.
///
/// Once output is produced, clients should call `slice_input_batch` to unbuffer
/// data that is no longer needed.
///
/// Below is a diagram of how buffered input is structured.
///
/// ```text
///
///                                     BUFFERED INPUT ROWS
///
///                        time     group columns       aggregate columns
///                       ╓────╥───┬───┬─────────────╥───┬───┬─────────────╖
/// context row         0 ║    ║   │   │   . . .     ║   │   │   . . .     ║
///                       ╟────╫───┼───┼─────────────╫───┼───┼─────────────╢
///  ┬────  cursor────► 1 ║    ║   │   │             ║   │   │             ║
///  │                    ╟────╫───┼───┼─────────────╫───┼───┼─────────────╢
///  │                  2 ║    ║   │   │             ║   │   │             ║
///  │                    ╟────╫───┼───┼─────────────╫───┼───┼─────────────╢
///  │                  3 ║    ║   │   │             ║   │   │             ║
///  │                      .                .                     .
/// output_batch_size       .                .                     .
///  │                      .                .                     .
///  │              n - 1 ║    ║   │   │             ║   │   │             ║
///  │                    ╟────╫───┼───┼─────────────╫───┼───┼─────────────╢
///  ┴────              n ║    ║   │   │             ║   │   │             ║
///                       ╟────╫───┼───┼─────────────╫───┼───┼─────────────╢
/// trailing row    n + 1 ║    ║   │   │             ║   │   │             ║
///                       ╙────╨───┴───┴─────────────╨───┴───┴─────────────╜
/// ```
///
/// Just before generating output, the cursor will generally point at offset 1
/// in the input, since offset 0 is a _context row_. The exception to this is
/// there is no context row when generating the first output batch.
///
/// Buffering at least `output_batch_size + 2` rows ensures that:
/// - `GapFiller` can produce enough rows to produce a complete output batch, since
///       every input row will appear in the output.
/// - There is a _context row_ that represents the last input row that got output before
///       the current output batch. Group column values will be taken from this row
///       (using the [`take`](take::take) kernel) when we are generating trailing gaps, i.e.,
///       when all of the input rows have been output for a series in the previous batch,
///       but there still remains missing rows to produce at the end.
/// - Having one additional _trailing row_ at the end ensures that `GapFiller` can
///       infer whether there is trailing gaps to produce at the beginning of the
///       next batch, since it can discover if the last row starts a new series.
#[derive(Clone, Debug, PartialEq)]
pub(super) struct GapFiller {
    /// The static parameters of gap-filling: time range start, end and the stride.
    params: GapFillParams,
    /// The current state of gap-filling, including the next timestamp,
    /// the offset of the next input row, and remaining space in output batch.
    cursor: Cursor,
    /// True if there are trailing gaps from processed input to produce
    /// at the beginning of an output batch.
    trailing_gaps: bool,
}

impl GapFiller {
    /// Initialize a [GapFiller] at the beginning of an input record batch.
    pub fn new(params: GapFillParams) -> Self {
        let next_ts = params.first_ts;
        let cursor = Cursor {
            next_input_offset: 0,
            next_ts,
            remaining_output_batch_size: 0,
        };
        Self {
            params,
            cursor,
            trailing_gaps: false,
        }
    }

    /// Returns true if `next_input_offset` points past the end of input
    /// and there are no trailing gaps to produce.
    pub fn done(&self, buffered_input_row_count: usize) -> bool {
        self.cursor.next_input_offset == buffered_input_row_count && !self.trailing_gaps
    }

    /// Produces a gap-filled output [RecordBatch].
    ///
    /// Input arrays are represented as pairs that include their offset in the
    /// schema at member `0`.
    pub fn build_gapfilled_output(
        &mut self,
        batch_size: usize,
        schema: SchemaRef,
        input_time_array: (usize, &TimestampNanosecondArray),
        group_arrays: &[(usize, ArrayRef)],
        aggr_arrays: &[(usize, ArrayRef)],
    ) -> Result<RecordBatch> {
        let series_ends = self.plan_output_batch(batch_size, input_time_array.1, group_arrays)?;
        self.cursor.remaining_output_batch_size = batch_size;
        let output_batch = self.build_output(
            schema,
            input_time_array,
            group_arrays,
            aggr_arrays,
            &series_ends,
        )?;

        let last_series_end = series_ends.last().expect("there is at least one series");

        // there are three possible states at this point:
        // 1. We ended output in the middle of input for a series. In this case
        //     cursor.next_input_offset < last_series.input_end
        // 2. We processed all the input for a series, but there are still more
        //     gaps to fill at the end. In this case cursor.next_ts <= params.last_ts.
        // 3. We ended exactly at the end of a series, and filled all the gaps.
        //     In this case cursor.next_ts > params.last_ts.

        if self.cursor.next_input_offset < *last_series_end {
            // No special action needed for possible state 1.
            self.trailing_gaps = false;
        } else {
            assert!(self.cursor.next_input_offset == *last_series_end);
            if self.cursor.next_ts <= self.params.last_ts {
                // Possible state 2:
                // Set this bit so that the output batch begins
                // with trailing gaps for the last series.
                self.trailing_gaps = true;
            } else {
                // Possible state 3
                self.cursor.next_ts = self.params.first_ts;
                self.trailing_gaps = false;
            }
        }

        Ok(output_batch)
    }

    /// Slice the input batch so that it has one context row before the next input offset.
    pub fn slice_input_batch(&mut self, batch: RecordBatch) -> Result<RecordBatch> {
        if self.cursor.next_input_offset < 2 {
            // nothing to do
            return Ok(batch);
        }

        let offset = self.cursor.next_input_offset - 1;
        let len = batch.num_rows() - offset;
        self.cursor.next_input_offset = 1;

        Ok(batch.slice(offset, len))
    }

    /// Produces a vector of offsets that are the exclusive ends of each series
    /// in the buffered input. It will return the ends of only those series
    /// that can at least  be started in the output batch.
    ///
    /// Uses [`lexicographical_partition_ranges`](arrow::compute::lexicographical_partition_ranges)
    /// to partition input rows into series.
    fn plan_output_batch(
        &mut self,
        batch_size: usize,
        input_time_array: &TimestampNanosecondArray,
        group_arr: &[(usize, ArrayRef)],
    ) -> Result<Vec<usize>> {
        if group_arr.is_empty() {
            // there are no group columns, so the output
            // will be just one big series.
            return Ok(vec![input_time_array.len()]);
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

        let mut series_ends = vec![];
        let mut cursor = self.cursor.clone();
        let mut output_row_count = 0;

        let start_offset = cursor.next_input_offset;
        assert!(start_offset <= 1, "input is sliced after it is consumed");
        while output_row_count < batch_size {
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

                    let nrows = cursor.count_series_rows(&self.params, input_time_array, end);
                    output_row_count += nrows;
                    series_ends.push(end);
                }
                None => break,
            }
        }

        Ok(series_ends)
    }

    /// Helper method that produces gap-filled record batches.
    ///
    /// This method works by producing each array in the output completely,
    /// for all series that have end offsets in `series_ends`, before producing
    /// subsequent arrays.
    fn build_output(
        &mut self,
        schema: SchemaRef,
        input_time_array: (usize, &TimestampNanosecondArray),
        group_arr: &[(usize, ArrayRef)],
        aggr_arr: &[(usize, ArrayRef)],
        series_ends: &[usize],
    ) -> Result<RecordBatch> {
        let mut output_arrays: Vec<(usize, ArrayRef)> =
            Vec::with_capacity(group_arr.len() + aggr_arr.len() + 1); // plus one for time column

        // build the time column
        let mut cursor = self.cursor.clone();
        let (time_idx, input_time_array) = input_time_array;
        let time_vec = cursor.build_time_vec(&self.params, series_ends, input_time_array)?;
        let output_time_len = time_vec.len();
        output_arrays.push((time_idx, Arc::new(TimestampNanosecondArray::from(time_vec))));
        // There may not be any aggregate or group columns, so use this cursor state as the new
        // GapFiller cursor once this output batch is complete.
        let final_cursor = cursor;

        // build the other group columns
        for (idx, ga) in group_arr.iter() {
            let mut cursor = self.cursor.clone();
            let take_vec =
                cursor.build_group_take_vec(&self.params, series_ends, input_time_array)?;
            if take_vec.len() != output_time_len {
                return Err(DataFusionError::Internal(format!(
                    "gapfill group column has {} rows, expected {}",
                    take_vec.len(),
                    output_time_len
                )));
            }
            let take_arr = UInt64Array::from(take_vec);
            output_arrays.push((*idx, take::take(ga, &take_arr, None)?))
        }

        // Build the aggregate columns
        for (idx, aa) in aggr_arr.iter() {
            let mut cursor = self.cursor.clone();
            let take_vec =
                cursor.build_aggr_take_vec(&self.params, series_ends, input_time_array)?;
            if take_vec.len() != output_time_len {
                return Err(DataFusionError::Internal(format!(
                    "gapfill aggr column has {} rows, expected {}",
                    take_vec.len(),
                    output_time_len
                )));
            }
            let take_arr = UInt64Array::from(take_vec);
            output_arrays.push((*idx, take::take(aa, &take_arr, None)?));
        }

        output_arrays.sort_by(|(a, _), (b, _)| a.cmp(b));
        let output_arrays: Vec<_> = output_arrays.into_iter().map(|(_, arr)| arr).collect();
        let batch = RecordBatch::try_new(Arc::clone(&schema), output_arrays)
            .map_err(DataFusionError::ArrowError)?;

        self.cursor = final_cursor;
        Ok(batch)
    }
}

/// Maintains the state needed to fill gaps in output columns. Also provides methods
/// for building vectors that build time, group, and aggregate output arrays.
#[derive(Clone, Debug, PartialEq)]
struct Cursor {
    /// Where to read the next row from the input.
    next_input_offset: usize,
    /// The next timestamp to be produced for the current series.
    next_ts: i64,
    /// How many rows may be output before we need to start a new record batch.
    remaining_output_batch_size: usize,
}

impl Cursor {
    /// Counts the number of rows that will be produced for a series that ends at
    /// `series_end`, including rows that have a null timestamp, if any.
    fn count_series_rows(
        &mut self,
        params: &GapFillParams,
        input_time_array: &TimestampNanosecondArray,
        series_end: usize,
    ) -> usize {
        let null_ts_count = if input_time_array.null_count() > 0 {
            let len = series_end - self.next_input_offset;
            let slice = input_time_array.slice(self.next_input_offset, len);
            slice.null_count()
        } else {
            0
        };

        let count = null_ts_count + params.valid_row_count(self.next_ts);

        self.next_input_offset = series_end;
        self.next_ts = params.first_ts;

        count
    }

    /// Builds a vector that can be used to produce a timestamp array.
    fn build_time_vec(
        &mut self,
        params: &GapFillParams,
        series_ends: &[usize],
        input_time_array: &TimestampNanosecondArray,
    ) -> Result<Vec<Option<i64>>> {
        let mut times = Vec::with_capacity(self.remaining_output_batch_size);
        self.build_vec(
            params,
            input_time_array,
            series_ends,
            |row_status| match row_status {
                RowStatus::NullTimestamp { .. } => times.push(None),
                RowStatus::Present { ts, .. } | RowStatus::Missing { ts, .. } => {
                    times.push(Some(ts))
                }
            },
        )?;

        Ok(times)
    }

    /// Builds a vector that can use the [`take`](take::take) kernel
    /// to produce a group column.
    fn build_group_take_vec(
        &mut self,
        params: &GapFillParams,
        series_ends: &[usize],
        input_time_array: &TimestampNanosecondArray,
    ) -> Result<Vec<u64>> {
        let mut take_idxs = Vec::with_capacity(self.remaining_output_batch_size);
        self.build_vec(
            params,
            input_time_array,
            series_ends,
            |row_status| match row_status {
                RowStatus::NullTimestamp {
                    series_end_offset, ..
                }
                | RowStatus::Present {
                    series_end_offset, ..
                }
                | RowStatus::Missing {
                    series_end_offset, ..
                } => take_idxs.push(series_end_offset as u64 - 1),
            },
        )?;

        Ok(take_idxs)
    }

    /// Builds a vector that can use the [`take`](take::take) kernel
    /// to produce an aggregate output column.
    fn build_aggr_take_vec(
        &mut self,
        params: &GapFillParams,
        series_ends: &[usize],
        input_time_array: &TimestampNanosecondArray,
    ) -> Result<Vec<Option<u64>>> {
        let mut take_idxs = Vec::with_capacity(self.remaining_output_batch_size);
        self.build_vec(
            params,
            input_time_array,
            series_ends,
            |row_status| match row_status {
                RowStatus::NullTimestamp { offset, .. } | RowStatus::Present { offset, .. } => {
                    take_idxs.push(Some(offset as u64))
                }
                RowStatus::Missing { .. } => take_idxs.push(None),
            },
        )?;

        Ok(take_idxs)
    }

    /// Helper method that iterates over each series
    /// that ends with offsets in `series_ends` and produces
    /// the appropriate output values.
    fn build_vec<F>(
        &mut self,
        params: &GapFillParams,
        input_time_array: &TimestampNanosecondArray,
        series_ends: &[usize],
        mut f: F,
    ) -> Result<()>
    where
        F: FnMut(RowStatus),
    {
        let first_series = series_ends.first().ok_or(DataFusionError::Internal(
            "expected at least one item in series batch".to_string(),
        ))?;

        // Process the first series separately as it may just be a part of what
        // did not fit in the previous output batch.
        self.append_series_items(params, input_time_array, *first_series, &mut f)?;

        for series in series_ends.iter().skip(1) {
            self.next_ts = params.first_ts;
            self.append_series_items(params, input_time_array, *series, &mut f)?;
        }
        Ok(())
    }

    /// Helper method that generates output for one series by invoking
    /// `append` for each output value in the column to be generated.
    fn append_series_items<F>(
        &mut self,
        params: &GapFillParams,
        input_times: &TimestampNanosecondArray,
        series_end: usize,
        mut append: F,
    ) -> Result<()>
    where
        F: FnMut(RowStatus),
    {
        // If there are any null timestamps for this group, they will be first.
        // These rows can just be copied into the output.
        // Append the corresponding values.
        while self.remaining_output_batch_size > 0
            && self.next_input_offset < series_end
            && input_times.is_null(self.next_input_offset)
        {
            append(RowStatus::NullTimestamp {
                series_end_offset: series_end,
                offset: self.next_input_offset,
            });
            self.remaining_output_batch_size -= 1;
            self.next_input_offset += 1;
        }

        let output_row_count = std::cmp::min(
            params.valid_row_count(self.next_ts),
            self.remaining_output_batch_size,
        );
        if output_row_count == 0 {
            return Ok(());
        }

        let mut next_ts = self.next_ts;
        // last_ts is the last timestamp that will fit in the output batch
        let last_ts = self.next_ts + (output_row_count - 1) as i64 * params.stride;

        loop {
            if self.next_input_offset >= series_end {
                break;
            }
            let in_ts = input_times.value(self.next_input_offset);
            if in_ts > last_ts {
                break;
            }
            while next_ts < in_ts {
                append(RowStatus::Missing {
                    series_end_offset: series_end,
                    ts: next_ts,
                });
                next_ts += params.stride;
            }
            append(RowStatus::Present {
                series_end_offset: series_end,
                offset: self.next_input_offset,
                ts: next_ts,
            });
            next_ts += params.stride;
            self.next_input_offset += 1;
        }

        // Add any additional missing values after the last of the input.
        while next_ts <= last_ts {
            append(RowStatus::Missing {
                series_end_offset: series_end,
                ts: next_ts,
            });
            next_ts += params.stride;
        }

        self.next_ts = last_ts + params.stride;
        self.remaining_output_batch_size -= output_row_count;
        Ok(())
    }
}

/// The state of an input row relative to gap-filled output.
enum RowStatus {
    /// This row had a null timestamp in the input.
    NullTimestamp {
        /// The exclusive offset of the series end in the input.
        series_end_offset: usize,
        /// The offset of the null timestamp in the input time array.
        offset: usize,
    },
    /// A row with this timestamp is present in the input.
    Present {
        /// The exclusive offset of the series end in the input.
        series_end_offset: usize,
        /// The offset of the value in the input time array.
        offset: usize,
        /// The timestamp corresponding to this row.
        ts: i64,
    },
    /// A row with this timestamp is missing from the input.
    Missing {
        /// The exclusive offset of the series end in the input.
        series_end_offset: usize,
        /// The timestamp corresponding to this row.
        ts: i64,
    },
}

#[cfg(test)]
mod tests {
    use arrow::array::TimestampNanosecondArray;
    use datafusion::error::Result;

    use crate::exec::gapfill::{algo::Cursor, params::GapFillParams};

    #[test]
    fn test_cursor_append_time_values() -> Result<()> {
        test_helpers::maybe_start_logging();
        let input_times = TimestampNanosecondArray::from(vec![1000, 1100, 1200]);
        let series = input_times.len();

        let params = GapFillParams {
            stride: 50,
            first_ts: 950,
            last_ts: 1250,
        };

        let output_batch_size = 10000;
        let mut cursor = Cursor {
            next_input_offset: 0,
            next_ts: params.first_ts,
            remaining_output_batch_size: output_batch_size,
        };

        let out_times = cursor.build_time_vec(&params, &[series], &input_times)?;
        assert_eq!(
            vec![
                Some(950),
                Some(1000),
                Some(1050),
                Some(1100),
                Some(1150),
                Some(1200),
                Some(1250)
            ],
            out_times
        );

        assert_eq!(
            Cursor {
                next_input_offset: input_times.len(),
                next_ts: params.last_ts + params.stride,
                remaining_output_batch_size: output_batch_size - 7
            },
            cursor
        );

        Ok(())
    }

    #[test]
    fn test_cursor_append_time_value_nulls() -> Result<()> {
        test_helpers::maybe_start_logging();
        let input_times =
            TimestampNanosecondArray::from(vec![None, None, Some(1000), Some(1100), Some(1200)]);
        let series = input_times.len();

        let params = GapFillParams {
            stride: 50,
            first_ts: 950,
            last_ts: 1250,
        };

        let output_batch_size = 10000;
        let mut cursor = Cursor {
            next_input_offset: 0,
            next_ts: params.first_ts,
            remaining_output_batch_size: output_batch_size,
        };
        let out_times = cursor.build_time_vec(&params, &[series], &input_times)?;
        assert_eq!(
            vec![
                None,
                None,
                Some(950),
                Some(1000),
                Some(1050),
                Some(1100),
                Some(1150),
                Some(1200),
                Some(1250)
            ],
            out_times
        );

        assert_eq!(
            Cursor {
                next_input_offset: input_times.len(),
                next_ts: params.last_ts + params.stride,
                remaining_output_batch_size: output_batch_size - 9
            },
            cursor
        );

        Ok(())
    }

    #[test]
    fn test_cursor_append_group_take() -> Result<()> {
        let input_times = TimestampNanosecondArray::from(vec![1000, 1100, 1200]);
        let series = input_times.len();

        let params = GapFillParams {
            stride: 50,
            first_ts: 950,
            last_ts: 1250,
        };

        let output_batch_size = 10000;
        let mut cursor = Cursor {
            next_input_offset: 0,
            next_ts: params.first_ts,
            remaining_output_batch_size: output_batch_size,
        };
        let take_idxs = cursor.build_group_take_vec(&params, &[series], &input_times)?;
        assert_eq!(vec![2; 7], take_idxs);

        assert_eq!(
            Cursor {
                next_input_offset: input_times.len(),
                next_ts: params.last_ts + params.stride,
                remaining_output_batch_size: output_batch_size - 7
            },
            cursor
        );

        Ok(())
    }

    #[test]
    fn test_cursor_append_aggr_take() -> Result<()> {
        let input_times = TimestampNanosecondArray::from(vec![1000, 1100, 1200]);
        let series = input_times.len();

        let params = GapFillParams {
            stride: 50,
            first_ts: 950,
            last_ts: 1250,
        };

        let output_batch_size = 10000;
        let mut cursor = Cursor {
            next_input_offset: 0,
            next_ts: params.first_ts,
            remaining_output_batch_size: output_batch_size,
        };

        let take_idxs = cursor.build_aggr_take_vec(&params, &[series], &input_times)?;
        assert_eq!(
            vec![None, Some(0), None, Some(1), None, Some(2), None],
            take_idxs
        );

        assert_eq!(
            Cursor {
                next_input_offset: input_times.len(),
                next_ts: params.last_ts + params.stride,
                remaining_output_batch_size: output_batch_size - 7
            },
            cursor
        );

        Ok(())
    }

    #[test]
    fn test_cursor_append_aggr_take_nulls() -> Result<()> {
        test_helpers::maybe_start_logging();
        let input_times =
            TimestampNanosecondArray::from(vec![None, None, Some(1000), Some(1100), Some(1200)]);
        let series = input_times.len();

        let params = GapFillParams {
            stride: 50,
            first_ts: 950,
            last_ts: 1250,
        };

        let output_batch_size = 10000;
        let mut cursor = Cursor {
            next_input_offset: 0,
            next_ts: params.first_ts,
            remaining_output_batch_size: output_batch_size,
        };

        let take_idxs = cursor.build_aggr_take_vec(&params, &[series], &input_times)?;
        assert_eq!(
            vec![
                Some(0), // corresopnds to null ts
                Some(1), // corresopnds to null ts
                None,
                Some(2),
                None,
                Some(3),
                None,
                Some(4),
                None
            ],
            take_idxs
        );

        assert_eq!(
            Cursor {
                next_input_offset: input_times.len(),
                next_ts: params.last_ts + params.stride,
                remaining_output_batch_size: output_batch_size - 9
            },
            cursor
        );

        Ok(())
    }

    #[test]
    fn test_cursor_multi_output_batch() -> Result<()> {
        let output_batch_size = 5;
        let params = GapFillParams {
            stride: 50,
            first_ts: 950,
            last_ts: 1350,
        };
        let input_times = TimestampNanosecondArray::from(vec![
            // 950
            1000, // 1050
            1100, // 1150 *split*
            1200, // 1250
            1300,
            // 1350
        ]);
        let series = input_times.len();

        let mut cursor = Cursor {
            next_input_offset: 0,
            next_ts: params.first_ts,
            remaining_output_batch_size: output_batch_size,
        };
        assert_eq!(
            9,
            cursor_series_output_row_count(&params, &cursor, series, &input_times)
        );

        assert_cursor_output(
            "first batch",
            &params,
            &input_times,
            &mut cursor,
            series,
            Expected {
                times: vec![Some(950), Some(1000), Some(1050), Some(1100), Some(1150)],
                group_take: vec![3, 3, 3, 3, 3],
                aggr_take: vec![None, Some(0), None, Some(1), None],
            },
        )?;

        assert_eq!(
            Cursor {
                next_input_offset: 2,
                next_ts: 1200,
                remaining_output_batch_size: 0
            },
            cursor
        );

        cursor.remaining_output_batch_size = output_batch_size;
        assert_cursor_output(
            "second batch",
            &params,
            &input_times,
            &mut cursor,
            series,
            Expected {
                times: vec![Some(1200), Some(1250), Some(1300), Some(1350)],
                group_take: vec![3, 3, 3, 3],
                aggr_take: vec![Some(2), None, Some(3), None],
            },
        )?;

        assert_eq!(
            Cursor {
                next_input_offset: input_times.len(),
                next_ts: params.last_ts + params.stride,
                remaining_output_batch_size: 1
            },
            cursor
        );

        Ok(())
    }

    #[test]
    fn test_cursor_multi_output_batch_with_nulls() -> Result<()> {
        test_helpers::maybe_start_logging();
        let output_batch_size = 6;
        let params = GapFillParams {
            stride: 50,
            first_ts: 950,
            last_ts: 1350,
        };
        let input_times = TimestampNanosecondArray::from(vec![
            None,
            None,
            // 950
            Some(1000),
            // 1050
            Some(1100),
            // split happens here
            // 1150
            Some(1200),
            // 1250
            Some(1300),
            // 1350
        ]);
        let series = input_times.len();
        let mut cursor = Cursor {
            next_input_offset: 0,
            next_ts: params.first_ts,
            remaining_output_batch_size: output_batch_size,
        };
        assert_eq!(
            11,
            cursor_series_output_row_count(&params, &cursor, series, &input_times)
        );

        // first two elements here are from input rows with null timestamps
        assert_cursor_output(
            "first batch",
            &params,
            &input_times,
            &mut cursor,
            series,
            Expected {
                times: vec![None, None, Some(950), Some(1000), Some(1050), Some(1100)],
                group_take: vec![5, 5, 5, 5, 5, 5],
                aggr_take: vec![Some(0), Some(1), None, Some(2), None, Some(3)],
            },
        )?;

        assert_eq!(
            Cursor {
                next_input_offset: 4,
                next_ts: 1150,
                remaining_output_batch_size: 0
            },
            cursor
        );

        cursor.remaining_output_batch_size = output_batch_size;
        assert_cursor_output(
            "second batch",
            &params,
            &input_times,
            &mut cursor,
            series,
            Expected {
                times: vec![Some(1150), Some(1200), Some(1250), Some(1300), Some(1350)],
                group_take: vec![5, 5, 5, 5, 5],
                aggr_take: vec![None, Some(4), None, Some(5), None],
            },
        )?;
        assert_eq!(
            Cursor {
                next_input_offset: input_times.len(),
                next_ts: params.last_ts + params.stride,
                remaining_output_batch_size: 1
            },
            cursor
        );
        Ok(())
    }

    #[test]
    fn test_cursor_multi_output_batch_with_more_nulls() -> Result<()> {
        // In this test case the output is split, but
        // the initial split gets all the null timestamps
        test_helpers::maybe_start_logging();
        let output_batch_size = 4;
        let params = GapFillParams {
            stride: 50,
            first_ts: 1000,
            last_ts: 1100,
        };
        let input_times = TimestampNanosecondArray::from(vec![
            None,
            None,
            None,
            None,
            // split happens here
            Some(1000),
            Some(1100),
        ]);
        let series = input_times.len();

        let mut cursor = Cursor {
            next_input_offset: 0,
            next_ts: params.first_ts,
            remaining_output_batch_size: output_batch_size,
        };
        assert_eq!(
            7,
            cursor_series_output_row_count(&params, &cursor, series, &input_times)
        );

        // This output batch is entirely from input rows that had null timestamps
        assert_cursor_output(
            "first batch",
            &params,
            &input_times,
            &mut cursor,
            series,
            Expected {
                times: vec![None, None, None, None],
                group_take: vec![5, 5, 5, 5],
                aggr_take: vec![Some(0), Some(1), Some(2), Some(3)],
            },
        )?;

        assert_eq!(
            Cursor {
                next_input_offset: 4,
                next_ts: 1000,
                remaining_output_batch_size: 0
            },
            cursor
        );

        cursor.remaining_output_batch_size = output_batch_size;
        assert_cursor_output(
            "second batch",
            &params,
            &input_times,
            &mut cursor,
            series,
            Expected {
                times: vec![Some(1000), Some(1050), Some(1100)],
                group_take: vec![5, 5, 5],
                aggr_take: vec![Some(4), None, Some(5)],
            },
        )?;

        assert_eq!(
            Cursor {
                next_input_offset: input_times.len(),
                next_ts: params.last_ts + params.stride,
                remaining_output_batch_size: 1
            },
            cursor,
        );

        Ok(())
    }

    #[test]
    fn test_cursor_multi_output_batch_with_yet_more_nulls() -> Result<()> {
        // In this test case the output is split, but
        // the initial split gets all null timestamps,
        // with the last null being the first element of the second batch.
        test_helpers::maybe_start_logging();
        let output_batch_size = 4;
        let params = GapFillParams {
            stride: 50,
            first_ts: 1000,
            last_ts: 1100,
        };
        let input_times = TimestampNanosecondArray::from(vec![
            None,
            None,
            None,
            None,
            // split happens here
            None,
            Some(1000),
            // 1050
            Some(1100),
        ]);
        let series = input_times.len();

        let mut cursor = Cursor {
            next_input_offset: 0,
            next_ts: params.first_ts,
            remaining_output_batch_size: output_batch_size,
        };
        assert_eq!(
            8,
            cursor_series_output_row_count(&params, &cursor, series, &input_times)
        );

        assert_cursor_output(
            "first batch",
            &params,
            &input_times,
            &mut cursor,
            series,
            Expected {
                times: vec![None, None, None, None],
                group_take: vec![6, 6, 6, 6],
                aggr_take: vec![Some(0), Some(1), Some(2), Some(3)],
            },
        )?;

        assert_eq!(
            Cursor {
                next_input_offset: 4,
                next_ts: 1000,
                remaining_output_batch_size: 0
            },
            cursor
        );

        cursor.remaining_output_batch_size = output_batch_size;
        assert_cursor_output(
            "second batch",
            &params,
            &input_times,
            &mut cursor,
            series,
            Expected {
                times: vec![None, Some(1000), Some(1050), Some(1100)],
                group_take: vec![6, 6, 6, 6],
                aggr_take: vec![Some(4), Some(5), None, Some(6)],
            },
        )?;

        assert_eq!(
            Cursor {
                next_input_offset: input_times.len(),
                next_ts: params.last_ts + params.stride,
                remaining_output_batch_size: 0
            },
            cursor,
        );

        Ok(())
    }

    #[test]
    fn test_cursor_multi_output_batch_three_batches() -> Result<()> {
        // A single series spread across three output batches.
        test_helpers::maybe_start_logging();
        let output_batch_size = 3;
        let params = GapFillParams {
            stride: 100,
            first_ts: 200,
            last_ts: 1000,
        };
        let input_times = TimestampNanosecondArray::from(vec![300, 500, 700, 800]);
        let series = input_times.len();

        let mut cursor = Cursor {
            next_input_offset: 0,
            next_ts: params.first_ts,
            remaining_output_batch_size: output_batch_size,
        };
        assert_eq!(
            9,
            cursor_series_output_row_count(&params, &cursor, series, &input_times)
        );

        assert_cursor_output(
            "first batch",
            &params,
            &input_times,
            &mut cursor,
            series,
            Expected {
                times: vec![Some(200), Some(300), Some(400)],
                group_take: vec![3, 3, 3],
                aggr_take: vec![None, Some(0), None],
            },
        )?;

        assert_eq!(
            Cursor {
                next_input_offset: 1,
                next_ts: 500,
                remaining_output_batch_size: 0
            },
            cursor
        );

        cursor.remaining_output_batch_size = output_batch_size;
        assert_cursor_output(
            "second batch",
            &params,
            &input_times,
            &mut cursor,
            series,
            Expected {
                times: vec![Some(500), Some(600), Some(700)],
                group_take: vec![3, 3, 3],
                aggr_take: vec![Some(1), None, Some(2)],
            },
        )?;

        assert_eq!(
            Cursor {
                next_input_offset: 3,
                next_ts: 800,
                remaining_output_batch_size: 0
            },
            cursor
        );

        cursor.remaining_output_batch_size = output_batch_size;
        assert_cursor_output(
            "third batch",
            &params,
            &input_times,
            &mut cursor,
            series,
            Expected {
                times: vec![Some(800), Some(900), Some(1000)],
                group_take: vec![3, 3, 3],
                aggr_take: vec![Some(3), None, None],
            },
        )?;

        assert_eq!(
            Cursor {
                next_input_offset: input_times.len(),
                next_ts: params.last_ts + params.stride,
                remaining_output_batch_size: 0
            },
            cursor
        );

        Ok(())
    }

    #[test]
    fn test_cursor_multi_output_batch_multi_series() -> Result<()> {
        // two series spread out over three series.
        let output_batch_size = 4;
        let params = GapFillParams {
            stride: 50,
            first_ts: 1000,
            last_ts: 1200,
        };
        let input_times = TimestampNanosecondArray::from(vec![
            1000, // 1050
            1100, // 1150
            1200, // next input series starts here
            // 1000
            1050, 1100, 1150, 1200,
            // end
        ]);
        let series0 = 3;
        let series1 = 7;

        let mut cursor = Cursor {
            next_input_offset: 0,
            next_ts: params.first_ts,
            remaining_output_batch_size: output_batch_size,
        };
        assert_eq!(
            5,
            cursor_series_output_row_count(&params, &cursor, series0, &input_times)
        );

        assert_cursor_output(
            "first batch, first series",
            &params,
            &input_times,
            &mut cursor,
            series0,
            Expected {
                times: vec![Some(1000), Some(1050), Some(1100), Some(1150)],
                group_take: vec![2, 2, 2, 2],
                aggr_take: vec![Some(0), None, Some(1), None],
            },
        )?;

        assert_eq!(
            Cursor {
                next_input_offset: 2,
                next_ts: 1200,
                remaining_output_batch_size: 0
            },
            cursor
        );

        cursor.remaining_output_batch_size = output_batch_size;
        assert_cursor_output(
            "second batch, first series",
            &params,
            &input_times,
            &mut cursor,
            series0,
            Expected {
                times: vec![Some(1200)],
                group_take: vec![2],
                aggr_take: vec![Some(2)],
            },
        )?;

        assert_eq!(
            Cursor {
                next_input_offset: 3,
                next_ts: params.last_ts + params.stride,
                remaining_output_batch_size: 3
            },
            cursor
        );

        cursor.next_ts = params.first_ts;

        assert_cursor_output(
            "second batch, second series",
            &params,
            &input_times,
            &mut cursor,
            series1,
            Expected {
                times: vec![Some(1000), Some(1050), Some(1100)],
                group_take: vec![6, 6, 6],
                aggr_take: vec![None, Some(3), Some(4)],
            },
        )?;

        assert_eq!(
            Cursor {
                next_input_offset: 5,
                next_ts: 1150,
                remaining_output_batch_size: 0
            },
            cursor
        );

        cursor.remaining_output_batch_size = output_batch_size;
        assert_cursor_output(
            "third batch, second series",
            &params,
            &input_times,
            &mut cursor,
            series1,
            Expected {
                times: vec![Some(1150), Some(1200)],
                group_take: vec![6, 6],
                aggr_take: vec![Some(5), Some(6)],
            },
        )?;

        assert_eq!(
            Cursor {
                next_input_offset: 7,
                next_ts: 1250,
                remaining_output_batch_size: 2
            },
            cursor
        );

        Ok(())
    }

    fn cursor_series_output_row_count(
        params: &GapFillParams,
        cursor: &Cursor,
        series_end: usize,
        input_times: &TimestampNanosecondArray,
    ) -> usize {
        let mut cursor = cursor.clone();
        cursor.count_series_rows(params, input_times, series_end)
    }

    struct Expected {
        times: Vec<Option<i64>>,
        group_take: Vec<u64>,
        aggr_take: Vec<Option<u64>>,
    }

    fn assert_cursor_output(
        desc: &'static str,
        params: &GapFillParams,
        input_times: &TimestampNanosecondArray,
        cursor: &mut Cursor,
        series_end: usize,
        expected: Expected,
    ) -> Result<()> {
        let actual_times = cursor
            .clone()
            .build_time_vec(params, &[series_end], input_times)?;
        assert_eq!(expected.times, actual_times, "{desc} times");

        let actual_group_take =
            cursor
                .clone()
                .build_group_take_vec(params, &[series_end], input_times)?;
        assert_eq!(expected.group_take, actual_group_take, "{desc} group take");

        let actual_aggr_take = cursor.build_aggr_take_vec(params, &[series_end], input_times)?;
        assert_eq!(expected.aggr_take, actual_aggr_take, "{desc} aggr take");

        Ok(())
    }
}
