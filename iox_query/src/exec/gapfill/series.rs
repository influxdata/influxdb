//! Contains the [SeriesAppender] type for encapsulating
//! logic for processing input to produce gap-filled tables.

use std::ops::Range;

use arrow::array::{Array, TimestampNanosecondArray};

use super::params::GapFillParams;

/// SeriesState tracks the current status of input being consumed
/// and timestamps being produced, as well as the amount of
/// space remaining in the current record batch to be output.
#[derive(Clone, Debug, PartialEq)]
pub(super) struct SeriesState {
    /// Where to read the next row from the input
    next_input_offset: usize,
    /// The next timestamp to be produced for the current series
    next_ts: i64,
    /// How many rows may be output before we need to start a new record batch.
    remaining_output_batch_size: usize,
}

impl SeriesState {
    /// Initialize a [SeriesState] at the beginning of an input record batch,
    /// with the given amount of space in the output batch.
    pub fn new(params: &GapFillParams, remaining_output_batch_size: usize) -> Self {
        Self {
            next_input_offset: 0,
            next_ts: params.first_ts,
            remaining_output_batch_size,
        }
    }

    /// The number of rows that the current output batch can accomodate.
    pub fn remaining_output_batch_size(&self) -> usize {
        self.remaining_output_batch_size
    }

    /// Update state to indicate there is a fresh output batch
    /// with the given amount of space.
    pub fn fresh_output_batch(&mut self, sz: usize) {
        self.remaining_output_batch_size = sz
    }

    /// Returns true if there are no more rows to produce for the given series.
    pub fn series_is_done(&self, params: &GapFillParams, series: &SeriesAppender) -> bool {
        let done = self.next_ts > params.last_ts;
        if done {
            debug_assert!(self.next_input_offset >= series.input_end)
        }
        done
    }

    /// A series was just completed, so the next timestamp should be
    /// the first one in the range.
    pub fn fresh_series(&mut self, params: &GapFillParams) {
        self.next_ts = params.first_ts
    }
}

/// Encapsulates logic for generating rows that fill
/// in gaps.
#[derive(Clone, Debug)]
pub(super) struct SeriesAppender {
    /// The exclusive offset of the end of data for this series in the input `RecordBatch`.
    input_end: usize,
}

impl SeriesAppender {
    /// Creates a new `SeriesAppender`.
    pub fn new_with_input_end_offset(input_end: usize) -> Self {
        Self { input_end }
    }

    /// Returns the count of output rows that have not yet
    /// been built for this series. Used to determine how many series
    /// will fit into a batch.
    /// This method accepts a [SeriesState] which will be updated with
    /// the next timestamp and input offset upon return.
    pub fn remaining_output_row_count(
        &self,
        params: &GapFillParams,
        input_times: &TimestampNanosecondArray,
        mut series_state: SeriesState,
    ) -> (usize, SeriesState) {
        let null_ts_count = {
            let len = self.input_end - series_state.next_input_offset;
            let slice = input_times.slice(series_state.next_input_offset, len);
            slice.null_count()
        };

        let count = null_ts_count + params.valid_row_count(series_state.next_ts);
        series_state.next_input_offset = self.input_end;
        series_state.next_ts = params.first_ts;
        (count, series_state)
    }

    /// Appends to the `times` vector that will be come the time array for the output.
    /// Returns an updatd [SeriesState].
    pub fn append_time_values(
        &self,
        params: &GapFillParams,
        input_times: &TimestampNanosecondArray,
        series_state: SeriesState,
        times: &mut Vec<Option<i64>>,
    ) -> SeriesState {
        self.append_items(
            params,
            input_times,
            series_state,
            |row_status| match row_status {
                RowStatus::NullTimestamp(_) => times.push(None),
                RowStatus::Present(_, ts) => times.push(Some(ts)),
                RowStatus::Missing(ts) => times.push(Some(ts)),
            },
        )
    }

    /// Appends to the `take_idxs` vector values suitable for creating
    /// the output group column using the Arrow `take` kernel.
    /// Returns an updatd [SeriesState].
    pub fn append_group_take(
        &self,
        params: &GapFillParams,
        input_times: &TimestampNanosecondArray,
        series_state: SeriesState,
        take_idxs: &mut Vec<u64>,
    ) -> SeriesState {
        // Group columns will have the same value for all the rows for this group.
        let last_input_offset = (self.input_end - 1) as u64;
        self.append_items(
            params,
            input_times,
            series_state,
            |row_status| match row_status {
                RowStatus::NullTimestamp(_) => take_idxs.push(last_input_offset),
                RowStatus::Present(_, _) => take_idxs.push(last_input_offset),
                RowStatus::Missing(_) => take_idxs.push(last_input_offset),
            },
        )
    }

    /// Appends values to `take_idxs` suitable for creating the corresponding
    /// aggregate column using the Arrow `take` kernel.
    /// Returns an updated [SeriesState].
    pub fn append_aggr_take(
        &self,
        params: &GapFillParams,
        input_times: &TimestampNanosecondArray,
        series_state: SeriesState,
        take_idxs: &mut Vec<Option<u64>>,
    ) -> SeriesState {
        self.append_items(
            params,
            input_times,
            series_state,
            |row_status| match row_status {
                RowStatus::NullTimestamp(input_offset) => take_idxs.push(Some(input_offset as u64)),
                RowStatus::Present(input_offset, _) => take_idxs.push(Some(input_offset as u64)),
                RowStatus::Missing(_) => take_idxs.push(None),
            },
        )
    }

    /// A helper method that iterates over the elemnts
    /// in the time array and invokes the closure that
    /// will produce output.
    fn append_items<F>(
        &self,
        params: &GapFillParams,
        input_times: &TimestampNanosecondArray,
        mut series_state: SeriesState,
        mut append: F,
    ) -> SeriesState
    where
        F: FnMut(RowStatus),
    {
        // If there are any null timestamps for this group, they will be first.
        // These rows can just be copied into the output.
        // Append the corresponding values.
        while series_state.remaining_output_batch_size > 0
            && series_state.next_input_offset < self.input_end
            && input_times.is_null(series_state.next_input_offset)
        {
            append(RowStatus::NullTimestamp(series_state.next_input_offset));
            series_state.remaining_output_batch_size -= 1;
            series_state.next_input_offset += 1;
        }

        let output_row_count = std::cmp::min(
            params.valid_row_count(series_state.next_ts),
            series_state.remaining_output_batch_size,
        );
        if output_row_count == 0 {
            return series_state;
        }

        let input_range = self.input_range(&series_state);
        let mut next_ts = series_state.next_ts;
        let last_ts = series_state.next_ts + (output_row_count - 1) as i64 * params.stride;

        let mut next_input_offset = input_range.start;
        let next_input_offset = loop {
            if next_input_offset >= input_range.end {
                break next_input_offset;
            }
            let in_ts = input_times.value(next_input_offset);
            if in_ts > last_ts {
                break next_input_offset;
            }
            while next_ts < in_ts {
                append(RowStatus::Missing(next_ts));
                next_ts += params.stride;
            }
            append(RowStatus::Present(next_input_offset, next_ts));
            next_ts += params.stride;
            next_input_offset += 1;
        };

        // Add any additional missing values after the last of the input.
        while next_ts <= last_ts {
            append(RowStatus::Missing(next_ts));
            next_ts += params.stride;
        }

        series_state.next_input_offset = next_input_offset;
        series_state.next_ts = last_ts + params.stride;
        series_state.remaining_output_batch_size -= output_row_count;
        series_state
    }

    /// Helper function to get the input range remaining for this series.
    fn input_range(&self, series_state: &SeriesState) -> Range<usize> {
        Range {
            start: series_state.next_input_offset,
            end: self.input_end,
        }
    }
}

/// The state of an input row relative to gap-filled output.
enum RowStatus {
    /// This row had a null timestamp in the input.
    /// This could happen if an outer join
    /// is input to gap filling, for example.
    /// The argument is the offset of the missing timestamp in the input.
    NullTimestamp(usize),
    /// A row with this timestamp is present in the input.
    /// The arguments are the input offset, and the corresponding timestamp.
    Present(usize, i64),
    /// A row with this timestamp is missing from the input.
    /// It is a gap that needs to be filled.
    /// The argument is the missing timestamp.
    Missing(i64),
}

#[cfg(test)]
mod tests {
    use arrow::array::TimestampNanosecondArray;
    use datafusion::error::Result;

    use crate::exec::gapfill::params::GapFillParams;

    use super::{SeriesAppender, SeriesState};

    #[test]
    fn test_append_time_values() -> Result<()> {
        test_helpers::maybe_start_logging();
        let input_times = TimestampNanosecondArray::from(vec![1000, 1100, 1200]);
        let series = SeriesAppender::new_with_input_end_offset(input_times.len());

        let params = GapFillParams {
            stride: 50,
            first_ts: 950,
            last_ts: 1250,
        };

        let mut out_times = Vec::new();
        let ss = series.append_time_values(
            &params,
            &input_times,
            SeriesState::new(&params, 10000),
            &mut out_times,
        );
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
            SeriesState {
                next_input_offset: input_times.len(),
                next_ts: params.last_ts + params.stride,
                remaining_output_batch_size: 10000 - 7,
            },
            ss
        );

        Ok(())
    }

    #[test]
    fn test_append_time_value_nulls() -> Result<()> {
        test_helpers::maybe_start_logging();
        let input_times =
            TimestampNanosecondArray::from(vec![None, None, Some(1000), Some(1100), Some(1200)]);
        let series = SeriesAppender::new_with_input_end_offset(input_times.len());

        let params = GapFillParams {
            stride: 50,
            first_ts: 950,
            last_ts: 1250,
        };

        let mut out_times = Vec::new();
        let ss = series.append_time_values(
            &params,
            &input_times,
            SeriesState::new(&params, 10000),
            &mut out_times,
        );
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
            SeriesState {
                next_input_offset: input_times.len(),
                next_ts: params.last_ts + params.stride,
                remaining_output_batch_size: 10000 - 9,
            },
            ss
        );

        Ok(())
    }

    #[test]
    fn test_append_group_take() -> Result<()> {
        let input_times = TimestampNanosecondArray::from(vec![1000, 1100, 1200]);
        let series = SeriesAppender::new_with_input_end_offset(input_times.len());

        let params = GapFillParams {
            stride: 50,
            first_ts: 950,
            last_ts: 1250,
        };

        let mut take_idxs = Vec::new();
        let ss = series.append_group_take(
            &params,
            &input_times,
            SeriesState::new(&params, 10000),
            &mut take_idxs,
        );
        assert_eq!(vec![2; 7], take_idxs);

        assert_eq!(
            SeriesState {
                next_input_offset: input_times.len(),
                next_ts: params.last_ts + params.stride,
                remaining_output_batch_size: 10000 - 7,
            },
            ss
        );

        Ok(())
    }

    #[test]
    fn test_append_aggr_take() -> Result<()> {
        let input_times = TimestampNanosecondArray::from(vec![1000, 1100, 1200]);
        let series = SeriesAppender::new_with_input_end_offset(input_times.len());

        let params = GapFillParams {
            stride: 50,
            first_ts: 950,
            last_ts: 1250,
        };

        let mut take_idxs = Vec::new();
        let ss = series.append_aggr_take(
            &params,
            &input_times,
            SeriesState::new(&params, 10000),
            &mut take_idxs,
        );
        assert_eq!(
            vec![None, Some(0), None, Some(1), None, Some(2), None],
            take_idxs
        );

        assert_eq!(
            SeriesState {
                next_input_offset: input_times.len(),
                next_ts: params.last_ts + params.stride,
                remaining_output_batch_size: 10000 - 7,
            },
            ss
        );

        Ok(())
    }

    #[test]
    fn test_append_aggr_take_nulls() -> Result<()> {
        test_helpers::maybe_start_logging();
        let input_times =
            TimestampNanosecondArray::from(vec![None, None, Some(1000), Some(1100), Some(1200)]);
        let series = SeriesAppender::new_with_input_end_offset(input_times.len());

        let params = GapFillParams {
            stride: 50,
            first_ts: 950,
            last_ts: 1250,
        };

        let mut take_idxs = Vec::new();
        let ss = series.append_aggr_take(
            &params,
            &input_times,
            SeriesState::new(&params, 10000),
            &mut take_idxs,
        );
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
            SeriesState {
                next_input_offset: input_times.len(),
                next_ts: params.last_ts + params.stride,
                remaining_output_batch_size: 10000 - 9,
            },
            ss
        );

        Ok(())
    }

    fn series_output_row_count(
        series: &SeriesAppender,
        params: &GapFillParams,
        input_times: &TimestampNanosecondArray,
        series_state: &SeriesState,
    ) -> usize {
        let (cnt, _) = series.remaining_output_row_count(params, input_times, series_state.clone());
        cnt
    }

    #[test]
    fn test_multi_output_batch() -> Result<()> {
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
        let series = SeriesAppender::new_with_input_end_offset(input_times.len());
        let series_state = SeriesState::new(&params, output_batch_size);
        assert_eq!(
            9,
            series_output_row_count(&series, &params, &input_times, &series_state)
        );

        let mut series_state = assert_series_output(
            "first_batch ",
            &params,
            &input_times,
            series_state,
            &series,
            Expected {
                times: vec![Some(950), Some(1000), Some(1050), Some(1100), Some(1150)],
                group_take: vec![3, 3, 3, 3, 3],
                aggr_take: vec![None, Some(0), None, Some(1), None],
            },
        );

        assert_eq!(
            SeriesState {
                next_input_offset: 2,
                next_ts: 1200,
                remaining_output_batch_size: 0,
            },
            series_state
        );
        assert!(!series_state.series_is_done(&params, &series));
        series_state.fresh_output_batch(output_batch_size);

        let series_state = assert_series_output(
            "second batch",
            &params,
            &input_times,
            series_state,
            &series,
            Expected {
                times: vec![Some(1200), Some(1250), Some(1300), Some(1350)],
                group_take: vec![3, 3, 3, 3],
                aggr_take: vec![Some(2), None, Some(3), None],
            },
        );

        assert_eq!(
            SeriesState {
                next_input_offset: input_times.len(),
                next_ts: params.last_ts + params.stride,
                remaining_output_batch_size: 1,
            },
            series_state
        );
        assert!(series_state.series_is_done(&params, &series));

        Ok(())
    }

    #[test]
    fn test_multi_output_batch_with_nulls() -> Result<()> {
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
        let series = SeriesAppender::new_with_input_end_offset(input_times.len());
        let series_state = SeriesState::new(&params, output_batch_size);
        assert_eq!(
            11,
            series_output_row_count(&series, &params, &input_times, &series_state)
        );

        // first two elements here are from input rows with null timestamps
        let mut series_state = assert_series_output(
            "first output batch",
            &params,
            &input_times,
            series_state,
            &series,
            Expected {
                times: vec![None, None, Some(950), Some(1000), Some(1050), Some(1100)],
                group_take: vec![5, 5, 5, 5, 5, 5],
                aggr_take: vec![Some(0), Some(1), None, Some(2), None, Some(3)],
            },
        );
        series_state.fresh_output_batch(output_batch_size);
        let series_state = assert_series_output(
            "second output batch",
            &params,
            &input_times,
            series_state,
            &series,
            Expected {
                times: vec![Some(1150), Some(1200), Some(1250), Some(1300), Some(1350)],
                group_take: vec![5, 5, 5, 5, 5],
                aggr_take: vec![None, Some(4), None, Some(5), None],
            },
        );
        assert_eq!(
            SeriesState {
                next_input_offset: input_times.len(),
                next_ts: params.last_ts + params.stride,
                remaining_output_batch_size: 1,
            },
            series_state
        );
        Ok(())
    }

    #[test]
    fn test_multi_output_batch_with_more_nulls() -> Result<()> {
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
        let series = SeriesAppender::new_with_input_end_offset(input_times.len());
        let series_state = SeriesState::new(&params, output_batch_size);
        assert_eq!(
            7,
            series_output_row_count(&series, &params, &input_times, &series_state)
        );

        // This output batch is entirely from input rows that had null timestamps
        let mut series_state = assert_series_output(
            "first output batch",
            &params,
            &input_times,
            series_state,
            &series,
            Expected {
                times: vec![None, None, None, None],
                group_take: vec![5, 5, 5, 5],
                aggr_take: vec![Some(0), Some(1), Some(2), Some(3)],
            },
        );
        series_state.fresh_output_batch(output_batch_size);
        let series_state = assert_series_output(
            "second output batch",
            &params,
            &input_times,
            series_state,
            &series,
            Expected {
                times: vec![Some(1000), Some(1050), Some(1100)],
                group_take: vec![5, 5, 5],
                aggr_take: vec![Some(4), None, Some(5)],
            },
        );
        assert_eq!(
            SeriesState {
                next_input_offset: input_times.len(),
                next_ts: params.last_ts + params.stride,
                remaining_output_batch_size: 1,
            },
            series_state
        );
        Ok(())
    }

    #[test]
    fn test_multi_output_batch_with_yet_more_nulls() -> Result<()> {
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
        let series = SeriesAppender::new_with_input_end_offset(input_times.len());
        let series_state = SeriesState::new(&params, output_batch_size);
        assert_eq!(
            8,
            series_output_row_count(&series, &params, &input_times, &series_state),
            "unsplit series"
        );

        let mut series_state = assert_series_output(
            "first batch",
            &params,
            &input_times,
            series_state,
            &series,
            Expected {
                times: vec![None, None, None, None],
                group_take: vec![6, 6, 6, 6],
                aggr_take: vec![Some(0), Some(1), Some(2), Some(3)],
            },
        );
        series_state.fresh_output_batch(output_batch_size);
        let series_state = assert_series_output(
            "second batch",
            &params,
            &input_times,
            series_state,
            &series,
            Expected {
                times: vec![None, Some(1000), Some(1050), Some(1100)],
                group_take: vec![6, 6, 6, 6],
                aggr_take: vec![Some(4), Some(5), None, Some(6)],
            },
        );
        assert_eq!(
            SeriesState {
                next_input_offset: input_times.len(),
                next_ts: params.last_ts + params.stride,
                remaining_output_batch_size: 0,
            },
            series_state
        );
        Ok(())
    }

    #[test]
    fn test_multi_output_batch_three_batches() -> Result<()> {
        // A single series spread across three output batches.
        test_helpers::maybe_start_logging();
        let output_batch_size = 3;
        let params = GapFillParams {
            stride: 100,
            first_ts: 200,
            last_ts: 1000,
        };
        let input_times = TimestampNanosecondArray::from(vec![300, 500, 700, 800]);
        let series = SeriesAppender::new_with_input_end_offset(input_times.len());
        let series_state = SeriesState::new(&params, output_batch_size);
        assert_eq!(
            9,
            series_output_row_count(&series, &params, &input_times, &series_state)
        );

        let mut series_state = assert_series_output(
            "series0",
            &params,
            &input_times,
            series_state,
            &series,
            Expected {
                times: vec![Some(200), Some(300), Some(400)],
                group_take: vec![3, 3, 3],
                aggr_take: vec![None, Some(0), None],
            },
        );
        series_state.fresh_output_batch(output_batch_size);
        let mut series_state = assert_series_output(
            "series1",
            &params,
            &input_times,
            series_state,
            &series,
            Expected {
                times: vec![Some(500), Some(600), Some(700)],
                group_take: vec![3, 3, 3],
                aggr_take: vec![Some(1), None, Some(2)],
            },
        );
        series_state.fresh_output_batch(output_batch_size);
        let series_state = assert_series_output(
            "series2",
            &params,
            &input_times,
            series_state,
            &series,
            Expected {
                times: vec![Some(800), Some(900), Some(1000)],
                group_take: vec![3, 3, 3],
                aggr_take: vec![Some(3), None, None],
            },
        );

        assert_eq!(
            SeriesState {
                next_input_offset: input_times.len(),
                next_ts: params.last_ts + params.stride,
                remaining_output_batch_size: 0,
            },
            series_state
        );

        Ok(())
    }

    #[test]
    fn test_multi_output_batch_multi_series() -> Result<()> {
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
        let series0 = SeriesAppender::new_with_input_end_offset(3);
        let series1 = SeriesAppender::new_with_input_end_offset(7);
        let series_state = SeriesState::new(&params, output_batch_size);
        assert_eq!(
            5,
            series_output_row_count(&series0, &params, &input_times, &series_state)
        );

        let mut series_state = assert_series_output(
            "first output batch, first series",
            &params,
            &input_times,
            series_state,
            &series0,
            Expected {
                times: vec![Some(1000), Some(1050), Some(1100), Some(1150)],
                group_take: vec![2, 2, 2, 2],
                aggr_take: vec![Some(0), None, Some(1), None],
            },
        );

        assert_eq!(
            SeriesState {
                next_input_offset: 2,
                next_ts: 1200,
                remaining_output_batch_size: 0,
            },
            series_state
        );
        assert!(!series_state.series_is_done(&params, &series0));
        assert!(series_state.remaining_output_batch_size == 0);
        series_state.fresh_output_batch(output_batch_size);

        let mut series_state = assert_series_output(
            "second batch, first series",
            &params,
            &input_times,
            series_state,
            &series0,
            Expected {
                times: vec![Some(1200)],
                group_take: vec![2],
                aggr_take: vec![Some(2)],
            },
        );

        assert_eq!(
            SeriesState {
                next_input_offset: 3,
                next_ts: 1250,
                remaining_output_batch_size: 3,
            },
            series_state
        );
        assert!(series_state.series_is_done(&params, &series0));
        assert!(!series_state.remaining_output_batch_size > 0);
        series_state.fresh_series(&params);

        let mut series_state = assert_series_output(
            "second batch, second series",
            &params,
            &input_times,
            series_state,
            &series1,
            Expected {
                times: vec![Some(1000), Some(1050), Some(1100)],
                group_take: vec![6, 6, 6],
                aggr_take: vec![None, Some(3), Some(4)],
            },
        );

        assert_eq!(
            SeriesState {
                next_input_offset: 5,
                next_ts: 1150,
                remaining_output_batch_size: 0,
            },
            series_state
        );
        assert!(!series_state.series_is_done(&params, &series0));
        assert!(series_state.remaining_output_batch_size == 0);
        series_state.fresh_output_batch(output_batch_size);

        let series_state = assert_series_output(
            "third batch, second series",
            &params,
            &input_times,
            series_state,
            &series1,
            Expected {
                times: vec![Some(1150), Some(1200)],
                group_take: vec![6, 6],
                aggr_take: vec![Some(5), Some(6)],
            },
        );

        assert_eq!(
            SeriesState {
                next_input_offset: 7,
                next_ts: 1250,
                remaining_output_batch_size: 2,
            },
            series_state
        );
        assert!(series_state.series_is_done(&params, &series1));
        Ok(())
    }

    struct Expected {
        times: Vec<Option<i64>>,
        group_take: Vec<u64>,
        aggr_take: Vec<Option<u64>>,
    }

    fn assert_series_output(
        desc: &'static str,
        params: &GapFillParams,
        input_times: &TimestampNanosecondArray,
        series_state: SeriesState,
        series: &SeriesAppender,
        expected: Expected,
    ) -> SeriesState {
        let mut actual_times = vec![];
        series.append_time_values(params, input_times, series_state.clone(), &mut actual_times);
        assert_eq!(expected.times, actual_times, "{desc} times");

        let mut actual_group_take = vec![];
        series.append_group_take(
            params,
            input_times,
            series_state.clone(),
            &mut actual_group_take,
        );
        assert_eq!(expected.group_take, actual_group_take, "{desc} group take");

        let mut actual_aggr_take = vec![];
        let series_state =
            series.append_aggr_take(params, input_times, series_state, &mut actual_aggr_take);
        assert_eq!(expected.aggr_take, actual_aggr_take, "{desc} aggr take");

        series_state
    }
}
