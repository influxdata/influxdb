//! Contains the [Series] type for encapsulating
//! logic for generating gap-filled tables.
use std::ops::Range;

use arrow::array::{Array, TimestampNanosecondArray};

use super::algo::GapFillParams;

/// Encapsulates logic for generating rows that fill
/// in gaps.
pub(super) struct Series {
    /// The offsets of the data for this series in the input `RecordBatch`.
    /// I.e., the offsets where the group columns change.
    input_range: Range<usize>,
    /// If there are any values in the time column that are null, they will
    /// be counted here.
    null_ts_count: usize,
}

impl Series {
    /// Creates a new series.
    pub fn new(input_range: Range<usize>, input_times: &TimestampNanosecondArray) -> Self {
        // Find out if there are any null timestamps at the beginning of this
        // group. Probably there will be very few of these, so just do a
        // linear search of the time array.
        let null_ts_count = {
            let len = input_range.end - input_range.start;
            let slice = input_times.slice(input_range.start, len);
            slice.null_count()
        };
        Self {
            input_range,
            null_ts_count,
        }
    }

    /// Returns the total row count for this series, including any
    /// rows that have null timestamps.
    pub fn total_output_row_count(&self, params: &GapFillParams) -> usize {
        self.null_ts_count + params.valid_row_count(..)
    }

    /// Appends to the `times` vector that will be come the time array for the output.
    pub fn append_time_values(&self, params: &GapFillParams, times: &mut Vec<Option<i64>>) {
        for _ in 0..self.null_ts_count {
            times.push(None);
        }
        for ts in params.iter_times() {
            times.push(Some(ts));
        }
    }

    /// Appends to the `take_idxs` vector values suitable for creating
    /// the output group column using the Arrow `take` kernel.
    pub fn append_group_take(&self, params: &GapFillParams, take_idxs: &mut Vec<u64>) {
        let count = self.null_ts_count + params.iter_times().count();
        for _ in 0..count {
            take_idxs.push(self.input_range.start as u64);
        }
    }

    fn null_ts_range(&self) -> Range<usize> {
        Range {
            start: self.input_range.start,
            end: self.input_range.start + self.null_ts_count,
        }
    }

    fn valid_ts_range(&self) -> Range<usize> {
        Range {
            start: self.input_range.start + self.null_ts_count,
            end: self.input_range.end,
        }
    }

    /// Appends `take_idxs` values suitable for creating the corresponding
    /// aggregate column using the Arrow `take` kernel.
    pub fn append_aggr_take(
        &self,
        params: &GapFillParams,
        input_times: &TimestampNanosecondArray,
        take_idxs: &mut Vec<Option<u64>>,
    ) {
        for i in self.null_ts_range() {
            take_idxs.push(Some(i as u64));
        }

        let input_range = self.valid_ts_range();
        let mut out_ts = params.first_ts;
        for ix in input_range {
            let in_ts = input_times.value(ix);
            while out_ts < in_ts {
                take_idxs.push(None);
                out_ts += params.stride;
            }
            take_idxs.push(Some(ix as u64));
            out_ts += params.stride;
        }
        while out_ts <= params.last_ts {
            take_idxs.push(None);
            out_ts += params.stride;
        }
    }
}

#[cfg(test)]
mod tests {
    use std::ops::Range;

    use arrow::array::TimestampNanosecondArray;
    use datafusion::error::Result;

    use crate::exec::gapfill::algo::GapFillParams;

    use super::Series;

    #[test]
    fn test_append_time_values() -> Result<()> {
        let input_times = TimestampNanosecondArray::from(vec![1000, 1100, 1200]);
        let series = Series::new(Range { start: 0, end: 3 }, &input_times);

        let params = GapFillParams {
            stride: 50,
            first_ts: 950,
            last_ts: 1250,
        };

        let mut out_times = Vec::new();
        series.append_time_values(&params, &mut out_times);
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

        Ok(())
    }

    #[test]
    fn test_append_time_value_nulls() -> Result<()> {
        let input_times =
            TimestampNanosecondArray::from(vec![None, None, Some(1000), Some(1100), Some(1200)]);
        let series = Series::new(Range { start: 0, end: 3 }, &input_times);

        let params = GapFillParams {
            stride: 50,
            first_ts: 950,
            last_ts: 1250,
        };

        let mut out_times = Vec::new();
        series.append_time_values(&params, &mut out_times);
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

        Ok(())
    }

    #[test]
    fn test_append_group_take() -> Result<()> {
        let input_times = TimestampNanosecondArray::from(vec![1000, 1100, 1200]);
        let series = Series::new(Range { start: 0, end: 3 }, &input_times);

        let params = GapFillParams {
            stride: 50,
            first_ts: 950,
            last_ts: 1250,
        };

        let mut take_idxs = Vec::new();
        series.append_group_take(&params, &mut take_idxs);
        assert_eq!(vec![0; 7], take_idxs);

        Ok(())
    }

    #[test]
    fn test_append_aggr_take() -> Result<()> {
        let input_times = TimestampNanosecondArray::from(vec![1000, 1100, 1200]);
        let series = Series::new(Range { start: 0, end: 3 }, &input_times);

        let params = GapFillParams {
            stride: 50,
            first_ts: 950,
            last_ts: 1250,
        };

        let mut take_idxs = Vec::new();
        series.append_aggr_take(&params, &input_times, &mut take_idxs);
        assert_eq!(
            vec![None, Some(0), None, Some(1), None, Some(2), None],
            take_idxs
        );
        Ok(())
    }

    #[test]
    fn test_append_aggr_take_nulls() -> Result<()> {
        test_helpers::maybe_start_logging();
        let input_times =
            TimestampNanosecondArray::from(vec![None, None, Some(1000), Some(1100), Some(1200)]);
        let series = Series::new(
            Range {
                start: 0,
                end: input_times.len(),
            },
            &input_times,
        );

        let params = GapFillParams {
            stride: 50,
            first_ts: 950,
            last_ts: 1250,
        };

        let mut take_idxs = Vec::new();
        series.append_aggr_take(&params, &input_times, &mut take_idxs);
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
        Ok(())
    }
}
