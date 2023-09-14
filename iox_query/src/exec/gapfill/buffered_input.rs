//! Logic for buffering record batches for gap filling.

use std::sync::Arc;

use arrow::{
    array::{as_struct_array, ArrayRef},
    datatypes::DataType,
    record_batch::RecordBatch,
    row::{RowConverter, Rows, SortField},
};
use datafusion::error::{DataFusionError, Result};
use hashbrown::HashSet;

use super::{params::GapFillParams, FillStrategy};

/// Encapsulate the logic around how to buffer input records.
///
/// If there are no columns with [`FillStrategy::LinearInterpolate`], then
/// we need to buffer up to the last input row that might appear in the output, plus
/// one additional row.
///
/// However, if there are columns filled via interpolation, then we need
/// to ensure that we read ahead far enough to a non-null value, or a change
/// of group columns, in the columns being interpolated.
///
/// [`FillStrategy::LinearInterpolate`]: super::FillStrategy::LinearInterpolate
/// [`GapFillStream`]: super::stream::GapFillStream
pub(super) struct BufferedInput {
    /// Indexes of group columns in the schema (not including time).
    group_cols: Vec<usize>,
    /// Indexes of aggregate columns filled via interpolation.
    interpolate_cols: Vec<usize>,
    /// Buffered records from the input stream.
    batches: Vec<RecordBatch>,
    /// When gap filling with interpolated values, this row converter
    /// is used to compare rows to see if group columns have changed.
    row_converter: Option<RowConverter>,
    /// When gap filling with interpolated values, cache a row-oriented
    /// representation of the last row that may appear in the output so
    /// it doesn't need to be computed more than once.
    last_output_row: Option<Rows>,
}

impl BufferedInput {
    pub(super) fn new(params: &GapFillParams, group_cols: Vec<usize>) -> Self {
        let interpolate_cols = params
            .fill_strategy
            .iter()
            .filter_map(|(col_offset, fs)| {
                (fs == &FillStrategy::LinearInterpolate).then_some(*col_offset)
            })
            .collect::<Vec<usize>>();
        Self {
            group_cols,
            interpolate_cols,
            batches: vec![],
            row_converter: None,
            last_output_row: None,
        }
    }
    /// Add a new batch of buffered records from the input stream.
    pub(super) fn push(&mut self, batch: RecordBatch) {
        self.batches.push(batch);
    }

    /// Transfer ownership of the buffered record batches to the caller for
    /// processing.
    pub(super) fn take(&mut self) -> Vec<RecordBatch> {
        self.last_output_row = None;
        std::mem::take(&mut self.batches)
    }

    /// Determine if we need more input before we start processing.
    pub(super) fn need_more(&mut self, last_output_row_offset: usize) -> Result<bool> {
        let record_count: usize = self.batches.iter().map(|rb| rb.num_rows()).sum();
        // min number of rows needed is the number of rows up to and including
        // the last row that may appear in the output, plus one more row.
        let min_needed = last_output_row_offset + 2;

        if record_count < min_needed {
            return Ok(true);
        } else if self.interpolate_cols.is_empty() {
            return Ok(false);
        }

        // Check to see if the last row that might appear in the output
        // has a different group column values than the last buffered row.
        // If they are different, then we have enough input to start.
        let (last_output_batch_offset, last_output_row_offset) = self
            .find_row_idx(last_output_row_offset)
            .expect("checked record count");
        if self.group_columns_changed((last_output_batch_offset, last_output_row_offset))? {
            return Ok(false);
        }

        // Now check if there are non-null values in the columns being interpolated.
        // We skip over the batches that come before the one that contains the last
        // possible output row. We start with the last buffered batch, so we can avoid
        // having to slice unless necessary.
        let mut cols_that_need_more =
            HashSet::<usize>::from_iter(self.interpolate_cols.iter().cloned());
        let mut to_remove = vec![];
        for (i, batch) in self
            .batches
            .iter()
            .enumerate()
            .skip(last_output_batch_offset)
            .rev()
        {
            for col_offset in cols_that_need_more.clone() {
                // If this is the batch containing the last possible output row, slice the
                // array so we are just looking at that value and the ones after.
                let array = batch.column(col_offset);
                let array = if i == last_output_batch_offset {
                    let length = array.len() - last_output_row_offset;
                    batch
                        .column(col_offset)
                        .slice(last_output_row_offset, length)
                } else {
                    Arc::clone(array)
                };

                let struct_value_col = if let DataType::Struct(fields) = array.data_type().clone() {
                    fields.find("value").map(|(n, _)| n)
                } else {
                    None
                };

                match struct_value_col {
                    Some(n) => {
                        let value_array = as_struct_array(&array).column(n);
                        if array.null_count() < array.len()
                            && value_array.null_count() < value_array.len()
                        {
                            to_remove.push(col_offset);
                        }
                    }
                    None => {
                        if array.null_count() < array.len() {
                            to_remove.push(col_offset);
                        }
                    }
                }
            }

            to_remove.drain(..).for_each(|c| {
                cols_that_need_more.remove(&c);
            });
            if cols_that_need_more.is_empty() {
                break;
            }
        }

        Ok(!cols_that_need_more.is_empty())
    }

    /// Check to see if the group column values have changed between the last row
    /// that may be in the output and the last buffered input row.
    ///
    /// This method uses the row-oriented representation of Arrow data from [`arrow::row`] to
    /// compare rows in different record batches.
    ///
    /// [`arrow::row`]: https://docs.rs/arrow-row/36.0.0/arrow_row/index.html
    fn group_columns_changed(&mut self, last_output_row_idx: (usize, usize)) -> Result<bool> {
        if self.group_cols.is_empty() {
            return Ok(false);
        }

        let last_buffered_row_idx = self.last_buffered_row_idx();
        if last_output_row_idx == last_buffered_row_idx {
            // the output row is also the last buffered row,
            // so there is nothing to compare.
            return Ok(false);
        }

        let last_input_rows = self.convert_row(self.last_buffered_row_idx())?;
        let last_row_in_output = self.last_output_row(last_output_row_idx)?;

        Ok(last_row_in_output.row(0) != last_input_rows.row(0))
    }

    /// Get a row converter for comparing records. Keep it in [`Self::row_converter`]
    /// to avoid creating it multiple times.
    fn get_row_converter(&mut self) -> Result<&mut RowConverter> {
        if self.row_converter.is_none() {
            let batch = self.batches.first().expect("at least one batch");
            let sort_fields = self
                .group_cols
                .iter()
                .map(|c| SortField::new(batch.column(*c).data_type().clone()))
                .collect();
            let row_converter =
                RowConverter::new(sort_fields).map_err(DataFusionError::ArrowError)?;
            self.row_converter = Some(row_converter);
        }
        Ok(self.row_converter.as_mut().expect("cannot be none"))
    }

    /// Convert a row to row-oriented format for easy comparison.
    fn convert_row(&mut self, row_idxs: (usize, usize)) -> Result<Rows> {
        let batch = &self.batches[row_idxs.0];
        let columns: Vec<ArrayRef> = self
            .group_cols
            .iter()
            .map(|col_idx| batch.column(*col_idx).slice(row_idxs.1, 1))
            .collect();
        self.get_row_converter()?
            .convert_columns(&columns)
            .map_err(DataFusionError::ArrowError)
    }

    /// Returns the row-oriented representation of the last buffered row that may appear in the next
    /// output batch. Since this row may be used multiple times, cache it in `self` to
    /// avoid computing it multiple times.
    fn last_output_row(&mut self, idxs: (usize, usize)) -> Result<&Rows> {
        if self.last_output_row.is_none() {
            let rows = self.convert_row(idxs)?;
            self.last_output_row = Some(rows);
        }
        Ok(self.last_output_row.as_ref().expect("cannot be none"))
    }

    /// Return the `(batch_idx, row_idx)` of the last buffered row.
    fn last_buffered_row_idx(&self) -> (usize, usize) {
        let last_batch_len = self.batches.last().unwrap().num_rows();
        (self.batches.len() - 1, last_batch_len - 1)
    }

    /// Return the `(batch_idx, row_idx)` of the `nth` row.
    fn find_row_idx(&self, mut nth: usize) -> Option<(usize, usize)> {
        let mut idx = None;
        for (i, batch) in self.batches.iter().enumerate() {
            if nth >= batch.num_rows() {
                nth -= batch.num_rows()
            } else {
                idx = Some((i, nth));
                break;
            }
        }
        idx
    }
}

#[cfg(test)]
mod tests {
    use std::collections::VecDeque;

    use arrow_util::test_util::batches_to_lines;

    use super::*;
    use crate::exec::gapfill::exec_tests::TestRecords;

    fn test_records(batch_size: usize) -> VecDeque<RecordBatch> {
        let records = TestRecords {
            group_cols: vec![
                std::iter::repeat(Some("a")).take(12).collect(),
                std::iter::repeat(Some("b"))
                    .take(6)
                    .chain(std::iter::repeat(Some("c")).take(6))
                    .collect(),
            ],
            time_col: (0..12).map(|i| Some(1000 + i * 5)).take(12).collect(),
            agg_cols: vec![
                vec![
                    Some(1),
                    None,
                    None,
                    None,
                    None,
                    None,
                    None,
                    None,
                    None,
                    None,
                    None,
                    Some(10),
                ],
                vec![
                    Some(2),
                    None,
                    None,
                    None,
                    None,
                    None,
                    None,
                    None,
                    Some(20),
                    None,
                    None,
                    None,
                ],
                (0..12).map(Some).collect(),
            ],
            struct_cols: vec![],
            input_batch_size: batch_size,
        };

        TryInto::<Vec<RecordBatch>>::try_into(records)
            .unwrap()
            .into()
    }

    fn test_struct_records(batch_size: usize) -> VecDeque<RecordBatch> {
        let records = TestRecords {
            group_cols: vec![
                std::iter::repeat(Some("a")).take(12).collect(),
                std::iter::repeat(Some("b"))
                    .take(6)
                    .chain(std::iter::repeat(Some("c")).take(6))
                    .collect(),
            ],
            time_col: (0..12).map(|i| Some(1000 + i * 5)).take(12).collect(),
            agg_cols: vec![],
            struct_cols: vec![
                vec![
                    Some(vec![1, 0]),
                    None,
                    None,
                    None,
                    None,
                    None,
                    None,
                    None,
                    None,
                    None,
                    None,
                    Some(vec![10, 0]),
                ],
                vec![
                    Some(vec![2, 0]),
                    None,
                    None,
                    None,
                    None,
                    None,
                    None,
                    None,
                    Some(vec![20, 0]),
                    None,
                    None,
                    None,
                ],
                (0..12).map(|n| Some(vec![n, 0])).collect(),
            ],
            input_batch_size: batch_size,
        };

        TryInto::<Vec<RecordBatch>>::try_into(records)
            .unwrap()
            .into()
    }

    fn test_params() -> GapFillParams {
        GapFillParams {
            stride: 50_000_000,
            first_ts: Some(1_000_000_000),
            last_ts: 1_055_000_000,
            fill_strategy: [
                (3, FillStrategy::LinearInterpolate),
                (4, FillStrategy::LinearInterpolate),
            ]
            .into(),
        }
    }

    // This test is just here so it's clear what the
    // test data is
    #[test]
    fn test_test_records() {
        let batch = test_records(1000).pop_front().unwrap();
        let actual = batches_to_lines(&[batch]);
        insta::assert_yaml_snapshot!(actual, @r###"
        ---
        - +----+----+--------------------------+----+----+----+
        - "| g0 | g1 | time                     | a0 | a1 | a2 |"
        - +----+----+--------------------------+----+----+----+
        - "| a  | b  | 1970-01-01T00:00:01Z     | 1  | 2  | 0  |"
        - "| a  | b  | 1970-01-01T00:00:01.005Z |    |    | 1  |"
        - "| a  | b  | 1970-01-01T00:00:01.010Z |    |    | 2  |"
        - "| a  | b  | 1970-01-01T00:00:01.015Z |    |    | 3  |"
        - "| a  | b  | 1970-01-01T00:00:01.020Z |    |    | 4  |"
        - "| a  | b  | 1970-01-01T00:00:01.025Z |    |    | 5  |"
        - "| a  | c  | 1970-01-01T00:00:01.030Z |    |    | 6  |"
        - "| a  | c  | 1970-01-01T00:00:01.035Z |    |    | 7  |"
        - "| a  | c  | 1970-01-01T00:00:01.040Z |    | 20 | 8  |"
        - "| a  | c  | 1970-01-01T00:00:01.045Z |    |    | 9  |"
        - "| a  | c  | 1970-01-01T00:00:01.050Z |    |    | 10 |"
        - "| a  | c  | 1970-01-01T00:00:01.055Z | 10 |    | 11 |"
        - +----+----+--------------------------+----+----+----+
        "###);
    }

    #[test]
    fn no_group_no_interpolate() {
        let batch_size = 3;
        let mut params = test_params();
        params.fill_strategy = [].into();

        let mut buffered_input = BufferedInput::new(&params, vec![]);
        let mut batches = test_records(batch_size);

        // There are no rows, so that is less than the batch size,
        // it needs more.
        assert!(buffered_input.need_more(batch_size - 1).unwrap());

        // There are now 3 rows, still less than batch_size + 1,
        // so it needs more.
        buffered_input.push(batches.pop_front().unwrap());
        assert!(buffered_input.need_more(batch_size - 1).unwrap());

        // We now have batch_size * 2, records, which is enough.
        buffered_input.push(batches.pop_front().unwrap());
        assert!(!buffered_input.need_more(batch_size - 1).unwrap());
    }

    #[test]
    fn no_group() {
        let batch_size = 3;
        let params = test_params();
        let mut buffered_input = BufferedInput::new(&params, vec![]);
        let mut batches = test_records(batch_size);

        // There are no rows, so that is less than the batch size,
        // it needs more.
        assert!(buffered_input.need_more(batch_size - 1).unwrap());

        // There are now 3 rows, still less than batch_size + 1,
        // so it needs more.
        buffered_input.push(batches.pop_front().unwrap());
        assert!(buffered_input.need_more(batch_size - 1).unwrap());

        // There are now 6 rows, if we were not interpolating,
        // this would be enough.
        buffered_input.push(batches.pop_front().unwrap());

        // If we are interpolating, there are no non null values
        // at offset 5.
        assert!(buffered_input.need_more(batch_size - 1).unwrap());

        // Push more rows, now totaling 9.
        buffered_input.push(batches.pop_front().unwrap());
        assert!(buffered_input.need_more(batch_size - 1).unwrap());
        // Column `a1` has a non-null value at offset 8.
        // If that were the only column being interpolated, we would have enough.

        // 12 rows, with non-null values in both columns being interpolated.
        buffered_input.push(batches.pop_front().unwrap());
        assert!(!buffered_input.need_more(batch_size - 1).unwrap());
    }

    #[test]
    fn with_group() {
        let params = test_params();
        let group_cols = vec![0, 1];
        let mut buffered_input = BufferedInput::new(&params, group_cols);

        let batch_size = 3;
        let mut batches = test_records(batch_size);

        // no rows
        assert!(buffered_input.need_more(batch_size - 1).unwrap());

        // 3 rows
        buffered_input.push(batches.pop_front().unwrap());
        assert!(buffered_input.need_more(batch_size - 1).unwrap());

        // 6 rows
        buffered_input.push(batches.pop_front().unwrap());
        assert!(buffered_input.need_more(batch_size - 1).unwrap());

        // 9 rows (series changes here)
        buffered_input.push(batches.pop_front().unwrap());
        assert!(!buffered_input.need_more(batch_size - 1).unwrap());
    }

    #[test]
    fn struct_with_group() {
        let params = test_params();
        let group_cols = vec![0, 1];
        let mut buffered_input = BufferedInput::new(&params, group_cols);

        let batch_size = 3;
        let mut batches = test_struct_records(batch_size);

        // no rows
        assert!(buffered_input.need_more(batch_size - 1).unwrap());

        // 3 rows
        buffered_input.push(batches.pop_front().unwrap());
        assert!(buffered_input.need_more(batch_size - 1).unwrap());

        // 6 rows
        buffered_input.push(batches.pop_front().unwrap());
        assert!(buffered_input.need_more(batch_size - 1).unwrap());

        // 9 rows (series changes here)
        buffered_input.push(batches.pop_front().unwrap());
        assert!(!buffered_input.need_more(batch_size - 1).unwrap());
    }
}
