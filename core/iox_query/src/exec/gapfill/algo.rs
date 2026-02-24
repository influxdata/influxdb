//! Contains the [GapFiller] type which does the
//! actual gap filling of record batches.

mod interpolate;

use std::{
    ops::{Bound, Range},
    sync::Arc,
};

use arrow::{
    array::{Array, ArrayRef, TimestampNanosecondArray, UInt64Array},
    compute::{
        kernels::{interleave, take},
        partition,
    },
    datatypes::SchemaRef,
    record_batch::RecordBatch,
};
use datafusion::{
    error::{DataFusionError, Result},
    scalar::ScalarValue,
};
use hashbrown::HashMap;

use self::interpolate::Segment;

use super::{FillStrategy, GapExpander, params::GapFillParams};

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
///  │                      .                .                     .
/// output_batch_size       .                .                     .
///  │                      .                .                     .
///  │                    ╟────╫───┼───┼─────────────╫───┼───┼─────────────╢
///  │              n - 1 ║    ║   │   │             ║   │   │             ║
///  │                    ╟────╫───┼───┼─────────────╫───┼───┼─────────────╢
///  ┴────              n ║    ║   │   │             ║   │   │             ║
///                       ╟────╫───┼───┼─────────────╫───┼───┼─────────────╢
/// trailing row(s) n + 1 ║    ║   │   │             ║   │   │             ║
///                       ╟────╫───┼───┼─────────────╫───┼───┼─────────────╢
///                         .                .                     .
///                         .                .                     .
///                         .                .                     .
/// ```
///
/// Just before generating output, the cursor will generally point at offset 1
/// in the input, since offset 0 is a _context row_. The exception to this is
/// there is no context row when generating the first output batch.
///
/// Buffering at least `output_batch_size + 2` rows ensures that:
/// - `GapFiller` can produce enough rows to produce a complete output batch, since
///   every input row will appear in the output.
/// - There is a _context row_ that represents the last input row that got output before
///   the current output batch. Group column values will be taken from this row
///   (using the [`take`](take::take) kernel) when we are generating trailing gaps, i.e.,
///   when all of the input rows have been output for a series in the previous batch,
///   but there still remains missing rows to produce at the end.
/// - Having at least one additional _trailing row_ at the end ensures that `GapFiller` can
///   infer whether there is trailing gaps to produce at the beginning of the
///   next batch, since it can discover if the last row starts a new series.
/// - If there are columns that have a fill strategy of [`LinearInterpolate`], then more
///   trailing rows may be necessary to find the next non-null value for the column.
///
/// [`LinearInterpolate`]: FillStrategy::LinearInterpolate
#[derive(Debug)]
pub(super) struct GapFiller {
    /// The static parameters of gap-filling: time range start, end and the stride.
    params: GapFillParams,
    /// The number of rows to produce in each output batch.
    batch_size: usize,
    /// The current state of gap-filling, including the next timestamp,
    /// the offset of the next input row, and remaining space in output batch.
    cursor: Cursor,
}

impl GapFiller {
    /// Initialize a [GapFiller] at the beginning of an input record batch.
    pub fn new(params: GapFillParams, batch_size: usize) -> Self {
        let cursor = Cursor::new(&params);
        Self {
            params,
            batch_size,
            cursor,
        }
    }

    /// Given that the cursor points at the input row that will be
    /// the first row in the next output batch, return the offset
    /// of last input row that could possibly be in the output.
    ///
    /// This offset is used by [`BufferedInput`] to determine how many
    /// rows need to be buffered.
    ///
    /// [`BufferedInput`]: super::buffered_input::BufferedInput
    pub(super) fn last_output_row_offset(&self) -> usize {
        self.cursor.next_input_offset + self.batch_size - 1
    }

    /// Returns true if there are no more output rows to produce given
    /// the number of rows of buffered input.
    pub fn done(&self, buffered_input_row_count: usize) -> bool {
        self.cursor.done(buffered_input_row_count)
    }

    /// Produces a gap-filled output [RecordBatch].
    ///
    /// Input arrays are represented as pairs that include their offset in the
    /// schema at member `0`.
    pub fn build_gapfilled_output(
        &mut self,
        schema: SchemaRef,
        input_time_array: &TimestampNanosecondArray,
        series_arrays: &[ArrayRef],
        fill_arrays: &[(usize, ArrayRef)],
    ) -> Result<RecordBatch> {
        let series_ends = self.plan_output_batch(input_time_array, series_arrays)?;
        self.cursor.remaining_output_batch_size = self.batch_size;
        self.build_output(
            schema,
            input_time_array,
            series_arrays,
            fill_arrays,
            &series_ends,
        )
    }

    /// Slice the input batch so that it has one context row before the next input offset.
    pub fn slice_input_batch(&mut self, batch: RecordBatch) -> Result<RecordBatch> {
        if self.cursor.next_input_offset < 2 {
            // nothing to do
            return Ok(batch);
        }

        let offset = self.cursor.next_input_offset - 1;
        self.cursor.slice(offset, &batch)?;

        let len = batch.num_rows() - offset;
        Ok(batch.slice(offset, len))
    }

    /// Produces a vector of offsets that are the exclusive ends of each series
    /// in the buffered input. It will return the ends of only those series
    /// that can at least be started in the output batch.
    fn plan_output_batch(
        &mut self,
        input_time_array: &TimestampNanosecondArray,
        series_arr: &[ArrayRef],
    ) -> Result<Vec<usize>> {
        if series_arr.is_empty() {
            // there are no group columns, so the output
            // will be just one big series.
            return Ok(vec![input_time_array.len()]);
        }

        let sort_columns = series_arr.to_vec();

        let mut ranges = partition(&sort_columns)?.ranges().into_iter();

        let mut series_ends = vec![];
        let mut cursor = self.cursor.clone_for_aggr_col(None)?;
        let mut output_row_count = 0;

        let start_offset = cursor.next_input_offset;
        assert!(start_offset <= 1, "input is sliced after it is consumed");
        while output_row_count < self.batch_size {
            match ranges.next() {
                Some(Range { end, .. }) => {
                    assert!(
                        end > 0,
                        "each lexicographical partition will have at least one row"
                    );

                    if let Some(nrows) =
                        cursor.count_series_rows(&self.params, input_time_array, end)?
                    {
                        output_row_count += nrows;
                        series_ends.push(end);
                    }
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
        input_time_array: &TimestampNanosecondArray,
        series_arr: &[ArrayRef],
        fill_arr: &[(usize, ArrayRef)],
        series_ends: &[usize],
    ) -> Result<RecordBatch> {
        let mut output_arrays: Vec<ArrayRef> =
            Vec::with_capacity(series_arr.len() + fill_arr.len() + 1); // plus one for time column

        // build the time column
        let mut cursor = self.cursor.clone_for_aggr_col(None)?;
        let time_vec = cursor.build_time_vec(&self.params, series_ends, input_time_array)?;
        let output_time_len = time_vec.len();
        let time_arr = Arc::new(
            TimestampNanosecondArray::from(time_vec).with_timezone_opt(input_time_array.timezone()),
        );
        // There may not be any aggregate or group columns, so use this cursor state as the new
        // GapFiller cursor once this output batch is complete.
        let mut final_cursor = cursor;

        // build the other group columns
        for ga in series_arr {
            let mut cursor = self.cursor.clone_for_aggr_col(None)?;
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
            output_arrays.push(take::take(ga, &take_arr, None)?);
        }
        output_arrays.push(time_arr);

        // Build the aggregate columns
        for (idx, aa) in fill_arr {
            let mut cursor = self.cursor.clone_for_aggr_col(Some(*idx))?;
            let output_array =
                cursor.build_aggr_col(&self.params, series_ends, input_time_array, aa)?;
            if output_array.len() != output_time_len {
                return Err(DataFusionError::Internal(format!(
                    "gapfill aggr column has {} rows, expected {}",
                    output_array.len(),
                    output_time_len
                )));
            }
            output_arrays.push(output_array);
            final_cursor.merge_aggr_col_cursor(cursor);
        }

        let batch = RecordBatch::try_new(Arc::clone(&schema), output_arrays).map_err(|err| {
            DataFusionError::ArrowError(Box::new(err), None).context("build_output")
        })?;

        self.cursor = final_cursor;
        Ok(batch)
    }
}

/// Maintains the state needed to fill gaps in output columns. Also provides methods
/// for building vectors that build time, group, and aggregate output arrays.
#[derive(Debug)]
pub(crate) struct Cursor {
    gap_expander: Arc<dyn GapExpander + Send + Sync>,
    /// Where to read the next row from the input.
    next_input_offset: usize,
    /// The next timestamp to be produced for the current series.
    /// Since the lower bound for gap filling could just be "whatever
    /// the first timestamp in the series is," this may be `None` before
    /// any rows with non-null timestamps are produced for a series.
    next_ts: Bound<i64>,
    /// How many rows may be output before we need to start a new record batch.
    remaining_output_batch_size: usize,
    /// True if there are trailing gaps from after the last input row for a series
    /// to be produced at the beginning of the next output batch.
    trailing_gaps: bool,
    /// State for each aggregate column, keyed on the columns offset in the schema.
    aggr_col_states: HashMap<usize, AggrColState>,
}

impl Cursor {
    /// Creates a new cursor.
    fn new(params: &GapFillParams) -> Self {
        let aggr_col_states = params
            .fill_strategy
            .iter()
            .map(|(idx, fs)| (*idx, AggrColState::new(fs)))
            .collect();
        let next_ts = match params.first_ts {
            Some(ts) => Bound::Included(ts),
            None => Bound::Unbounded,
        };
        Self {
            gap_expander: Arc::clone(&params.gap_expander),
            next_input_offset: 0,
            next_ts,
            remaining_output_batch_size: 0,
            trailing_gaps: false,
            aggr_col_states,
        }
    }

    /// Returns true of we point past all rows of buffered input and there
    /// are no trailing gaps left to produce.
    fn done(&self, buffered_input_row_count: usize) -> bool {
        self.next_input_offset == buffered_input_row_count && !self.trailing_gaps
    }

    /// Make a clone of this cursor to be used for creating an aggregate column,
    /// if `idx` is `Some`. The resulting `Cursor` will only contain [AggrColState]
    /// for the indicated column.
    ///
    /// When `idx` is `None`, return a `Cursor` with an empty [Cursor::aggr_col_states].
    fn clone_for_aggr_col(&self, idx: Option<usize>) -> Result<Self> {
        let mut cur = Self {
            gap_expander: Arc::clone(&self.gap_expander),
            next_input_offset: self.next_input_offset,
            next_ts: self.next_ts,
            remaining_output_batch_size: self.remaining_output_batch_size,
            trailing_gaps: self.trailing_gaps,
            aggr_col_states: HashMap::default(),
        };
        if let Some(idx) = idx {
            let state = self
                .aggr_col_states
                .get(&idx)
                .ok_or(DataFusionError::Internal(format!(
                    "could not find aggr col with offset {idx}"
                )))?;
            cur.aggr_col_states.insert(idx, state.clone());
        }
        Ok(cur)
    }

    /// Update [Cursor::aggr_col_states] with updated state for an
    /// aggregate column. `cursor` will have been created via `Cursor::clone_for_aggr_col`,
    /// so [Cursor::aggr_col_states] will contain exactly one item.
    ///
    /// # Panics
    ///
    /// Will panic if input cursor's [Cursor::aggr_col_states] does not contain exactly one item.
    fn merge_aggr_col_cursor(&mut self, cursor: Self) {
        assert_eq!(1, cursor.aggr_col_states.len());
        for (idx, state) in cursor.aggr_col_states.into_iter() {
            self.aggr_col_states.insert(idx, state);
        }
    }

    /// Get the [AggrColState] for this cursor. `self` will have been created via
    /// `Cursor::clone_for_aggr_col`, so [Cursor::aggr_col_states] will contain exactly one item.
    ///
    /// # Panics
    ///
    /// Will panic if [Cursor::aggr_col_states] does not contain exactly one item.
    fn get_aggr_col_state(&self) -> &AggrColState {
        assert_eq!(1, self.aggr_col_states.len());
        self.aggr_col_states.iter().next().unwrap().1
    }

    /// Set the [AggrColState] for this cursor. `self` will have been created via
    /// `Cursor::clone_for_aggr_col`, so [Cursor::aggr_col_states] will contain exactly one item.
    ///
    /// # Panics
    ///
    /// Will panic if [Cursor::aggr_col_states] does not contain exactly one item.
    fn set_aggr_col_state(&mut self, new_state: AggrColState) {
        assert_eq!(1, self.aggr_col_states.len());
        let (_idx, state) = self.aggr_col_states.iter_mut().next().unwrap();
        *state = new_state;
    }

    /// Counts the number of rows that will be produced for a series that ends (exclusively)
    /// at `series_end`, including rows that have a null timestamp, if any.
    ///
    /// Produces `None` for the case where `next_input_offset` is equal to `series_end`,
    /// and there are no trailing gaps to produce.
    fn count_series_rows(
        &mut self,
        params: &GapFillParams,
        input_time_array: &TimestampNanosecondArray,
        series_end: usize,
    ) -> Result<Option<usize>> {
        if !self.trailing_gaps && self.next_input_offset == series_end {
            return Ok(None);
        }

        let mut count = if input_time_array.null_count() > 0 {
            let len = series_end - self.next_input_offset;
            let slice = input_time_array.slice(self.next_input_offset, len);
            slice.null_count()
        } else {
            0
        };

        self.next_input_offset += count;
        let tz = input_time_array.timezone().map(Arc::from);
        let range = Range {
            start: self
                .next_ts
                .map(|ts| ScalarValue::TimestampNanosecond(Some(ts), tz.clone())),
            end: Bound::Included(ScalarValue::TimestampNanosecond(
                Some(params.last_ts),
                tz.clone(),
            )),
        };
        let array =
            input_time_array.slice(self.next_input_offset, series_end - self.next_input_offset);
        count += self.gap_expander.as_ref().count_rows(range, &array)?;

        self.next_input_offset = series_end;
        self.next_ts = Bound::Excluded(params.last_ts);

        Ok(Some(count))
    }

    /// Update this cursor to reflect that `offset` older rows are being sliced off from the
    /// buffered input.
    fn slice(&mut self, offset: usize, batch: &RecordBatch) -> Result<()> {
        for (idx, aggr_col_state) in &mut self.aggr_col_states {
            aggr_col_state.slice(offset, batch.column(*idx))?;
        }
        self.next_input_offset -= offset;
        Ok(())
    }

    /// Builds a vector that can be used to produce a timestamp array.
    fn build_time_vec(
        &mut self,
        params: &GapFillParams,
        series_ends: &[usize],
        input_time_array: &TimestampNanosecondArray,
    ) -> Result<Vec<Option<i64>>> {
        struct TimeBuilder {
            times: Vec<Option<i64>>,
        }

        impl VecBuilder for TimeBuilder {
            fn push(&mut self, row_status: RowStatus) -> Result<()> {
                match row_status {
                    RowStatus::NullTimestamp { .. } => self.times.push(None),
                    RowStatus::Present { ts, .. } | RowStatus::Missing { ts, .. } => {
                        self.times.push(Some(ts))
                    }
                }
                Ok(())
            }
        }

        let mut time_builder = TimeBuilder {
            times: Vec::with_capacity(self.remaining_output_batch_size),
        };
        self.build_vec(params, input_time_array, series_ends, &mut time_builder)?;

        Ok(time_builder.times)
    }

    /// Builds a vector that can use the [`take`](take::take) kernel
    /// to produce a group column.
    fn build_group_take_vec(
        &mut self,
        params: &GapFillParams,
        series_ends: &[usize],
        input_time_array: &TimestampNanosecondArray,
    ) -> Result<Vec<u64>> {
        struct GroupBuilder {
            take_idxs: Vec<u64>,
        }

        impl VecBuilder for GroupBuilder {
            fn push(&mut self, row_status: RowStatus) -> Result<()> {
                match row_status {
                    RowStatus::NullTimestamp {
                        series_end_offset, ..
                    }
                    | RowStatus::Present {
                        series_end_offset, ..
                    }
                    | RowStatus::Missing {
                        series_end_offset, ..
                    } => self.take_idxs.push(series_end_offset as u64 - 1),
                }
                Ok(())
            }
        }

        let mut group_builder = GroupBuilder {
            take_idxs: Vec::with_capacity(self.remaining_output_batch_size),
        };
        self.build_vec(params, input_time_array, series_ends, &mut group_builder)?;

        Ok(group_builder.take_idxs)
    }

    /// Produce a gap-filled array for the aggregate column
    /// in [`Self::aggr_col_states`].
    ///
    /// # Panics
    ///
    /// Will panic if [Cursor::aggr_col_states] does not contain exactly one item.
    fn build_aggr_col(
        &mut self,
        params: &GapFillParams,
        series_ends: &[usize],
        input_time_array: &TimestampNanosecondArray,
        input_aggr_array: &ArrayRef,
    ) -> Result<ArrayRef> {
        match self.get_aggr_col_state() {
            AggrColState::PrevNullAsIntentional { .. } | AggrColState::PrevNullAsMissing { .. } => {
                self.build_aggr_fill_prev(params, series_ends, input_time_array, input_aggr_array)
            }
            AggrColState::PrevNullAsMissingStashed { .. } => self.build_aggr_fill_prev_stashed(
                params,
                series_ends,
                input_time_array,
                input_aggr_array,
            ),
            AggrColState::LinearInterpolate(_) => self.build_aggr_fill_interpolate(
                params,
                series_ends,
                input_time_array,
                input_aggr_array,
            ),
            AggrColState::Default(val) => self.build_aggr_fill_val(
                params,
                series_ends,
                input_time_array,
                input_aggr_array,
                val.clone(),
            ),
        }
    }

    /// Build a gap-filled array that takes from input_aggr_array and fills with `val` wherever
    /// `input_aggr_array` does not have a value. Assumes that `val` has the same datatype as
    /// `input_aggr_array`. Uses the [`interleave::interleave`] kernel to produce this output.
    fn build_aggr_fill_val(
        &mut self,
        params: &GapFillParams,
        series_ends: &[usize],
        input_time_array: &TimestampNanosecondArray,
        input_aggr_array: &ArrayRef,
        val: ScalarValue,
    ) -> Result<ArrayRef> {
        // at this point, we assume that the data type of `val` is the same as the data type of
        // `input_aggr_array`. This should be true as long as the AggregateFunction that created
        // this array upheld the invariants of its trait contract (specifically, that it returns a
        // value of Datatype `X` when someone calls `return_type(_)` with an argument of
        // DataType::X). If they're not the same, the `interleave` kernel will throw an error and
        // we'll bubble it up

        let other_arr = val.to_array()?;

        struct AggrBuilder {
            // slice to pass into interleave::interleave as the second arg
            idxes: Vec<(usize, usize)>,
        }

        impl VecBuilder for AggrBuilder {
            fn push(&mut self, row_status: RowStatus) -> Result<()> {
                match row_status {
                    RowStatus::NullTimestamp { offset, .. } | RowStatus::Present { offset, .. } => {
                        self.idxes.push((0, offset));
                    }
                    RowStatus::Missing { .. } => self.idxes.push((1, 0)),
                }
                Ok(())
            }
        }

        let mut aggr_builder = AggrBuilder {
            idxes: Vec::with_capacity(self.remaining_output_batch_size),
        };

        self.build_vec(params, input_time_array, series_ends, &mut aggr_builder)?;

        interleave::interleave(&[input_aggr_array, &other_arr], &aggr_builder.idxes)
            .map_err(|err| DataFusionError::ArrowError(Box::new(err), None))
    }

    /// Builds an array using the [`take`](take::take) kernel
    /// to produce an aggregate output column, filling gaps with the
    /// previous values in the column.
    fn build_aggr_fill_prev(
        &mut self,
        params: &GapFillParams,
        series_ends: &[usize],
        input_time_array: &TimestampNanosecondArray,
        input_aggr_array: &ArrayRef,
    ) -> Result<ArrayRef> {
        struct AggrBuilder<'a> {
            take_idxs: Vec<Option<u64>>,
            prev_offset: Option<u64>,
            input_aggr_array: &'a ArrayRef,
            null_as_missing: bool,
        }

        impl VecBuilder for AggrBuilder<'_> {
            fn push(&mut self, row_status: RowStatus) -> Result<()> {
                match row_status {
                    RowStatus::NullTimestamp { offset, .. } => {
                        self.take_idxs.push(Some(offset as u64))
                    }
                    RowStatus::Present { offset, .. } => {
                        if !self.null_as_missing || self.input_aggr_array.is_valid(offset) {
                            self.take_idxs.push(Some(offset as u64));
                            self.prev_offset = Some(offset as u64);
                        } else {
                            self.take_idxs.push(self.prev_offset);
                        }
                    }
                    RowStatus::Missing { .. } => self.take_idxs.push(self.prev_offset),
                }
                Ok(())
            }
            fn start_new_series(&mut self) -> Result<()> {
                self.prev_offset = None;
                Ok(())
            }
        }

        let null_as_missing = matches!(
            self.get_aggr_col_state(),
            AggrColState::PrevNullAsMissing { .. }
        );

        let mut aggr_builder = AggrBuilder {
            take_idxs: Vec::with_capacity(self.remaining_output_batch_size),
            prev_offset: self.get_aggr_col_state().prev_offset(),
            input_aggr_array,
            null_as_missing,
        };
        self.build_vec(params, input_time_array, series_ends, &mut aggr_builder)?;

        let AggrBuilder {
            take_idxs,
            prev_offset,
            ..
        } = aggr_builder;
        self.set_aggr_col_state(match null_as_missing {
            false => AggrColState::PrevNullAsIntentional {
                offset: prev_offset,
            },
            true => AggrColState::PrevNullAsMissing {
                offset: prev_offset,
            },
        });

        let take_arr = UInt64Array::from(take_idxs);
        take::take(input_aggr_array, &take_arr, None)
            .map_err(|err| DataFusionError::ArrowError(Box::new(err), None))
    }

    /// Builds an array using the [`interleave`](arrow::compute::interleave) kernel
    /// to produce an aggregate output column, filling gaps with the
    /// previous values in the column.
    fn build_aggr_fill_prev_stashed(
        &mut self,
        params: &GapFillParams,
        series_ends: &[usize],
        input_time_array: &TimestampNanosecondArray,
        input_aggr_array: &ArrayRef,
    ) -> Result<ArrayRef> {
        let stash = self.get_aggr_col_state().stash();
        let mut aggr_builder = StashedAggrBuilder {
            interleave_idxs: Vec::with_capacity(self.remaining_output_batch_size),
            state: StashedAggrState::Stashed,
            stash,
            input_aggr_array,
        };
        self.build_vec(params, input_time_array, series_ends, &mut aggr_builder)?;
        let output_array = aggr_builder.build()?;

        // Update the aggregate column state for this cursor to prime it for the
        // next batch.
        let StashedAggrBuilder { state, .. } = aggr_builder;
        match state {
            StashedAggrState::Stashed => (), // nothing changes
            StashedAggrState::PrevNone => {
                self.set_aggr_col_state(AggrColState::PrevNullAsMissing { offset: None })
            }
            StashedAggrState::PrevSome { offset } => {
                self.set_aggr_col_state(AggrColState::PrevNullAsMissing {
                    offset: Some(offset as u64),
                })
            }
        };

        Ok(output_array)
    }

    /// Helper method that iterates over each series
    /// that ends with offsets in `series_ends` and produces
    /// the appropriate output values.
    fn build_vec(
        &mut self,
        params: &GapFillParams,
        input_time_array: &TimestampNanosecondArray,
        series_ends: &[usize],
        vec_builder: &mut impl VecBuilder,
    ) -> Result<()> {
        for series in series_ends {
            if self.next_ts == Bound::Excluded(params.last_ts) {
                vec_builder.start_new_series()?;
                self.next_ts = match params.first_ts {
                    Some(ts) => Bound::Included(ts),
                    None => Bound::Unbounded,
                };
            }

            self.append_series_items(params, input_time_array, *series, vec_builder)?;
        }

        let last_series_end = series_ends.last().ok_or(DataFusionError::Internal(
            "expected at least one item in series batch".to_string(),
        ))?;

        self.trailing_gaps = self.next_input_offset == *last_series_end
            && self.next_ts != Bound::Excluded(params.last_ts);
        Ok(())
    }

    /// Helper method that generates output for one series by invoking
    /// [VecBuilder::push] for each output value in the column to be generated.
    fn append_series_items(
        &mut self,
        params: &GapFillParams,
        input_times: &TimestampNanosecondArray,
        series_end: usize,
        vec_builder: &mut impl VecBuilder,
    ) -> Result<()> {
        // If there are any null timestamps for this group, they will be first.
        // These rows can just be copied into the output.
        // Append the corresponding values.
        while self.remaining_output_batch_size > 0
            && self.next_input_offset < series_end
            && input_times.is_null(self.next_input_offset)
        {
            vec_builder.push(RowStatus::NullTimestamp {
                series_end_offset: series_end,
                offset: self.next_input_offset,
            })?;
            self.remaining_output_batch_size -= 1;
            self.next_input_offset += 1;
        }

        if self.remaining_output_batch_size == 0 {
            return Ok(());
        }
        let array = input_times.slice(self.next_input_offset, series_end - self.next_input_offset);
        let tz = input_times.timezone().map(Arc::from);
        let range = Range {
            start: self
                .next_ts
                .map(|ts| ScalarValue::TimestampNanosecond(Some(ts), tz.clone())),
            end: Bound::Included(ScalarValue::TimestampNanosecond(Some(params.last_ts), tz)),
        };
        let (pairs, input_rows_processed) =
            self.gap_expander
                .expand_gaps(range, &array, self.remaining_output_batch_size)?;
        for (ts, idx) in pairs {
            let ts = match ts {
                ScalarValue::TimestampNanosecond(Some(ts), _) => ts,
                _ => {
                    return Err(DataFusionError::Execution(format!(
                        "gap expander produced unexpected type for timestamp: {:?}",
                        ts.data_type()
                    )));
                }
            };
            self.next_ts = Bound::Excluded(ts);
            vec_builder.push(match idx {
                Some(idx) => RowStatus::Present {
                    series_end_offset: series_end,
                    offset: self.next_input_offset + idx,
                    ts,
                },
                None => RowStatus::Missing {
                    series_end_offset: series_end,
                    ts,
                },
            })?;
            self.remaining_output_batch_size -= 1;
        }
        self.next_input_offset += input_rows_processed;
        Ok(())
    }
}

/// Maintains the state needed to fill gaps in an aggregate column,
/// depending on the fill strategy.
#[derive(Clone, Debug)]
enum AggrColState {
    /// For [FillStrategy::Default]
    Default(ScalarValue),
    /// For [FillStrategy::PrevNullAsIntentional].
    PrevNullAsIntentional { offset: Option<u64> },
    /// For [FillStrategy::PrevNullAsMissing].
    PrevNullAsMissing { offset: Option<u64> },
    /// For [FillStrategy::PrevNullAsMissing], when
    /// the fill value must be stashed in a separate array so it
    /// can persist across output batches.
    ///
    /// This state happens when the previous value in the buffered input
    /// rows has gone away during a call to [`GapFiller::slice_input_batch`].
    PrevNullAsMissingStashed { stash: ArrayRef },
    /// For [FillStrategy::LinearInterpolate], this tracks if we are in the middle
    /// of a "segment" (two non-null points in the input separated by more
    /// than the stride) between output batches.
    LinearInterpolate(Option<Segment<ScalarValue>>),
}

impl AggrColState {
    /// Create a new [AggrColState] based on the [FillStrategy] for the column.
    fn new(fill_strategy: &FillStrategy) -> Self {
        match fill_strategy {
            FillStrategy::Default(val) => Self::Default(val.clone()),
            FillStrategy::PrevNullAsIntentional => Self::PrevNullAsIntentional { offset: None },
            FillStrategy::PrevNullAsMissing => Self::PrevNullAsMissing { offset: None },
            FillStrategy::LinearInterpolate => Self::LinearInterpolate(None),
        }
    }

    /// Return the offset in the input from which to fill gaps.
    ///
    /// # Panics
    ///
    /// This method will panic if `self` is not [AggrColState::PrevNullAsIntentional]
    /// or [AggrColState::PrevNullAsMissing].
    fn prev_offset(&self) -> Option<u64> {
        match self {
            Self::PrevNullAsIntentional { offset } | Self::PrevNullAsMissing { offset } => *offset,
            Self::Default(_)
            | Self::LinearInterpolate(_)
            | Self::PrevNullAsMissingStashed { stash: _ } => unreachable!(),
        }
    }

    /// Update state to reflect that older rows in the buffered input
    /// are being sliced away.
    fn slice(&mut self, offset: usize, array: &ArrayRef) -> Result<()> {
        let offset = offset as u64;
        match self {
            Self::PrevNullAsMissing { offset: Some(v) } if offset > *v => {
                // The element in the buffered input that may be in the output
                // will be sliced away, so store it on the side.
                let stash = StashedAggrBuilder::create_stash(array, *v)?;
                *self = Self::PrevNullAsMissingStashed { stash };
            }
            Self::PrevNullAsIntentional { offset: Some(v) }
            | Self::PrevNullAsMissing { offset: Some(v) } => *v -= offset,
            _ => (),
        };
        Ok(())
    }

    /// Return the stashed previous value used to fill gaps.
    ///
    /// # Panics
    ///
    /// This method will panic if `self` is not [AggrColState::PrevNullAsMissingStashed].
    fn stash(&self) -> ArrayRef {
        match self {
            Self::PrevNullAsMissingStashed { stash } => Arc::clone(stash),
            _ => unreachable!(),
        }
    }

    /// Return the segment being interpolated, if any.
    ///
    /// # Panics
    ///
    /// This method will panic if `self` is not [AggrColState::LinearInterpolate].
    fn segment(&self) -> &Option<Segment<ScalarValue>> {
        match self {
            Self::LinearInterpolate(segment) => segment,
            _ => unreachable!(),
        }
    }
}

/// A trait that lets implementors describe how to build the
/// vectors used to create Arrow arrays in the output.
trait VecBuilder {
    /// Pushes a new value based on the output row's
    /// relation to the input row.
    fn push(&mut self, _: RowStatus) -> Result<()>;

    /// Called just before a new series starts.
    fn start_new_series(&mut self) -> Result<()> {
        Ok(())
    }
}

/// The state of an input row relative to gap-filled output.
#[derive(Debug)]
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

/// Implements [`VecBuilder`] for [`FillStrategy::PrevNullAsMissing`],
/// specifically for the case where a previous value that needs to be
/// propagated into a new output batch has been sliced off from
/// buffered input rows.
struct StashedAggrBuilder<'a> {
    interleave_idxs: Vec<(usize, usize)>,
    state: StashedAggrState,
    stash: ArrayRef,
    input_aggr_array: &'a ArrayRef,
}

impl StashedAggrBuilder<'_> {
    /// Create a 2-element array containing a null value and the value from
    /// `input_aggr_array` at `offset` for use with the [`interleave`](arrow::compute::interleave)
    /// kernel.
    fn create_stash(input_aggr_array: &ArrayRef, offset: u64) -> Result<ArrayRef> {
        let take_arr: UInt64Array = vec![None, Some(offset)].into();
        let stash = take::take(input_aggr_array, &take_arr, None)
            .map_err(|err| DataFusionError::ArrowError(Box::new(err), None))?;
        Ok(stash)
    }

    /// Build the output column.
    fn build(&self) -> Result<ArrayRef> {
        arrow::compute::interleave(&[&self.stash, self.input_aggr_array], &self.interleave_idxs)
            .map_err(|err| DataFusionError::ArrowError(Box::new(err), None))
    }

    fn buffered_input(offset: usize) -> (usize, usize) {
        (Self::BUFFERED_INPUT_ARRAY, offset)
    }

    const STASHED_NULL: (usize, usize) = (0, 0);
    const STASHED_VALUE: (usize, usize) = (0, 1);
    const BUFFERED_INPUT_ARRAY: usize = 1;
}

/// Stores state about how to fill the output aggregate column
/// for [`StashedAggrBuilder`].
enum StashedAggrState {
    /// Fill the next missing or null element with the
    /// stashed value.
    Stashed,
    /// Fill the next missing or null element with a null value.
    PrevNone,
    /// Fill the next missing or null element with the element in the
    /// input at `offset`.
    PrevSome { offset: usize },
}

impl VecBuilder for StashedAggrBuilder<'_> {
    fn push(&mut self, row_status: RowStatus) -> Result<()> {
        match row_status {
            RowStatus::NullTimestamp { offset, .. } => {
                self.interleave_idxs.push(Self::buffered_input(offset));
                self.state = StashedAggrState::PrevNone;
            }
            RowStatus::Present { offset, .. } if self.input_aggr_array.is_valid(offset) => {
                self.interleave_idxs.push(Self::buffered_input(offset));
                self.state = StashedAggrState::PrevSome { offset };
            }
            RowStatus::Present { .. } | RowStatus::Missing { .. } => match self.state {
                StashedAggrState::Stashed => self.interleave_idxs.push(Self::STASHED_VALUE),
                StashedAggrState::PrevNone => self.interleave_idxs.push(Self::STASHED_NULL),
                StashedAggrState::PrevSome { offset } => {
                    self.interleave_idxs.push(Self::buffered_input(offset))
                }
            },
        }

        Ok(())
    }

    fn start_new_series(&mut self) -> Result<()> {
        self.state = StashedAggrState::PrevNone;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use std::{ops::Bound, sync::Arc};

    use arrow::{
        array::{ArrayRef, Float64Array, TimestampNanosecondArray},
        datatypes::{DataType, Field, Schema},
        record_batch::RecordBatch,
    };
    use arrow_util::test_util::batches_to_lines;
    use datafusion::{common::ScalarValue, error::Result};
    use hashbrown::HashMap;
    use schema::{InfluxColumnType, TIME_DATA_TIMEZONE};

    use crate::exec::gapfill::{
        FillStrategy,
        algo::{AggrColState, Cursor},
        date_bin_gap_expander::DateBinGapExpander,
        params::GapFillParams,
    };

    #[test]
    fn test_cursor_append_time_values() -> Result<()> {
        test_helpers::maybe_start_logging();
        let input_times = TimestampNanosecondArray::from(vec![1000, 1100, 1200]);
        let series = input_times.len();

        let params = GapFillParams {
            gap_expander: Arc::new(DateBinGapExpander::new(50)),
            first_ts: Some(950),
            last_ts: 1250,
            fill_strategy: simple_fill_strategy(DataType::Null),
        };

        let output_batch_size = 10000;
        let mut cursor = new_cursor_with_batch_size(&params, output_batch_size);

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

        assert_cursor_end_state(&cursor, &input_times, &params);

        Ok(())
    }

    #[test]
    fn test_cursor_append_time_values_no_first_ts() {
        test_helpers::maybe_start_logging();
        let input_times = TimestampNanosecondArray::from(vec![1100, 1200]);
        let series = input_times.len();

        let params = GapFillParams {
            gap_expander: Arc::new(DateBinGapExpander::new(50)),
            first_ts: None,
            last_ts: 1250,
            fill_strategy: simple_fill_strategy(DataType::Null),
        };

        let output_batch_size = 10000;
        let mut cursor = new_cursor_with_batch_size(&params, output_batch_size);

        let out_times = cursor
            .build_time_vec(&params, &[series], &input_times)
            .unwrap();
        assert_eq!(
            vec![Some(1100), Some(1150), Some(1200), Some(1250)],
            out_times
        );

        assert_cursor_end_state(&cursor, &input_times, &params);
    }

    #[test]
    fn test_cursor_append_time_value_nulls() -> Result<()> {
        test_helpers::maybe_start_logging();
        let input_times =
            TimestampNanosecondArray::from(vec![None, None, Some(1000), Some(1100), Some(1200)]);
        let series = input_times.len();

        let params = GapFillParams {
            gap_expander: Arc::new(DateBinGapExpander::new(50)),
            first_ts: Some(950),
            last_ts: 1250,
            fill_strategy: simple_fill_strategy(DataType::Null),
        };

        let output_batch_size = 10000;
        let mut cursor = new_cursor_with_batch_size(&params, output_batch_size);
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

        assert_cursor_end_state(&cursor, &input_times, &params);

        Ok(())
    }

    #[test]
    fn test_cursor_append_group_take() -> Result<()> {
        let input_times = TimestampNanosecondArray::from(vec![1000, 1100, 1200]);
        let series = input_times.len();

        let params = GapFillParams {
            gap_expander: Arc::new(DateBinGapExpander::new(50)),
            first_ts: Some(950),
            last_ts: 1250,
            fill_strategy: simple_fill_strategy(DataType::Null),
        };

        let output_batch_size = 10000;
        let mut cursor = new_cursor_with_batch_size(&params, output_batch_size);
        let take_idxs = cursor.build_group_take_vec(&params, &[series], &input_times)?;
        assert_eq!(vec![2; 7], take_idxs);

        assert_cursor_end_state(&cursor, &input_times, &params);

        Ok(())
    }

    #[test]
    fn test_cursor_append_aggr_take() {
        let input_times = TimestampNanosecondArray::from(vec![1000, 1100, 1200]);
        let input_aggr_array: ArrayRef = Arc::new(Float64Array::from(vec![10.0, 11.0, 12.0]));
        let series = input_times.len();

        let params = GapFillParams {
            gap_expander: Arc::new(DateBinGapExpander::new(50)),
            first_ts: Some(950),
            last_ts: 1250,
            fill_strategy: simple_fill_strategy(DataType::Float64),
        };

        let output_batch_size = 10000;
        let mut cursor = new_cursor_with_batch_size(&params, output_batch_size);

        let time_arr = TimestampNanosecondArray::from(
            cursor
                .clone_for_aggr_col(None)
                .unwrap()
                .build_time_vec(&params, &[series], &input_times)
                .unwrap(),
        )
        .with_timezone_opt(TIME_DATA_TIMEZONE());
        let arr = cursor
            .build_aggr_fill_val(
                &params,
                &[series],
                &input_times,
                &input_aggr_array,
                ScalarValue::Float64(None),
            )
            .unwrap();
        insta::assert_yaml_snapshot!(array_to_lines(&time_arr, &arr), @r#"
        - +--------------------------------+------+
        - "| time                           | a0   |"
        - +--------------------------------+------+
        - "| 1970-01-01T00:00:00.000000950Z |      |"
        - "| 1970-01-01T00:00:00.000001Z    | 10.0 |"
        - "| 1970-01-01T00:00:00.000001050Z |      |"
        - "| 1970-01-01T00:00:00.000001100Z | 11.0 |"
        - "| 1970-01-01T00:00:00.000001150Z |      |"
        - "| 1970-01-01T00:00:00.000001200Z | 12.0 |"
        - "| 1970-01-01T00:00:00.000001250Z |      |"
        - +--------------------------------+------+
        "#);

        assert_cursor_end_state(&cursor, &input_times, &params);
    }

    #[test]
    fn test_cursor_append_aggr_take_nulls() -> Result<()> {
        test_helpers::maybe_start_logging();
        let input_times =
            TimestampNanosecondArray::from(vec![None, None, Some(1000), Some(1100), Some(1200)]);
        let input_aggr_array: ArrayRef =
            Arc::new(Float64Array::from(vec![0.1, 0.2, 10.0, 11.0, 12.0]));
        let series = input_times.len();

        let params = GapFillParams {
            gap_expander: Arc::new(DateBinGapExpander::new(50)),
            first_ts: Some(950),
            last_ts: 1250,
            fill_strategy: simple_fill_strategy(DataType::Float64),
        };

        let output_batch_size = 10000;
        let mut cursor = new_cursor_with_batch_size(&params, output_batch_size);

        let time_arr = TimestampNanosecondArray::from(
            cursor
                .clone_for_aggr_col(None)
                .unwrap()
                .build_time_vec(&params, &[series], &input_times)
                .unwrap(),
        )
        .with_timezone_opt(TIME_DATA_TIMEZONE());
        let arr = cursor.build_aggr_fill_val(
            &params,
            &[series],
            &input_times,
            &input_aggr_array,
            ScalarValue::Float64(None),
        )?;
        insta::assert_yaml_snapshot!(array_to_lines(&time_arr, &arr), @r#"
        - +--------------------------------+------+
        - "| time                           | a0   |"
        - +--------------------------------+------+
        - "|                                | 0.1  |"
        - "|                                | 0.2  |"
        - "| 1970-01-01T00:00:00.000000950Z |      |"
        - "| 1970-01-01T00:00:00.000001Z    | 10.0 |"
        - "| 1970-01-01T00:00:00.000001050Z |      |"
        - "| 1970-01-01T00:00:00.000001100Z | 11.0 |"
        - "| 1970-01-01T00:00:00.000001150Z |      |"
        - "| 1970-01-01T00:00:00.000001200Z | 12.0 |"
        - "| 1970-01-01T00:00:00.000001250Z |      |"
        - +--------------------------------+------+
        "#);

        assert_cursor_end_state(&cursor, &input_times, &params);

        Ok(())
    }

    #[test]
    fn test_cursor_append_aggr_take_prev() {
        let input_times = TimestampNanosecondArray::from(vec![
            // 950
            1000, // 1050
            1100, // 1150
            1200,
            // 1250
        ]);
        let input_aggr_array: ArrayRef = Arc::new(Float64Array::from(vec![10.0, 11.0, 12.0]));
        let series = input_times.len();

        let idx = 0;
        let params = GapFillParams {
            gap_expander: Arc::new(DateBinGapExpander::new(50)),
            first_ts: Some(950),
            last_ts: 1250,
            fill_strategy: prev_fill_strategy(idx),
        };

        let output_batch_size = 10000;
        let mut cursor = new_cursor_with_batch_size(&params, output_batch_size);

        let time_arr = TimestampNanosecondArray::from(
            cursor
                .clone_for_aggr_col(None)
                .unwrap()
                .build_time_vec(&params, &[series], &input_times)
                .unwrap(),
        )
        .with_timezone_opt(TIME_DATA_TIMEZONE());
        let arr = cursor
            .build_aggr_fill_prev(&params, &[series], &input_times, &input_aggr_array)
            .unwrap();
        insta::assert_yaml_snapshot!(array_to_lines(&time_arr, &arr), @r#"
        - +--------------------------------+------+
        - "| time                           | a0   |"
        - +--------------------------------+------+
        - "| 1970-01-01T00:00:00.000000950Z |      |"
        - "| 1970-01-01T00:00:00.000001Z    | 10.0 |"
        - "| 1970-01-01T00:00:00.000001050Z | 10.0 |"
        - "| 1970-01-01T00:00:00.000001100Z | 11.0 |"
        - "| 1970-01-01T00:00:00.000001150Z | 11.0 |"
        - "| 1970-01-01T00:00:00.000001200Z | 12.0 |"
        - "| 1970-01-01T00:00:00.000001250Z | 12.0 |"
        - +--------------------------------+------+
        "#);

        assert_cursor_end_state(&cursor, &input_times, &params);
    }

    #[test]
    fn test_cursor_append_aggr_take_prev_with_nulls() {
        let input_times = TimestampNanosecondArray::from(vec![
            None,
            None,
            // 950,
            Some(1000),
            // 1050
            Some(1100),
            // 1150
            Some(1200),
            // 1250
            //
        ]);
        let input_aggr_array: ArrayRef =
            Arc::new(Float64Array::from(vec![0.0, 0.1, 10.0, 11.0, 12.0]));
        let series = input_times.len();

        let idx = 0;
        let params = GapFillParams {
            gap_expander: Arc::new(DateBinGapExpander::new(50)),
            first_ts: Some(950),
            last_ts: 1250,
            fill_strategy: prev_fill_strategy(idx),
        };

        let output_batch_size = 10000;
        let mut cursor = new_cursor_with_batch_size(&params, output_batch_size);

        let time_arr = TimestampNanosecondArray::from(
            cursor
                .clone_for_aggr_col(None)
                .unwrap()
                .build_time_vec(&params, &[series], &input_times)
                .unwrap(),
        )
        .with_timezone_opt(TIME_DATA_TIMEZONE());
        let arr = cursor
            .build_aggr_fill_prev(&params, &[series], &input_times, &input_aggr_array)
            .unwrap();
        insta::assert_yaml_snapshot!(array_to_lines(&time_arr, &arr), @r#"
        - +--------------------------------+------+
        - "| time                           | a0   |"
        - +--------------------------------+------+
        - "|                                | 0.0  |"
        - "|                                | 0.1  |"
        - "| 1970-01-01T00:00:00.000000950Z |      |"
        - "| 1970-01-01T00:00:00.000001Z    | 10.0 |"
        - "| 1970-01-01T00:00:00.000001050Z | 10.0 |"
        - "| 1970-01-01T00:00:00.000001100Z | 11.0 |"
        - "| 1970-01-01T00:00:00.000001150Z | 11.0 |"
        - "| 1970-01-01T00:00:00.000001200Z | 12.0 |"
        - "| 1970-01-01T00:00:00.000001250Z | 12.0 |"
        - +--------------------------------+------+
        "#);

        assert_cursor_end_state(&cursor, &input_times, &params);
    }

    #[test]
    fn test_cursor_append_aggr_take_prev_multi_series() {
        let input_times = TimestampNanosecondArray::from(vec![
            // 950
            // 1000
            Some(1050),
            // 1100
            // --- new series
            // 950
            // 1000
            Some(1050),
            // 1100
        ])
        .with_timezone_opt(TIME_DATA_TIMEZONE());
        let input_aggr_array: ArrayRef = Arc::new(Float64Array::from(vec![10.0, 11.0]));
        let series_ends = vec![1, 2];

        let idx = 0;
        let params = GapFillParams {
            gap_expander: Arc::new(DateBinGapExpander::new(50)),
            first_ts: Some(950),
            last_ts: 1100,
            fill_strategy: prev_fill_strategy(idx),
        };

        let output_batch_size = 10000;
        let mut cursor = new_cursor_with_batch_size(&params, output_batch_size);

        let time_arr = TimestampNanosecondArray::from(
            cursor
                .clone_for_aggr_col(None)
                .unwrap()
                .build_time_vec(&params, &series_ends, &input_times)
                .unwrap(),
        )
        .with_timezone_opt(TIME_DATA_TIMEZONE());
        let arr = cursor
            .build_aggr_fill_val(
                &params,
                &series_ends,
                &input_times,
                &input_aggr_array,
                ScalarValue::Float64(None),
            )
            .unwrap();
        insta::assert_yaml_snapshot!(array_to_lines(&time_arr, &arr), @r#"
        - +--------------------------------+------+
        - "| time                           | a0   |"
        - +--------------------------------+------+
        - "| 1970-01-01T00:00:00.000000950Z |      |"
        - "| 1970-01-01T00:00:00.000001Z    |      |"
        - "| 1970-01-01T00:00:00.000001050Z | 10.0 |"
        - "| 1970-01-01T00:00:00.000001100Z |      |"
        - "| 1970-01-01T00:00:00.000000950Z |      |"
        - "| 1970-01-01T00:00:00.000001Z    |      |"
        - "| 1970-01-01T00:00:00.000001050Z | 11.0 |"
        - "| 1970-01-01T00:00:00.000001100Z |      |"
        - +--------------------------------+------+
        "#);

        assert_cursor_end_state(&cursor, &input_times, &params);
    }

    #[test]
    fn test_cursor_aggr_prev_null_as_missing() {
        let input_times = TimestampNanosecondArray::from(vec![
            // 950
            // 1000
            Some(1050),
            Some(1100),
            // --- new series
            Some(950),
            Some(1000),
            Some(1050),
            Some(1100),
        ])
        .with_timezone_opt(TIME_DATA_TIMEZONE());
        let input_aggr_array: ArrayRef = Arc::new(Float64Array::from(vec![
            // 950
            // 1000
            Some(10.0), // 1050
            None,       // 1100
            Some(20.0), // 950
            None,       // 1000
            Some(21.0), // 1050
            None,       // 1100
        ]));
        let series_ends = vec![2, 6];

        let idx = 0;
        let params = GapFillParams {
            gap_expander: Arc::new(DateBinGapExpander::new(50)),
            first_ts: Some(950),
            last_ts: 1100,
            fill_strategy: prev_null_as_missing_fill_strategy(idx),
        };

        let output_batch_size = 10000;
        let mut cursor = new_cursor_with_batch_size(&params, output_batch_size);

        let time_arr = TimestampNanosecondArray::from(
            cursor
                .clone_for_aggr_col(None)
                .unwrap()
                .build_time_vec(&params, &series_ends, &input_times)
                .unwrap(),
        )
        .with_timezone_opt(TIME_DATA_TIMEZONE());
        let arr = cursor
            .build_aggr_fill_prev(&params, &series_ends, &input_times, &input_aggr_array)
            .unwrap();
        insta::assert_yaml_snapshot!(array_to_lines(&time_arr, &arr), @r#"
        - +--------------------------------+------+
        - "| time                           | a0   |"
        - +--------------------------------+------+
        - "| 1970-01-01T00:00:00.000000950Z |      |"
        - "| 1970-01-01T00:00:00.000001Z    |      |"
        - "| 1970-01-01T00:00:00.000001050Z | 10.0 |"
        - "| 1970-01-01T00:00:00.000001100Z | 10.0 |"
        - "| 1970-01-01T00:00:00.000000950Z | 20.0 |"
        - "| 1970-01-01T00:00:00.000001Z    | 20.0 |"
        - "| 1970-01-01T00:00:00.000001050Z | 21.0 |"
        - "| 1970-01-01T00:00:00.000001100Z | 21.0 |"
        - +--------------------------------+------+
        "#);

        assert_cursor_end_state(&cursor, &input_times, &params);
    }

    #[test]
    fn test_cursor_aggr_prev_null_as_missing_stashed() {
        // This test is intended to simulate producing output with
        // prev-null-as-missing when the previous element has been
        // sliced away from the buffered input and is "stashed" in
        // another array on the side.
        let input_times = TimestampNanosecondArray::from(vec![
            // Some(950), // output in last batch
            // ^^^^^^^^^ this element has been sliced off
            // 1000 // <-- cursor.next_ts
            Some(1050), // context row
            Some(1100), // <-- cursor.next_input_offset
            // 1150
            // --- new series
            None, // null timestamp
            // 950
            Some(1000),
            Some(1050),
            Some(1100),
            Some(1150),
        ])
        .with_timezone_opt(TIME_DATA_TIMEZONE());
        let input_aggr_array: ArrayRef = Arc::new(Float64Array::from(vec![
            // Some(9.0) //  950
            // ^^^^^^^^^ this element has been sliced off
            // 1000 // filled with stashed because missing
            None,       // 1050 // filled with stashed because null
            Some(10.0), // 1100 // present
            // 1150 // filled with previous because missing
            // -- new series
            Some(-20.0), // null timestamp
            // 950 // null because no value for this series yet
            None, // 1000 // still null
            Some(21.1),
            None, // 1100 // filled with previous because null value in column
            None, // 1150 // filled with previous because null value in column
        ]));
        let series_ends = vec![2, 7];

        let aggr_col_idx = 0;
        let params = GapFillParams {
            gap_expander: Arc::new(DateBinGapExpander::new(50)),
            first_ts: Some(950),
            last_ts: 1150,
            fill_strategy: prev_null_as_missing_fill_strategy(aggr_col_idx),
        };

        let stash: Float64Array = vec![None, Some(9.0)].into();
        let stash: ArrayRef = Arc::new(stash);
        let output_batch_size = 10000;
        let mut cursor = Cursor {
            gap_expander: Arc::new(DateBinGapExpander::new(50)),
            next_input_offset: 1,
            next_ts: Bound::Included(1000),
            remaining_output_batch_size: output_batch_size,
            trailing_gaps: false,
            aggr_col_states: std::iter::once((
                aggr_col_idx,
                AggrColState::PrevNullAsMissingStashed { stash },
            ))
            .collect(),
        };

        let time_arr = TimestampNanosecondArray::from(
            cursor
                .clone_for_aggr_col(None)
                .unwrap()
                .build_time_vec(&params, &series_ends, &input_times)
                .unwrap(),
        )
        .with_timezone_opt(TIME_DATA_TIMEZONE());
        let arr = cursor
            .build_aggr_fill_prev_stashed(&params, &series_ends, &input_times, &input_aggr_array)
            .unwrap();
        insta::assert_yaml_snapshot!(array_to_lines(&time_arr, &arr), @r#"
        - +--------------------------------+-------+
        - "| time                           | a0    |"
        - +--------------------------------+-------+
        - "| 1970-01-01T00:00:00.000001Z    | 9.0   |"
        - "| 1970-01-01T00:00:00.000001050Z | 9.0   |"
        - "| 1970-01-01T00:00:00.000001100Z | 10.0  |"
        - "| 1970-01-01T00:00:00.000001150Z | 10.0  |"
        - "|                                | -20.0 |"
        - "| 1970-01-01T00:00:00.000000950Z |       |"
        - "| 1970-01-01T00:00:00.000001Z    |       |"
        - "| 1970-01-01T00:00:00.000001050Z | 21.1  |"
        - "| 1970-01-01T00:00:00.000001100Z | 21.1  |"
        - "| 1970-01-01T00:00:00.000001150Z | 21.1  |"
        - +--------------------------------+-------+
        "#);

        assert_cursor_end_state(&cursor, &input_times, &params);
    }

    pub(crate) fn array_to_lines(
        time_array: &TimestampNanosecondArray,
        aggr_array: &ArrayRef,
    ) -> Vec<String> {
        let data_type = aggr_array.data_type().clone();
        let schema = Schema::new(vec![
            Field::new(
                "time".to_string(),
                (&InfluxColumnType::Timestamp).into(),
                true,
            ),
            Field::new("a0".to_string(), data_type, true),
        ]);

        let time_array: ArrayRef = Arc::new(time_array.clone());
        let arrays = vec![time_array, Arc::clone(aggr_array)];
        let rb = RecordBatch::try_new(Arc::new(schema), arrays).unwrap();
        batches_to_lines(&[rb])
    }

    pub(crate) fn new_cursor_with_batch_size(params: &GapFillParams, batch_size: usize) -> Cursor {
        let mut cursor = Cursor::new(params);
        cursor.remaining_output_batch_size = batch_size;
        cursor
    }

    pub(crate) fn assert_cursor_end_state(
        cursor: &Cursor,
        input_times: &TimestampNanosecondArray,
        params: &GapFillParams,
    ) {
        assert_eq!(input_times.len(), cursor.next_input_offset);
        assert_eq!(Bound::Excluded(params.last_ts), cursor.next_ts);
    }

    fn simple_fill_strategy(dt: DataType) -> HashMap<usize, FillStrategy> {
        std::iter::once((1, FillStrategy::Default(dt.try_into().unwrap()))).collect()
    }

    fn prev_fill_strategy(idx: usize) -> HashMap<usize, FillStrategy> {
        std::iter::once((idx, FillStrategy::PrevNullAsIntentional)).collect()
    }

    fn prev_null_as_missing_fill_strategy(idx: usize) -> HashMap<usize, FillStrategy> {
        std::iter::once((idx, FillStrategy::PrevNullAsMissing)).collect()
    }
}
