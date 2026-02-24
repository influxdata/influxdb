//! A panic-safe write abstraction for [`MutableBatch`]

use crate::{
    MutableBatch,
    column::{Column, ColumnData, NULL_DID},
};
use arrow::util::bit_iterator::{BitIndexIterator, BitSliceIterator};
use arrow_util::bitset::BitSet;
use data_types::{IsNan, StatValues, Statistics};
use hashbrown::hash_map::EntryRef;
use schema::{
    InfluxColumnType, InfluxFieldType,
    builder::{ColumnInsertValidator, InvalidInsertionError, NoopColumnInsertValidator},
};
use snafu::{ResultExt, Snafu};
use std::{fmt::Debug, iter::repeat_n, num::NonZeroU64, ops::Range};

#[expect(missing_docs)]
#[derive(Debug, Snafu)]
pub enum Error {
    #[snafu(display("Unable to insert {inserted} type into column {column} with type {existing}"))]
    TypeMismatch {
        column: String,
        existing: InfluxColumnType,
        inserted: InfluxColumnType,
    },

    #[snafu(display("Incorrect number of values provided"))]
    InsufficientValues,

    #[snafu(display("Key not found in dictionary: {}", key))]
    KeyNotFound { key: usize },

    #[snafu(display("Could not insert column: {source}"))]
    ColumnInsertionRejected { source: InvalidInsertionError },
}

/// A specialized `Error` for [`Writer`] errors
pub type Result<T, E = Error> = std::result::Result<T, E>;

/// [`Writer`] provides a panic-safe abstraction to append a number of rows to a [`MutableBatch`]
///
/// If a [`Writer`] is dropped without calling [`Writer::commit`], the [`MutableBatch`] will be
/// truncated to the original number of rows, and the statistics not updated
#[derive(Debug)]
pub struct Writer<'a, T> {
    /// A check is delegated to this validator before a new, unseen column is
    /// added to `batch`, in order to allow consumers of the writer to impose
    /// additional constraints outside of the writer's knowledge.
    column_insert_validator: T,
    /// The mutable batch that is being mutated
    batch: &'a mut MutableBatch,
    /// A list of column index paired with Statistics
    ///
    /// Statistics updates are deferred to commit time
    statistics: Vec<(usize, Statistics)>,
    /// The initial number of rows in the MutableBatch
    initial_rows: usize,
    /// The initial number of columns in the MutableBatch
    initial_cols: usize,
    /// The number of rows to insert
    to_insert: usize,
    /// If this Writer committed successfully
    success: bool,
}
impl<'a> Writer<'a, NoopColumnInsertValidator> {
    /// Create a [`Writer`] for inserting `to_insert` rows to the provided `batch`
    ///
    /// If the writer is dropped without calling commit all changes will be rolled back
    pub fn new(batch: &'a mut MutableBatch, to_insert: usize) -> Self {
        Self::new_with_column_validator(batch, to_insert, Default::default())
    }
}

trait ContainedInVecInColumn: Sized {
    const INFLUX_COL_TY: InfluxColumnType;
    fn extract(data: &mut ColumnData) -> Option<&mut Vec<Self>>;
    fn statvalues_to_statistics(vals: StatValues<Self>) -> Statistics;
}

impl ContainedInVecInColumn for i64 {
    const INFLUX_COL_TY: InfluxColumnType = InfluxColumnType::Field(InfluxFieldType::Integer);

    #[inline]
    fn extract(data: &mut ColumnData) -> Option<&mut Vec<Self>> {
        match data {
            ColumnData::I64(v, _) => Some(v),
            _ => None,
        }
    }

    #[inline]
    fn statvalues_to_statistics(vals: StatValues<Self>) -> Statistics {
        Statistics::I64(vals)
    }
}

impl ContainedInVecInColumn for u64 {
    const INFLUX_COL_TY: InfluxColumnType = InfluxColumnType::Field(InfluxFieldType::UInteger);

    #[inline]
    fn extract(data: &mut ColumnData) -> Option<&mut Vec<Self>> {
        match data {
            ColumnData::U64(v, _) => Some(v),
            _ => None,
        }
    }

    #[inline]
    fn statvalues_to_statistics(vals: StatValues<Self>) -> Statistics {
        Statistics::U64(vals)
    }
}

impl ContainedInVecInColumn for f64 {
    const INFLUX_COL_TY: InfluxColumnType = InfluxColumnType::Field(InfluxFieldType::Float);

    #[inline]
    fn extract(data: &mut ColumnData) -> Option<&mut Vec<Self>> {
        match data {
            ColumnData::F64(v, _) => Some(v),
            _ => None,
        }
    }

    #[inline]
    fn statvalues_to_statistics(vals: StatValues<Self>) -> Statistics {
        Statistics::F64(vals)
    }
}

impl<'a, T> Writer<'a, T>
where
    T: ColumnInsertValidator,
{
    /// Create a [`Writer`] for inserting `to_insert` rows into the provided
    /// `batch`, accepting new columns only on passing checks from the
    /// [`ColumnInsertValidator`].
    ///
    /// If the writer is dropped without calling commit all changes will be rolled back
    pub fn new_with_column_validator(
        batch: &'a mut MutableBatch,
        to_insert: usize,
        column_insert_validator: T,
    ) -> Self {
        let initial_rows = batch.rows();
        let initial_cols = batch.columns.len();
        Self {
            column_insert_validator,
            batch,
            statistics: vec![],
            initial_rows,
            initial_cols,
            to_insert,
            success: false,
        }
    }

    fn write_from_slice_with_mask<V>(
        &mut self,
        name: &str,
        mut values: &[V],
        valid_mask: &[u8],
    ) -> Result<()>
    where
        V: Copy + IsNan + PartialOrd + Default + Debug + ContainedInVecInColumn,
    {
        let initial_rows = self.initial_rows;
        let to_insert = self.to_insert;

        // get the column that we want to write to
        let (col_idx, col) = self.column_mut(name, V::INFLUX_COL_TY)?;

        // prepare the statistics for this specific write
        let mut stats = StatValues::new_empty();

        // and get the vec that we're gonna write our data to. It must, at this point, have a length
        // of exactly `self.initial_rows`.
        let Some(col_data) = V::extract(&mut col.data) else {
            panic!(
                "Expected {} got {} for column {name}",
                std::any::type_name::<V>(),
                col.data
            );
        };

        // Then reseve the extra space to be able to push the values that we're gonna push.
        col_data.reserve(to_insert);

        // `last_value` is necessary to push values to the end of `col_data` when we run out of
        // values in `values`, since that is allowed per
        // https://github.com/influxdata/influxdb-pb-data-protocol#optimization-2-trim-repeated-tail-values
        let last_value = values.last().ok_or(Error::InsufficientValues)?;

        let mut last_valid = 0;

        // here we iterate over all the indices we're gonna push
        for (start, end) in BitSliceIterator::new(valid_mask, 0, to_insert) {
            let copy_len = end - start;

            let (slice, copied_final_values) = if copy_len > values.len() {
                (values, copy_len - values.len())
            } else {
                (&values[..copy_len], 0)
            };

            let num_nulls = start - last_valid;
            if num_nulls > 0 {
                col_data.extend(repeat_n(V::default(), num_nulls));
            }

            // so copy over the values
            col_data.extend_from_slice(slice);

            // update the stats
            for value in slice
                .iter()
                .chain(repeat_n(last_value, copied_final_values))
            {
                stats.update(value);
            }

            // push the copied final values, if there are any
            if copied_final_values > 0 {
                col_data.extend(repeat_n(last_value, copied_final_values));
            }

            // and then update values to make it easier to slice next time
            last_valid = end;

            values = &values[copy_len.min(values.len())..]
        }

        // Fill the remainder with nulls, just in case there were no valid indices at all.
        col_data.resize(initial_rows + to_insert, V::default());

        append_valid_mask(col, Some(valid_mask), to_insert);

        stats.update_for_nulls(to_insert as u64 - stats.total_count);
        self.statistics
            .push((col_idx, V::statvalues_to_statistics(stats)));

        Ok(())
    }

    fn write_from_slice<V>(&mut self, name: &str, values: &[V]) -> Result<()>
    where
        V: Copy + IsNan + PartialOrd + ContainedInVecInColumn,
    {
        self.write_from_slice_with_col_ty(name, values, V::INFLUX_COL_TY)
    }

    fn write_from_slice_with_col_ty<V>(
        &mut self,
        name: &str,
        values: &[V],
        col_ty: InfluxColumnType,
    ) -> Result<()>
    where
        V: Copy + IsNan + PartialOrd + ContainedInVecInColumn,
    {
        let initial_rows = self.initial_rows;
        let to_insert = self.to_insert;

        let (col_idx, col) = self.column_mut(name, col_ty)?;

        let mut stats = StatValues::new_empty();
        let Some(col_data) = V::extract(&mut col.data) else {
            panic!(
                "Expected {} got {} for column {name}",
                std::any::type_name::<V>(),
                col.data
            );
        };

        debug_assert_eq!(col_data.len(), initial_rows);

        let extra_times = to_insert.saturating_sub(values.len());
        let last = values.last().ok_or(Error::InsufficientValues)?;

        // Add each item given to stats
        for t in values.iter().chain(repeat_n(last, extra_times)) {
            stats.update(t);
        }

        // Get the available space that we need and push to it
        col_data.reserve(to_insert);
        col_data.extend_from_slice(values);
        col_data.extend(repeat_n(*last, extra_times));

        append_valid_mask(col, None, to_insert);

        stats.update_for_nulls(to_insert as u64 - stats.total_count);
        self.statistics
            .push((col_idx, V::statvalues_to_statistics(stats)));

        Ok(())
    }

    /// Write the f64 typed column identified by `name`
    ///
    /// For each set bit in `valid_mask` an a value from `values` is inserted at the
    /// corresponding index in the column. Nulls are inserted for the other rows
    ///
    /// # Panic
    ///
    /// - panics if this column has already been written to by this `Writer`
    ///
    pub fn write_f64<I>(&mut self, name: &str, valid_mask: &[u8], mut values: I) -> Result<()>
    where
        I: Iterator<Item = f64>,
    {
        let initial_rows = self.initial_rows;
        let to_insert = self.to_insert;

        let (col_idx, col) =
            self.column_mut(name, InfluxColumnType::Field(InfluxFieldType::Float))?;

        let mut stats = StatValues::new_empty();
        match &mut col.data {
            ColumnData::F64(col_data, _) => {
                col_data.resize(initial_rows + to_insert, 0_f64);
                for idx in set_position_iterator(valid_mask, to_insert) {
                    let value = values.next().ok_or(Error::InsufficientValues)?;
                    col_data[initial_rows + idx] = value;
                    stats.update(&value);
                }
            }
            x => unreachable!("expected f64 got {} for column \"{}\"", x, name),
        }

        append_valid_mask(col, Some(valid_mask), to_insert);

        stats.update_for_nulls(to_insert as u64 - stats.total_count);
        self.statistics.push((col_idx, Statistics::F64(stats)));

        Ok(())
    }

    /// ALMOST Functionally equivalent to calling [`Self::write_f64`] with an all-valid valid mask,
    /// but more performant when your values already exist in a slice that can be memcpy'd over.
    ///
    /// Functionally different in that it will copy the last value of `values` multiple times to
    /// fill in the remaining space if `values` doesn't contain at least `self.to_insert` values.
    pub fn write_f64s_from_slice(&mut self, name: &str, values: &[f64]) -> Result<()> {
        self.write_from_slice(name, values)
    }

    /// ALMOST Functionally equivalent to calling [`Self::write_f64`], but more performant when your
    /// values already exist in a slice that can be memcpy'd over in segments.
    ///
    /// Functionally different in that it will copy the last value of `values` multiple times to
    /// fill in the remaining space if `values` doesn't contain at least `self.to_insert` values.
    pub fn write_f64s_from_slice_with_mask(
        &mut self,
        name: &str,
        valid_mask: &[u8],
        values: &[f64],
    ) -> Result<()> {
        self.write_from_slice_with_mask(name, values, valid_mask)
    }

    /// Write the i64 typed column identified by `name`
    ///
    /// For each set bit in `valid_mask` an a value from `values` is inserted at the
    /// corresponding index in the column. Nulls are inserted for the other rows
    ///
    /// # Panic
    ///
    /// - panics if this column has already been written to by this `Writer`
    ///
    pub fn write_i64<I>(&mut self, name: &str, valid_mask: &[u8], mut values: I) -> Result<()>
    where
        I: Iterator<Item = i64>,
    {
        let initial_rows = self.initial_rows;
        let to_insert = self.to_insert;

        let (col_idx, col) =
            self.column_mut(name, InfluxColumnType::Field(InfluxFieldType::Integer))?;

        let mut stats = StatValues::new_empty();
        match &mut col.data {
            ColumnData::I64(col_data, _) => {
                col_data.resize(initial_rows + to_insert, 0_i64);
                for idx in set_position_iterator(valid_mask, to_insert) {
                    let value = values.next().ok_or(Error::InsufficientValues)?;
                    col_data[initial_rows + idx] = value;
                    stats.update(&value);
                }
            }
            x => unreachable!("expected i64 got {} for column \"{}\"", x, name),
        }

        append_valid_mask(col, Some(valid_mask), to_insert);

        stats.update_for_nulls(to_insert as u64 - stats.total_count);
        self.statistics.push((col_idx, Statistics::I64(stats)));

        Ok(())
    }

    /// ALMOST Functionally equivalent to calling [`Self::write_i64`] with an all-valid valid mask,
    /// but more performant when your values already exist in a slice that can be memcpy'd over.
    ///
    /// Functionally different in that it will copy the last value of `values` multiple times to
    /// fill in the remaining space if `values` doesn't contain at least `self.to_insert` values.
    pub fn write_i64s_from_slice(&mut self, name: &str, values: &[i64]) -> Result<()> {
        self.write_from_slice(name, values)
    }

    /// ALMOST Functionally equivalent to calling [`Self::write_i64`], but more performant when your
    /// values already exist in a slice that can be memcpy'd over in segments.
    ///
    /// Functionally different in that it will copy the last value of `values` multiple times to
    /// fill in the remaining space if `values` doesn't contain at least `self.to_insert` values.
    pub fn write_i64s_from_slice_with_mask(
        &mut self,
        name: &str,
        valid_mask: &[u8],
        values: &[i64],
    ) -> Result<()> {
        self.write_from_slice_with_mask(name, values, valid_mask)
    }

    /// Write the u64 typed column identified by `name`
    ///
    /// For each set bit in `valid_mask` an a value from `values` is inserted at the
    /// corresponding index in the column. Nulls are inserted for the other rows
    ///
    /// # Panic
    ///
    /// - panics if this column has already been written to by this `Writer`
    ///
    pub fn write_u64<I>(&mut self, name: &str, valid_mask: &[u8], mut values: I) -> Result<()>
    where
        I: Iterator<Item = u64>,
    {
        let initial_rows = self.initial_rows;
        let to_insert = self.to_insert;

        let (col_idx, col) =
            self.column_mut(name, InfluxColumnType::Field(InfluxFieldType::UInteger))?;

        let mut stats = StatValues::new_empty();
        match &mut col.data {
            ColumnData::U64(col_data, _) => {
                col_data.resize(initial_rows + to_insert, 0_u64);
                for idx in set_position_iterator(valid_mask, to_insert) {
                    let value = values.next().ok_or(Error::InsufficientValues)?;
                    col_data[initial_rows + idx] = value;
                    stats.update(&value);
                }
            }
            x => unreachable!("expected u64 got {} for column \"{}\"", x, name),
        }

        append_valid_mask(col, Some(valid_mask), to_insert);

        stats.update_for_nulls(to_insert as u64 - stats.total_count);
        self.statistics.push((col_idx, Statistics::U64(stats)));

        Ok(())
    }

    /// ALMOST Functionally equivalent to calling [`Self::write_u64`] with an all-valid valid mask,
    /// but more performant when your values already exist in a slice that can be memcpy'd over.
    ///
    /// Functionally different in that it will copy the last value of `values` multiple times to
    /// fill in the remaining space if `values` doesn't contain at least `self.to_insert` values.
    pub fn write_u64s_from_slice(&mut self, name: &str, values: &[u64]) -> Result<()> {
        self.write_from_slice(name, values)
    }

    /// ALMOST Functionally equivalent to calling [`Self::write_u64`], but more performant when your
    /// values already exist in a slice that can be memcpy'd over in segments.
    ///
    /// Functionally different in that it will copy the last value of `values` multiple times to
    /// fill in the remaining space if `values` doesn't contain at least `self.to_insert` values.
    pub fn write_u64s_from_slice_with_mask(
        &mut self,
        name: &str,
        valid_mask: &[u8],
        values: &[u64],
    ) -> Result<()> {
        self.write_from_slice_with_mask(name, values, valid_mask)
    }

    /// Write the boolean typed column identified by `name`
    ///
    /// For each set bit in `valid_mask` an a value from `values` is inserted at the
    /// corresponding index in the column. Nulls are inserted for the other rows
    ///
    /// # Panic
    ///
    /// - panics if this column has already been written to by this `Writer`
    ///
    pub fn write_bool<I>(
        &mut self,
        name: &str,
        valid_mask: Option<&[u8]>,
        mut values: I,
    ) -> Result<()>
    where
        I: Iterator<Item = bool>,
    {
        let initial_rows = self.initial_rows;
        let to_insert = self.to_insert;

        let (col_idx, col) =
            self.column_mut(name, InfluxColumnType::Field(InfluxFieldType::Boolean))?;

        let mut stats = StatValues::new_empty();
        match &mut col.data {
            ColumnData::Bool(col_data, _) => {
                col_data.append_unset(to_insert);
                let mut push = |idx: usize| {
                    let value = values.next().ok_or(Error::InsufficientValues)?;
                    if value {
                        col_data.set(initial_rows + idx);
                    }
                    stats.update(&value);
                    Ok::<_, Error>(())
                };

                match valid_mask {
                    Some(mask) => {
                        for idx in set_position_iterator(mask, to_insert) {
                            push(idx)?;
                        }
                    }
                    None => {
                        for idx in 0..to_insert {
                            push(idx)?;
                        }
                    }
                }
            }
            x => unreachable!("expected bool got {} for column \"{}\"", x, name),
        }

        append_valid_mask(col, valid_mask, to_insert);

        stats.update_for_nulls(to_insert as u64 - stats.total_count);
        self.statistics.push((col_idx, Statistics::Bool(stats)));

        Ok(())
    }

    /// Write the string field typed column identified by `name`
    ///
    /// For each set bit in `valid_mask` an a value from `values` is inserted at the
    /// corresponding index in the column. Nulls are inserted for the other rows
    ///
    /// # Panic
    ///
    /// - panics if this column has already been written to by this `Writer`
    ///
    pub fn write_string<'s, I>(
        &mut self,
        name: &str,
        valid_mask: Option<&[u8]>,
        mut values: I,
    ) -> Result<()>
    where
        I: Iterator<Item = &'s str>,
    {
        let initial_rows = self.initial_rows;
        let to_insert = self.to_insert;

        let (col_idx, col) =
            self.column_mut(name, InfluxColumnType::Field(InfluxFieldType::String))?;

        let mut stats = StatValues::new_empty();
        match &mut col.data {
            ColumnData::String(col_data, _) => {
                let mut push = |idx: usize| {
                    let value = values.next().ok_or(Error::InsufficientValues)?;
                    col_data.extend(initial_rows + idx - col_data.len());
                    col_data.append(value);
                    stats.update(value);
                    Ok::<(), Error>(())
                };

                match valid_mask {
                    Some(mask) => {
                        for idx in set_position_iterator(mask, to_insert) {
                            push(idx)?;
                        }
                    }
                    None => {
                        for idx in 0..to_insert {
                            push(idx)?;
                        }
                    }
                }

                col_data.extend(initial_rows + to_insert - col_data.len());
            }
            x => unreachable!("expected tag got {} for column \"{}\"", x, name),
        }

        append_valid_mask(col, valid_mask, to_insert);

        stats.update_for_nulls(to_insert as u64 - stats.total_count);
        self.statistics.push((col_idx, Statistics::String(stats)));

        Ok(())
    }

    /// Write the tag typed column identified by `name`
    ///
    /// For each set bit in `valid_mask` an a value from `values` is inserted at the
    /// corresponding index in the column. Nulls are inserted for the other rows
    ///
    /// # Panic
    ///
    /// - panics if this column has already been written to by this `Writer`
    ///
    pub fn write_tag<'s, I>(
        &mut self,
        name: &str,
        valid_mask: Option<&[u8]>,
        mut values: I,
    ) -> Result<()>
    where
        I: Iterator<Item = &'s str>,
    {
        let initial_rows = self.initial_rows;
        let to_insert = self.to_insert;

        let (col_idx, col) = self.column_mut(name, InfluxColumnType::Tag)?;

        let mut stats = StatValues::new_empty();
        match &mut col.data {
            ColumnData::Tag(col_data, dict, _) => {
                col_data.resize(initial_rows + to_insert, NULL_DID);

                let mut push = |idx: usize| {
                    let value = values.next().ok_or(Error::InsufficientValues)?;
                    col_data[initial_rows + idx] = dict.lookup_value_or_insert(value);
                    stats.update(value);
                    Ok::<_, Error>(())
                };

                match valid_mask {
                    Some(mask) => {
                        for idx in set_position_iterator(mask, to_insert) {
                            push(idx)?;
                        }
                    }
                    None => {
                        for idx in 0..to_insert {
                            push(idx)?;
                        }
                    }
                }
            }
            x => unreachable!("expected tag got {} for column \"{}\"", x, name),
        }

        append_valid_mask(col, valid_mask, to_insert);

        stats.update_for_nulls(to_insert as u64 - stats.total_count);
        self.statistics.push((col_idx, Statistics::String(stats)));

        Ok(())
    }

    /// Write the tag typed column identified by `name`
    ///
    /// For each set bit in `valid_mask` an a value from `values` is inserted at the
    /// corresponding index in the column. Nulls are inserted for the other rows
    ///
    /// # Panic
    ///
    /// - panics if this column has already been written to by this `Writer`
    ///
    pub fn write_tag_dict<'s, K, V>(
        &mut self,
        name: &str,
        valid_mask: Option<&[u8]>,
        mut keys: K,
        values: V,
    ) -> Result<()>
    where
        K: Iterator<Item = usize>,
        V: ExactSizeIterator<Item = &'s str>,
    {
        let initial_rows = self.initial_rows;
        let to_insert = self.to_insert;

        let (col_idx, col) = self.column_mut(name, InfluxColumnType::Tag)?;

        let mut stats = StatValues::new_empty();
        match &mut col.data {
            ColumnData::Tag(col_data, dict, _) => {
                // Lazily compute mappings to handle dictionaries with unused mappings
                let mut mapping: Vec<_> = values.map(|value| (value, None)).collect();

                // make space for all the values that we need to insert
                col_data.resize(initial_rows + to_insert, NULL_DID);

                let mut push = |idx: usize| {
                    // for each index, get the next key to insert...
                    let key = keys.next().ok_or(Error::InsufficientValues)?;

                    // and then get the already-known value for this key and maybe its did
                    let (value, maybe_did) =
                        mapping.get_mut(key).ok_or(Error::KeyNotFound { key })?;

                    let was_already_known = maybe_did.is_some();

                    // If we don't already have the did, then compute it and insert it. for next
                    // time.
                    let did = maybe_did.get_or_insert_with(|| dict.lookup_value_or_insert(value));

                    col_data[initial_rows + idx] = *did;

                    // we know that `update` does a bunch of comparisons to find mins and maxes, but
                    // if we know that this string value was already seen, we know that the mins and
                    // maxes won't change. so we can just short-circuit to count one more value
                    // instead.
                    if was_already_known {
                        stats.add_one_value();
                    } else {
                        stats.update(*value);
                    }

                    Ok::<_, Error>(())
                };

                // then iterate over all the indices that are actually valid, using the given valid
                // mask.
                match valid_mask {
                    Some(mask) => {
                        for idx in set_position_iterator(mask, to_insert) {
                            push(idx)?;
                        }
                    }
                    None => {
                        for idx in 0..to_insert {
                            push(idx)?;
                        }
                    }
                }
            }
            x => unreachable!("expected tag got {} for column \"{}\"", x, name),
        }

        append_valid_mask(col, valid_mask, to_insert);

        stats.update_for_nulls(to_insert as u64 - stats.total_count);
        self.statistics.push((col_idx, Statistics::String(stats)));

        Ok(())
    }

    /// Write the time typed column identified by `name`
    ///
    /// For each set bit in `valid_mask` an a value from `values` is inserted at the
    /// corresponding index in the column. Nulls are inserted for the other rows
    ///
    /// # Panic
    ///
    /// - panics if this column has already been written to by this `Writer`
    ///
    pub fn write_time<I>(&mut self, name: &str, mut values: I) -> Result<()>
    where
        I: Iterator<Item = i64>,
    {
        let initial_rows = self.initial_rows;
        let to_insert = self.to_insert;

        let (col_idx, col) = self.column_mut(name, InfluxColumnType::Timestamp)?;

        let mut stats = StatValues::new_empty();
        match &mut col.data {
            ColumnData::I64(col_data, _) => {
                col_data.resize(initial_rows + to_insert, 0_i64);
                for idx in 0..to_insert {
                    let value = values.next().ok_or(Error::InsufficientValues)?;
                    col_data[initial_rows + idx] = value;
                    stats.update(&value)
                }
            }
            x => unreachable!("expected i64 got {} for column \"{}\"", x, name),
        }

        append_valid_mask(col, None, to_insert);

        stats.update_for_nulls(to_insert as u64 - stats.total_count);
        self.statistics.push((col_idx, Statistics::I64(stats)));

        Ok(())
    }

    /// Write the time typed column identified by `name`, with no nulls and copying directly from
    /// the given slice while filling in the spots not covered by the slice with the last value of
    /// the slice
    ///
    /// # Panic
    ///
    /// - panics if this column has already been written to by this `Writer`
    ///
    pub fn write_time_from_slice(&mut self, name: &str, times: &[i64]) -> Result<()> {
        self.write_from_slice_with_col_ty(name, times, InfluxColumnType::Timestamp)
    }

    /// Write the provided MutableBatch
    pub(crate) fn write_batch(&mut self, src: &MutableBatch) -> Result<()> {
        assert_eq!(src.row_count, self.to_insert);

        for (src_col_name, src_col_idx) in &src.column_names {
            let src_col = &src.columns[*src_col_idx];
            let (dst_col_idx, dst_col) = self.column_mut(src_col_name, src_col.influx_type)?;

            let stats = match (&mut dst_col.data, &src_col.data) {
                (ColumnData::F64(dst_data, _), ColumnData::F64(src_data, stats)) => {
                    dst_data.extend_from_slice(src_data);
                    Statistics::F64(stats.clone())
                }
                (ColumnData::I64(dst_data, _), ColumnData::I64(src_data, stats)) => {
                    dst_data.extend_from_slice(src_data);
                    Statistics::I64(stats.clone())
                }
                (ColumnData::U64(dst_data, _), ColumnData::U64(src_data, stats)) => {
                    dst_data.extend_from_slice(src_data);
                    Statistics::U64(stats.clone())
                }
                (ColumnData::Bool(dst_data, _), ColumnData::Bool(src_data, stats)) => {
                    dst_data.extend_from(src_data);
                    Statistics::Bool(stats.clone())
                }
                (ColumnData::String(dst_data, _), ColumnData::String(src_data, stats)) => {
                    dst_data.extend_from(src_data);
                    Statistics::String(stats.clone())
                }
                (
                    ColumnData::Tag(dst_data, dst_dict, _),
                    ColumnData::Tag(src_data, src_dict, stats),
                ) => {
                    let mapping: Vec<_> = src_dict
                        .values()
                        .iter()
                        .map(|value| dst_dict.lookup_value_or_insert(value))
                        .collect();

                    dst_data.extend(src_data.iter().map(|src_id| match *src_id {
                        NULL_DID => NULL_DID,
                        _ => mapping[*src_id as usize],
                    }));

                    Statistics::String(stats.clone())
                }
                _ => unreachable!("src: {}, dst: {}", src_col.data, dst_col.data),
            };

            dst_col.valid.extend_from(&src_col.valid);
            self.statistics.push((dst_col_idx, stats));
        }
        Ok(())
    }

    /// Write `range` rows from the provided MutableBatch
    pub(crate) fn write_batch_range(
        &mut self,
        src: &MutableBatch,
        range: Range<usize>,
    ) -> Result<()> {
        self.write_batch_ranges(src, &[range])
    }

    /// Write the rows identified by `ranges` to the provided MutableBatch
    pub(crate) fn write_batch_ranges(
        &mut self,
        src: &MutableBatch,
        ranges: &[Range<usize>],
    ) -> Result<()> {
        let to_insert = self.to_insert;

        if to_insert == src.row_count {
            return self.write_batch(src);
        }

        for (src_col_name, src_col_idx) in &src.column_names {
            let src_col = &src.columns[*src_col_idx];
            let (dst_col_idx, dst_col) = self.column_mut(src_col_name, src_col.influx_type)?;
            let stats = match (&mut dst_col.data, &src_col.data) {
                (ColumnData::F64(dst_data, _), ColumnData::F64(src_data, _)) => Statistics::F64(
                    write_slice(to_insert, ranges, src_col.valid.bytes(), src_data, dst_data),
                ),
                (ColumnData::I64(dst_data, _), ColumnData::I64(src_data, _)) => Statistics::I64(
                    write_slice(to_insert, ranges, src_col.valid.bytes(), src_data, dst_data),
                ),
                (ColumnData::U64(dst_data, _), ColumnData::U64(src_data, _)) => Statistics::U64(
                    write_slice(to_insert, ranges, src_col.valid.bytes(), src_data, dst_data),
                ),
                (ColumnData::Bool(dst_data, _), ColumnData::Bool(src_data, _)) => {
                    dst_data.reserve(to_insert);
                    let mut stats = StatValues::new_empty();
                    for range in ranges {
                        dst_data.extend_from_range(src_data, range.clone());
                        compute_bool_stats(
                            src_col.valid.bytes(),
                            range.clone(),
                            src_data,
                            &mut stats,
                        )
                    }
                    Statistics::Bool(stats)
                }
                (ColumnData::String(dst_data, _), ColumnData::String(src_data, _)) => {
                    let mut stats = StatValues::new_empty();
                    for range in ranges {
                        dst_data.extend_from_range(src_data, range.clone());
                        compute_stats(src_col.valid.bytes(), range.clone(), &mut stats, |x| {
                            src_data.get(x).unwrap()
                        })
                    }
                    Statistics::String(stats)
                }
                (
                    ColumnData::Tag(dst_data, dst_dict, _),
                    ColumnData::Tag(src_data, src_dict, _),
                ) => {
                    dst_data.reserve(to_insert);

                    let mut mapping: Vec<_> = vec![None; src_dict.values().len()];
                    let mut stats = StatValues::new_empty();
                    for range in ranges {
                        dst_data.extend(src_data[range.clone()].iter().map(
                            |src_id| match *src_id {
                                NULL_DID => {
                                    stats.update_for_nulls(1);
                                    NULL_DID
                                }
                                _ => {
                                    let maybe_did = &mut mapping[*src_id as usize];
                                    match maybe_did {
                                        Some(did) => {
                                            stats.total_count += 1;
                                            *did
                                        }
                                        None => {
                                            let value = src_dict.lookup_id(*src_id).unwrap();
                                            stats.update(value);

                                            let did = dst_dict.lookup_value_or_insert(value);
                                            *maybe_did = Some(did);
                                            did
                                        }
                                    }
                                }
                            },
                        ));
                    }

                    Statistics::String(stats)
                }
                _ => unreachable!(),
            };

            dst_col.valid.reserve(to_insert);
            for range in ranges {
                dst_col
                    .valid
                    .extend_from_range(&src_col.valid, range.clone());
            }

            self.statistics.push((dst_col_idx, stats));
        }
        Ok(())
    }

    fn column_mut(
        &mut self,
        name: &str,
        influx_type: InfluxColumnType,
    ) -> Result<(usize, &mut Column)> {
        let columns_len = self.batch.columns.len();

        // Fetch the index of the column with `name`, inserting a new column
        // into the writer's batch if none exists and the column insert
        // validator allows.
        let column_idx = *match self.batch.column_names.entry_ref(name) {
            EntryRef::Occupied(ref v) => v.get(),
            EntryRef::Vacant(v) => {
                self.column_insert_validator
                    .validate_insertion(name, influx_type)
                    .context(ColumnInsertionRejectedSnafu)?;
                v.insert(columns_len)
            }
        };

        if columns_len == column_idx {
            self.batch
                .columns
                .push(Column::new(self.initial_rows, influx_type))
        }

        let col = &mut self.batch.columns[column_idx];

        if col.influx_type != influx_type {
            return Err(Error::TypeMismatch {
                column: name.to_string(),
                existing: col.influx_type,
                inserted: influx_type,
            });
        }

        assert_eq!(
            col.valid.len(),
            self.initial_rows,
            "expected {} rows in column \"{}\" got {} when performing write of {} rows",
            self.initial_rows,
            name,
            col.valid.len(),
            self.to_insert
        );

        Ok((column_idx, col))
    }

    /// Commits the writes performed on this [`Writer`]. This will update the statistics
    /// and pad any unwritten columns with nulls
    pub fn commit(mut self) {
        let initial_rows = self.initial_rows;
        let to_insert = self.to_insert;
        let final_rows = initial_rows + to_insert;

        self.statistics
            .sort_unstable_by_key(|(col_idx, _)| *col_idx);
        let mut statistics = self.statistics.iter();

        for (col_idx, col) in self.batch.columns.iter_mut().enumerate() {
            // All columns should either have received a write and have statistics or not
            if col.valid.len() == initial_rows {
                col.push_nulls_to_len(final_rows);
            } else {
                assert_eq!(
                    col.valid.len(),
                    final_rows,
                    "expected {} rows in column index {} got {} when performing write of {} rows",
                    final_rows,
                    col_idx,
                    col.valid.len(),
                    to_insert
                );

                let (stats_col_idx, stats) = statistics.next().unwrap();
                assert_eq!(*stats_col_idx, col_idx);
                assert_eq!(stats.total_count(), to_insert as u64);

                match (&mut col.data, stats) {
                    (ColumnData::F64(col_data, stats), Statistics::F64(new)) => {
                        assert_eq!(col_data.len(), final_rows);
                        stats.update_from(new);
                    }
                    (ColumnData::I64(col_data, stats), Statistics::I64(new)) => {
                        assert_eq!(col_data.len(), final_rows);
                        stats.update_from(new);
                    }
                    (ColumnData::U64(col_data, stats), Statistics::U64(new)) => {
                        assert_eq!(col_data.len(), final_rows);
                        stats.update_from(new);
                    }
                    (ColumnData::String(col_data, stats), Statistics::String(new)) => {
                        assert_eq!(col_data.len(), final_rows);
                        stats.update_from(new);
                    }
                    (ColumnData::Bool(col_data, stats), Statistics::Bool(new)) => {
                        assert_eq!(col_data.len(), final_rows);
                        stats.update_from(new);
                    }
                    (ColumnData::Tag(col_data, dict, stats), Statistics::String(new)) => {
                        assert_eq!(col_data.len(), final_rows);
                        stats.update_from(new);
                        stats.distinct_count = match stats.null_count {
                            Some(0) => NonZeroU64::new(dict.values().len() as u64),
                            Some(_) => NonZeroU64::new(dict.values().len() as u64 + 1),
                            None => unreachable!("mutable batch keeps null counts"),
                        }
                    }
                    _ => unreachable!("column: {}, statistics: {}", col.data, stats.type_name()),
                }
            }
        }
        self.batch.row_count = final_rows;
        self.success = true;
    }
}

fn set_position_iterator(valid_mask: &[u8], to_insert: usize) -> BitIndexIterator<'_> {
    BitIndexIterator::new(valid_mask, 0, to_insert)
}

fn append_valid_mask(column: &mut Column, valid_mask: Option<&[u8]>, to_insert: usize) {
    match valid_mask {
        Some(mask) => column.valid.append_bits(to_insert, mask),
        None => column.valid.append_set(to_insert),
    }
}

fn compute_bool_stats(
    valid: &[u8],
    range: Range<usize>,
    col_data: &BitSet,
    stats: &mut StatValues<bool>,
) {
    // There are likely faster ways to do this
    let index_offsets = BitIndexIterator::new(valid, range.start, range.end - range.start);

    let mut non_null_count = 0_u64;
    for offset in index_offsets {
        let value = col_data.get(offset + range.start);
        stats.update(&value);
        non_null_count += 1;
    }

    let to_insert = range.end - range.start;
    stats.update_for_nulls(to_insert as u64 - non_null_count);
}

fn write_slice<T>(
    to_insert: usize,
    ranges: &[Range<usize>],
    valid: &[u8],
    src_data: &[T],
    dst_data: &mut Vec<T>,
) -> StatValues<T>
where
    T: Clone + PartialOrd + IsNan,
{
    dst_data.reserve(to_insert);
    let mut stats = StatValues::new_empty();
    for range in ranges {
        dst_data.extend_from_slice(&src_data[range.clone()]);
        compute_stats(valid, range.clone(), &mut stats, |x| &src_data[x]);
    }
    stats
}

fn compute_stats<'a, T, U, F>(
    valid: &[u8],
    range: Range<usize>,
    stats: &mut StatValues<T>,
    accessor: F,
) where
    U: 'a + ToOwned<Owned = T> + PartialOrd + ?Sized + IsNan,
    F: Fn(usize) -> &'a U,
    T: std::borrow::Borrow<U>,
{
    let index_offsets = BitIndexIterator::new(valid, range.start, range.end - range.start);

    let mut non_null_count = 0_u64;
    for offset in index_offsets {
        let value = accessor(offset + range.start);
        stats.update(value);
        non_null_count += 1;
    }

    let to_insert = range.end - range.start;
    stats.update_for_nulls(to_insert as u64 - non_null_count);
}

impl<T> Drop for Writer<'_, T> {
    fn drop(&mut self) {
        if !self.success {
            let initial_rows = self.initial_rows;
            let initial_cols = self.initial_cols;

            if self.batch.columns.len() != initial_cols {
                self.batch.columns.truncate(initial_cols);
                self.batch.column_names.retain(|_, v| *v < initial_cols)
            }

            for col in &mut self.batch.columns {
                col.valid.truncate(initial_rows);
                match &mut col.data {
                    ColumnData::F64(col_data, _) => col_data.truncate(initial_rows),
                    ColumnData::I64(col_data, _) => col_data.truncate(initial_rows),
                    ColumnData::U64(col_data, _) => col_data.truncate(initial_rows),
                    ColumnData::String(col_data, _) => col_data.truncate(initial_rows),
                    ColumnData::Bool(col_data, _) => col_data.truncate(initial_rows),
                    ColumnData::Tag(col_data, dict, _) => {
                        col_data.truncate(initial_rows);
                        match col_data.iter().max() {
                            Some(max) => dict.truncate(*max),
                            None => dict.clear(),
                        }
                    }
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use core::iter::Copied;
    use std::slice;

    use arrow::{array::BooleanBufferBuilder, buffer::BooleanBuffer};
    use assert_matches::assert_matches;
    use proptest::{
        option,
        prelude::{Arbitrary, Just, Strategy, any},
        proptest,
    };

    use super::*;

    struct AlwaysRejectingValidator<F> {
        return_err: F,
    }

    impl<F> ColumnInsertValidator for AlwaysRejectingValidator<F>
    where
        F: Fn() -> InvalidInsertionError,
    {
        fn validate_insertion(
            &self,
            _col_name: &str,
            _col_type: InfluxColumnType,
        ) -> std::result::Result<(), InvalidInsertionError> {
            Err((self.return_err)())
        }
    }

    /// A basic test to assert that each write method results in the
    /// [`ColumnInsertValidator`] being invoked.
    #[test]
    fn writer_delegates_to_column_validator() {
        const COLUMN: &str = "anything";

        let mut b = MutableBatch::new();
        // Provide the writer with a validator that always returns a well know error.
        let mut w = Writer::new_with_column_validator(
            &mut b,
            7,
            AlwaysRejectingValidator {
                return_err: || InvalidInsertionError::TableSchemaConflict {
                    column: COLUMN.to_string(),
                    table_type: InfluxColumnType::Timestamp,
                    given: InfluxColumnType::Tag,
                },
            },
        );

        assert_matches!(
            w.write_tag("bread", None, std::iter::once("fluffy")),
            Err(Error::ColumnInsertionRejected { source }) => {
                assert_matches!(source, InvalidInsertionError::TableSchemaConflict { column, .. } if column == COLUMN)
            }
        );
        assert_matches!(
            w.write_tag_dict(
                "bread",
                None,
                vec![1, 0, 0, 1].into_iter(),
                vec!["fluffy", "stale"].into_iter(),
            ),
            Err(Error::ColumnInsertionRejected { source }) => {
                assert_matches!(source, InvalidInsertionError::TableSchemaConflict { column, .. } if column == COLUMN)
            }
        );
        assert_matches!(
            w.write_f64("foo", &[1], std::iter::once(4.2)),
            Err(Error::ColumnInsertionRejected { source }) => {
                assert_matches!(source, InvalidInsertionError::TableSchemaConflict { column, .. } if column == COLUMN)
            }
        );

        assert_matches!(
            w.write_f64s_from_slice("foo", &[4.2]),
            Err(Error::ColumnInsertionRejected { source }) => {
                assert_matches!(source, InvalidInsertionError::TableSchemaConflict { column, .. } if column == COLUMN)
            }
        );

        assert_matches!(
            w.write_i64("bar", &[1], std::iter::once(-42)),
            Err(Error::ColumnInsertionRejected { source }) => {
                assert_matches!(source, InvalidInsertionError::TableSchemaConflict { column, .. } if column == COLUMN)
            }
        );
        assert_matches!(
            w.write_i64s_from_slice("bar", &[-42]),
            Err(Error::ColumnInsertionRejected { source }) => {
                assert_matches!(source, InvalidInsertionError::TableSchemaConflict { column, .. } if column == COLUMN)
            }
        );

        assert_matches!(
            w.write_u64("baz", &[1], std::iter::once(42)),
            Err(Error::ColumnInsertionRejected { source }) => {
                assert_matches!(source, InvalidInsertionError::TableSchemaConflict { column, .. } if column == COLUMN)
            }
        );
        assert_matches!(
            w.write_u64s_from_slice("baz", &[42]),
            Err(Error::ColumnInsertionRejected { source }) => {
                assert_matches!(source, InvalidInsertionError::TableSchemaConflict { column, .. } if column == COLUMN)
            }
        );

        assert_matches!(
            w.write_string("bananas", None, std::iter::once("great")),
            Err(Error::ColumnInsertionRejected { source }) => {
                assert_matches!(source, InvalidInsertionError::TableSchemaConflict { column, .. } if column == COLUMN)
            }
        );
        assert_matches!(
            w.write_bool("answer", None, std::iter::once(true)),
            Err(Error::ColumnInsertionRejected { source }) => {
                assert_matches!(source, InvalidInsertionError::TableSchemaConflict { column, .. } if column == COLUMN)
            }
        );
    }

    /// Iterator wrapper that repeats the last element forever, stolen from
    /// `mutable_batch_pb::decode` for these tests
    ///
    /// This will just yield `None` if the wrapped iterator was empty.
    struct RepeatLastElement<I>
    where
        I: Iterator,
        I::Item: Clone,
    {
        next: Option<I::Item>,
        inner: I,
    }

    impl<I> RepeatLastElement<I>
    where
        I: Iterator,
        I::Item: Clone,
    {
        fn new(mut inner: I) -> Self {
            let next = inner.next();

            Self { inner, next }
        }
    }

    impl<I> Iterator for RepeatLastElement<I>
    where
        I: Iterator,
        I::Item: Clone,
    {
        type Item = I::Item;

        #[inline]
        fn next(&mut self) -> Option<Self::Item> {
            match self.inner.next() {
                None => self.next.clone(),
                Some(n) => self.next.replace(n),
            }
        }
    }

    // generate a bunch of values (`Vec<A>`), an accompanying valid mask (`Vec<u8>`) and a count of
    // how many items reside in that value vec and the valid mask combined (when you account for
    // valid values and nulls)
    //
    // This function will never generate an empty value vector, and thus will also never generate a
    // count that is less than 1 or a mask that is all null.
    fn generate_values_and_mask<A: Arbitrary + Clone>()
    -> impl Strategy<Value = (BooleanBuffer, Vec<A>)> {
        (
            any::<A>(),
            proptest::collection::vec(option::of(any::<A>()), 1..80),
        )
            .prop_flat_map(|(guaranteed_val, vec)| (Just(guaranteed_val), 0..vec.len(), Just(vec)))
            .prop_map(|(guaranteed_value, guaranteed_idx, mut maybe_null_vec)| {
                maybe_null_vec.insert(guaranteed_idx, Some(guaranteed_value));

                let mut mask = BooleanBufferBuilder::new(maybe_null_vec.len());

                let values = maybe_null_vec
                    .into_iter()
                    .filter_map(|o| {
                        match o {
                            Some(_) => mask.append(true),
                            None => mask.append(false),
                        }

                        o
                    })
                    .collect();

                (mask.finish(), values)
            })
    }

    const COL_NAME: &str = "the_column";

    // A test to make sure some optimized versions of writing values to a `Writer` (which utilize
    // some better slice copies and such instead of iterators) work exactly the same as the previous
    // methods. (they should only be more performant, not differing in behavior).
    fn test_writing_variations<T: Copy>(
        to_insert: usize,
        mask: &[u8],
        values: &[T],
        write_slice_with_mask_fn: impl Fn(
            &mut Writer<'_, NoopColumnInsertValidator>,
            &str,
            &[u8],
            &[T],
        ) -> Result<()>,
        write_iter_with_mask_fn: impl Fn(
            &mut Writer<'_, NoopColumnInsertValidator>,
            &str,
            &[u8],
            RepeatLastElement<Copied<slice::Iter<'_, T>>>,
        ) -> Result<()>,
        write_slice_fn: impl Fn(&mut Writer<'_, NoopColumnInsertValidator>, &str, &[T]) -> Result<()>,
    ) {
        // so first we want to check writing with a valid mask to the slice version and the iter
        // version
        let mut slice_mask_batch = MutableBatch::default();
        let mut slice_mask_writer = Writer::new(&mut slice_mask_batch, to_insert);
        write_slice_with_mask_fn(&mut slice_mask_writer, COL_NAME, mask, values).unwrap();

        let mut iter_mask_batch = MutableBatch::default();
        let mut iter_mask_writer = Writer::new(&mut iter_mask_batch, to_insert);
        write_iter_with_mask_fn(
            &mut iter_mask_writer,
            COL_NAME,
            mask,
            RepeatLastElement::new(values.iter().copied()),
        )
        .unwrap();

        drop(slice_mask_writer);
        drop(iter_mask_writer);

        // then compare the batches to make sure they act the same
        assert_eq!(iter_mask_batch, slice_mask_batch);

        // We want to write them all again to make sure it works correctly when there's values
        // in the batch AND when there's not.
        let mut slice_mask_writer = Writer::new(&mut slice_mask_batch, to_insert);
        write_slice_with_mask_fn(&mut slice_mask_writer, COL_NAME, mask, values).unwrap();

        let mut iter_mask_writer = Writer::new(&mut iter_mask_batch, to_insert);
        write_iter_with_mask_fn(
            &mut iter_mask_writer,
            COL_NAME,
            mask,
            RepeatLastElement::new(values.iter().copied()),
        )
        .unwrap();

        drop(slice_mask_writer);
        drop(iter_mask_writer);

        // and compare the batches again
        assert_eq!(iter_mask_batch, slice_mask_batch);

        // Ok and now to test that our fn to write without a mask works the same as if we wrote
        // with an iterator and an all-valid mask

        let mut all_valid_mask = BooleanBufferBuilder::new(values.len());
        all_valid_mask.append_n(values.len(), true);
        let all_valid_mask = all_valid_mask.finish();

        // we're only gonna insert as many values exist in `values` here
        let to_insert = values.len();

        let mut slice_batch = MutableBatch::default();
        let mut slice_writer = Writer::new(&mut slice_batch, to_insert);
        write_slice_fn(&mut slice_writer, COL_NAME, values).unwrap();

        let mut iter_batch = MutableBatch::default();
        let mut iter_writer = Writer::new(&mut iter_batch, to_insert);
        write_iter_with_mask_fn(
            &mut iter_writer,
            COL_NAME,
            all_valid_mask.values(),
            RepeatLastElement::new(values.iter().copied()),
        )
        .unwrap();

        drop(slice_writer);
        drop(iter_writer);

        assert_eq!(iter_batch, slice_batch);

        // We want to write them all again to make sure it works correctly when there's values
        // in the batch AND when there's not.
        let mut slice_writer = Writer::new(&mut slice_batch, to_insert);
        write_slice_fn(&mut slice_writer, COL_NAME, values).unwrap();

        let mut iter_writer = Writer::new(&mut iter_batch, to_insert);
        write_iter_with_mask_fn(
            &mut iter_writer,
            COL_NAME,
            all_valid_mask.values(),
            RepeatLastElement::new(values.iter().copied()),
        )
        .unwrap();

        drop(slice_writer);
        drop(iter_writer);

        assert_eq!(iter_batch, slice_batch);
    }

    proptest! {
        #[test]
        fn write_i64s_slice_works_same_as_iter(
            (mask, values) in generate_values_and_mask::<i64>()
        ) {
            test_writing_variations(
                mask.len(),
                mask.values(),
                &values,
                |writer, name, mask, vals| writer.write_i64s_from_slice_with_mask(name, mask, vals),
                |writer, name, mask, iter| writer.write_i64(name, mask, iter),
                |writer, name, vals| writer.write_i64s_from_slice(name, vals)
            );
        }

        #[test]
        fn write_u64s_slice_works_same_as_iter(
            (mask, values) in generate_values_and_mask::<u64>()
        ) {
            test_writing_variations(
                mask.len(),
                mask.values(),
                &values,
                |writer, name, mask, vals| writer.write_u64s_from_slice_with_mask(name, mask, vals),
                |writer, name, mask, iter| writer.write_u64(name, mask, iter),
                |writer, name, vals| writer.write_u64s_from_slice(name, vals)
            );
        }

        #[test]
        fn write_f64s_slice_works_same_as_iter(
            (mask, values) in generate_values_and_mask::<f64>()
        ) {
            test_writing_variations(
                mask.len(),
                mask.values(),
                &values,
                |writer, name, mask, vals| writer.write_f64s_from_slice_with_mask(name, mask, vals),
                |writer, name, mask, iter| writer.write_f64(name, mask, iter),
                |writer, name, vals| writer.write_f64s_from_slice(name, vals)
            );
        }
    }
}
