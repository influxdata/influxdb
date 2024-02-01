//! A [`Column`] stores the rows for a given column name

use arrow::{
    array::{
        ArrayDataBuilder, ArrayRef, BooleanArray, Float64Array, Int64Array,
        TimestampNanosecondArray, UInt64Array,
    },
    buffer::NullBuffer,
    datatypes::DataType,
    error::ArrowError,
};
use arrow_util::{bitset::BitSet, string::PackedStringArray};
use data_types::{StatValues, Statistics};
use schema::{InfluxColumnType, InfluxFieldType, TIME_DATA_TYPE};
use snafu::{ResultExt, Snafu};
use std::{fmt::Formatter, iter, mem, num::NonZeroU64, sync::Arc};

/// A "dictionary ID" (DID) is a compact numeric representation of an interned
/// string in the dictionary. The same string always maps the same DID.
///
/// DIDs can be compared, hashed and cheaply copied around, just like small integers.
///
/// An i32 is used to match the default for Arrow dictionaries
#[allow(clippy::upper_case_acronyms)]
pub(crate) type DID = i32;

/// An invalid DID used for NULL rows
pub(crate) const NULL_DID: DID = -1;

/// The type of the dictionary used
type Dictionary = arrow_util::dictionary::StringDictionary<DID>;

/// A type-agnostic way of splitting the various [`ColumnData`] arrays.
///
/// This macro is required because it's not possible to write a generic function
/// that operates on all "data" types across [`ColumnData`] variants.`
macro_rules! split_off_column {
    ($self:expr, $data:expr, $n:expr, $stats:expr, $right_nulls:expr, $($ty:tt)+) => {{
        // Compute the new number of nulls in the left side of the split.
        let left_nulls = $stats.null_count.map(|v| v - $right_nulls);

        // Update the stats for the left side of the split.
        *$stats = StatValues::new(None, None, $self.valid.len() as u64, left_nulls);

        // Generate the right side of the split (with minimal stats).
        let right_data = $data.split_off($n);
        let right_len = right_data.len();
        $($ty)+(
            right_data,
            StatValues::new(
                None,
                None,
                right_len as _,
                Some($right_nulls),
            ),
        )
    }};
}

#[derive(Debug, Snafu)]
#[allow(missing_copy_implementations, missing_docs)]
pub enum Error {
    #[snafu(display(
        "Invalid null mask, expected to be {} bytes but was {}",
        expected_bytes,
        actual_bytes
    ))]
    InvalidNullMask {
        expected_bytes: usize,
        actual_bytes: usize,
    },

    #[snafu(display("Internal MUB error constructing Arrow Array: {}", source))]
    CreatingArrowArray { source: ArrowError },
}

/// A specialized `Error` for [`Column`] errors
pub type Result<T, E = Error> = std::result::Result<T, E>;

/// Stores the actual data for columns in a chunk along with summary
/// statistics
#[derive(Debug, Clone)]
pub struct Column {
    pub(crate) influx_type: InfluxColumnType,
    pub(crate) valid: BitSet,
    pub(crate) data: ColumnData,
}

/// The data for a column
#[derive(Debug, Clone)]
#[allow(missing_docs)]
pub enum ColumnData {
    /// These types contain arrays that contain an element for every logical row
    /// (including nulls).
    ///
    /// Null values are padded with an arbitrary dummy value.
    F64(Vec<f64>, StatValues<f64>), // NaN is ignored when computing statistics.
    I64(Vec<i64>, StatValues<i64>),
    U64(Vec<u64>, StatValues<u64>),
    Bool(BitSet, StatValues<bool>),

    /// The String encoding contains an entry for every logical row, and
    /// explicitly stores an empty string in the PackedStringArray for NULL
    /// values.
    String(PackedStringArray<i32>, StatValues<String>),

    /// Whereas the dictionary encoding does not store an explicit empty string
    /// in the internal PackedStringArray, nor does it create an entry in the
    /// dedupe map. A NULL entry is padded into the data vec using the
    /// [`NULL_DID`] value.
    ///
    /// Every distinct, non-null value is stored in the dictionary exactly once,
    /// and the data arrays contains the dictionary ID for every logical row
    /// (including nulls as described above).
    Tag(Vec<DID>, Dictionary, StatValues<String>),
}

impl std::fmt::Display for ColumnData {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::F64(col_data, _) => write!(f, "F64({})", col_data.len()),
            Self::I64(col_data, _) => write!(f, "I64({})", col_data.len()),
            Self::U64(col_data, _) => write!(f, "U64({})", col_data.len()),
            Self::String(col_data, _) => write!(f, "String({})", col_data.len()),
            Self::Bool(col_data, _) => write!(f, "Bool({})", col_data.len()),
            Self::Tag(col_data, dictionary, _) => write!(
                f,
                "Tag(keys:{},values:{})",
                col_data.len(),
                dictionary.values().len()
            ),
        }
    }
}

impl Column {
    pub(crate) fn new(row_count: usize, column_type: InfluxColumnType) -> Self {
        let mut valid = BitSet::new();
        valid.append_unset(row_count);

        // Keep track of how many total rows there are
        let total_count = row_count as u64;

        // If there are no values, there are no distinct values.
        let distinct_count = if row_count > 0 { Some(1) } else { None };

        let data = match column_type {
            InfluxColumnType::Field(InfluxFieldType::Boolean) => {
                let mut data = BitSet::new();
                data.append_unset(row_count);
                ColumnData::Bool(data, StatValues::new_all_null(total_count, None))
            }
            InfluxColumnType::Field(InfluxFieldType::UInteger) => ColumnData::U64(
                vec![0; row_count],
                StatValues::new_all_null(total_count, None),
            ),
            InfluxColumnType::Field(InfluxFieldType::Float) => ColumnData::F64(
                vec![0.0; row_count],
                StatValues::new_all_null(total_count, None),
            ),
            InfluxColumnType::Field(InfluxFieldType::Integer) | InfluxColumnType::Timestamp => {
                ColumnData::I64(
                    vec![0; row_count],
                    StatValues::new_all_null(total_count, None),
                )
            }
            InfluxColumnType::Field(InfluxFieldType::String) => ColumnData::String(
                PackedStringArray::new_empty(row_count),
                StatValues::new_all_null(total_count, distinct_count),
            ),
            InfluxColumnType::Tag => ColumnData::Tag(
                vec![NULL_DID; row_count],
                Default::default(),
                StatValues::new_all_null(total_count, distinct_count),
            ),
        };

        Self {
            influx_type: column_type,
            valid,
            data,
        }
    }

    /// Returns the [`InfluxColumnType`] of this column
    pub fn influx_type(&self) -> InfluxColumnType {
        self.influx_type
    }

    /// Returns the validity bitmask of this column
    pub fn valid_mask(&self) -> &BitSet {
        &self.valid
    }

    /// Returns a reference to this column's data
    pub fn data(&self) -> &ColumnData {
        &self.data
    }

    /// Ensures that the total length of this column is `len` rows,
    /// padding it with trailing NULLs if necessary
    pub(crate) fn push_nulls_to_len(&mut self, len: usize) {
        if self.valid.len() == len {
            return;
        }
        assert!(len > self.valid.len(), "cannot shrink column");
        let delta = len - self.valid.len();
        self.valid.append_unset(delta);

        match &mut self.data {
            ColumnData::F64(data, stats) => {
                data.resize(len, 0.);
                stats.update_for_nulls(delta as u64);
            }
            ColumnData::I64(data, stats) => {
                data.resize(len, 0);
                stats.update_for_nulls(delta as u64);
            }
            ColumnData::U64(data, stats) => {
                data.resize(len, 0);
                stats.update_for_nulls(delta as u64);
            }
            ColumnData::String(data, stats) => {
                data.extend(delta);
                stats.update_for_nulls(delta as u64);
            }
            ColumnData::Bool(data, stats) => {
                data.append_unset(delta);
                stats.update_for_nulls(delta as u64);
            }
            ColumnData::Tag(data, _dict, stats) => {
                data.resize(len, NULL_DID);
                stats.update_for_nulls(delta as u64);
            }
        }
    }

    /// Returns the number of rows in this column
    pub fn len(&self) -> usize {
        self.valid.len()
    }

    /// Returns true if this column contains no rows
    pub fn is_empty(&self) -> bool {
        self.valid.is_empty()
    }

    /// Returns this column's [`Statistics`]
    pub fn stats(&self) -> Statistics {
        match &self.data {
            ColumnData::F64(_, stats) => Statistics::F64(stats.clone()),
            ColumnData::I64(_, stats) => Statistics::I64(stats.clone()),
            ColumnData::U64(_, stats) => Statistics::U64(stats.clone()),
            ColumnData::Bool(_, stats) => Statistics::Bool(stats.clone()),
            ColumnData::String(_, stats) => Statistics::String(stats.clone()),
            ColumnData::Tag(_, dictionary, stats) => {
                let mut distinct_count = dictionary.values().len() as u64;
                if stats.null_count.expect("mutable batch keeps null counts") > 0 {
                    distinct_count += 1;
                }

                let mut stats = stats.clone();
                stats.distinct_count = distinct_count.try_into().ok();
                Statistics::String(stats)
            }
        }
    }

    /// The approximate memory size of the data in the column.
    ///
    /// This includes the size of `self`.
    pub fn size(&self) -> usize {
        let data_size = match &self.data {
            ColumnData::F64(v, stats) => {
                mem::size_of::<f64>() * v.capacity() + mem::size_of_val(stats)
            }
            ColumnData::I64(v, stats) => {
                mem::size_of::<i64>() * v.capacity() + mem::size_of_val(stats)
            }
            ColumnData::U64(v, stats) => {
                mem::size_of::<u64>() * v.capacity() + mem::size_of_val(stats)
            }
            ColumnData::Bool(v, stats) => v.byte_len() + mem::size_of_val(stats),
            ColumnData::Tag(v, dictionary, stats) => {
                mem::size_of::<DID>() * v.capacity() + dictionary.size() + mem::size_of_val(stats)
            }
            ColumnData::String(v, stats) => {
                v.size() + mem::size_of_val(stats) + stats.string_size()
            }
        };
        mem::size_of::<Self>() + data_size + self.valid.byte_len()
    }

    /// The approximate memory size of the data in the column, not counting for stats or self or
    /// whatever extra space has been allocated for the vecs
    pub fn size_data(&self) -> usize {
        match &self.data {
            ColumnData::F64(_, _) => mem::size_of::<f64>() * self.len(),
            ColumnData::I64(_, _) => mem::size_of::<i64>() * self.len(),
            ColumnData::U64(_, _) => mem::size_of::<u64>() * self.len(),
            ColumnData::Bool(_, _) => mem::size_of::<bool>() * self.len(),
            ColumnData::Tag(_, dictionary, _) => {
                mem::size_of::<DID>() * self.len() + dictionary.size()
            }
            ColumnData::String(v, _) => v.size(),
        }
    }

    /// Converts this column to an arrow [`ArrayRef`]
    pub fn to_arrow(&self) -> Result<ArrayRef> {
        let nulls = Some(NullBuffer::new(self.valid.to_arrow()));

        let data: ArrayRef = match &self.data {
            ColumnData::F64(data, _) => {
                let data = ArrayDataBuilder::new(DataType::Float64)
                    .len(data.len())
                    .add_buffer(data.iter().cloned().collect())
                    .nulls(nulls)
                    .build()
                    .context(CreatingArrowArraySnafu)?;
                Arc::new(Float64Array::from(data))
            }
            ColumnData::I64(data, _) => match self.influx_type {
                InfluxColumnType::Timestamp => {
                    let data = ArrayDataBuilder::new(TIME_DATA_TYPE())
                        .len(data.len())
                        .add_buffer(data.iter().cloned().collect())
                        .nulls(nulls)
                        .build()
                        .context(CreatingArrowArraySnafu)?;
                    Arc::new(TimestampNanosecondArray::from(data))
                }

                InfluxColumnType::Field(InfluxFieldType::Integer) => {
                    let data = ArrayDataBuilder::new(DataType::Int64)
                        .len(data.len())
                        .add_buffer(data.iter().cloned().collect())
                        .nulls(nulls)
                        .build()
                        .context(CreatingArrowArraySnafu)?;
                    Arc::new(Int64Array::from(data))
                }
                _ => unreachable!(),
            },
            ColumnData::U64(data, _) => {
                let data = ArrayDataBuilder::new(DataType::UInt64)
                    .len(data.len())
                    .add_buffer(data.iter().cloned().collect())
                    .nulls(nulls)
                    .build()
                    .context(CreatingArrowArraySnafu)?;
                Arc::new(UInt64Array::from(data))
            }
            ColumnData::String(data, _) => Arc::new(data.to_arrow(nulls)),
            ColumnData::Bool(data, _) => {
                let data = ArrayDataBuilder::new(DataType::Boolean)
                    .len(data.len())
                    .add_buffer(data.to_arrow().into_inner())
                    .nulls(nulls)
                    .build()
                    .context(CreatingArrowArraySnafu)?;
                Arc::new(BooleanArray::from(data))
            }
            ColumnData::Tag(data, dictionary, _) => {
                Arc::new(dictionary.to_arrow(data.iter().cloned(), nulls))
            }
        };

        assert_eq!(data.len(), self.len());

        Ok(data)
    }

    /// Split this [`Column`] at the specified row boundary, such that after
    /// this call, `self` contains the range of rows indexed from `[0, n)` and
    /// the returned value contains `[n, len)`.
    ///
    /// # Statistics
    ///
    /// For performance reasons, this operation leaves `self` and the returned
    /// [`Column`] with reduced summary statistics available.
    ///
    /// This allows the caller to selectively reconstruct the statistics that
    /// will be useful to the caller, instead of always paying the price of
    /// recomputing statistics, even if unused.
    ///
    /// For the following column types:
    ///
    ///  - [`ColumnData::F64`]
    ///  - [`ColumnData::I64`]
    ///  - [`ColumnData::U64`]
    ///  - [`ColumnData::Bool`]
    ///  - [`ColumnData::String`]
    ///
    /// The statistics for both [`Column`] contain only:
    ///
    ///  - Total count
    ///  - NULL count (see below)
    ///
    /// The NULL count is always present in the returned [`Column`], and only
    /// present in `self` if it had a NULL count statistic prior to the split.
    ///
    /// For [`ColumnData::Tag`] all the statistics above are included, with the
    /// addition of the distinct count.
    ///
    /// # Performance
    ///
    /// This call is `O(n)` where `n` is the number of elements in the right
    /// side of the split (the `[n, len)` interval) due to the need to copy
    /// and process these elements only.
    ///
    /// The size of the left-side interval (the [0, n) interval) does not affect
    /// performance of this call.
    pub fn split_off(&mut self, n: usize) -> Self {
        if n > self.len() {
            return Self::new(0, self.influx_type);
        }

        // Split the null mask into [0, n) and [n, len).
        let right_bitmap = self.valid.split_off(n);

        // Compute the null count for the right side.
        let right_nulls = right_bitmap.count_zeros() as u64;

        // Split the actual data and update/compute the statistics.
        let right_data = match &mut self.data {
            ColumnData::F64(data, left_stats) => {
                split_off_column!(self, data, n, left_stats, right_nulls, ColumnData::F64)
            }
            ColumnData::I64(data, left_stats) => {
                split_off_column!(self, data, n, left_stats, right_nulls, ColumnData::I64)
            }
            ColumnData::U64(data, left_stats) => {
                split_off_column!(self, data, n, left_stats, right_nulls, ColumnData::U64)
            }
            ColumnData::String(data, left_stats) => {
                split_off_column!(self, data, n, left_stats, right_nulls, ColumnData::String)
            }
            ColumnData::Bool(data, left_stats) => {
                split_off_column!(self, data, n, left_stats, right_nulls, ColumnData::Bool)
            }
            ColumnData::Tag(data, dict, left_stats) => {
                // Split the tag data at the value index.
                let mut new_data = data.split_off(n);

                // "new_data" now contains values [n, len), and likely no longer
                // references all the values in the current dictionary.
                //
                // Generate a dictionary for "new_data" that contains only the
                // values that appear in "new_data", and rewrite the dictionary
                // IDs in "new_data" to reflect this new mapping.
                let new_dict = rebuild_dictionary(dict, &mut new_data);

                // The original "dict" may now contain references to keys that
                // appear only in "new_data", and never in the "data" that
                // remains.
                //
                // Rewrite this dictionary, to shrink it to contain only entries
                // that appear in "data".
                //
                // Note: this may not be required if Arrow can tolerate a
                // dictionary with more keys than necessary, but it optimises
                // for memory utilisation.
                *dict = rebuild_dictionary(dict, data);

                // Compute how many NULLs are left in the left side.
                let left_nulls = left_stats.null_count.map(|v| v - right_nulls);

                // It's effectively free to compute the distinct count of a
                // column using dictionary encoding - it's simply the length of
                // the dictionary, and plus one if a NULL exists - maintain
                // distinct counts in the returned statistics.
                let make_distinct_count = |dict: &Dictionary, has_null| {
                    let mut count = dict.values().len();
                    if has_null {
                        count += 1;
                    }
                    NonZeroU64::try_from(count as u64).ok()
                };

                let left_distinct = make_distinct_count(dict, left_nulls.unwrap_or_default() > 0);
                let right_distinct = make_distinct_count(&new_dict, right_nulls > 0);

                // Update the stats for the left side of the split.
                *left_stats = StatValues::new_with_distinct(
                    None,
                    None,
                    self.valid.len() as _,
                    left_nulls,
                    left_distinct,
                );

                // Generate the right side of the split.
                let new_len = new_data.len();
                ColumnData::Tag(
                    new_data,
                    new_dict,
                    StatValues::new_with_distinct(
                        None,
                        None,
                        new_len as _,
                        Some(right_nulls),
                        right_distinct,
                    ),
                )
            }
        };

        Self {
            influx_type: self.influx_type,
            valid: right_bitmap,
            data: right_data,
        }
    }
}

/// Constructs a new, minimal dictionary for `data`, rewriting the dictionary
/// IDs in `data` to use the new returned dictionary.
fn rebuild_dictionary(original: &Dictionary, data: &mut [DID]) -> Dictionary {
    let mut dict = Dictionary::new();

    for id in data.iter_mut() {
        if *id == NULL_DID {
            continue;
        }
        let value = original
            .lookup_id(*id)
            .expect("original dictionary does not contain value");
        *id = dict.lookup_value_or_insert(value);
    }

    dict
}

/// Recompute the min/max values for the given [`Column`].
///
/// This is an `O(n)` operation for:
///
///  - [`ColumnData::F64`]
///  - [`ColumnData::I64`]
///  - [`ColumnData::U64`]
///  - [`ColumnData::Bool`]
///  - [`ColumnData::String`]
///
/// This is an `O(distinct(n))` operation for [`ColumnData::Tag`].
pub fn recompute_min_max(c: &mut Column) {
    match &mut c.data {
        // A specialised implementation for floats is required to filter out NaN
        // values in order to match the behaviour of `StatValues::update()`.
        ColumnData::F64(data, stats) => {
            data.iter()
                .zip(c.valid.iter())
                .filter_map(|(v, valid)| {
                    if !valid || v.is_nan() {
                        // NaN are completely ignored in stats.
                        return None;
                    }
                    Some(*v)
                })
                .for_each(|v| {
                    stats.min = Some(stats.min.unwrap_or(v).min(v));
                    stats.max = Some(stats.max.unwrap_or(v).max(v));
                });
        }

        // A specialised implementation for boolean values for significantly
        // improved performance.
        ColumnData::Bool(data, stats) => {
            // Process 8 values at a time by evaluating against the underlying
            // bytes directly in both the validity and value bitsets.
            //
            // Invariant: the excess bits beyond "bitset.len()" are always 0.
            let iter = c.valid.bytes().iter().zip(data.bytes().iter());

            let mut contains_false = false;
            let mut contains_true = false;

            for (valid, data) in iter {
                // Set bits only if they're non-null and 1.
                contains_true |= valid & data > 0;

                // Set bits only if they're non-null and 0.
                contains_false |= valid & !data > 0;

                // Short circuit if both have been observed.
                if contains_false && contains_true {
                    break;
                }
            }

            // If all values are NULL, no real values were observed, and the
            // stats should be cleared (as the stats ignore NULLs).
            if !contains_false && !contains_true {
                stats.min = None;
                stats.max = None;
                return;
            }

            stats.min = Some(!contains_false);
            stats.max = Some(contains_true);
        }

        // The rest of the data types use `recompute_min_max_for()`.
        ColumnData::I64(data, stats) => {
            if let Some((min, max)) = recompute_min_max_for(data.iter(), c.valid.iter()) {
                stats.min = Some(*min);
                stats.max = Some(*max);
            }
        }
        ColumnData::U64(data, stats) => {
            if let Some((min, max)) = recompute_min_max_for(data.iter(), c.valid.iter()) {
                stats.min = Some(*min);
                stats.max = Some(*max);
            }
        }

        // Optimised to avoid cloning the string for every change in min/max
        // value, instead this clones the strings at most once for each of
        // min/max.
        //
        // This applies to both the String and Tag data types.
        ColumnData::String(data, stats) => {
            if let Some((min, max)) = recompute_min_max_for(data.iter(), c.valid.iter()) {
                stats.min = Some(min.to_string());
                stats.max = Some(max.to_string());
            }
        }
        ColumnData::Tag(_, dict, stats) => {
            // The dictionary does not store a representation of NULL, so all
            // the values in the dictionary are candidates for min/max.
            if let Some((min, max)) =
                recompute_min_max_for(dict.values().iter(), iter::repeat(true))
            {
                stats.min = Some(min.to_string());
                stats.max = Some(max.to_string());
            }
        }
    }
}

/// Compute the min/max values of `data`, filtering out any values with
/// corresponding positions in `valid` that are `false`.
fn recompute_min_max_for<'a, T>(
    data: impl IntoIterator<Item = &'a T>,
    valid: impl IntoIterator<Item = bool>,
) -> Option<(&'a T, &'a T)>
where
    T: Ord + ?Sized,
{
    let (min, max) = data
        .into_iter()
        .zip(valid.into_iter())
        .filter_map(|(v, valid)| if valid { Some(v) } else { None })
        .fold((None, None), |acc, v| {
            (
                Some(acc.0.unwrap_or(v).min(v)),
                Some(acc.1.unwrap_or(v).max(v)),
            )
        });

    min.zip(max)
}

#[cfg(test)]
mod tests {
    use std::{borrow::Borrow, collections::HashSet, fmt::Debug, mem::discriminant};

    use arrow::record_batch::RecordBatch;
    use arrow_util::assert_batches_eq;
    use assert_matches::assert_matches;
    use data_types::IsNan;
    use proptest::prelude::*;

    use super::*;

    fn hydrate(dict: &Dictionary, data: &[DID]) -> Vec<String> {
        data.iter()
            .map(|&id| dict.lookup_id(id).unwrap().to_string())
            .collect::<Vec<_>>()
    }

    /// Take an iterator of nullable `T`, and convert it into a vector of
    /// non-optional values and a null mask compatible with [`ColumnData`].
    ///
    /// Returns the number of nulls in `data`.
    fn densify<T, U>(data: impl IntoIterator<Item = Option<U>>) -> (Vec<T>, BitSet, usize)
    where
        U: ToOwned<Owned = T>,
        T: Default,
    {
        let mut out = Vec::new();
        let mut bitmap = BitSet::new();
        let mut nulls = 0;
        for v in data.into_iter() {
            match v {
                Some(v) => {
                    bitmap.append_set(1);
                    out.push(v.to_owned());
                }
                None => {
                    out.push(Default::default());
                    bitmap.append_unset(1);
                    nulls += 1;
                }
            }
        }

        (out, bitmap, nulls)
    }

    #[test]
    #[allow(clippy::bool_assert_comparison)]
    fn test_densify() {
        let input = [None, Some(42), None, None, Some(24)];

        let (got, nulls, count) = densify(input);
        assert_eq!(got, [0, 42, 0, 0, 24]); // NULLS are populated with 0 (not sparse representation)
        assert_eq!(nulls.get(0), false);
        assert_eq!(nulls.get(1), true);
        assert_eq!(nulls.get(2), false);
        assert_eq!(nulls.get(3), false);
        assert_eq!(nulls.get(4), true);
        assert_eq!(nulls.len(), 5);
        assert_eq!(count, 3);
    }

    #[test]
    fn test_rewrite_dictionary() {
        let mut original = Dictionary::new();
        let mut data = vec![];

        // Input strings to be dictionary encoded.
        let input = [
            "bananas", "platanos", "bananas", "platanos", "ananas", "ananas", "ananas",
        ];

        for v in input {
            data.push(original.lookup_value_or_insert(v));
        }

        assert_eq!(data.len(), input.len());
        assert_eq!(original.values().len(), 3); // 3 distinct values

        let mut new_data = data.split_off(3);
        let new_dict = rebuild_dictionary(&original, &mut new_data);
        let old_dict = rebuild_dictionary(&original, &mut data);

        let new_data_hydrated = hydrate(&new_dict, &new_data);
        let old_data_hydrated = hydrate(&old_dict, &data);

        assert_eq!(
            new_data_hydrated,
            ["platanos", "ananas", "ananas", "ananas"]
        );
        assert_eq!(old_data_hydrated, ["bananas", "platanos", "bananas"]);

        assert_eq!(new_dict.values().len(), 2); // 2 distinct values
        assert_eq!(old_dict.values().len(), 2); // 2 distinct values
    }

    #[test]
    fn test_split_off() {
        let (data, valid, _) = densify([Some(42), None, None, Some(24)]);
        valid.to_arrow();

        let mut col = Column {
            influx_type: InfluxColumnType::Field(InfluxFieldType::UInteger),
            valid,
            data: ColumnData::U64(data, StatValues::new(None, None, 4, Some(2))),
        };

        let mut schema = schema::SchemaBuilder::new();
        schema.influx_column("bananas", col.influx_type());
        let schema = schema.build().unwrap();

        // Before the split
        let batch = RecordBatch::try_new(
            schema.clone().into(),
            vec![col.to_arrow().expect("failed to covert column to arrow")],
        )
        .expect("failed to build record batch");
        assert_batches_eq!(
            [
                "+---------+",
                "| bananas |",
                "+---------+",
                "| 42      |",
                "|         |",
                "|         |",
                "| 24      |",
                "+---------+",
            ],
            &[batch]
        );

        let col2 = col.split_off(2);

        // After the split, the input column
        let batch = RecordBatch::try_new(
            schema.clone().into(),
            vec![col.to_arrow().expect("failed to covert column to arrow")],
        )
        .expect("failed to build record batch");
        assert_batches_eq!(
            [
                "+---------+",
                "| bananas |",
                "+---------+",
                "| 42      |",
                "|         |",
                "+---------+",
            ],
            &[batch]
        );

        // After the split, the split off column
        let batch = RecordBatch::try_new(
            schema.into(),
            vec![col2.to_arrow().expect("failed to covert column to arrow")],
        )
        .expect("failed to build record batch");
        assert_batches_eq!(
            [
                "+---------+",
                "| bananas |",
                "+---------+",
                "|         |",
                "| 24      |",
                "+---------+",
            ],
            &[batch]
        );
    }

    const MAX_ROWS: usize = 20;

    /// Returns a vector of `Option<T>`.
    fn sparse_array<T>(s: impl Strategy<Value = T>) -> impl Strategy<Value = Vec<Option<T>>>
    where
        T: Debug,
    {
        prop::collection::vec(prop::option::of(s), 0..MAX_ROWS)
    }

    /// Produces a valid [`Column`]` of an arbitrary data type and data.
    ///
    /// The embedded statistics do not contain min/max values but otherwise
    /// model a column within a [`MutableBatch`] produced by a [`Writer`].
    ///
    /// [`MutableBatch`]: crate::MutableBatch
    /// [`Writer`]: crate::writer::Writer
    fn arbitrary_column() -> impl Strategy<Value = Column> {
        prop_oneof![
            sparse_array(any::<f64>()).prop_map(|v| {
                let (data, valid, null_count) = densify(v.clone());
                Column {
                    influx_type: InfluxColumnType::Field(InfluxFieldType::Float),
                    valid,
                    data: ColumnData::F64(
                        data,
                        StatValues::new(None, None, v.len() as _, Some(null_count as _)),
                    ),
                }
            }),
            sparse_array(any::<i64>()).prop_map(|v| {
                let (data, valid, null_count) = densify(v.clone());
                Column {
                    influx_type: InfluxColumnType::Field(InfluxFieldType::Integer),
                    valid,
                    data: ColumnData::I64(
                        data,
                        StatValues::new(None, None, v.len() as _, Some(null_count as _)),
                    ),
                }
            }),
            sparse_array(any::<u64>()).prop_map(|v| {
                let (data, valid, null_count) = densify(v.clone());
                Column {
                    influx_type: InfluxColumnType::Field(InfluxFieldType::UInteger),
                    valid,
                    data: ColumnData::U64(
                        data,
                        StatValues::new(None, None, v.len() as _, Some(null_count as _)),
                    ),
                }
            }),
            sparse_array(any::<String>()).prop_map(|v| {
                let (strings, valid, null_count) = densify(v.clone());
                let mut data = PackedStringArray::new();
                for s in strings {
                    data.append(&s);
                }
                Column {
                    influx_type: InfluxColumnType::Field(InfluxFieldType::String),
                    valid,
                    data: ColumnData::String(
                        data,
                        StatValues::new(None, None, v.len() as _, Some(null_count as _)),
                    ),
                }
            }),
            sparse_array(any::<bool>()).prop_map(|v| {
                let (values, valid, null_count) = densify(v.clone());
                let mut data = BitSet::new();
                for v in values {
                    match v {
                        true => data.append_set(1),
                        false => data.append_unset(1),
                    }
                }
                Column {
                    influx_type: InfluxColumnType::Field(InfluxFieldType::Boolean),
                    valid,
                    data: ColumnData::Bool(
                        data,
                        StatValues::new(None, None, v.len() as _, Some(null_count as _)),
                    ),
                }
            }),
            // This artificially weights string generation to produce arrays
            // with a higher chance of covering both dense and sparse arrays
            // where distinct values != array length.
            prop_oneof![
                sparse_array(
                    prop::string::string_regex("[a-b]").expect("invalid repetition regex")
                ),
                sparse_array(any::<String>()),
            ]
            .prop_map(|v| {
                // The NULL encoding of the dictionary is a bit of a snowflake.
                //
                // Walk the NULL-able input, and for any NULLs insert NULL_DID
                // into the data array without inserting into the dictionary.
                let mut data = Vec::new();
                let mut dict = Dictionary::new();
                let mut valid = BitSet::new();

                let mut nulls = 0;
                for v in &v {
                    match v {
                        Some(v) => {
                            valid.append_set(1);
                            data.push(dict.lookup_value_or_insert(v));
                        }
                        None => {
                            data.push(NULL_DID);
                            valid.append_unset(1);
                            nulls += 1;
                        }
                    }
                }

                // A NULL is a distinct value, that does not appear in the
                // dictionary.
                let distinct_count = if nulls > 0 {
                    dict.values().len() + 1
                } else {
                    dict.values().len()
                };

                Column {
                    influx_type: InfluxColumnType::Tag,
                    valid,
                    data: ColumnData::Tag(
                        data,
                        dict,
                        StatValues::new_with_distinct(
                            None,
                            None,
                            v.len() as _,
                            Some(nulls),
                            NonZeroU64::try_from(distinct_count as u64).ok(),
                        ),
                    ),
                }
            }),
        ]
    }
    // Set the number of test cases higher than the default (256) to ensure better
    // coverage of the generated arbitrary columns without compromising too
    // much on the input space.
    proptest! {
        #![proptest_config(ProptestConfig::with_cases(2048))]

        /// Asserts the correctness of the [`Column::split_off()`] method, using
        /// the Arrow "Array" slice method as a test oracle.
        ///
        /// Asserts the following invariants after splitting:
        ///
        ///  - Never panics due to out-of-bounds split position
        ///  - Data types remain unchanged
        ///  - Metadata for influx data model unchanged
        ///  - NULL mask is of the correct length
        ///  - Data length matches count statistics
        ///  - NULL value count matches NULL count statistics
        ///  - Tag distinct values matches distinct count statistics
        ///  - Tag dictionary contains correct number of entries, with NULLs
        ///  - Total count statistics are equal to input statistics
        ///  - NULL count statistics are equal to input statistics
        ///  - Both sides of the split match equivalent Arrow oracle splits
        ///
        #[test]
        fn prop_split_off(
            input in arbitrary_column(),
            split_at in 0..=MAX_ROWS,
        ) {
            // Split the column.
            let mut col = input.clone();
            let col2 = col.split_off(split_at);

            // Assert no rows were lost.
            assert_eq!(col.len() + col2.len(), input.len());

            // Because "split_at" may be greater than the number of rows in the
            // input column, compute how many rows should remain after the
            // split.
            let want_remaining_rows = input.len().min(split_at);
            assert_eq!(col.len(), want_remaining_rows);

            // And validate the rest of the rows wound up in the col2 half.
            assert_eq!(col2.len(), input.len() - want_remaining_rows);

            for c in [&col, &col2] {
                // The data type should remain the same.
                assert_eq!(c.influx_type(), input.influx_type());
                assert_eq!(discriminant(c.data()), discriminant(input.data()));

                // Inspect the statistics for each.
                let data_len = match c.data() {
                    ColumnData::F64(data, _) => data.len(),
                    ColumnData::I64(data, _) => data.len(),
                    ColumnData::U64(data, _) => data.len(),
                    ColumnData::String(data, _) => data.len(),
                    ColumnData::Bool(data, _) => data.len(),
                    ColumnData::Tag(data, dict, stats) => {
                        // Tags have an additional distinct count statistics
                        // maintained throughout the split.
                        let want = stats.distinct_count.map(|v| v.get()).unwrap_or_default();
                        let have = data.iter().collect::<HashSet<_>>().len() as u64;
                        assert_eq!(have, want);

                        // If there are no nulls, the dictionary length must
                        // match the number of distinct values. If there are
                        // NULLs, +1 to the dictionary length (it does not
                        // contain NULLs).
                        if stats.null_count.unwrap_or_default() == 0 {
                            assert_eq!(have, dict.values().len() as u64);
                        } else {
                            // Otherwise there must be one more distinct value.
                            assert_eq!(have, dict.values().len() as u64 + 1);
                        }

                        data.len()
                    },
                };

                // First check the consistency of the total count:
                assert_eq!(c.valid_mask().len(), data_len);
                assert_eq!(data_len as u64, c.stats().total_count());

                // Null counts:
                let nulls = c.valid_mask().count_zeros() as u64;
                assert_eq!(c.stats().null_count(), Some(nulls));
            }

            // The sum of various statistics must match the input counts.
            let count = col.stats().total_count() + col2.stats().total_count();
            assert_eq!(input.stats().total_count(), count);

            // Null counts must sum to the input count
            let nulls = col.stats().null_count().unwrap_or_default() +
            col2.stats().null_count().unwrap_or_default();
            assert_eq!(input.stats().null_count().unwrap_or_default(), nulls);

            // Generate arrow arrays from both inputs
            let col = col.to_arrow().unwrap();
            let col2 = col2.to_arrow().unwrap();

            // And the test oracle
            let input = input.to_arrow().unwrap();

            // Slice the input data using arrow's slice methods.
            let want = input.slice(0, split_at.min(input.len()));

            // And assert the split_off() data is equal.
            assert!(col.eq(&want));

            // Only attempt to slice off and validate the right side if it would
            // be non-empty (or arrow panics)
            if split_at >= input.len() {
                assert_eq!(col2.len(), 0);
            } else {
                let want2 = input.slice(split_at, input.len() - split_at);
                assert!(col2.eq(&want2));
            }
        }

        /// Exercise [`recompute_min_max()`] against a [`Column`], asserting the
        /// resulting [`StatValues`] match that produced by using the [`Writer`]
        /// to populate the [`Column`].
        #[test]
        fn prop_recompute_min_max(
            mut input in arbitrary_column(),
        ) {
            // Compute a `StatValues` using the test oracle implementation.
            fn stats_oracle<S, T, U>(data: S, valid: impl IntoIterator<Item = bool>) -> StatValues<T>
            where
                S: IntoIterator<Item = U>,
                T: Borrow<U>,
                U: ToOwned<Owned = T> + PartialOrd + IsNan,
            {
                data.into_iter()
                    .zip(valid.into_iter())
                    .filter_map(|(v, valid)| if valid { Some(v) } else { None })
                    .fold(StatValues::default(), |mut acc, v| {
                        acc.update(&v);
                        acc
                    })
            }

            match input.clone().data() {
                ColumnData::F64(data,_) => {
                    let want = stats_oracle(data, input.valid.iter());

                    recompute_min_max(&mut input);
                    let got = assert_matches!(input.stats(), Statistics::F64(v) => v);

                    assert_eq!(want.min.cloned(), got.min);
                    assert_eq!(want.max.cloned(), got.max);
                    assert!(got.min <= got.max);
                },
                ColumnData::I64(data, _) => {
                    let want = stats_oracle(data, input.valid.iter());

                    recompute_min_max(&mut input);
                    let got = assert_matches!(input.stats(), Statistics::I64(v) => v);

                    assert_eq!(want.min.cloned(), got.min);
                    assert_eq!(want.max.cloned(), got.max);
                    assert!(got.min <= got.max);
                },
                ColumnData::U64(data, _) => {
                    let want = stats_oracle(data, input.valid.iter());

                    recompute_min_max(&mut input);
                    let got = assert_matches!(input.stats(), Statistics::U64(v) => v);

                    assert_eq!(want.min.cloned(), got.min);
                    assert_eq!(want.max.cloned(), got.max);
                    assert!(got.min <= got.max);
                },
                ColumnData::Bool(data, _) => {
                    let want = stats_oracle(data.iter(), input.valid.iter());

                    recompute_min_max(&mut input);
                    let got = assert_matches!(input.stats(), Statistics::Bool(v) => v);

                    assert_eq!(want.min, got.min);
                    assert_eq!(want.max, got.max);
                    assert!(got.min <= got.max);
                },
                ColumnData::String(data, _) => {
                    let want = stats_oracle(data.iter().map(ToString::to_string), input.valid.iter());

                    recompute_min_max(&mut input);
                    let got = assert_matches!(input.stats(), Statistics::String(v) => v);

                    assert_eq!(want.min, got.min);
                    assert_eq!(want.max, got.max);
                    assert!(got.min <= got.max);
                },
                ColumnData::Tag(_data, dict, _) => {
                    let want = stats_oracle(
                        dict.values().iter().map(ToString::to_string),
                        iter::repeat(true)
                    );

                    recompute_min_max(&mut input);
                    let got = assert_matches!(input.stats(), Statistics::String(v) => v);

                    assert_eq!(want.min, got.min);
                    assert_eq!(want.max, got.max);
                    assert!(got.min <= got.max);
                },
            }
        }
    }
}
