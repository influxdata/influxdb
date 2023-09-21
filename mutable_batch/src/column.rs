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
use std::{fmt::Formatter, mem, sync::Arc};

/// A "dictionary ID" (DID) is a compact numeric representation of an interned
/// string in the dictionary. The same string always maps the same DID.
///
/// DIDs can be compared, hashed and cheaply copied around, just like small integers.
///
/// An i32 is used to match the default for Arrow dictionaries
#[allow(clippy::upper_case_acronyms)]
pub(crate) type DID = i32;

/// An invalid DID used for NULL rows
pub(crate) const INVALID_DID: DID = -1;

/// The type of the dictionary used
type Dictionary = arrow_util::dictionary::StringDictionary<DID>;

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
    F64(Vec<f64>, StatValues<f64>),
    I64(Vec<i64>, StatValues<i64>),
    U64(Vec<u64>, StatValues<u64>),
    String(PackedStringArray<i32>, StatValues<String>),
    Bool(BitSet, StatValues<bool>),
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
                StatValues::new_all_null(total_count, Some(1)),
            ),
            InfluxColumnType::Tag => ColumnData::Tag(
                vec![INVALID_DID; row_count],
                Default::default(),
                StatValues::new_all_null(total_count, Some(1)),
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
                data.resize(len, INVALID_DID);
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
}
