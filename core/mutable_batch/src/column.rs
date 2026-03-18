//! A [`Column`] stores the rows for a given column name

use arrow::{
    array::{
        ArrayDataBuilder, ArrayRef, BooleanArray, Float64Array, Int64Array,
        TimestampNanosecondArray, UInt64Array,
    },
    buffer::{Buffer, NullBuffer},
    datatypes::DataType,
    error::ArrowError,
};
use arrow_util::{bitset::BitSet, string::PackedStringArray};
use schema::{InfluxColumnType, InfluxFieldType, TIME_DATA_TYPE};
use snafu::{ResultExt, Snafu};
use std::{fmt::Formatter, mem, sync::Arc};

/// A "dictionary ID" (DID) is a compact numeric representation of an interned
/// string in the dictionary. The same string always maps the same DID.
///
/// DIDs can be compared, hashed and cheaply copied around, just like small integers.
///
/// An i32 is used to match the default for Arrow dictionaries
#[expect(clippy::upper_case_acronyms)]
pub(crate) type DID = i32;

/// An invalid DID used for NULL rows
pub(crate) const NULL_DID: DID = -1;

/// The type of the dictionary used
type Dictionary = arrow_util::dictionary::StringDictionary<DID>;

#[derive(Debug, Snafu)]
#[expect(missing_docs)]
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
#[derive(Debug, Clone, PartialEq)]
pub struct Column {
    pub(crate) influx_type: InfluxColumnType,
    pub(crate) valid: BitSet,
    pub(crate) data: ColumnData,
}

/// The data for a column
#[derive(Debug, Clone, PartialEq)]
#[expect(missing_docs)]
pub enum ColumnData {
    /// These types contain arrays that contain an element for every logical row
    /// (including nulls).
    ///
    /// Null values are padded with an arbitrary dummy value.
    F64(Vec<f64>),
    I64(Vec<i64>),
    U64(Vec<u64>),
    Bool(BitSet),

    /// The String encoding contains an entry for every logical row, and
    /// explicitly stores an empty string in the PackedStringArray for NULL
    /// values.
    String(PackedStringArray<i32>),

    /// Whereas the dictionary encoding does not store an explicit empty string
    /// in the internal PackedStringArray, nor does it create an entry in the
    /// dedupe map. A NULL entry is padded into the data vec using the
    /// [`NULL_DID`] value.
    ///
    /// Every distinct, non-null value is stored in the dictionary exactly once,
    /// and the data arrays contains the dictionary ID for every logical row
    /// (including nulls as described above).
    Tag(Vec<DID>, Dictionary),
}

impl std::fmt::Display for ColumnData {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::F64(col_data) => write!(f, "F64({})", col_data.len()),
            Self::I64(col_data) => write!(f, "I64({})", col_data.len()),
            Self::U64(col_data) => write!(f, "U64({})", col_data.len()),
            Self::String(col_data) => write!(f, "String({})", col_data.len()),
            Self::Bool(col_data) => write!(f, "Bool({})", col_data.len()),
            Self::Tag(col_data, dictionary) => write!(
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

        let data = match column_type {
            InfluxColumnType::Field(InfluxFieldType::Boolean) => {
                let mut data = BitSet::new();
                data.append_unset(row_count);
                ColumnData::Bool(data)
            }
            InfluxColumnType::Field(InfluxFieldType::UInteger) => {
                ColumnData::U64(vec![0; row_count])
            }
            InfluxColumnType::Field(InfluxFieldType::Float) => {
                ColumnData::F64(vec![0.0; row_count])
            }
            InfluxColumnType::Field(InfluxFieldType::Integer) | InfluxColumnType::Timestamp => {
                ColumnData::I64(vec![0; row_count])
            }
            InfluxColumnType::Field(InfluxFieldType::String) => {
                ColumnData::String(PackedStringArray::new_empty(row_count))
            }
            InfluxColumnType::Tag => ColumnData::Tag(vec![NULL_DID; row_count], Default::default()),
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
            ColumnData::F64(data) => {
                data.resize(len, 0.);
            }
            ColumnData::I64(data) => {
                data.resize(len, 0);
            }
            ColumnData::U64(data) => {
                data.resize(len, 0);
            }
            ColumnData::String(data) => {
                data.extend(delta);
            }
            ColumnData::Bool(data) => {
                data.append_unset(delta);
            }
            ColumnData::Tag(data, _dict) => {
                data.resize(len, NULL_DID);
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

    /// The approximate memory size of the data in the column.
    ///
    /// This includes the size of `self`.
    pub fn size(&self) -> usize {
        let data_size = match &self.data {
            ColumnData::F64(v) => mem::size_of::<f64>() * v.capacity(),
            ColumnData::I64(v) => mem::size_of::<i64>() * v.capacity(),
            ColumnData::U64(v) => mem::size_of::<u64>() * v.capacity(),
            ColumnData::Bool(v) => v.byte_len(),
            ColumnData::Tag(v, dictionary) => {
                mem::size_of::<DID>() * v.capacity() + dictionary.size()
            }
            ColumnData::String(v) => v.size(),
        };
        mem::size_of::<Self>() + data_size + self.valid.byte_len()
    }

    /// The approximate memory size of the data in the column, not counting for stats or self or
    /// whatever extra space has been allocated for the vecs
    pub fn size_data(&self) -> usize {
        match &self.data {
            ColumnData::F64(_) => mem::size_of::<f64>() * self.len(),
            ColumnData::I64(_) => mem::size_of::<i64>() * self.len(),
            ColumnData::U64(_) => mem::size_of::<u64>() * self.len(),
            ColumnData::Bool(_) => mem::size_of::<bool>() * self.len(),
            ColumnData::Tag(_, dictionary) => {
                mem::size_of::<DID>() * self.len() + dictionary.size()
            }
            ColumnData::String(v) => v.size(),
        }
    }

    /// Converts this column to an arrow [`ArrayRef`]
    pub fn try_into_arrow(self) -> Result<ArrayRef> {
        let len = self.len();
        let nulls = Some(NullBuffer::new(self.valid.into_arrow()));

        let data: ArrayRef = match self.data {
            ColumnData::F64(data) => {
                let data = ArrayDataBuilder::new(DataType::Float64)
                    .len(data.len())
                    .add_buffer(Buffer::from(data))
                    .nulls(nulls)
                    .build()
                    .context(CreatingArrowArraySnafu)?;
                Arc::new(Float64Array::from(data))
            }
            ColumnData::I64(data) => match self.influx_type {
                InfluxColumnType::Timestamp => {
                    let data = ArrayDataBuilder::new(TIME_DATA_TYPE())
                        .len(data.len())
                        .add_buffer(Buffer::from(data))
                        .nulls(nulls)
                        .build()
                        .context(CreatingArrowArraySnafu)?;
                    Arc::new(TimestampNanosecondArray::from(data))
                }

                InfluxColumnType::Field(InfluxFieldType::Integer) => {
                    let data = ArrayDataBuilder::new(DataType::Int64)
                        .len(data.len())
                        .add_buffer(Buffer::from(data))
                        .nulls(nulls)
                        .build()
                        .context(CreatingArrowArraySnafu)?;
                    Arc::new(Int64Array::from(data))
                }
                _ => unreachable!(),
            },
            ColumnData::U64(data) => {
                let data = ArrayDataBuilder::new(DataType::UInt64)
                    .len(data.len())
                    .add_buffer(Buffer::from(data))
                    .nulls(nulls)
                    .build()
                    .context(CreatingArrowArraySnafu)?;
                Arc::new(UInt64Array::from(data))
            }
            ColumnData::String(data) => Arc::new(data.to_arrow(nulls)),
            ColumnData::Bool(data) => {
                let data = ArrayDataBuilder::new(DataType::Boolean)
                    .len(data.len())
                    .add_buffer(data.into_arrow().into_inner())
                    .nulls(nulls)
                    .build()
                    .context(CreatingArrowArraySnafu)?;
                Arc::new(BooleanArray::from(data))
            }
            ColumnData::Tag(data, dictionary) => Arc::new(dictionary.to_arrow(data, nulls)),
        };

        assert_eq!(data.len(), len);

        Ok(data)
    }
}
