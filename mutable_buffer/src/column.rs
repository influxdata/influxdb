use snafu::{ensure, Snafu};

use crate::dictionary::{Dictionary, DID};
use data_types::partition_metadata::{StatValues, Statistics};
use internal_types::entry::Column as EntryColumn;

use crate::bitset::{iter_set_positions, BitSet};
use arrow::{
    array::{
        ArrayData, ArrayDataBuilder, ArrayRef, BooleanArray, DictionaryArray, Float64Array,
        Int64Array, StringArray, TimestampNanosecondArray, UInt64Array,
    },
    datatypes::{DataType, Int32Type},
};
use internal_types::schema::{InfluxColumnType, InfluxFieldType, TIME_DATA_TYPE};
use std::iter::FromIterator;
use std::mem;
use std::sync::Arc;

#[derive(Debug, Snafu)]
#[allow(missing_copy_implementations)]
pub enum Error {
    #[snafu(display("Unable to insert {} type into a column of {}", inserted, existing,))]
    TypeMismatch {
        existing: InfluxColumnType,
        inserted: InfluxColumnType,
    },

    #[snafu(display(
        "Invalid null mask, expected to be {} bytes but was {}",
        expected_bytes,
        actual_bytes
    ))]
    InvalidNullMask {
        expected_bytes: usize,
        actual_bytes: usize,
    },
}
pub type Result<T, E = Error> = std::result::Result<T, E>;

/// Stores the actual data for columns in a chunk along with summary
/// statistics
#[derive(Debug)]
pub struct Column {
    influx_type: InfluxColumnType,
    valid: BitSet,
    data: ColumnData,
}

#[derive(Debug)]
pub enum ColumnData {
    F64(Vec<f64>, StatValues<f64>),
    I64(Vec<i64>, StatValues<i64>),
    U64(Vec<u64>, StatValues<u64>),
    String(Vec<String>, StatValues<String>),
    Bool(BitSet, StatValues<bool>),
    Tag(Vec<DID>, StatValues<String>),
}

impl Column {
    pub fn new(row_count: usize, column_type: InfluxColumnType) -> Self {
        let mut valid = BitSet::new();
        valid.append_unset(row_count);

        let data = match column_type {
            InfluxColumnType::Field(InfluxFieldType::Boolean) => {
                let mut data = BitSet::new();
                data.append_unset(row_count);
                ColumnData::Bool(data, StatValues::new())
            }
            InfluxColumnType::Field(InfluxFieldType::UInteger) => {
                ColumnData::U64(vec![0; row_count], StatValues::new())
            }
            InfluxColumnType::Field(InfluxFieldType::Float) => {
                ColumnData::F64(vec![0.0; row_count], StatValues::new())
            }
            InfluxColumnType::Field(InfluxFieldType::Integer) | InfluxColumnType::Timestamp => {
                ColumnData::I64(vec![0; row_count], StatValues::new())
            }
            InfluxColumnType::Field(InfluxFieldType::String) => {
                ColumnData::String(vec![String::new(); row_count], StatValues::new())
            }
            InfluxColumnType::Tag => {
                ColumnData::Tag(vec![DID::invalid(); row_count], StatValues::new())
            }
        };

        Self {
            influx_type: column_type,
            valid,
            data,
        }
    }

    pub fn validate_schema(&self, entry: &EntryColumn<'_>) -> Result<()> {
        let entry_type = entry.influx_type();

        ensure!(
            entry_type == self.influx_type,
            TypeMismatch {
                existing: self.influx_type,
                inserted: entry_type
            }
        );

        Ok(())
    }

    pub fn influx_type(&self) -> InfluxColumnType {
        self.influx_type
    }

    pub fn append(&mut self, entry: &EntryColumn<'_>, dictionary: &mut Dictionary) -> Result<()> {
        self.validate_schema(entry)?;

        let row_count = entry.row_count;
        if row_count == 0 {
            return Ok(());
        }

        let mask = construct_valid_mask(entry)?;

        match &mut self.data {
            ColumnData::Bool(col_data, stats) => {
                let entry_data = entry
                    .inner()
                    .values_as_bool_values()
                    .expect("invalid flatbuffer")
                    .values()
                    .expect("invalid payload");

                let data_offset = col_data.len();
                col_data.append_unset(row_count);

                let initial_non_null_count = stats.count;

                for (idx, value) in iter_set_positions(&mask).zip(entry_data) {
                    stats.update(value);

                    if *value {
                        col_data.set(data_offset + idx);
                    }
                }
                assert_eq!(
                    stats.count - initial_non_null_count,
                    entry_data.len() as u64
                );
            }
            ColumnData::U64(col_data, stats) => {
                let entry_data = entry
                    .inner()
                    .values_as_u64values()
                    .expect("invalid flatbuffer")
                    .values()
                    .expect("invalid payload")
                    .into_iter();

                handle_write(row_count, &mask, entry_data, col_data, stats);
            }
            ColumnData::F64(col_data, stats) => {
                let entry_data = entry
                    .inner()
                    .values_as_f64values()
                    .expect("invalid flatbuffer")
                    .values()
                    .expect("invalid payload")
                    .into_iter();

                handle_write(row_count, &mask, entry_data, col_data, stats);
            }
            ColumnData::I64(col_data, stats) => {
                let entry_data = entry
                    .inner()
                    .values_as_i64values()
                    .expect("invalid flatbuffer")
                    .values()
                    .expect("invalid payload")
                    .into_iter();

                handle_write(row_count, &mask, entry_data, col_data, stats);
            }
            ColumnData::String(col_data, stats) => {
                let entry_data = entry
                    .inner()
                    .values_as_string_values()
                    .expect("invalid flatbuffer")
                    .values()
                    .expect("invalid payload")
                    .into_iter()
                    .map(ToString::to_string);

                handle_write(row_count, &mask, entry_data, col_data, stats);
            }
            ColumnData::Tag(col_data, stats) => {
                let entry_data = entry
                    .inner()
                    .values_as_string_values()
                    .expect("invalid flatbuffer")
                    .values()
                    .expect("invalid payload");

                let data_offset = col_data.len();
                col_data.resize(data_offset + row_count, DID::invalid());

                let initial_non_null_count = stats.count;
                let to_add = entry_data.len();

                for (idx, value) in iter_set_positions(&mask).zip(entry_data) {
                    stats.update(value);
                    col_data[data_offset + idx] = dictionary.lookup_value_or_insert(value);
                }

                assert_eq!(stats.count - initial_non_null_count, to_add as u64);
            }
        };

        self.valid.append_bits(entry.row_count, &mask);
        Ok(())
    }

    pub fn push_nulls_to_len(&mut self, len: usize) {
        if self.valid.len() == len {
            return;
        }
        assert!(len > self.valid.len(), "cannot shrink column");
        let delta = len - self.valid.len();
        self.valid.append_unset(delta);

        match &mut self.data {
            ColumnData::F64(data, _) => data.resize(len, 0.),
            ColumnData::I64(data, _) => data.resize(len, 0),
            ColumnData::U64(data, _) => data.resize(len, 0),
            ColumnData::String(data, _) => data.resize(len, String::new()),
            ColumnData::Bool(data, _) => data.append_unset(delta),
            ColumnData::Tag(data, _) => data.resize(len, DID::invalid()),
        }
    }

    pub fn len(&self) -> usize {
        self.valid.len()
    }

    pub fn stats(&self) -> Statistics {
        match &self.data {
            ColumnData::F64(_, stats) => Statistics::F64(stats.clone()),
            ColumnData::I64(_, stats) => Statistics::I64(stats.clone()),
            ColumnData::U64(_, stats) => Statistics::U64(stats.clone()),
            ColumnData::Bool(_, stats) => Statistics::Bool(stats.clone()),
            ColumnData::String(_, stats) | ColumnData::Tag(_, stats) => {
                Statistics::String(stats.clone())
            }
        }
    }

    /// The approximate memory size of the data in the column. Note that
    /// the space taken for the tag string values is represented in
    /// the dictionary size in the chunk that holds the table that has this
    /// column. The size returned here is only for their identifiers.
    pub fn size(&self) -> usize {
        let data_size = match &self.data {
            ColumnData::F64(v, stats) => mem::size_of::<f64>() * v.len() + mem::size_of_val(&stats),
            ColumnData::I64(v, stats) => mem::size_of::<i64>() * v.len() + mem::size_of_val(&stats),
            ColumnData::U64(v, stats) => mem::size_of::<u64>() * v.len() + mem::size_of_val(&stats),
            ColumnData::Bool(v, stats) => v.byte_len() + mem::size_of_val(&stats),
            ColumnData::Tag(v, stats) => mem::size_of::<DID>() * v.len() + mem::size_of_val(&stats),
            ColumnData::String(v, stats) => {
                let string_bytes_size = v.iter().fold(0, |acc, val| acc + val.len());
                let vec_pointer_sizes = mem::size_of::<String>() * v.len();
                string_bytes_size + vec_pointer_sizes + mem::size_of_val(&stats)
            }
        };
        data_size + self.valid.byte_len()
    }

    pub fn to_arrow(&self, dictionary: &ArrayData) -> Result<ArrayRef> {
        let nulls = self.valid.to_arrow();
        let data: ArrayRef = match &self.data {
            ColumnData::F64(data, _) => {
                let data = ArrayDataBuilder::new(DataType::Float64)
                    .len(data.len())
                    .add_buffer(data.iter().cloned().collect())
                    .null_bit_buffer(nulls)
                    .build();
                Arc::new(Float64Array::from(data))
            }
            ColumnData::I64(data, _) => match self.influx_type {
                InfluxColumnType::Timestamp => {
                    let data = ArrayDataBuilder::new(TIME_DATA_TYPE())
                        .len(data.len())
                        .add_buffer(data.iter().cloned().collect())
                        .null_bit_buffer(nulls)
                        .build();
                    Arc::new(TimestampNanosecondArray::from(data))
                }
                InfluxColumnType::Field(InfluxFieldType::Integer) => {
                    let data = ArrayDataBuilder::new(DataType::Int64)
                        .len(data.len())
                        .add_buffer(data.iter().cloned().collect())
                        .null_bit_buffer(nulls)
                        .build();

                    Arc::new(Int64Array::from(data))
                }
                _ => unreachable!(),
            },
            ColumnData::U64(data, _) => {
                let data = ArrayDataBuilder::new(DataType::UInt64)
                    .len(data.len())
                    .add_buffer(data.iter().cloned().collect())
                    .null_bit_buffer(nulls)
                    .build();
                Arc::new(UInt64Array::from(data))
            }
            ColumnData::String(data, _) => {
                // TODO: Store this closer to the arrow representation
                let iter = data
                    .iter()
                    .enumerate()
                    .map(|(idx, value)| self.valid.get(idx).then(|| value) as _);

                let array = StringArray::from_iter(iter);
                Arc::new(array)
            }
            ColumnData::Bool(data, _) => {
                let data = ArrayDataBuilder::new(DataType::Boolean)
                    .len(data.len())
                    .add_buffer(data.to_arrow())
                    .null_bit_buffer(nulls)
                    .build();
                Arc::new(BooleanArray::from(data))
            }
            ColumnData::Tag(data, _) => {
                let data = ArrayDataBuilder::new(DataType::Dictionary(
                    Box::new(DataType::Int32),
                    Box::new(DataType::Utf8),
                ))
                .len(data.len())
                .add_buffer(data.iter().map(|x| x.as_i32()).collect())
                .null_bit_buffer(nulls)
                .add_child_data(dictionary.clone())
                .build();

                let array = DictionaryArray::<Int32Type>::from(data);

                Arc::new(array)
            }
        };

        assert_eq!(data.len(), self.len());

        Ok(data)
    }
}

/// Construct a validity mask from the given column's null mask
fn construct_valid_mask(column: &EntryColumn<'_>) -> Result<Vec<u8>> {
    let buf_len = (column.row_count + 7) >> 3;
    match column.inner().null_mask() {
        Some(data) => {
            ensure!(
                data.len() == buf_len,
                InvalidNullMask {
                    expected_bytes: buf_len,
                    actual_bytes: data.len()
                }
            );

            Ok(data
                .iter()
                .map(|x| {
                    // Currently the bit mask is backwards
                    !x.reverse_bits()
                })
                .collect())
        }
        None => {
            // If no null mask they're all valid
            let mut data = Vec::new();
            data.resize(buf_len, 0xFF);
            Ok(data)
        }
    }
}

/// Writes entry data into a column based on the valid mask
fn handle_write<T, E>(
    row_count: usize,
    valid_mask: &[u8],
    entry_data: E,
    col_data: &mut Vec<T>,
    stats: &mut StatValues<T>,
) where
    T: Clone + Default + PartialOrd,
    E: Iterator<Item = T> + ExactSizeIterator,
{
    let data_offset = col_data.len();
    col_data.resize(data_offset + row_count, Default::default());

    let initial_non_null_count = stats.count;
    let to_add = entry_data.len();

    for (idx, value) in iter_set_positions(valid_mask).zip(entry_data) {
        stats.update(&value);
        col_data[data_offset + idx] = value;
    }

    assert_eq!(stats.count - initial_non_null_count, to_add as u64);
}
