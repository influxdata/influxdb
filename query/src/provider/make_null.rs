use std::{iter::repeat, sync::Arc};

use arrow_deps::arrow::{
    array::{
        ArrayRef, BooleanArray, Float32Array, Float64Array, Int16Array, Int32Array, Int64Array,
        Int8Array, LargeStringArray, NullArray, StringArray, UInt16Array, UInt32Array, UInt64Array,
        UInt8Array,
    },
    datatypes::DataType,
    error::{ArrowError, Result as ArrowResult},
};
use std::iter::FromIterator;

/// Creates a column of all NULL values of the specified type and size
///
/// This should be replaced by a call to NullArray::new_with_type
///
/// We can't currently ue that call due to a bug in pretty printing:
/// https://github.com/apache/arrow/pull/9468
///
/// Once that is merged and we have updated Arrow, this function should be
/// replaced
pub fn make_null_column(data_type: &DataType, num_rows: usize) -> ArrowResult<ArrayRef> {
    println!("Making null column: {:?}", data_type);
    match data_type {
        DataType::Null => Ok(Arc::new(NullArray::new(num_rows))),
        DataType::Boolean => Ok(Arc::new(BooleanArray::from_iter(
            repeat(None).take(num_rows),
        ))),
        DataType::Int8 => Ok(Arc::new(Int8Array::from_iter(repeat(None).take(num_rows)))),
        DataType::Int16 => Ok(Arc::new(Int16Array::from_iter(repeat(None).take(num_rows)))),
        DataType::Int32 => Ok(Arc::new(Int32Array::from_iter(repeat(None).take(num_rows)))),
        DataType::Int64 => Ok(Arc::new(Int64Array::from_iter(repeat(None).take(num_rows)))),
        DataType::UInt8 => Ok(Arc::new(UInt8Array::from_iter(repeat(None).take(num_rows)))),
        DataType::UInt16 => Ok(Arc::new(UInt16Array::from_iter(
            repeat(None).take(num_rows),
        ))),
        DataType::UInt32 => Ok(Arc::new(UInt32Array::from_iter(
            repeat(None).take(num_rows),
        ))),
        DataType::UInt64 => Ok(Arc::new(UInt64Array::from_iter(
            repeat(None).take(num_rows),
        ))),
        DataType::Float16 => make_error(data_type),
        DataType::Float32 => Ok(Arc::new(Float32Array::from_iter(
            repeat(None).take(num_rows),
        ))),
        DataType::Float64 => Ok(Arc::new(Float64Array::from_iter(
            repeat(None).take(num_rows),
        ))),
        DataType::Timestamp(_, _) => make_error(data_type),
        DataType::Date32 => make_error(data_type),
        DataType::Date64 => make_error(data_type),
        DataType::Time32(_) => make_error(data_type),
        DataType::Time64(_) => make_error(data_type),
        DataType::Duration(_) => make_error(data_type),
        DataType::Interval(_) => make_error(data_type),
        DataType::Binary => make_error(data_type),
        DataType::FixedSizeBinary(_) => make_error(data_type),
        DataType::LargeBinary => make_error(data_type),
        DataType::Utf8 => Ok(Arc::new(StringArray::from_iter(
            repeat(None as Option<&str>).take(num_rows),
        ))),
        DataType::LargeUtf8 => Ok(Arc::new(LargeStringArray::from_iter(
            repeat(None as Option<&str>).take(num_rows),
        ))),
        DataType::List(_) => make_error(data_type),
        DataType::FixedSizeList(_, _) => make_error(data_type),
        DataType::LargeList(_) => make_error(data_type),
        DataType::Struct(_) => make_error(data_type),
        DataType::Union(_) => make_error(data_type),
        DataType::Dictionary(_, _) => make_error(data_type),
        DataType::Decimal(_, _) => make_error(data_type),
    }
}

fn make_error(data_type: &DataType) -> ArrowResult<ArrayRef> {
    Err(ArrowError::NotYetImplemented(format!(
        "make_null_column: Unsupported type {:?}",
        data_type
    )))
}
