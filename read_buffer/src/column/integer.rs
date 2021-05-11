use arrow::{self, array::Array};

use super::encoding::{fixed::Fixed, fixed_null::FixedNull};
use super::{cmp, Statistics};
use crate::column::{EncodedValues, RowIDs, Scalar, Value, Values};

pub enum IntegerEncoding {
    I64I64(Fixed<i64>),
    I64I32(Fixed<i32>),
    I64U32(Fixed<u32>),
    I64I16(Fixed<i16>),
    I64U16(Fixed<u16>),
    I64I8(Fixed<i8>),
    I64U8(Fixed<u8>),

    U64U64(Fixed<u64>),
    U64U32(Fixed<u32>),
    U64U16(Fixed<u16>),
    U64U8(Fixed<u8>),

    // Nullable encodings - TODO, add variants for smaller physical types.
    I64I64N(FixedNull<arrow::datatypes::Int64Type>),
    U64U64N(FixedNull<arrow::datatypes::UInt64Type>),
}

impl IntegerEncoding {
    /// The total size in bytes of the store columnar data.
    pub fn size(&self) -> usize {
        match self {
            Self::I64I64(enc) => enc.size(),
            Self::I64I32(enc) => enc.size(),
            Self::I64U32(enc) => enc.size(),
            Self::I64I16(enc) => enc.size(),
            Self::I64U16(enc) => enc.size(),
            Self::I64I8(enc) => enc.size(),
            Self::I64U8(enc) => enc.size(),
            Self::U64U64(enc) => enc.size(),
            Self::U64U32(enc) => enc.size(),
            Self::U64U16(enc) => enc.size(),
            Self::U64U8(enc) => enc.size(),
            Self::I64I64N(enc) => enc.size(),
            Self::U64U64N(enc) => enc.size(),
        }
    }

    /// The total number of rows in the column.
    pub fn num_rows(&self) -> u32 {
        match self {
            Self::I64I64(enc) => enc.num_rows(),
            Self::I64I32(enc) => enc.num_rows(),
            Self::I64U32(enc) => enc.num_rows(),
            Self::I64I16(enc) => enc.num_rows(),
            Self::I64U16(enc) => enc.num_rows(),
            Self::I64I8(enc) => enc.num_rows(),
            Self::I64U8(enc) => enc.num_rows(),
            Self::U64U64(enc) => enc.num_rows(),
            Self::U64U32(enc) => enc.num_rows(),
            Self::U64U16(enc) => enc.num_rows(),
            Self::U64U8(enc) => enc.num_rows(),
            Self::I64I64N(enc) => enc.num_rows(),
            Self::U64U64N(enc) => enc.num_rows(),
        }
    }

    // Returns statistics about the physical layout of columns
    pub(crate) fn storage_stats(&self) -> Statistics {
        Statistics {
            enc_type: self.name(),
            log_data_type: self.logical_datatype(),
            values: self.num_rows(),
            nulls: self.null_count(),
            bytes: self.size(),
        }
    }

    /// Determines if the column contains a NULL value.
    pub fn contains_null(&self) -> bool {
        match self {
            Self::I64I64N(enc) => enc.contains_null(),
            Self::U64U64N(enc) => enc.contains_null(),
            _ => false,
        }
    }

    /// The total number of rows in the column.
    pub fn null_count(&self) -> u32 {
        match self {
            Self::I64I64(_) => 0,
            Self::I64I32(_) => 0,
            Self::I64U32(_) => 0,
            Self::I64I16(_) => 0,
            Self::I64U16(_) => 0,
            Self::I64I8(_) => 0,
            Self::I64U8(_) => 0,
            Self::U64U64(_) => 0,
            Self::U64U32(_) => 0,
            Self::U64U16(_) => 0,
            Self::U64U8(_) => 0,
            Self::I64I64N(enc) => enc.null_count(),
            Self::U64U64N(enc) => enc.null_count(),
        }
    }

    /// Determines if the column contains a non-null value.
    pub fn has_any_non_null_value(&self) -> bool {
        match self {
            Self::I64I64N(enc) => enc.has_any_non_null_value(),
            Self::U64U64N(enc) => enc.has_any_non_null_value(),
            _ => true,
        }
    }

    /// Determines if the column contains a non-null value at one of the
    /// provided rows.
    pub fn has_non_null_value(&self, row_ids: &[u32]) -> bool {
        match self {
            Self::I64I64N(enc) => enc.has_non_null_value(row_ids),
            Self::U64U64N(enc) => enc.has_non_null_value(row_ids),
            _ => !row_ids.is_empty(), // all rows will be non-null
        }
    }

    /// Returns the logical value found at the provided row id.
    pub fn value(&self, row_id: u32) -> Value<'_> {
        match &self {
            // N.B., The `Scalar` variant determines the physical type `U` that
            // `c.value` should return as the logical type

            // signed 64-bit variants - logical type is i64 for all these
            Self::I64I64(c) => Value::Scalar(Scalar::I64(c.value(row_id))),
            Self::I64I32(c) => Value::Scalar(Scalar::I64(c.value(row_id))),
            Self::I64U32(c) => Value::Scalar(Scalar::I64(c.value(row_id))),
            Self::I64I16(c) => Value::Scalar(Scalar::I64(c.value(row_id))),
            Self::I64U16(c) => Value::Scalar(Scalar::I64(c.value(row_id))),
            Self::I64I8(c) => Value::Scalar(Scalar::I64(c.value(row_id))),
            Self::I64U8(c) => Value::Scalar(Scalar::I64(c.value(row_id))),

            // unsigned 64-bit variants - logical type is u64 for all these
            Self::U64U64(c) => Value::Scalar(Scalar::U64(c.value(row_id))),
            Self::U64U32(c) => Value::Scalar(Scalar::U64(c.value(row_id))),
            Self::U64U16(c) => Value::Scalar(Scalar::U64(c.value(row_id))),
            Self::U64U8(c) => Value::Scalar(Scalar::U64(c.value(row_id))),

            Self::I64I64N(c) => match c.value(row_id) {
                Some(v) => Value::Scalar(Scalar::I64(v)),
                None => Value::Null,
            },
            Self::U64U64N(c) => match c.value(row_id) {
                Some(v) => Value::Scalar(Scalar::U64(v)),
                None => Value::Null,
            },
        }
    }

    /// Returns the logical values found at the provided row ids.
    ///
    /// TODO(edd): perf - provide a pooling mechanism for these destination
    /// vectors so that they can be re-used.
    pub fn values(&self, row_ids: &[u32]) -> Values<'_> {
        match &self {
            // signed 64-bit variants - logical type is i64 for all these
            Self::I64I64(c) => Values::I64(c.values::<i64>(row_ids, vec![])),
            Self::I64I32(c) => Values::I64(c.values::<i64>(row_ids, vec![])),
            Self::I64U32(c) => Values::I64(c.values::<i64>(row_ids, vec![])),
            Self::I64I16(c) => Values::I64(c.values::<i64>(row_ids, vec![])),
            Self::I64U16(c) => Values::I64(c.values::<i64>(row_ids, vec![])),
            Self::I64I8(c) => Values::I64(c.values::<i64>(row_ids, vec![])),
            Self::I64U8(c) => Values::I64(c.values::<i64>(row_ids, vec![])),

            // unsigned 64-bit variants - logical type is u64 for all these
            Self::U64U64(c) => Values::U64(c.values::<u64>(row_ids, vec![])),
            Self::U64U32(c) => Values::U64(c.values::<u64>(row_ids, vec![])),
            Self::U64U16(c) => Values::U64(c.values::<u64>(row_ids, vec![])),
            Self::U64U8(c) => Values::U64(c.values::<u64>(row_ids, vec![])),

            Self::I64I64N(c) => Values::I64N(c.values(row_ids, vec![])),
            Self::U64U64N(c) => Values::U64N(c.values(row_ids, vec![])),
        }
    }

    /// Returns all logical values in the column.
    ///
    /// TODO(edd): perf - provide a pooling mechanism for these destination
    /// vectors so that they can be re-used.
    pub fn all_values(&self) -> Values<'_> {
        match &self {
            // signed 64-bit variants - logical type is i64 for all these
            Self::I64I64(c) => Values::I64(c.all_values::<i64>(vec![])),
            Self::I64I32(c) => Values::I64(c.all_values::<i64>(vec![])),
            Self::I64U32(c) => Values::I64(c.all_values::<i64>(vec![])),
            Self::I64I16(c) => Values::I64(c.all_values::<i64>(vec![])),
            Self::I64U16(c) => Values::I64(c.all_values::<i64>(vec![])),
            Self::I64I8(c) => Values::I64(c.all_values::<i64>(vec![])),
            Self::I64U8(c) => Values::I64(c.all_values::<i64>(vec![])),

            // unsigned 64-bit variants - logical type is u64 for all these
            Self::U64U64(c) => Values::U64(c.all_values::<u64>(vec![])),
            Self::U64U32(c) => Values::U64(c.all_values::<u64>(vec![])),
            Self::U64U16(c) => Values::U64(c.all_values::<u64>(vec![])),
            Self::U64U8(c) => Values::U64(c.all_values::<u64>(vec![])),

            Self::I64I64N(c) => Values::I64N(c.all_values(vec![])),
            Self::U64U64N(c) => Values::U64N(c.all_values(vec![])),
        }
    }

    /// Returns the encoded values found at the provided row ids. For an
    /// `IntegerEncoding` the encoded values are typically just the raw values.
    pub fn encoded_values(&self, row_ids: &[u32], dst: EncodedValues) -> EncodedValues {
        // Right now the use-case for encoded values on non-string columns is
        // that it's used for grouping with timestamp columns, which should be
        // non-null signed 64-bit integers.
        match dst {
            EncodedValues::I64(dst) => match &self {
                Self::I64I64(data) => EncodedValues::I64(data.values(row_ids, dst)),
                Self::I64I32(data) => EncodedValues::I64(data.values(row_ids, dst)),
                Self::I64U32(data) => EncodedValues::I64(data.values(row_ids, dst)),
                Self::I64I16(data) => EncodedValues::I64(data.values(row_ids, dst)),
                Self::I64U16(data) => EncodedValues::I64(data.values(row_ids, dst)),
                Self::I64I8(data) => EncodedValues::I64(data.values(row_ids, dst)),
                Self::I64U8(data) => EncodedValues::I64(data.values(row_ids, dst)),
                _ => unreachable!("encoded values on encoding type not currently supported"),
            },
            _ => unreachable!("currently only support encoded values as i64"),
        }
    }

    /// All encoded values for the column. For `IntegerEncoding` this is
    /// typically equivalent to `all_values`.
    pub fn all_encoded_values(&self, dst: EncodedValues) -> EncodedValues {
        // Right now the use-case for encoded values on non-string columns is
        // that it's used for grouping with timestamp columns, which should be
        // non-null signed 64-bit integers.
        match dst {
            EncodedValues::I64(dst) => match &self {
                Self::I64I64(data) => EncodedValues::I64(data.all_values(dst)),
                Self::I64I32(data) => EncodedValues::I64(data.all_values(dst)),
                Self::I64U32(data) => EncodedValues::I64(data.all_values(dst)),
                Self::I64I16(data) => EncodedValues::I64(data.all_values(dst)),
                Self::I64U16(data) => EncodedValues::I64(data.all_values(dst)),
                Self::I64I8(data) => EncodedValues::I64(data.all_values(dst)),
                Self::I64U8(data) => EncodedValues::I64(data.all_values(dst)),
                _ => unreachable!("encoded values on encoding type not supported"),
            },
            _ => unreachable!("currently only support encoded values as i64"),
        }
    }

    /// Returns the row ids that satisfy the provided predicate.
    ///
    /// Note: it is the caller's responsibility to ensure that the provided
    /// `Scalar` value will fit within the physical type of the encoded column.
    /// `row_ids_filter` will panic if this invariant is broken.
    pub fn row_ids_filter(&self, op: &cmp::Operator, value: &Scalar, dst: RowIDs) -> RowIDs {
        match &self {
            Self::I64I64(c) => c.row_ids_filter(value.as_i64(), op, dst),
            Self::I64I32(c) => c.row_ids_filter(value.as_i32(), op, dst),
            Self::I64U32(c) => c.row_ids_filter(value.as_u32(), op, dst),
            Self::I64I16(c) => c.row_ids_filter(value.as_i16(), op, dst),
            Self::I64U16(c) => c.row_ids_filter(value.as_u16(), op, dst),
            Self::I64I8(c) => c.row_ids_filter(value.as_i8(), op, dst),
            Self::I64U8(c) => c.row_ids_filter(value.as_u8(), op, dst),

            Self::U64U64(c) => c.row_ids_filter(value.as_u64(), op, dst),
            Self::U64U32(c) => c.row_ids_filter(value.as_u32(), op, dst),
            Self::U64U16(c) => c.row_ids_filter(value.as_u16(), op, dst),
            Self::U64U8(c) => c.row_ids_filter(value.as_u8(), op, dst),

            Self::I64I64N(c) => c.row_ids_filter(value.as_i64(), op, dst),
            Self::U64U64N(c) => c.row_ids_filter(value.as_u64(), op, dst),
        }
    }

    /// Returns the row ids that satisfy both the provided predicates.
    ///
    /// Note: it is the caller's responsibility to ensure that the provided
    /// `Scalar` value will fit within the physical type of the encoded column.
    /// `row_ids_filter` will panic if this invariant is broken.
    pub fn row_ids_filter_range(
        &self,
        low: (&cmp::Operator, &Scalar),
        high: (&cmp::Operator, &Scalar),
        dst: RowIDs,
    ) -> RowIDs {
        match &self {
            Self::I64I64(c) => {
                c.row_ids_filter_range((low.1.as_i64(), low.0), (high.1.as_i64(), high.0), dst)
            }
            Self::I64I32(c) => {
                c.row_ids_filter_range((low.1.as_i32(), low.0), (high.1.as_i32(), high.0), dst)
            }
            Self::I64U32(c) => {
                c.row_ids_filter_range((low.1.as_u32(), low.0), (high.1.as_u32(), high.0), dst)
            }
            Self::I64I16(c) => {
                c.row_ids_filter_range((low.1.as_i16(), low.0), (high.1.as_i16(), high.0), dst)
            }
            Self::I64U16(c) => {
                c.row_ids_filter_range((low.1.as_u16(), low.0), (high.1.as_u16(), high.0), dst)
            }
            Self::I64I8(c) => {
                c.row_ids_filter_range((low.1.as_i8(), low.0), (high.1.as_i8(), high.0), dst)
            }
            Self::I64U8(c) => {
                c.row_ids_filter_range((low.1.as_u8(), low.0), (high.1.as_u8(), high.0), dst)
            }

            Self::U64U64(c) => {
                c.row_ids_filter_range((low.1.as_u64(), low.0), (high.1.as_u64(), high.0), dst)
            }
            Self::U64U32(c) => {
                c.row_ids_filter_range((low.1.as_u32(), low.0), (high.1.as_u32(), high.0), dst)
            }
            Self::U64U16(c) => {
                c.row_ids_filter_range((low.1.as_u16(), low.0), (high.1.as_u16(), high.0), dst)
            }
            Self::U64U8(c) => {
                c.row_ids_filter_range((low.1.as_u8(), low.0), (high.1.as_u8(), high.0), dst)
            }

            Self::I64I64N(_) => todo!(),
            Self::U64U64N(_) => todo!(),
        }
    }

    pub fn min(&self, row_ids: &[u32]) -> Value<'_> {
        match &self {
            Self::I64I64(c) => Value::Scalar(Scalar::I64(c.min(row_ids))),
            Self::I64I32(c) => Value::Scalar(Scalar::I64(c.min(row_ids))),
            Self::I64U32(c) => Value::Scalar(Scalar::I64(c.min(row_ids))),
            Self::I64I16(c) => Value::Scalar(Scalar::I64(c.min(row_ids))),
            Self::I64U16(c) => Value::Scalar(Scalar::I64(c.min(row_ids))),
            Self::I64I8(c) => Value::Scalar(Scalar::I64(c.min(row_ids))),
            Self::I64U8(c) => Value::Scalar(Scalar::I64(c.min(row_ids))),
            Self::U64U64(c) => Value::Scalar(Scalar::U64(c.min(row_ids))),
            Self::U64U32(c) => Value::Scalar(Scalar::U64(c.min(row_ids))),
            Self::U64U16(c) => Value::Scalar(Scalar::U64(c.min(row_ids))),
            Self::U64U8(c) => Value::Scalar(Scalar::U64(c.min(row_ids))),
            Self::I64I64N(c) => match c.min(row_ids) {
                Some(v) => Value::Scalar(Scalar::I64(v)),
                None => Value::Null,
            },
            Self::U64U64N(c) => match c.min(row_ids) {
                Some(v) => Value::Scalar(Scalar::U64(v)),
                None => Value::Null,
            },
        }
    }

    pub fn max(&self, row_ids: &[u32]) -> Value<'_> {
        match &self {
            Self::I64I64(c) => Value::Scalar(Scalar::I64(c.max(row_ids))),
            Self::I64I32(c) => Value::Scalar(Scalar::I64(c.max(row_ids))),
            Self::I64U32(c) => Value::Scalar(Scalar::I64(c.max(row_ids))),
            Self::I64I16(c) => Value::Scalar(Scalar::I64(c.max(row_ids))),
            Self::I64U16(c) => Value::Scalar(Scalar::I64(c.max(row_ids))),
            Self::I64I8(c) => Value::Scalar(Scalar::I64(c.max(row_ids))),
            Self::I64U8(c) => Value::Scalar(Scalar::I64(c.max(row_ids))),
            Self::U64U64(c) => Value::Scalar(Scalar::U64(c.max(row_ids))),
            Self::U64U32(c) => Value::Scalar(Scalar::U64(c.max(row_ids))),
            Self::U64U16(c) => Value::Scalar(Scalar::U64(c.max(row_ids))),
            Self::U64U8(c) => Value::Scalar(Scalar::U64(c.max(row_ids))),
            Self::I64I64N(c) => match c.max(row_ids) {
                Some(v) => Value::Scalar(Scalar::I64(v)),
                None => Value::Null,
            },
            Self::U64U64N(c) => match c.max(row_ids) {
                Some(v) => Value::Scalar(Scalar::U64(v)),
                None => Value::Null,
            },
        }
    }

    pub fn sum(&self, row_ids: &[u32]) -> Scalar {
        match &self {
            Self::I64I64(c) => Scalar::I64(c.sum(row_ids)),
            Self::I64I32(c) => Scalar::I64(c.sum(row_ids)),
            Self::I64U32(c) => Scalar::I64(c.sum(row_ids)),
            Self::I64I16(c) => Scalar::I64(c.sum(row_ids)),
            Self::I64U16(c) => Scalar::I64(c.sum(row_ids)),
            Self::I64I8(c) => Scalar::I64(c.sum(row_ids)),
            Self::I64U8(c) => Scalar::I64(c.sum(row_ids)),
            Self::U64U64(c) => Scalar::U64(c.sum(row_ids)),
            Self::U64U32(c) => Scalar::U64(c.sum(row_ids)),
            Self::U64U16(c) => Scalar::U64(c.sum(row_ids)),
            Self::U64U8(c) => Scalar::U64(c.sum(row_ids)),
            Self::I64I64N(c) => match c.sum(row_ids) {
                Some(v) => Scalar::I64(v),
                None => Scalar::Null,
            },
            Self::U64U64N(c) => match c.sum(row_ids) {
                Some(v) => Scalar::U64(v),
                None => Scalar::Null,
            },
        }
    }

    pub fn count(&self, row_ids: &[u32]) -> u32 {
        match &self {
            Self::I64I64(c) => c.count(row_ids),
            Self::I64I32(c) => c.count(row_ids),
            Self::I64U32(c) => c.count(row_ids),
            Self::I64I16(c) => c.count(row_ids),
            Self::I64U16(c) => c.count(row_ids),
            Self::I64I8(c) => c.count(row_ids),
            Self::I64U8(c) => c.count(row_ids),
            Self::U64U64(c) => c.count(row_ids),
            Self::U64U32(c) => c.count(row_ids),
            Self::U64U16(c) => c.count(row_ids),
            Self::U64U8(c) => c.count(row_ids),
            Self::I64I64N(c) => c.count(row_ids),
            Self::U64U64N(c) => c.count(row_ids),
        }
    }

    /// The name of this encoding.
    pub fn name(&self) -> &'static str {
        match &self {
            Self::I64I64(_) => "None",
            Self::I64I32(_) => "BT_I32",
            Self::I64U32(_) => "BT_U32",
            Self::I64I16(_) => "BT_I16",
            Self::I64U16(_) => "BT_U16",
            Self::I64I8(_) => "BT_I8",
            Self::I64U8(_) => "BT_U8",
            Self::U64U64(_) => "None",
            Self::U64U32(_) => "BT_U32",
            Self::U64U16(_) => "BT_U16",
            Self::U64U8(_) => "BT_U8",
            Self::I64I64N(_) => "None",
            Self::U64U64N(_) => "None",
        }
    }

    /// The logical datatype of this encoding.
    pub fn logical_datatype(&self) -> &'static str {
        match &self {
            Self::I64I64(_) => "i64",
            Self::I64I32(_) => "i64",
            Self::I64U32(_) => "i64",
            Self::I64I16(_) => "i64",
            Self::I64U16(_) => "i64",
            Self::I64I8(_) => "i64",
            Self::I64U8(_) => "i64",
            Self::U64U64(_) => "u64",
            Self::U64U32(_) => "u64",
            Self::U64U16(_) => "u64",
            Self::U64U8(_) => "u64",
            Self::I64I64N(_) => "i64",
            Self::U64U64N(_) => "u64",
        }
    }
}

impl std::fmt::Display for IntegerEncoding {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let name = self.name();
        match self {
            Self::I64I64(enc) => write!(f, "[{}]: {}", name, enc),
            Self::I64I32(enc) => write!(f, "[{}]: {}", name, enc),
            Self::I64U32(enc) => write!(f, "[{}]: {}", name, enc),
            Self::I64I16(enc) => write!(f, "[{}]: {}", name, enc),
            Self::I64U16(enc) => write!(f, "[{}]: {}", name, enc),
            Self::I64I8(enc) => write!(f, "[{}]: {}", name, enc),
            Self::I64U8(enc) => write!(f, "[{}]: {}", name, enc),
            Self::U64U64(enc) => write!(f, "[{}]: {}", name, enc),
            Self::U64U32(enc) => write!(f, "[{}]: {}", name, enc),
            Self::U64U16(enc) => write!(f, "[{}]: {}", name, enc),
            Self::U64U8(enc) => write!(f, "[{}]: {}", name, enc),
            Self::I64I64N(enc) => write!(f, "[{}]: {}", name, enc),
            Self::U64U64N(enc) => write!(f, "[{}]: {}", name, enc),
        }
    }
}

/// Converts a slice of i64 values into an IntegerEncoding.
///
/// The most compact physical type needed to store the columnar values is
/// determined, and a `Fixed` encoding is used for storage.
///
/// Panics if the provided slice is empty.
impl From<&[i64]> for IntegerEncoding {
    fn from(arr: &[i64]) -> Self {
        // determine min and max values.
        let mut min = arr[0];
        let mut max = arr[0];
        for &v in arr.iter().skip(1) {
            min = min.min(v);
            max = max.max(v);
        }

        // This match is carefully ordered. It prioritises smaller physical
        // datatypes that can safely represent the provided logical data
        match (min, max) {
            // encode as u8 values
            (min, max) if min >= 0 && max <= u8::MAX as i64 => Self::I64U8(Fixed::<u8>::from(arr)),
            // encode as i8 values
            (min, max) if min >= i8::MIN as i64 && max <= i8::MAX as i64 => {
                Self::I64I8(Fixed::<i8>::from(arr))
            }
            // encode as u16 values
            (min, max) if min >= 0 && max <= u16::MAX as i64 => {
                Self::I64U16(Fixed::<u16>::from(arr))
            }
            // encode as i16 values
            (min, max) if min >= i16::MIN as i64 && max <= i16::MAX as i64 => {
                Self::I64I16(Fixed::<i16>::from(arr))
            }
            // encode as u32 values
            (min, max) if min >= 0 && max <= u32::MAX as i64 => {
                Self::I64U32(Fixed::<u32>::from(arr))
            }
            // encode as i32 values
            (min, max) if min >= i32::MIN as i64 && max <= i32::MAX as i64 => {
                Self::I64I32(Fixed::<i32>::from(arr))
            }
            // otherwise, encode with the same physical type (i64)
            (_, _) => Self::I64I64(Fixed::<i64>::from(arr)),
        }
    }
}

/// Converts an Arrow array into an IntegerEncoding.
///
/// TODO(edd): convert underlying type of Arrow data to smallest physical
/// representation.
impl From<arrow::array::Int64Array> for IntegerEncoding {
    fn from(arr: arrow::array::Int64Array) -> Self {
        if arr.null_count() == 0 {
            return Self::from(arr.values());
        }

        // TODO(edd): currently fixed null only supports 64-bit logical/physical
        // types. Need to add support for storing as smaller physical types.
        Self::I64I64N(FixedNull::<arrow::datatypes::Int64Type>::from(arr))
    }
}

/// Converts a slice of u64 values into an IntegerEncoding.
///
/// The most compact physical type needed to store the columnar values is
/// determined, and a `Fixed` encoding is used for storage.
///
/// Panics if the provided slice is empty.
impl From<&[u64]> for IntegerEncoding {
    fn from(arr: &[u64]) -> Self {
        // determine min and max values.
        let mut min = arr[0];
        let mut max = arr[0];
        for &v in arr.iter().skip(1) {
            min = min.min(v);
            max = max.max(v);
        }

        // This match is carefully ordered. It prioritises smaller physical
        // datatypes that can safely represent the provided logical data
        match (min, max) {
            // encode as u8 values
            (_, max) if max <= u8::MAX as u64 => Self::U64U8(Fixed::<u8>::from(arr)),
            // encode as u16 values
            (_, max) if max <= u16::MAX as u64 => Self::U64U16(Fixed::<u16>::from(arr)),
            // encode as u32 values
            (_, max) if max <= u32::MAX as u64 => Self::U64U32(Fixed::<u32>::from(arr)),
            // otherwise, encode with the same physical type (u64)
            (_, _) => Self::U64U64(Fixed::<u64>::from(arr)),
        }
    }
}

/// Converts an Arrow array into an IntegerEncoding.
///
/// TODO(edd): convert underlying type of Arrow data to smallest physical
/// representation.
impl From<arrow::array::UInt64Array> for IntegerEncoding {
    fn from(arr: arrow::array::UInt64Array) -> Self {
        if arr.null_count() == 0 {
            return Self::from(arr.values());
        }

        // TODO(edd): currently fixed null only supports 64-bit logical/physical
        // types. Need to add support for storing as smaller physical types.
        Self::U64U64N(FixedNull::<arrow::datatypes::UInt64Type>::from(arr))
    }
}

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn from_slice_i64() {
        let cases = vec![
            vec![0_i64, 2, 245, 3],
            vec![0_i64, -120, 127, 3],
            vec![399_i64, 2, 2452, 3],
            vec![-399_i64, 2, 2452, 3],
            vec![u32::MAX as i64, 2, 245, 3],
            vec![i32::MAX as i64, 2, 245, 3],
            vec![0_i64, 2, 245, u32::MAX as i64 + 1],
        ];

        let exp = vec![
            IntegerEncoding::I64U8(Fixed::<u8>::from(cases[0].as_slice())),
            IntegerEncoding::I64I8(Fixed::<i8>::from(cases[1].as_slice())),
            IntegerEncoding::I64U16(Fixed::<u16>::from(cases[2].as_slice())),
            IntegerEncoding::I64I16(Fixed::<i16>::from(cases[3].as_slice())),
            IntegerEncoding::I64U32(Fixed::<u32>::from(cases[4].as_slice())),
            IntegerEncoding::I64I32(Fixed::<i32>::from(cases[5].as_slice())),
            IntegerEncoding::I64I64(Fixed::<i64>::from(cases[6].as_slice())),
        ];

        for (_case, _exp) in cases.iter().zip(exp.iter()) {
            // TODO - add debug
            //assert_eq!(IntegerEncoding::from(&case), exp);
        }
    }
}
