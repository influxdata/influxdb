use std::fmt::Display;
use std::iter::FromIterator;

use arrow::array::PrimitiveArray;
use arrow::{self, array::Array, datatypes::*};
use either::Either;

use super::encoding::scalar::{
    transcoders::{ByteTrimmer, NoOpTranscoder, Transcoder},
    ScalarEncoding,
};
use super::encoding::{scalar::Fixed, scalar::FixedNull};
use super::{cmp, Statistics};
use crate::column::{RowIDs, Scalar, Value, Values};

/// A representation of a column encoding for integer data, providing an
/// API for working against that data in an immutable way.
#[derive(Debug)]
pub enum IntegerEncoding {
    // (encoding, name_of_encoding)
    I64(Box<dyn ScalarEncoding<i64>>, String),
    U64(Box<dyn ScalarEncoding<u64>>, String),
}

impl IntegerEncoding {
    /// The total size in bytes of the store columnar data.
    pub fn size(&self) -> usize {
        match self {
            Self::I64(enc, _) => enc.size(),
            Self::U64(enc, _) => enc.size(),
        }
    }

    /// The estimated total size in bytes of the underlying integer values in
    /// the column if they were stored contiguously and uncompressed (natively
    /// as i64/u64). `include_nulls` will effectively size each NULL value as 8b
    /// if `true`.
    pub fn size_raw(&self, include_nulls: bool) -> usize {
        match self {
            Self::I64(enc, _) => enc.size_raw(include_nulls),
            Self::U64(enc, _) => enc.size_raw(include_nulls),
        }
    }

    /// The total number of rows in the column.
    pub fn num_rows(&self) -> u32 {
        match self {
            Self::I64(enc, _) => enc.num_rows(),
            Self::U64(enc, _) => enc.num_rows(),
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
            raw_bytes: self.size_raw(true),
            raw_bytes_no_null: self.size_raw(false),
        }
    }

    /// Determines if the column contains a NULL value.
    pub fn contains_null(&self) -> bool {
        self.null_count() > 0
    }

    /// The total number of rows in the column.
    pub fn null_count(&self) -> u32 {
        match self {
            Self::I64(enc, _) => enc.null_count(),
            Self::U64(enc, _) => enc.null_count(),
        }
    }

    /// Determines if the column contains a non-null value.
    pub fn has_any_non_null_value(&self) -> bool {
        match self {
            Self::I64(enc, _) => enc.has_any_non_null_value(),
            Self::U64(enc, _) => enc.has_any_non_null_value(),
        }
    }

    /// Determines if the column contains a non-null value at one of the
    /// provided rows.
    pub fn has_non_null_value(&self, row_ids: &[u32]) -> bool {
        match self {
            Self::I64(enc, _) => enc.has_non_null_value(row_ids),
            Self::U64(enc, _) => enc.has_non_null_value(row_ids),
        }
    }

    /// Returns the logical value found at the provided row id.
    pub fn value(&self, row_id: u32) -> Value<'_> {
        match self {
            Self::I64(enc, _) => match enc.value(row_id) {
                Some(v) => Value::Scalar(Scalar::I64(v)),
                None => Value::Null,
            },
            Self::U64(enc, _) => match enc.value(row_id) {
                Some(v) => Value::Scalar(Scalar::U64(v)),
                None => Value::Null,
            },
        }
    }

    /// Returns the logical values found at the provided ordinal offsets.
    pub fn values(&self, row_ids: &[u32]) -> Values<'_> {
        match self {
            Self::I64(enc, _) => match enc.values(row_ids) {
                Either::Left(values) => Values::I64(values),
                Either::Right(values) => Values::I64N(values),
            },
            Self::U64(enc, _) => match enc.values(row_ids) {
                Either::Left(values) => Values::U64(values),
                Either::Right(values) => Values::U64N(values),
            },
        }
    }

    /// Returns all logical values in the column.
    ///
    /// TODO(edd): perf - provide a pooling mechanism for these destination
    /// vectors so that they can be re-used.
    pub fn all_values(&self) -> Values<'_> {
        match self {
            Self::I64(enc, _) => match enc.all_values() {
                Either::Left(values) => Values::I64(values),
                Either::Right(values) => Values::I64N(values),
            },
            Self::U64(enc, _) => match enc.all_values() {
                Either::Left(values) => Values::U64(values),
                Either::Right(values) => Values::U64N(values),
            },
        }
    }

    /// Returns the ordinal offsets that satisfy the provided predicate.
    ///
    /// Note: it is the caller's responsibility to ensure that the provided
    /// `Scalar` value will fit within the physical type of the encoded column.
    /// `row_ids_filter` will panic if this invariant is broken.
    pub fn row_ids_filter(&self, op: &cmp::Operator, value: &Scalar, dst: RowIDs) -> RowIDs {
        match &self {
            Self::I64(enc, _) => enc.row_ids_filter(value.as_i64(), op, dst),
            Self::U64(enc, _) => enc.row_ids_filter(value.as_u64(), op, dst),
        }
    }

    /// Returns the ordinal offsets that satisfy both the provided predicates.
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
            Self::I64(enc, _) => {
                let left = (low.1.as_i64(), low.0);
                let right = (high.1.as_i64(), high.0);
                enc.row_ids_filter_range(left, right, dst)
            }
            Self::U64(enc, _) => {
                let left = (low.1.as_u64(), low.0);
                let right = (high.1.as_u64(), high.0);
                enc.row_ids_filter_range(left, right, dst)
            }
        }
    }

    pub fn min(&self, row_ids: &[u32]) -> Value<'_> {
        match &self {
            Self::I64(enc, _) => match enc.min(row_ids) {
                Some(v) => Value::Scalar(Scalar::I64(v)),
                None => Value::Null,
            },
            Self::U64(enc, _) => match enc.min(row_ids) {
                Some(v) => Value::Scalar(Scalar::U64(v)),
                None => Value::Null,
            },
        }
    }

    pub fn max(&self, row_ids: &[u32]) -> Value<'_> {
        match &self {
            Self::I64(enc, _) => match enc.max(row_ids) {
                Some(v) => Value::Scalar(Scalar::I64(v)),
                None => Value::Null,
            },
            Self::U64(enc, _) => match enc.max(row_ids) {
                Some(v) => Value::Scalar(Scalar::U64(v)),
                None => Value::Null,
            },
        }
    }

    pub fn sum(&self, row_ids: &[u32]) -> Scalar {
        match &self {
            Self::I64(enc, _) => match enc.sum(row_ids) {
                Some(v) => Scalar::I64(v),
                None => Scalar::Null,
            },
            Self::U64(enc, _) => match enc.sum(row_ids) {
                Some(v) => Scalar::U64(v),
                None => Scalar::Null,
            },
        }
    }

    pub fn count(&self, row_ids: &[u32]) -> u32 {
        match &self {
            Self::I64(enc, _) => enc.count(row_ids),
            Self::U64(enc, _) => enc.count(row_ids),
        }
    }

    /// The name of this encoding.
    pub fn name(&self) -> String {
        match self {
            Self::I64(_, name) => name.clone(),
            Self::U64(_, name) => name.clone(),
        }
    }

    /// The logical datatype of this encoding.
    pub fn logical_datatype(&self) -> &'static str {
        match self {
            Self::I64(_, _) => "i64",
            Self::U64(_, _) => "u64",
        }
    }
}

impl Display for IntegerEncoding {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::I64(enc, _) => write!(f, "[{}]: {}", self.name(), enc),
            Self::U64(enc, _) => write!(f, "[{}]: {}", self.name(), enc),
        }
    }
}

/// Converts a slice of i64 values into an IntegerEncoding.
///
/// The most compact physical type needed to store the columnar values is
/// determined, and a `Fixed` encoding is used for storage.
///
/// #Panics
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
        let transcoder = ByteTrimmer {};
        match (min, max) {
            // encode as u8 values
            (min, max) if min >= 0 && max <= u8::MAX as i64 => {
                let arr = arr
                    .iter()
                    .map::<u8, _>(|v| transcoder.encode(*v))
                    .collect::<Vec<_>>();
                let enc = Box::new(Fixed::new(arr, transcoder));
                let name = enc.name();
                Self::I64(enc, format!("BT_U8-{}", name))
            }
            // encode as i8 values
            (min, max) if min >= i8::MIN as i64 && max <= i8::MAX as i64 => {
                let arr = arr
                    .iter()
                    .map(|v| transcoder.encode(*v))
                    .collect::<Vec<i8>>();
                let enc = Box::new(Fixed::new(arr, transcoder));
                let name = enc.name();
                Self::I64(enc, format!("BT_I8-{}", name))
            }
            // encode as u16 values
            (min, max) if min >= 0 && max <= u16::MAX as i64 => {
                let arr = arr
                    .iter()
                    .map::<u16, _>(|v| transcoder.encode(*v))
                    .collect::<Vec<u16>>();
                let enc = Box::new(Fixed::new(arr, transcoder));
                let name = enc.name();
                Self::I64(enc, format!("BT_U16-{}", name))
            }
            // encode as i16 values
            (min, max) if min >= i16::MIN as i64 && max <= i16::MAX as i64 => {
                let arr = arr
                    .iter()
                    .map(|v| transcoder.encode(*v))
                    .collect::<Vec<i16>>();
                let enc = Box::new(Fixed::new(arr, transcoder));
                let name = enc.name();
                Self::I64(enc, format!("BT_I16-{}", name))
            }
            // encode as u32 values
            (min, max) if min >= 0 && max <= u32::MAX as i64 => {
                let arr = arr
                    .iter()
                    .map(|v| transcoder.encode(*v))
                    .collect::<Vec<u32>>();
                let enc = Box::new(Fixed::new(arr, transcoder));
                let name = enc.name();
                Self::I64(enc, format!("BT_U32-{}", name))
            }
            // encode as i32 values
            (min, max) if min >= i32::MIN as i64 && max <= i32::MAX as i64 => {
                let arr = arr
                    .iter()
                    .map(|v| transcoder.encode(*v))
                    .collect::<Vec<i32>>();
                let enc = Box::new(Fixed::new(arr, transcoder));
                let name = enc.name();
                Self::I64(enc, format!("BT_I32-{}", name))
            }
            // otherwise, encode with the same physical type (i64)
            (_, _) => {
                let enc = Box::new(Fixed::new(arr.to_vec(), NoOpTranscoder {}));
                let name = enc.name();
                Self::I64(enc, format!("None-{}", name))
            }
        }
    }
}

/// Converts an Arrow array into an IntegerEncoding.
///
/// The most compact physical Arrow array type is used to store the column
/// within a `FixedNull` encoding.
impl From<arrow::array::Int64Array> for IntegerEncoding {
    fn from(arr: arrow::array::Int64Array) -> Self {
        if arr.null_count() == 0 {
            return Self::from(arr.values());
        }

        // determine min and max values.
        let min = arrow::compute::kernels::aggregate::min(&arr);
        let max = arrow::compute::kernels::aggregate::max(&arr);

        // This match is carefully ordered. It prioritises smaller physical
        // datatypes that can safely represent the provided logical data
        let transcoder = ByteTrimmer {};
        match (min, max) {
            // data is all NULL. Store as single byte column for now.
            // TODO(edd): this will be smaller when stored using RLE
            (None, None) => {
                let arr = PrimitiveArray::from_iter(arr.iter().map::<Option<u8>, _>(|_| None));
                let enc = Box::new(FixedNull::<UInt8Type, i64, _>::new(arr, transcoder));
                let name = enc.name();
                Self::I64(enc, format!("BT_U8-{}", name))
            }
            // encode as u8 values
            (min, max) if min >= Some(0) && max <= Some(u8::MAX as i64) => {
                let arr = PrimitiveArray::from_iter(
                    arr.into_iter().map(|v| v.map(|v| transcoder.encode(v))), // encode i64 as u8
                );

                let enc = Box::new(FixedNull::<UInt8Type, i64, _>::new(arr, transcoder));
                let name = enc.name();
                Self::I64(enc, format!("BT_U8-{}", name))
            }
            // encode as i8 values
            (min, max) if min >= Some(i8::MIN as i64) && max <= Some(i8::MAX as i64) => {
                let arr = PrimitiveArray::from_iter(
                    arr.into_iter().map(|v| v.map(|v| transcoder.encode(v))), // encode i64 as i8
                );

                let enc = Box::new(FixedNull::<Int8Type, i64, _>::new(arr, transcoder));
                let name = enc.name();
                Self::I64(enc, format!("BT_I8-{}", name))
            }
            // encode as u16 values
            (min, max) if min >= Some(0) && max <= Some(u16::MAX as i64) => {
                let arr = PrimitiveArray::from_iter(
                    arr.into_iter().map(|v| v.map(|v| transcoder.encode(v))), // encode i64 as u16
                );

                let enc = Box::new(FixedNull::<UInt16Type, i64, _>::new(arr, transcoder));
                let name = enc.name();
                Self::I64(enc, format!("BT_U16-{}", name))
            }
            // encode as i16 values
            (min, max) if min >= Some(i16::MIN as i64) && max <= Some(i16::MAX as i64) => {
                let arr = PrimitiveArray::from_iter(
                    arr.into_iter().map(|v| v.map(|v| transcoder.encode(v))), // encode i64 as i16
                );

                let enc = Box::new(FixedNull::<Int16Type, i64, _>::new(arr, transcoder));
                let name = enc.name();
                Self::I64(enc, format!("BT_I16-{}", name))
            }
            // encode as u32 values
            (min, max) if min >= Some(0) && max <= Some(u32::MAX as i64) => {
                let arr = PrimitiveArray::from_iter(
                    arr.into_iter().map(|v| v.map(|v| transcoder.encode(v))), // encode i64 as u32
                );

                let enc = Box::new(FixedNull::<UInt32Type, i64, _>::new(arr, transcoder));
                let name = enc.name();
                Self::I64(enc, format!("BT_U32-{}", name))
            }
            // encode as i32 values
            (min, max) if min >= Some(i32::MIN as i64) && max <= Some(i32::MAX as i64) => {
                let arr = PrimitiveArray::from_iter(
                    arr.into_iter().map(|v| v.map(|v| transcoder.encode(v))), // encode i64 as i32
                );

                let enc = Box::new(FixedNull::<Int32Type, i64, _>::new(arr, transcoder));
                let name = enc.name();
                Self::I64(enc, format!("BT_I32-{}", name))
            }
            // otherwise, encode with the same physical type (i64)
            (_, _) => {
                let enc = Box::new(FixedNull::<Int64Type, i64, _>::new(arr, NoOpTranscoder {}));
                let name = enc.name();
                Self::I64(enc, format!("None-{}", name))
            }
        }
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
        // determine max value.
        let mut max = arr[0];
        for &v in arr.iter().skip(1) {
            max = max.max(v);
        }

        // This match is carefully ordered. It prioritises smaller physical
        // datatypes that can safely represent the provided logical data
        let transcoder = ByteTrimmer {};
        match max {
            // encode as u8 values
            max if max <= u8::MAX as u64 => {
                let arr = arr
                    .iter()
                    .map::<u8, _>(|v| transcoder.encode(*v)) // u64 -> u8
                    .collect::<Vec<u8>>();
                let enc = Box::new(Fixed::<u8, u64, _>::new(arr, transcoder));
                let name = enc.name();
                Self::U64(enc, format!("BT_U8-{}", name))
            }
            // encode as u16 values
            max if max <= u16::MAX as u64 => {
                let arr = arr
                    .iter()
                    .map::<u16, _>(|v| transcoder.encode(*v)) // u64 -> u16
                    .collect::<Vec<u16>>();
                let enc = Box::new(Fixed::<u16, u64, _>::new(arr, transcoder));
                let name = enc.name();
                Self::U64(enc, format!("BT_U16-{}", name))
            }
            // encode as u32 values
            max if max <= u32::MAX as u64 => {
                let arr = arr
                    .iter()
                    .map::<u32, _>(|v| transcoder.encode(*v)) // u64 -> u32
                    .collect::<Vec<u32>>();
                let enc = Box::new(Fixed::<u32, u64, _>::new(arr, transcoder));
                let name = enc.name();
                Self::U64(enc, format!("BT_U32-{}", name))
            }
            // otherwise, encode with the same physical type (u64)
            _ => {
                let enc = Box::new(Fixed::<u64, u64, _>::new(arr.to_vec(), NoOpTranscoder {})); // no transcoding needed
                let name = enc.name();
                Self::U64(enc, format!("None-{}", name))
            }
        }
    }
}

/// Converts an Arrow array into an IntegerEncoding.
///
/// The most compact physical Arrow array type is used to store the column
/// within a `FixedNull` encoding.
impl From<arrow::array::UInt64Array> for IntegerEncoding {
    fn from(arr: arrow::array::UInt64Array) -> Self {
        if arr.null_count() == 0 {
            return Self::from(arr.values());
        }

        // determine max value.
        let max = arrow::compute::kernels::aggregate::max(&arr);

        // This match is carefully ordered. It prioritises smaller physical
        // datatypes that can safely represent the provided logical data
        let transcoder = ByteTrimmer {};
        match max {
            // encode as u8 values
            max if max <= Some(u8::MAX as u64) => {
                let arr = PrimitiveArray::from_iter(
                    arr.into_iter().map(|v| v.map(|v| transcoder.encode(v))), // encode u64 as u8
                );

                let enc = Box::new(FixedNull::<UInt8Type, u64, _>::new(arr, transcoder));
                let name = enc.name();
                Self::U64(enc, format!("BT_U8-{}", name))
            }
            // encode as u16 values
            max if max <= Some(u16::MAX as u64) => {
                let arr = PrimitiveArray::from_iter(
                    arr.into_iter().map(|v| v.map(|v| transcoder.encode(v))), // encode u64 as u16
                );

                let enc = Box::new(FixedNull::<UInt16Type, u64, _>::new(arr, transcoder));
                let name = enc.name();
                Self::U64(enc, format!("BT_U16-{}", name))
            }
            // encode as u32 values
            max if max <= Some(u32::MAX as u64) => {
                let arr = PrimitiveArray::from_iter(
                    arr.into_iter().map(|v| v.map(|v| transcoder.encode(v))), // encode u64 as u32
                );

                let enc = Box::new(FixedNull::<UInt32Type, u64, _>::new(arr, transcoder));
                let name = enc.name();
                Self::U64(enc, format!("BT_U32-{}", name))
            }
            // otherwise, encode with the same physical type (u64)
            _ => {
                let enc = Box::new(FixedNull::<UInt64Type, u64, _>::new(arr, NoOpTranscoder {}));
                let name = enc.name();
                Self::U64(enc, format!("None-{}", name))
            }
        }
    }
}

#[cfg(test)]
mod test {
    use arrow::array::{Int64Array, UInt64Array};

    use super::*;

    #[test]
    // Tests that input data gets byte trimmed correctly.
    fn from_slice_i64() {
        let cases = vec![
            (vec![0_i64, 2, 245, 3], 28_usize),             // u8 fixed array
            (vec![0_i64, -120, 127, 3], 28),                // i8 fixed array
            (vec![399_i64, 2, 2452, 3], 32),                // u16 fixed array
            (vec![-399_i64, 2, 2452, 3], 32),               // u16 fixed array
            (vec![u32::MAX as i64, 2, 245, 3], 40),         // u32 fixed array
            (vec![i32::MIN as i64, 2, 245, 3], 40),         // i32 fixed array
            (vec![0_i64, 2, 245, u32::MAX as i64 + 1], 56), // u64 fixed array
            (vec![0_i64, 2, 245, i64::MIN], 56),            // i64 fixed array
        ];

        for (case, size) in cases.into_iter() {
            let enc = IntegerEncoding::from(case.as_slice());
            assert_eq!(enc.size(), size, "failed: {:?}", enc);
        }
    }

    #[test]
    fn from_slice_u64() {
        let cases = vec![
            (vec![0_u64, 2, 245, 3], 28_usize),             // u8 fixed array
            (vec![399_u64, 2, 2452, 3], 32),                // u16 fixed array
            (vec![u32::MAX as u64, 2, 245, 3], 40),         // u32 fixed array
            (vec![0_u64, 2, 245, u32::MAX as u64 + 1], 56), // u64 fixed array
        ];

        for (case, exp) in cases.into_iter() {
            let enc = IntegerEncoding::from(case.as_slice());
            assert_eq!(enc.size(), exp, "failed: {:?}", enc);
        }
    }

    #[test]
    fn from_arrow_i64_array() {
        let cases = vec![
            (vec![0_i64, 2, 245, 3], 28_usize),             // u8 fixed array
            (vec![0_i64, -120, 127, 3], 28),                // i8 fixed array
            (vec![399_i64, 2, 2452, 3], 32),                // u16 fixed array
            (vec![-399_i64, 2, 2452, 3], 32),               // i16 fixed array
            (vec![u32::MAX as i64, 2, 245, 3], 40),         // u32 fixed array
            (vec![i32::MIN as i64, 2, 245, 3], 40),         // i32 fixed array
            (vec![0_i64, 2, 245, u32::MAX as i64 + 1], 56), // u64 fixed array
            (vec![0_i64, 2, 245, i64::MIN], 56),            // i64 fixed array
        ];

        // for Arrow arrays with no nulls we can store the column using a
        // non-nullable fixed encoding
        for (case, size) in cases.iter().cloned() {
            let arr = Int64Array::from(case);
            let enc = IntegerEncoding::from(arr);
            assert_eq!(enc.size(), size, "failed: {:?}", enc);
        }

        // Input data containing NULL will be stored in an Arrow array encoding
        let cases = vec![
            (vec![None, Some(0_i64)], 344_usize),         // u8 Arrow array
            (vec![None, Some(-120_i64)], 344),            // i8
            (vec![None, Some(399_i64)], 344),             // u16
            (vec![None, Some(-399_i64)], 344),            // i16
            (vec![None, Some(u32::MAX as i64)], 344),     // u32
            (vec![None, Some(i32::MIN as i64)], 344),     // i32
            (vec![None, Some(u32::MAX as i64 + 1)], 344), //u64
        ];

        for (case, size) in cases.iter().cloned() {
            let arr = Int64Array::from(case);
            let enc = IntegerEncoding::from(arr);
            assert_eq!(enc.size(), size, "failed: {:?}", enc);
        }
    }

    #[test]
    fn from_arrow_u64_array() {
        let cases = vec![
            (vec![0_u64, 2, 245, 3], 28_usize),     // stored in Fixed u8 array
            (vec![399_u64, 2, 2452, 3], 32),        // stored in Fixed u16 array
            (vec![u32::MAX as u64, 2, 245, 3], 40), // stored in Fixed u32 array
            (vec![0_u64, 2, 245, u32::MAX as u64 + 1], 56), // Fixed u64 array
        ];

        // for Arrow arrays with no nulls we can store the column using a
        // non-nullable fixed encoding
        for (case, size) in cases.iter().cloned() {
            let arr = UInt64Array::from(case);
            let enc = IntegerEncoding::from(arr);
            assert_eq!(enc.size(), size, "failed: {:?}", enc);
        }

        // Input data containing NULL will be stored in an Arrow array encoding
        let cases = vec![
            (vec![None, Some(0_u64)], 344_usize),
            (vec![None, Some(399_u64)], 344),
            (vec![None, Some(u32::MAX as u64)], 344),
            (vec![None, Some(u64::MAX)], 344),
        ];

        for (case, size) in cases.iter().cloned() {
            let arr = UInt64Array::from(case);
            let enc = IntegerEncoding::from(arr);
            assert_eq!(enc.size(), size, "failed: {:?}", enc);
        }
    }
}
