use arrow::array::Array;
use arrow::array::Float64Array;
use arrow::array::PrimitiveArray;
use arrow::datatypes::Float64Type;
use arrow::datatypes::Int16Type;
use arrow::datatypes::Int32Type;
use arrow::datatypes::Int8Type;
use arrow::datatypes::UInt16Type;
use arrow::datatypes::UInt32Type;
use arrow::datatypes::UInt8Type;
use std::iter::FromIterator;
use std::mem::size_of;

use super::encoding::scalar::{rle, transcoders::*, ScalarEncoding};
use super::encoding::{
    scalar::Fixed,
    scalar::{rle::RLE, FixedNull},
};
use super::{cmp, Statistics};
use crate::column::{RowIDs, Scalar, Value, Values};

#[allow(clippy::upper_case_acronyms)] // TODO(edd): these will be OK in 1.52
#[derive(Debug)]
/// A representation of a column encoding for floating point data.
///
/// Note: an enum to make supporting a logical `f32` type in the future a bit
/// simpler.
pub enum FloatEncoding {
    F64(Box<dyn ScalarEncoding<f64>>, String),
}

impl FloatEncoding {
    /// The total size in bytes of to store columnar data in memory.
    pub fn size(&self) -> usize {
        match self {
            Self::F64(enc, _) => enc.size(),
        }
    }

    /// The estimated total size in bytes of the underlying float values in the
    /// column if they were stored contiguously and uncompressed. `include_nulls`
    /// will effectively size each NULL value as 8b if `true`.
    pub fn size_raw(&self, include_nulls: bool) -> usize {
        match self {
            Self::F64(enc, _) => enc.size_raw(include_nulls),
        }
    }

    /// The total number of rows in the column.
    pub fn num_rows(&self) -> u32 {
        match self {
            Self::F64(enc, _) => enc.num_rows(),
        }
    }

    // Returns statistics about the physical layout of columns
    pub(crate) fn storage_stats(&self) -> Statistics {
        Statistics {
            enc_type: self.name().into(),
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

    /// The total number of NULL values in the column.
    pub fn null_count(&self) -> u32 {
        match self {
            Self::F64(enc, _) => enc.null_count(),
        }
    }

    /// Determines if the column contains a non-null value.
    pub fn has_any_non_null_value(&self) -> bool {
        match self {
            Self::F64(enc, _) => enc.has_any_non_null_value(),
        }
    }

    /// Determines if the column contains a non-null value at one or more of the
    /// provided ordinal offsets.
    pub fn has_non_null_value(&self, row_ids: &[u32]) -> bool {
        match self {
            Self::F64(enc, _) => enc.has_non_null_value(row_ids),
        }
    }

    /// Returns the logical value found at the provided ordinal offset.
    pub fn value(&self, row_id: u32) -> Value<'_> {
        match self {
            Self::F64(enc, _) => match enc.value(row_id) {
                Some(v) => Value::Scalar(Scalar::F64(v)),
                None => Value::Null,
            },
        }
    }

    /// Returns the logical values found at the provided ordinal offsets.
    pub fn values(&self, row_ids: &[u32]) -> Values<'_> {
        match self {
            Self::F64(enc, _) => match enc.values(row_ids) {
                either::Either::Left(values) => Values::F64(values),
                either::Either::Right(values) => Values::F64N(values),
            },
        }
    }

    /// Returns all logical values in the column.
    pub fn all_values(&self) -> Values<'_> {
        match self {
            Self::F64(enc, _) => match enc.all_values() {
                either::Either::Left(values) => Values::F64(values),
                either::Either::Right(values) => Values::F64N(values),
            },
        }
    }

    /// Returns the ordinal offsets that satisfy the provided predicate.
    ///
    /// Note: it is the caller's responsibility to ensure that the provided
    /// `Scalar` value will fit within the physical type of the encoded column.
    /// `row_ids_filter` will panic if this invariant is broken.
    pub fn row_ids_filter(&self, op: &cmp::Operator, value: &Scalar, dst: RowIDs) -> RowIDs {
        match self {
            Self::F64(enc, _) => enc.row_ids_filter(value.as_f64(), op, dst),
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
        match self {
            Self::F64(enc, _) => {
                let left = (low.1.as_f64(), low.0);
                let right = (high.1.as_f64(), high.0);
                enc.row_ids_filter_range(left, right, dst)
            }
        }
    }

    pub fn min(&self, row_ids: &[u32]) -> Value<'_> {
        match self {
            Self::F64(enc, _) => match enc.min(row_ids) {
                Some(min) => Value::Scalar(Scalar::F64(min)),
                None => Value::Null,
            },
        }
    }

    pub fn max(&self, row_ids: &[u32]) -> Value<'_> {
        match self {
            Self::F64(enc, _) => match enc.max(row_ids) {
                Some(max) => Value::Scalar(Scalar::F64(max)),
                None => Value::Null,
            },
        }
    }

    pub fn sum(&self, row_ids: &[u32]) -> Scalar {
        match self {
            Self::F64(enc, _) => match enc.sum(row_ids) {
                Some(sum) => Scalar::F64(sum),
                None => Scalar::Null,
            },
        }
    }

    pub fn count(&self, row_ids: &[u32]) -> u32 {
        match self {
            Self::F64(enc, _) => enc.count(row_ids),
        }
    }

    /// The name of this encoding.
    pub fn name(&self) -> String {
        match self {
            Self::F64(_, name) => name.clone(),
        }
    }

    /// The logical datatype of this encoding.
    pub fn logical_datatype(&self) -> &'static str {
        match self {
            Self::F64(_, _) => "f64",
        }
    }
}

impl std::fmt::Display for FloatEncoding {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::F64(enc, _) => write!(f, "[Float]: {}", enc),
        }
    }
}

/// A lever to decide the minimum size in bytes that RLE the column needs to
/// reduce the overall footprint by. 0.1 means that the size of the column must
/// be reduced by 10%
pub const MIN_RLE_SIZE_REDUCTION: f64 = 0.1; // 10%

#[allow(clippy::float_cmp)]
fn is_natural_number(v: f64) -> bool {
    // if `v` can be round tripped to an `i64` and back to an `f64` without loss
    // of precision then we can _potentially_ safely store it in 32-bits or
    // less.
    v == (v as i64) as f64
}

/// Converts a slice of `f64` values into a `FloatEncoding`.
///
/// There are four possible encodings for &[f64]:
///
///    * "FIXED":      Effectively store the slice in a vector as f64.
///    * "BT_X-FIXED": Store floats as integers and trim them to a smaller
///                    physical size (X).
///    * "RLE":        If the data has sufficiently low cardinality they may
///                    benefit from being run-length encoded.
///    * "BT_X-RLE":   Convert to byte trimmed integers and then also RLE.
///
/// The encoding is chosen based on the heuristics in the `From` implementation
impl From<&[f64]> for FloatEncoding {
    fn from(arr: &[f64]) -> Self {
        // Are:
        //   * all the values natural numbers?
        //   * all the values able to be represented in 32-bits or less?
        //
        // Yes to the above means we can convert the data to integers and then
        // trim them, potentially applying RLE afterwards.
        let mut min = arr[0];
        let mut max = arr[0];
        let all_z = arr.iter().all(|&v| {
            min = min.min(v);
            max = max.max(v);
            is_natural_number(v)
        });

        // check they are all natural numbers that can be stored in 32 bits or
        // less.
        if all_z
            && ((min >= 0.0 && max <= u32::MAX as f64)
                || (min >= i32::MIN as f64 && max <= i32::MAX as f64))
        {
            return from_slice_with_byte_trimming(arr, (min, max));
        }

        // Store as f64, potentially with RLE.

        // Check if we gain space savings by encoding as RLE.
        if should_rle_from_iter(arr.len(), arr.iter().map(Some)) {
            let enc = Box::new(RLE::new_from_iter(
                arr.iter().cloned(),
                NoOpTranscoder {}, // No transcoding of values (store as physical type f64)
            ));
            let name = enc.name();
            return Self::F64(enc, name.to_string());
        }

        // Don't apply a compression encoding to the column
        let enc = Box::new(Fixed::<f64, f64, _>::new(arr.to_vec(), NoOpTranscoder {}));
        let name = enc.name();
        Self::F64(enc, name.to_owned())
    }
}

// Applies a heuristic to decide whether the input data should be encoded using
// run-length encoding.
fn should_rle_from_iter<T: PartialOrd>(len: usize, iter: impl Iterator<Item = Option<T>>) -> bool {
    let base_size = len * size_of::<T>();
    (base_size as f64 - rle::estimate_rle_size(iter) as f64) / base_size as f64
        >= MIN_RLE_SIZE_REDUCTION
}

// Convert float data to byte-trimmed integers and potentially RLE. It is the
// caller's responsibility to ensure all data are natural numbers that can be
// round tripped from float to int and back without loss of precision.
fn from_slice_with_byte_trimming(arr: &[f64], range: (f64, f64)) -> FloatEncoding {
    // determine min and max values.
    let min = range.0;
    let max = range.1;

    // If true then use RLE after byte trimming.
    let rle = should_rle_from_iter(arr.len(), arr.iter().map(Some));

    // This match is carefully ordered. It prioritises smaller physical
    // datatypes that can correctly represent the provided logical data.
    let transcoder = FloatByteTrimmer {};
    let transcoder_name = format!("{}", &transcoder);
    let (enc, name) = match (min, max) {
        // encode as u8 values
        (min, max) if min >= 0.0 && max <= u8::MAX as f64 => {
            let arr = arr
                .iter()
                .map::<u8, _>(|v| transcoder.encode(*v))
                .collect::<Vec<_>>();
            let enc: Box<dyn ScalarEncoding<f64>> = if rle {
                Box::new(RLE::new_from_iter(arr.into_iter(), transcoder))
            } else {
                Box::new(Fixed::new(arr, transcoder))
            };
            let name = enc.name();
            (enc, format!("{}_U8-{}", transcoder_name, name))
        }
        // encode as i8 values
        (min, max) if min >= i8::MIN as f64 && max <= i8::MAX as f64 => {
            let arr = arr
                .iter()
                .map(|v| transcoder.encode(*v))
                .collect::<Vec<i8>>();
            let enc: Box<dyn ScalarEncoding<f64>> = if rle {
                Box::new(RLE::new_from_iter(arr.into_iter(), transcoder))
            } else {
                Box::new(Fixed::new(arr, transcoder))
            };
            let name = enc.name();
            (enc, format!("{}_I8-{}", transcoder_name, name))
        }
        // encode as u16 values
        (min, max) if min >= 0.0 && max <= u16::MAX as f64 => {
            let arr = arr
                .iter()
                .map::<u16, _>(|v| transcoder.encode(*v))
                .collect::<Vec<u16>>();
            let enc: Box<dyn ScalarEncoding<f64>> = if rle {
                Box::new(RLE::new_from_iter(arr.into_iter(), transcoder))
            } else {
                Box::new(Fixed::new(arr, transcoder))
            };
            let name = enc.name();
            (enc, format!("{}_U16-{}", transcoder_name, name))
        }
        // encode as i16 values
        (min, max) if min >= i16::MIN as f64 && max <= i16::MAX as f64 => {
            let arr = arr
                .iter()
                .map(|v| transcoder.encode(*v))
                .collect::<Vec<i16>>();
            let enc: Box<dyn ScalarEncoding<f64>> = if rle {
                Box::new(RLE::new_from_iter(arr.into_iter(), transcoder))
            } else {
                Box::new(Fixed::new(arr, transcoder))
            };
            let name = enc.name();
            (enc, format!("{}_I16-{}", transcoder_name, name))
        }
        // encode as u32 values
        (min, max) if min >= 0.0 && max <= u32::MAX as f64 => {
            let arr = arr
                .iter()
                .map(|v| transcoder.encode(*v))
                .collect::<Vec<u32>>();
            let enc: Box<dyn ScalarEncoding<f64>> = if rle {
                Box::new(RLE::new_from_iter(arr.into_iter(), transcoder))
            } else {
                Box::new(Fixed::new(arr, transcoder))
            };
            let name = enc.name();
            (enc, format!("{}_U32-{}", transcoder_name, name))
        }
        // encode as i32 values
        (min, max) if min >= i32::MIN as f64 && max <= i32::MAX as f64 => {
            let arr = arr
                .iter()
                .map(|v| transcoder.encode(*v))
                .collect::<Vec<i32>>();
            let enc: Box<dyn ScalarEncoding<f64>> = if rle {
                Box::new(RLE::new_from_iter(arr.into_iter(), transcoder))
            } else {
                Box::new(Fixed::new(arr, transcoder))
            };
            let name = enc.name();
            (enc, format!("{}_I32-{}", transcoder_name, name))
        }
        (_, _) => unreachable!(
            "float column not byte trimmable: range [{:?}, {:?}]",
            min, max
        ),
    };
    FloatEncoding::F64(enc, name)
}

/// Converts an Arrow `Float64Array` into a `FloatEncoding`.
///
/// There are four possible encodings for `Float64Array`:
///
///    * "FIXEDN":      Effectively store in the `Float64Array`.
///    * "BT_X-FIXEDN": Store floats as integers and trim them to a smaller
///                     physical size (X). Backed by Arrow array.
///    * "RLE":         If the data has sufficiently low cardinality they may
///                     benefit from being run-length encoded.
///    * "BT_X-RLE":    Convert to byte trimmed integers and then also RLE.
///
/// The encoding is chosen based on the heuristics in the `From` implementation
impl From<Float64Array> for FloatEncoding {
    fn from(arr: Float64Array) -> Self {
        if arr.null_count() == 0 {
            return Self::from(arr.values());
        }

        // Are:
        //   * all the values natural numbers?
        //   * all the values able to be represented in 32-bits or less?
        //
        // Yes to the above means we can convert the data to integers and then
        // trim them, potentially applying RLE afterwards.
        let min = arrow::compute::kernels::aggregate::min(&arr);
        let max = arrow::compute::kernels::aggregate::max(&arr);
        let all_z = arr.iter().all(|v| match v {
            Some(v) => is_natural_number(v),
            None => true,
        });

        // Column is all NULL - encode as RLE u8
        if min.is_none() {
            let arr: PrimitiveArray<UInt8Type> =
                PrimitiveArray::from_iter(arr.iter().map::<Option<u8>, _>(|_| None));
            let enc: Box<dyn ScalarEncoding<f64>> =
                Box::new(RLE::new_from_iter_opt(arr.iter(), FloatByteTrimmer {}));
            let name = enc.name();
            return Self::F64(enc, name.to_owned());
        }

        let min = min.unwrap();
        let max = max.unwrap();
        // check they are all natural numbers that can be stored in 32 bits or
        // less.
        if all_z
            && ((min >= 0.0 && max <= u32::MAX as f64)
                || (min >= i32::MIN as f64 && max <= i32::MAX as f64))
        {
            return from_array_with_byte_trimming(arr, (min, max));
        }

        // Store as f64, potentially with RLE.

        // The number of rows we would reduce the column by if we encoded it
        // as RLE.
        if should_rle_from_iter(arr.len(), arr.iter()) {
            let enc = Box::new(RLE::new_from_iter_opt(
                arr.iter(),
                NoOpTranscoder {}, // No transcoding of values (store as physical type f64)
            ));
            let name = enc.name();
            return Self::F64(enc, name.to_owned());
        }

        // Just store as nullable vector.
        let enc = Box::new(FixedNull::<Float64Type, f64, _>::new(
            arr,
            NoOpTranscoder {},
        ));
        let name = enc.name();
        Self::F64(enc, name.to_owned())
    }
}

// Convert float data to byte-trimmed integers and potentially RLE. It is the
// caller's responsibility to ensure all data are natural numbers that can be
// round tripped from float to int and back without loss of precision.
fn from_array_with_byte_trimming(arr: Float64Array, range: (f64, f64)) -> FloatEncoding {
    // determine min and max values.
    let min = range.0;
    let max = range.1;

    // If true then use RLE after byte trimming.
    let rle = should_rle_from_iter(arr.len(), arr.iter());

    // This match is carefully ordered. It prioritises smaller physical
    // datatypes that can correctly represent the provided logical data.
    let transcoder = FloatByteTrimmer {};
    let transcoder_name = format!("{}", &transcoder);
    let (enc, name) = match (min, max) {
        // encode as u8 values
        (min, max) if min >= 0.0 && max <= u8::MAX as f64 => {
            let arr: PrimitiveArray<UInt8Type> =
                PrimitiveArray::from_iter(arr.into_iter().map(|v| v.map(|v| transcoder.encode(v))));
            let enc: Box<dyn ScalarEncoding<f64>> = if rle {
                Box::new(RLE::new_from_iter_opt(arr.iter(), transcoder))
            } else {
                Box::new(FixedNull::new(arr, transcoder))
            };
            let name = enc.name();
            (enc, format!("{}_U8-{}", transcoder_name, name))
        }
        // encode as i8 values
        (min, max) if min >= i8::MIN as f64 && max <= i8::MAX as f64 => {
            let arr: PrimitiveArray<Int8Type> =
                PrimitiveArray::from_iter(arr.into_iter().map(|v| v.map(|v| transcoder.encode(v))));
            let enc: Box<dyn ScalarEncoding<f64>> = if rle {
                Box::new(RLE::new_from_iter_opt(arr.iter(), transcoder))
            } else {
                Box::new(FixedNull::new(arr, transcoder))
            };
            let name = enc.name();
            (enc, format!("{}_I8-{}", transcoder_name, name))
        }
        // encode as u16 values
        (min, max) if min >= 0.0 && max <= u16::MAX as f64 => {
            let arr: PrimitiveArray<UInt16Type> =
                PrimitiveArray::from_iter(arr.into_iter().map(|v| v.map(|v| transcoder.encode(v))));
            let enc: Box<dyn ScalarEncoding<f64>> = if rle {
                Box::new(RLE::new_from_iter_opt(arr.iter(), transcoder))
            } else {
                Box::new(FixedNull::new(arr, transcoder))
            };
            let name = enc.name();
            (enc, format!("{}_U16-{}", transcoder_name, name))
        }
        // encode as i16 values
        (min, max) if min >= i16::MIN as f64 && max <= i16::MAX as f64 => {
            let arr: PrimitiveArray<Int16Type> =
                PrimitiveArray::from_iter(arr.into_iter().map(|v| v.map(|v| transcoder.encode(v))));
            let enc: Box<dyn ScalarEncoding<f64>> = if rle {
                Box::new(RLE::new_from_iter_opt(arr.iter(), transcoder))
            } else {
                Box::new(FixedNull::new(arr, transcoder))
            };
            let name = enc.name();
            (enc, format!("{}_I16-{}", transcoder_name, name))
        }
        // encode as u32 values
        (min, max) if min >= 0.0 && max <= u32::MAX as f64 => {
            let arr: PrimitiveArray<UInt32Type> =
                PrimitiveArray::from_iter(arr.into_iter().map(|v| v.map(|v| transcoder.encode(v))));
            let enc: Box<dyn ScalarEncoding<f64>> = if rle {
                Box::new(RLE::new_from_iter_opt(arr.iter(), transcoder))
            } else {
                Box::new(FixedNull::new(arr, transcoder))
            };
            let name = enc.name();
            (enc, format!("{}_U32-{}", transcoder_name, name))
        }
        // encode as i32 values
        (min, max) if min >= i32::MIN as f64 && max <= i32::MAX as f64 => {
            let arr: PrimitiveArray<Int32Type> =
                PrimitiveArray::from_iter(arr.into_iter().map(|v| v.map(|v| transcoder.encode(v))));
            let enc: Box<dyn ScalarEncoding<f64>> = if rle {
                Box::new(RLE::new_from_iter_opt(arr.iter(), transcoder))
            } else {
                Box::new(FixedNull::new(arr, transcoder))
            };
            let name = enc.name();
            (enc, format!("{}_I32-{}", transcoder_name, name))
        }
        (_, _) => unreachable!(
            "float column not byte trimmable: range [{:?}, {:?}]",
            min, max
        ),
    };
    FloatEncoding::F64(enc, name)
}

#[cfg(test)]
mod test {
    use arrow::array::{Float64Array, PrimitiveArray};

    use super::*;
    use cmp::Operator;

    #[test]
    // Tests that input data with natural numbers gets byte trimmed correctly.
    fn from_slice_f64() {
        let cases = vec![
            (vec![0.0, 2.0, 245.0, 3.0], "FBT_U8-FIXED"), // u8 fixed array
            (vec![0.0, -120.0, 127.0, 3.0], "FBT_I8-FIXED"), // i8 fixed array
            (vec![399.0, 2.0, 2452.0, 3.0], "FBT_U16-FIXED"), // u16 fixed array
            (vec![-399.0, 2.0, 2452.0, 3.0], "FBT_I16-FIXED"), // i16 fixed array
            (vec![u32::MAX as f64, 2.0, 245.0, 3.0], "FBT_U32-FIXED"), // u32 fixed array
            (vec![u32::MAX as f64, 0.0], "FBT_U32-FIXED"), // u32 fixed array
            (vec![i32::MIN as f64, i32::MAX as f64], "FBT_I32-FIXED"), // i32 fixed array
            (vec![i32::MIN as f64, 2.0, 245.0, 3.0], "FBT_I32-FIXED"), // i32 fixed array
            (vec![u32::MAX as f64 + 1.0, 2.0, 245.0, 3.0], "FIXED"), // f64 fixed array
            (vec![u32::MAX as f64, -1.0], "FIXED"),       // can't byte trim due to range
            (vec![i32::MAX as f64 + 1.0, -1.0], "FIXED"), // can't byte trim due to range
            (vec![i32::MIN as f64 - 1.0, -1.0], "FIXED"), // can't byte trim due to range
        ];

        for (case, name) in cases.into_iter() {
            let enc = FloatEncoding::from(case.as_slice());
            assert_eq!(enc.name(), name, "failed: {:?}", enc);
        }

        // Verify RLE conversion
        let input = vec![1_f64; 1000]
            .into_iter()
            .chain(vec![2_f64; 1000]) // 1,1,1,1,1,2,2,2,2,2,2....
            .collect::<Vec<f64>>();
        let enc = FloatEncoding::from(input.as_slice());
        assert_eq!(enc.name(), "FBT_U8-RLE", "failed: {:?}", enc);

        let input = vec![f64::MAX; 1000]
            .into_iter()
            .chain(vec![f64::MIN; 1000])
            .collect::<Vec<f64>>();
        let enc = FloatEncoding::from(input.as_slice());
        assert_eq!(enc.name(), "RLE", "failed: {:?}", enc);
    }

    #[test]
    fn from_arrow_array() {
        // Verify that implementation falls back to "fixed" encodings when there
        // are no NULL values present.
        let arr = Float64Array::from(vec![0.0, 2.0, 245.0, 3.0]);
        let enc = FloatEncoding::from(arr);
        assert_eq!(enc.name(), "FBT_U8-FIXED", "failed: {:?}", enc);

        // Verify that byte trimming works on Arrow arrays.
        let cases = vec![
            (vec![Some(0.0), Some(2.0), None], "FBT_U8-FIXEDN"), // u8 fixed array
            (vec![Some(0.0), Some(-120.0), None], "FBT_I8-FIXEDN"), // i8 fixed array
            (vec![Some(399.0), None, Some(2452.0)], "FBT_U16-FIXEDN"), // u16 fixed array
            (vec![Some(-399.0), Some(2452.0), None], "FBT_I16-FIXEDN"), // i16 fixed array
            (vec![Some(u32::MAX as f64), None], "FBT_U32-FIXEDN"), // u32 fixed array
            // i32 fixed array
            (
                vec![Some(i32::MIN as f64), None, Some(i32::MAX as f64)],
                "FBT_I32-FIXEDN",
            ),
            // i32 fixed array
            (
                vec![Some(i32::MIN as f64), Some(2.0), None],
                "FBT_I32-FIXEDN",
            ),
            (vec![Some(u32::MAX as f64 + 1.0), Some(2.0), None], "FIXEDN"), // f64 fixed array
            // can't byte trim due to range
            (
                vec![Some(u32::MAX as f64 - 1.0), Some(-1.0), None],
                "FIXEDN",
            ),
            // can't byte trim due to range
            (
                vec![Some(i32::MAX as f64 + 1.0), None, Some(-1.0)],
                "FIXEDN",
            ),
            // can't byte trim due to range
            (
                vec![Some(i32::MIN as f64 - 1.0), None, Some(-1.0)],
                "FIXEDN",
            ),
        ];

        for (case, name) in cases.into_iter() {
            let arr = Float64Array::from(case);
            let enc = FloatEncoding::from(arr);
            assert_eq!(enc.name(), name, "failed: {:?}", enc);
        }

        // Verify RLE conversion
        let input = vec![1_f64; 1000]
            .into_iter()
            .chain(vec![2_f64; 1000]) // 1,1,1,1,1,2,2,2,2,2,2....
            .collect::<Vec<f64>>();
        let arr = Float64Array::from(input);
        let enc = FloatEncoding::from(arr);
        assert_eq!(enc.name(), "FBT_U8-RLE", "failed: {:?}", enc);

        let input = vec![f64::MAX; 1000]
            .into_iter()
            .chain(vec![f64::MIN; 1000])
            .collect::<Vec<f64>>();
        let arr = Float64Array::from(input);
        let enc = FloatEncoding::from(arr);
        assert_eq!(enc.name(), "RLE", "failed: {:?}", enc);
    }

    #[test]
    // Test NaN behaviour when `FloatEncoder`s are used.
    //
    // TODO(edd): I need to add the correct comparators to the scalar encodings
    //            so that they behave the same as PG
    //
    fn row_ids_filter_nan() {
        let data = vec![22.3, f64::NAN, f64::NEG_INFINITY, f64::INFINITY];

        let cases: Vec<Box<dyn ScalarEncoding<f64>>> = vec![
            Box::new(RLE::<_, _, _>::new_from_iter(
                data.iter().cloned(),
                NoOpTranscoder {}, // No transcoding of values (store as physical type f64)
            )),
            Box::new(FixedNull::<Float64Type, f64, _>::new(
                PrimitiveArray::from(data.clone()),
                NoOpTranscoder {},
            )),
            Box::new(Fixed::<f64, f64, _>::new(data, NoOpTranscoder {})),
        ];

        for enc in cases {
            _row_ids_filter_nan(enc)
        }
    }

    fn _row_ids_filter_nan(_enc: Box<dyn ScalarEncoding<f64>>) {
        // These cases replicate the behaviour in PG.
        let cases = vec![
            (2.0, Operator::Equal, vec![0]),                        // 22.3
            (2.0, Operator::NotEqual, vec![0, 1, 2, 3]),            // 22.3, NaN, -∞, ∞
            (2.0, Operator::LT, vec![2]),                           // -∞
            (2.0, Operator::LTE, vec![2]),                          // -∞
            (2.0, Operator::GT, vec![0, 1, 3]),                     // 22.3, NaN, ∞
            (2.0, Operator::GTE, vec![0, 1, 3]),                    // 22.3, NaN, ∞
            (f64::NAN, Operator::Equal, vec![1]),                   // NaN
            (f64::NAN, Operator::NotEqual, vec![0, 2, 3]),          // 22.3, -∞, ∞
            (f64::NAN, Operator::LT, vec![0, 2, 3]),                // 22.3, -∞, ∞
            (f64::NAN, Operator::LTE, vec![0, 1, 2, 3]),            // 22.3, NaN, -∞, ∞
            (f64::NAN, Operator::GT, vec![]),                       //
            (f64::NAN, Operator::GTE, vec![1]),                     // NaN
            (f64::INFINITY, Operator::Equal, vec![3]),              // ∞
            (f64::INFINITY, Operator::NotEqual, vec![0, 1, 2]),     // 22.3, NaN, -∞
            (f64::INFINITY, Operator::LT, vec![0, 2]),              // 22.3, -∞
            (f64::INFINITY, Operator::LTE, vec![0, 2, 3]),          // 22.3, -∞, ∞
            (f64::INFINITY, Operator::GT, vec![1]),                 // NaN
            (f64::INFINITY, Operator::GTE, vec![1, 3]),             // NaN, ∞
            (f64::NEG_INFINITY, Operator::Equal, vec![2]),          // -∞
            (f64::NEG_INFINITY, Operator::NotEqual, vec![0, 1, 3]), // 22.3, NaN, ∞
            (f64::NEG_INFINITY, Operator::LT, vec![]),              //
            (f64::NEG_INFINITY, Operator::LTE, vec![2]),            // -∞
            (f64::NEG_INFINITY, Operator::GT, vec![0, 1, 3]),       // 22.3, NaN, ∞
            (f64::NEG_INFINITY, Operator::GTE, vec![0, 1, 2, 3]),   // 22.3, NaN, -∞, ∞
        ];

        // TODO(edd): I need to add support for PG-like comparators for NaN etc.
        for (_v, _op, _exp) in cases {
            //let dst = enc.row_ids_filter(v, &op, RowIDs::new_vector());
            //assert_eq!(dst.unwrap_vector(), &exp, "example '{} {:?}' failed", op, v);
        }
    }
}
