use arrow::array::Array;
use arrow::datatypes::Float64Type;
use std::{cmp::Ordering, mem::size_of};

use super::encoding::scalar::transcoders::NoOpTranscoder;
use super::encoding::scalar::ScalarEncoding;
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
    F64(Box<dyn ScalarEncoding<f64>>),
}

impl FloatEncoding {
    /// The total size in bytes of to store columnar data in memory.
    pub fn size(&self) -> usize {
        match self {
            Self::F64(enc) => enc.size(),
        }
    }

    /// The estimated total size in bytes of the underlying float values in the
    /// column if they were stored contiguously and uncompressed. `include_nulls`
    /// will effectively size each NULL value as 8b if `true`.
    pub fn size_raw(&self, include_nulls: bool) -> usize {
        match self {
            Self::F64(enc) => enc.size_raw(include_nulls),
        }
    }

    /// The total number of rows in the column.
    pub fn num_rows(&self) -> u32 {
        match self {
            Self::F64(enc) => enc.num_rows(),
        }
    }

    // Returns statistics about the physical layout of columns
    pub(crate) fn storage_stats(&self) -> Statistics {
        Statistics {
            enc_type: self.name().to_string(),
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
            Self::F64(enc) => enc.null_count(),
        }
    }

    /// Determines if the column contains a non-null value.
    pub fn has_any_non_null_value(&self) -> bool {
        match self {
            Self::F64(enc) => enc.has_any_non_null_value(),
        }
    }

    /// Determines if the column contains a non-null value at one or more of the
    /// provided ordinal offsets.
    pub fn has_non_null_value(&self, row_ids: &[u32]) -> bool {
        match self {
            Self::F64(enc) => enc.has_non_null_value(row_ids),
        }
    }

    /// Returns the logical value found at the provided ordinal offset.
    pub fn value(&self, row_id: u32) -> Value<'_> {
        match self {
            Self::F64(enc) => match enc.value(row_id) {
                Some(v) => Value::Scalar(Scalar::F64(v)),
                None => Value::Null,
            },
        }
    }

    /// Returns the logical values found at the provided ordinal offsets.
    pub fn values(&self, row_ids: &[u32]) -> Values<'_> {
        match self {
            Self::F64(enc) => match enc.values(row_ids) {
                either::Either::Left(values) => Values::F64(values),
                either::Either::Right(values) => Values::F64N(values),
            },
        }
    }

    /// Returns all logical values in the column.
    pub fn all_values(&self) -> Values<'_> {
        match self {
            Self::F64(enc) => match enc.all_values() {
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
            Self::F64(enc) => enc.row_ids_filter(value.as_f64(), op, dst),
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
            Self::F64(enc) => {
                let left = (low.1.as_f64(), low.0);
                let right = (high.1.as_f64(), high.0);
                enc.row_ids_filter_range(left, right, dst)
            }
        }
    }

    pub fn min(&self, row_ids: &[u32]) -> Value<'_> {
        match self {
            Self::F64(enc) => match enc.min(row_ids) {
                Some(min) => Value::Scalar(Scalar::F64(min)),
                None => Value::Null,
            },
        }
    }

    pub fn max(&self, row_ids: &[u32]) -> Value<'_> {
        match self {
            Self::F64(enc) => match enc.max(row_ids) {
                Some(max) => Value::Scalar(Scalar::F64(max)),
                None => Value::Null,
            },
        }
    }

    pub fn sum(&self, row_ids: &[u32]) -> Scalar {
        match self {
            Self::F64(enc) => match enc.sum(row_ids) {
                Some(sum) => Scalar::F64(sum),
                None => Scalar::Null,
            },
        }
    }

    pub fn count(&self, row_ids: &[u32]) -> u32 {
        match self {
            Self::F64(enc) => enc.count(row_ids),
        }
    }

    /// The name of this encoding.
    pub fn name(&self) -> &'static str {
        match self {
            Self::F64(enc) => enc.name(),
        }
    }

    /// The logical datatype of this encoding.
    pub fn logical_datatype(&self) -> &'static str {
        match self {
            Self::F64(_) => "f64",
        }
    }
}

impl std::fmt::Display for FloatEncoding {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::F64(enc) => write!(f, "[Float]: {}", enc),
        }
    }
}

// helper to determine how many rows the slice would have if it were RLE
// encoded.
fn rle_rows(arr: &[f64]) -> usize {
    arr.len()
        - arr
            .iter()
            .zip(arr.iter().skip(1))
            .filter(|(curr, next)| matches!(curr.partial_cmp(next), Some(Ordering::Equal)))
            .count()
}

fn rle_rows_opt(mut itr: impl Iterator<Item = Option<f64>>) -> usize {
    let mut v = match itr.next() {
        Some(v) => v,
        None => return 0,
    };

    let mut total_rows = 0;
    for next in itr {
        if let Some(Ordering::Equal) = v.partial_cmp(&next) {
            continue;
        }

        total_rows += 1;
        v = next;
    }

    total_rows + 1 // account for original run
}

/// A lever to decide the minimum size in bytes that RLE the column needs to
/// reduce the overall footprint by. 0.1 means that the size of the column must
/// be reduced by 10%
pub const MIN_RLE_SIZE_REDUCTION: f64 = 0.1; // 10%

/// Converts a slice of `f64` values into a `FloatEncoding`.
///
/// There are two possible encodings for &[f64]:
///    * "None": effectively store the slice in a vector;
///    * "RLE": for slices that have a sufficiently low cardinality they may
///             benefit from being run-length encoded.
///
/// The encoding is chosen based on the heuristics in the `From` implementation
impl From<&[f64]> for FloatEncoding {
    fn from(arr: &[f64]) -> Self {
        // The number of rows we would reduce the column by if we encoded it
        // as RLE.
        let base_size = arr.len() * size_of::<f64>();
        let rle_size = rle_rows(arr) * size_of::<(u32, Option<f64>)>(); // size of a run length
        if (base_size as f64 - rle_size as f64) / base_size as f64 >= MIN_RLE_SIZE_REDUCTION {
            let enc = Box::new(RLE::new_from_iter(
                arr.iter().cloned(),
                NoOpTranscoder {}, // No transcoding of values (store as physical type f64)
            ));
            return Self::F64(enc);
        }

        // Don't apply a compression encoding to the column
        let enc = Box::new(Fixed::<f64, f64, _>::new(arr.to_vec(), NoOpTranscoder {}));
        Self::F64(enc)
    }
}

/// Converts an Arrow Float array into a `FloatEncoding`.
///
/// There are two possible encodings for an Arrow array:
///    * "None": effectively keep the data in its Arrow array;
///    * "RLE": for arrays that have a sufficiently large number of NULL values
///             they may benefit from being run-length encoded.
///
/// The encoding is chosen based on the heuristics in the `From` implementation
impl From<arrow::array::Float64Array> for FloatEncoding {
    fn from(arr: arrow::array::Float64Array) -> Self {
        if arr.null_count() == 0 {
            return Self::from(arr.values());
        }

        // The number of rows we would reduce the column by if we encoded it
        // as RLE.
        let base_size = arr.len() * size_of::<f64>();
        let rle_size = rle_rows_opt(arr.iter()) * size_of::<(u32, Option<f64>)>(); // size of a run length
        if (base_size as f64 - rle_size as f64) / base_size as f64 >= MIN_RLE_SIZE_REDUCTION {
            let enc = Box::new(RLE::new_from_iter_opt(
                arr.iter(),
                NoOpTranscoder {}, // No transcoding of values (store as physical type f64)
            ));
            return Self::F64(enc);
        }

        // Just store as nullable vector.
        let enc = Box::new(FixedNull::<Float64Type, f64, _>::new(
            arr,
            NoOpTranscoder {},
        ));
        Self::F64(enc)
    }
}

#[cfg(test)]
mod test {
    use std::iter;

    use arrow::array::{Float64Array, PrimitiveArray};

    use super::*;
    use crate::column::encoding::scalar::{fixed, fixed_null, rle};
    use cmp::Operator;

    #[test]
    fn rle_rows() {
        let cases = vec![
            (vec![1.0, 2.0, 3.0, 4.0, 5.0, 6.0, 0.0, 0.0, 0.0, 0.0], 7),
            (vec![0.0, 0.0, 1.0, 2.0, 3.0, 4.0, 5.0, 6.0, 7.0, 8.0], 9),
            (vec![0.0, 0.0], 1),
            (vec![1.0, 2.0, 1.0], 3),
            (vec![1.0, 2.0, 1.0, 1.0], 3),
            (vec![1.0], 1),
        ];

        for (input, exp) in cases {
            assert_eq!(super::rle_rows(input.as_slice()), exp);
        }
    }

    #[test]
    fn rle_rows_opt() {
        let cases = vec![
            (vec![Some(0.0), Some(2.0), Some(1.0)], 3),
            (vec![Some(0.0), Some(0.0)], 1),
        ];

        for (input, exp) in cases {
            assert_eq!(super::rle_rows_opt(input.into_iter()), exp);
        }
    }

    #[test]
    fn from_arrow_array() {
        // Rows not reduced
        let input: Vec<Option<f64>> = vec![Some(33.2), Some(1.2), Some(2.2), None, Some(3.2)];
        let arr = Float64Array::from(input);
        let enc = FloatEncoding::from(arr);
        assert_eq!(enc.name(), fixed_null::ENCODING_NAME);

        // Rows not reduced and no nulls so can go in `Fixed64`.
        let input: Vec<Option<f64>> = vec![Some(33.2), Some(1.2), Some(2.2), Some(3.2)];
        let arr = Float64Array::from(input);
        let enc = FloatEncoding::from(arr);
        assert_eq!(enc.name(), fixed::ENCODING_NAME);

        // Goldilocks - encode as RLE
        let input: Vec<Option<f64>> = vec![Some(33.2); 10];
        let arr = Float64Array::from(input);
        let enc = FloatEncoding::from(arr);
        assert_eq!(enc.name(), rle::ENCODING_NAME);

        // Goldilocks - encode as RLE
        let mut input: Vec<Option<f64>> = vec![Some(33.2); 10];
        input.extend(iter::repeat(None).take(10));
        let arr = Float64Array::from(input);
        let enc = FloatEncoding::from(arr);
        assert_eq!(enc.name(), rle::ENCODING_NAME);
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
