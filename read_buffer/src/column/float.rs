use std::{cmp::Ordering, mem::size_of};

use arrow::{self, array::Array};

use super::encoding::{
    scalar::Fixed,
    scalar::{rle::RLE, FixedNull},
};
use super::{cmp, Statistics};
use crate::column::{RowIDs, Scalar, Value, Values};

#[allow(clippy::upper_case_acronyms)] // TODO(edd): these will be OK in 1.52
#[derive(Debug)]
pub enum FloatEncoding {
    // A fixed-width "no compression" vector of non-nullable values
    Fixed64(Fixed<f64>),

    // A fixed-width "no compression" vector of nullable values (as Arrow array)
    FixedNull64(FixedNull<arrow::datatypes::Float64Type>),

    // A RLE compressed encoding of nullable values.
    RLE64(RLE<f64>),
}

impl FloatEncoding {
    /// The total size in bytes of to store columnar data in memory.
    pub fn size(&self) -> usize {
        match self {
            Self::Fixed64(enc) => enc.size(),
            Self::FixedNull64(enc) => enc.size(),
            Self::RLE64(enc) => enc.size(),
        }
    }

    /// The estimated total size in bytes of the underlying float values in the
    /// column if they were stored contiguously and uncompressed. `include_nulls`
    /// will effectively size each NULL value as 8b if `true`.
    pub fn size_raw(&self, include_nulls: bool) -> usize {
        match self {
            // this will be the size of a Vec<f64>
            Self::Fixed64(enc) => {
                size_of::<Vec<f64>>() + (enc.num_rows() as usize * size_of::<f64>())
            }
            Self::FixedNull64(enc) => enc.size_raw(include_nulls),
            Self::RLE64(enc) => enc.size_raw(include_nulls),
        }
    }

    /// The total number of rows in the column.
    pub fn num_rows(&self) -> u32 {
        match self {
            Self::Fixed64(enc) => enc.num_rows(),
            Self::FixedNull64(enc) => enc.num_rows(),
            Self::RLE64(enc) => enc.num_rows(),
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
        match self {
            Self::Fixed64(_) => false,
            Self::FixedNull64(enc) => enc.contains_null(),
            Self::RLE64(enc) => enc.contains_null(),
        }
    }

    /// The total number of rows in the column.
    pub fn null_count(&self) -> u32 {
        match self {
            Self::Fixed64(_) => 0,
            Self::FixedNull64(enc) => enc.null_count(),
            Self::RLE64(enc) => enc.null_count(),
        }
    }

    /// Determines if the column contains a non-null value.
    pub fn has_any_non_null_value(&self) -> bool {
        match self {
            Self::Fixed64(_) => true,
            Self::FixedNull64(enc) => enc.has_any_non_null_value(),
            Self::RLE64(enc) => enc.has_any_non_null_value(),
        }
    }

    /// Determines if the column contains a non-null value at one of the
    /// provided rows.
    pub fn has_non_null_value(&self, row_ids: &[u32]) -> bool {
        match self {
            Self::Fixed64(_) => !row_ids.is_empty(), // all rows will be non-null
            Self::FixedNull64(enc) => enc.has_non_null_value(row_ids),
            Self::RLE64(enc) => enc.has_non_null_value(row_ids),
        }
    }

    /// Returns the logical value found at the provided row id.
    pub fn value(&self, row_id: u32) -> Value<'_> {
        match &self {
            Self::Fixed64(c) => Value::Scalar(Scalar::F64(c.value(row_id))),
            Self::FixedNull64(c) => match c.value(row_id) {
                Some(v) => Value::Scalar(Scalar::F64(v)),
                None => Value::Null,
            },
            Self::RLE64(c) => match c.value(row_id) {
                Some(v) => Value::Scalar(Scalar::F64(v)),
                None => Value::Null,
            },
        }
    }

    /// Returns the logical values found at the provided row ids.
    ///
    /// TODO(edd): perf - pooling of destination vectors.
    pub fn values(&self, row_ids: &[u32]) -> Values<'_> {
        match &self {
            Self::Fixed64(c) => Values::F64(c.values::<f64>(row_ids, vec![])),
            Self::FixedNull64(c) => Values::F64N(c.values(row_ids, vec![])),
            Self::RLE64(c) => Values::F64N(c.values(row_ids, vec![])),
        }
    }

    /// Returns all logical values in the column.
    ///
    /// TODO(edd): perf - pooling of destination vectors.
    pub fn all_values(&self) -> Values<'_> {
        match &self {
            Self::Fixed64(c) => Values::F64(c.all_values::<f64>(vec![])),
            Self::FixedNull64(c) => Values::F64N(c.all_values(vec![])),
            Self::RLE64(c) => Values::F64N(c.all_values(vec![])),
        }
    }

    /// Returns the row ids that satisfy the provided predicate.
    ///
    /// Note: it is the caller's responsibility to ensure that the provided
    /// `Scalar` value will fit within the physical type of the encoded column.
    /// `row_ids_filter` will panic if this invariant is broken.
    pub fn row_ids_filter(&self, op: &cmp::Operator, value: &Scalar, dst: RowIDs) -> RowIDs {
        match &self {
            Self::Fixed64(c) => c.row_ids_filter(value.as_f64(), op, dst),
            Self::FixedNull64(c) => c.row_ids_filter(value.as_f64(), op, dst),
            Self::RLE64(c) => c.row_ids_filter(value.as_f64(), op, dst),
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
            Self::Fixed64(c) => {
                c.row_ids_filter_range((low.1.as_f64(), &low.0), (high.1.as_f64(), &high.0), dst)
            }
            Self::FixedNull64(_) => todo!(),
            Self::RLE64(c) => {
                c.row_ids_filter_range((low.1.as_f64(), &low.0), (high.1.as_f64(), &high.0), dst)
            }
        }
    }

    pub fn min(&self, row_ids: &[u32]) -> Value<'_> {
        match &self {
            Self::Fixed64(c) => Value::Scalar(Scalar::F64(c.min(row_ids))),
            Self::FixedNull64(c) => match c.min(row_ids) {
                Some(v) => Value::Scalar(Scalar::F64(v)),
                None => Value::Null,
            },
            Self::RLE64(c) => match c.min(row_ids) {
                Some(v) => Value::Scalar(Scalar::F64(v)),
                None => Value::Null,
            },
        }
    }

    pub fn max(&self, row_ids: &[u32]) -> Value<'_> {
        match &self {
            Self::Fixed64(c) => Value::Scalar(Scalar::F64(c.max(row_ids))),
            Self::FixedNull64(c) => match c.max(row_ids) {
                Some(v) => Value::Scalar(Scalar::F64(v)),
                None => Value::Null,
            },
            Self::RLE64(c) => match c.max(row_ids) {
                Some(v) => Value::Scalar(Scalar::F64(v)),
                None => Value::Null,
            },
        }
    }

    pub fn sum(&self, row_ids: &[u32]) -> Scalar {
        match &self {
            Self::Fixed64(c) => Scalar::F64(c.sum(row_ids)),
            Self::FixedNull64(c) => match c.sum(row_ids) {
                Some(v) => Scalar::F64(v),
                None => Scalar::Null,
            },
            Self::RLE64(c) => match c.sum(row_ids) {
                Some(v) => Scalar::F64(v),
                None => Scalar::Null,
            },
        }
    }

    pub fn count(&self, row_ids: &[u32]) -> u32 {
        match &self {
            Self::Fixed64(c) => c.count(row_ids),
            Self::FixedNull64(c) => c.count(row_ids),
            Self::RLE64(c) => c.count(row_ids),
        }
    }

    /// The name of this encoding.
    pub fn name(&self) -> &'static str {
        match &self {
            Self::Fixed64(_) => "None",
            Self::FixedNull64(_) => "None",
            Self::RLE64(enc) => enc.name(),
        }
    }

    /// The logical datatype of this encoding.
    pub fn logical_datatype(&self) -> &'static str {
        match &self {
            Self::Fixed64(_) => "f64",
            Self::FixedNull64(_) => "f64",
            Self::RLE64(_) => "f64",
        }
    }
}

impl std::fmt::Display for FloatEncoding {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let name = self.name();
        match self {
            Self::Fixed64(enc) => write!(f, "[{}]: {}", name, enc),
            Self::FixedNull64(enc) => write!(f, "[{}]: {}", name, enc),
            Self::RLE64(enc) => write!(f, "[{}]: {}", name, enc),
        }
    }
}

fn rle_rows(arr: &[f64]) -> usize {
    let mut v = arr[0];
    let mut total_rows = 0;
    for next in arr.iter().skip(1) {
        if let Some(Ordering::Equal) = v.partial_cmp(next) {
            continue;
        }

        total_rows += 1;
        v = *next;
    }

    total_rows + 1 // account for original run
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
pub const MIN_RLE_SIZE_REDUCTION: f64 = 0.3; // 30%

/// Converts a slice of `f64` values into a `FloatEncoding`.
///
/// TODO(edd): figure out what sensible heuristics look like.
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
            return Self::RLE64(RLE::from(arr));
        }

        // Don't apply a compression encoding to the column
        Self::Fixed64(Fixed::<f64>::from(arr))
    }
}

/// Converts an Arrow Float array into a `FloatEncoding`.
///
/// TODO(edd): figure out what sensible heuristics look like.
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
            return Self::RLE64(RLE::from(arr));
        }

        Self::FixedNull64(FixedNull::<arrow::datatypes::Float64Type>::from(arr))
    }
}

#[cfg(test)]
mod test {
    use std::iter;

    use super::*;
    use arrow::array::Float64Array;
    use cmp::Operator;

    #[test]
    fn size_raw() {
        let enc = FloatEncoding::from(&[2.2, 22.2, 12.2, 31.2][..]);
        // (4 * 8) + 24
        assert_eq!(enc.size_raw(true), 56);
        assert_eq!(enc.size_raw(false), 56);

        let enc = FloatEncoding::FixedNull64(FixedNull::<arrow::datatypes::Float64Type>::from(
            &[2.0, 2.02, 1.02, 3.01][..],
        ));
        // (4 * 8) + 24
        assert_eq!(enc.size_raw(true), 56);
        assert_eq!(enc.size_raw(false), 56);

        let enc = FloatEncoding::FixedNull64(FixedNull::<arrow::datatypes::Float64Type>::from(
            &[Some(2.0), Some(2.02), None, Some(1.02), Some(3.01)][..],
        ));
        // (5 * 8) + 24
        assert_eq!(enc.size_raw(true), 64);
        assert_eq!(enc.size_raw(false), 56);
    }

    fn rle_rows() {
        let cases = vec![
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
        assert!(matches!(enc, FloatEncoding::FixedNull64(_)));

        // Rows not reduced and no nulls so can go in `Fixed64`.
        let input: Vec<Option<f64>> = vec![Some(33.2), Some(1.2), Some(2.2), Some(3.2)];
        let arr = Float64Array::from(input);
        let enc = FloatEncoding::from(arr);
        assert!(matches!(enc, FloatEncoding::Fixed64(_)));

        // Goldilocks - encode as RLE
        let input: Vec<Option<f64>> = vec![Some(33.2); 10];
        let arr = Float64Array::from(input);
        let enc = FloatEncoding::from(arr);
        assert!(matches!(enc, FloatEncoding::RLE64(_)));

        // Goldilocks - encode as RLE
        let mut input: Vec<Option<f64>> = vec![Some(33.2); 10];
        input.extend(iter::repeat(None).take(10));
        let arr = Float64Array::from(input);
        let enc = FloatEncoding::from(arr);
        assert!(matches!(enc, FloatEncoding::RLE64(_)));
    }

    #[test]
    // Test NaN behaviour when `FloatEncoder`s are used.
    //
    // TODO(edd): I need to add the correct comparators to the scalar encodings
    //            so that they behave the same as PG
    //
    fn row_ids_filter_nan() {
        let data = vec![22.3, f64::NAN, f64::NEG_INFINITY, f64::INFINITY];

        let cases = vec![
            FloatEncoding::RLE64(RLE::from(data.clone())),
            FloatEncoding::Fixed64(Fixed::from(data.as_slice())),
            FloatEncoding::FixedNull64(FixedNull::from(data.as_slice())),
        ];

        for enc in cases {
            _row_ids_filter_nan(enc)
        }
    }

    fn _row_ids_filter_nan(_enc: FloatEncoding) {
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
