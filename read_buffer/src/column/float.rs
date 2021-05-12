use std::mem::size_of;

use arrow::{self, array::Array};

use super::encoding::{
    scalar::Fixed,
    scalar::{rle::RLE, FixedNull},
};
use super::{cmp, Statistics};
use crate::column::{RowIDs, Scalar, Value, Values};

#[allow(clippy::upper_case_acronyms)] // TODO(edd): these will be OK in 1.52
pub enum FloatEncoding {
    Fixed64(Fixed<f64>),
    FixedNull64(FixedNull<arrow::datatypes::Float64Type>),
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

/// Converts a slice of `f64` values into a `FloatEncoding`.
impl From<&[f64]> for FloatEncoding {
    fn from(arr: &[f64]) -> Self {
        Self::Fixed64(Fixed::<f64>::from(arr))
    }
}

/// Converts an Arrow `Float64Array` into a `FloatEncoding`.
impl From<arrow::array::Float64Array> for FloatEncoding {
    fn from(arr: arrow::array::Float64Array) -> Self {
        if arr.null_count() == 0 {
            return Self::from(arr.values());
        }
        Self::FixedNull64(FixedNull::<arrow::datatypes::Float64Type>::from(arr))
    }
}

#[cfg(test)]
mod test {
    use super::*;

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
}
