use arrow::{self, array::Array};

use super::encoding::{fixed::Fixed, fixed_null::FixedNull};
use super::{cmp, Statistics};
use crate::column::{RowIDs, Scalar, Value, Values};

pub enum FloatEncoding {
    Fixed64(Fixed<f64>),
    FixedNull64(FixedNull<arrow::datatypes::Float64Type>),
}

impl FloatEncoding {
    /// The total size in bytes of the store columnar data.
    pub fn size(&self) -> usize {
        match self {
            Self::Fixed64(enc) => enc.size(),
            Self::FixedNull64(enc) => enc.size(),
        }
    }

    /// The total number of rows in the column.
    pub fn num_rows(&self) -> u32 {
        match self {
            Self::Fixed64(enc) => enc.num_rows(),
            Self::FixedNull64(enc) => enc.num_rows(),
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
            Self::Fixed64(_) => false,
            Self::FixedNull64(enc) => enc.contains_null(),
        }
    }

    /// The total number of rows in the column.
    pub fn null_count(&self) -> u32 {
        match self {
            Self::Fixed64(_) => 0,
            Self::FixedNull64(enc) => enc.null_count(),
        }
    }

    /// Determines if the column contains a non-null value.
    pub fn has_any_non_null_value(&self) -> bool {
        match self {
            Self::Fixed64(_) => true,
            Self::FixedNull64(enc) => enc.has_any_non_null_value(),
        }
    }

    /// Determines if the column contains a non-null value at one of the
    /// provided rows.
    pub fn has_non_null_value(&self, row_ids: &[u32]) -> bool {
        match self {
            Self::Fixed64(_) => !row_ids.is_empty(), // all rows will be non-null
            Self::FixedNull64(enc) => enc.has_non_null_value(row_ids),
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
        }
    }

    /// Returns the logical values found at the provided row ids.
    ///
    /// TODO(edd): perf - pooling of destination vectors.
    pub fn values(&self, row_ids: &[u32]) -> Values<'_> {
        match &self {
            Self::Fixed64(c) => Values::F64(c.values::<f64>(row_ids, vec![])),
            Self::FixedNull64(c) => Values::F64N(c.values(row_ids, vec![])),
        }
    }

    /// Returns all logical values in the column.
    ///
    /// TODO(edd): perf - pooling of destination vectors.
    pub fn all_values(&self) -> Values<'_> {
        match &self {
            Self::Fixed64(c) => Values::F64(c.all_values::<f64>(vec![])),
            Self::FixedNull64(c) => Values::F64N(c.all_values(vec![])),
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
        }
    }

    pub fn min(&self, row_ids: &[u32]) -> Value<'_> {
        match &self {
            Self::Fixed64(c) => Value::Scalar(Scalar::F64(c.min(row_ids))),
            Self::FixedNull64(c) => match c.min(row_ids) {
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
        }
    }

    pub fn sum(&self, row_ids: &[u32]) -> Scalar {
        match &self {
            Self::Fixed64(c) => Scalar::F64(c.sum(row_ids)),
            Self::FixedNull64(c) => match c.sum(row_ids) {
                Some(v) => Scalar::F64(v),
                None => Scalar::Null,
            },
        }
    }

    pub fn count(&self, row_ids: &[u32]) -> u32 {
        match &self {
            Self::Fixed64(c) => c.count(row_ids),
            Self::FixedNull64(c) => c.count(row_ids),
        }
    }

    /// The name of this encoding.
    pub fn name(&self) -> &'static str {
        match &self {
            Self::Fixed64(_) => "None",
            Self::FixedNull64(_) => "None",
        }
    }

    /// The logical datatype of this encoding.
    pub fn logical_datatype(&self) -> &'static str {
        match &self {
            Self::Fixed64(_) => "f64",
            Self::FixedNull64(_) => "f64",
        }
    }
}

impl std::fmt::Display for FloatEncoding {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let name = self.name();
        match self {
            Self::Fixed64(enc) => write!(f, "[{}]: {}", name, enc),
            Self::FixedNull64(enc) => write!(f, "[{}]: {}", name, enc),
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
