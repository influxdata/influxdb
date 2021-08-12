use super::encoding::bool::Bool;
use super::{cmp, Statistics};
use crate::column::{RowIDs, Value, Values};

/// Encodings for boolean values.
pub enum BooleanEncoding {
    BooleanNull(Bool),
}

impl BooleanEncoding {
    /// The total size in bytes of the store columnar data.
    pub fn size(&self) -> usize {
        match self {
            Self::BooleanNull(enc) => enc.size(false),
        }
    }

    /// The estimated total size in bytes of the underlying bool values in the
    /// column if they were stored contiguously and uncompressed. If
    /// `include_nulls` is false, NULL values will be excluded from the count;
    /// otherwise NULL values will still occupy the space of a slot of the
    /// underlying type, in bool's case 1b.
    pub fn size_raw(&self, include_nulls: bool) -> usize {
        match self {
            Self::BooleanNull(enc) => enc.size_raw(include_nulls),
        }
    }

    /// The total number of rows in the column.
    pub fn num_rows(&self) -> u32 {
        match self {
            Self::BooleanNull(enc) => enc.num_rows(),
        }
    }

    // Returns statistics about the physical layout of columns
    pub(crate) fn storage_stats(&self) -> Statistics {
        Statistics {
            enc_type: self.name().into(),
            log_data_type: "bool",
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
            Self::BooleanNull(enc) => enc.contains_null(),
        }
    }

    /// The total number of rows in the column.
    pub fn null_count(&self) -> u32 {
        match self {
            Self::BooleanNull(enc) => enc.null_count(),
        }
    }

    /// Determines if the column contains a non-null value.
    pub fn has_any_non_null_value(&self) -> bool {
        match self {
            Self::BooleanNull(enc) => enc.has_any_non_null_value(),
        }
    }

    /// Determines if the column contains a non-null value at one of the
    /// provided rows.
    pub fn has_non_null_value(&self, row_ids: &[u32]) -> bool {
        match self {
            Self::BooleanNull(enc) => enc.has_non_null_value(row_ids),
        }
    }

    /// Returns the logical value found at the provided row id.
    pub fn value(&self, row_id: u32) -> Value<'_> {
        match &self {
            Self::BooleanNull(c) => match c.value(row_id) {
                Some(v) => Value::Boolean(v),
                None => Value::Null,
            },
        }
    }

    /// Returns the logical values found at the provided row ids.
    ///
    /// TODO(edd): perf - pooling of destination vectors.
    pub fn values(&self, row_ids: &[u32]) -> Values<'_> {
        match &self {
            Self::BooleanNull(c) => Values::Bool(c.values(row_ids, vec![])),
        }
    }

    /// Returns all logical values in the column.
    ///
    /// TODO(edd): perf - pooling of destination vectors.
    pub fn all_values(&self) -> Values<'_> {
        match &self {
            Self::BooleanNull(c) => Values::Bool(c.all_values(vec![])),
        }
    }

    /// Returns the row ids that satisfy the provided predicate.
    ///
    /// Note: it is the caller's responsibility to ensure that the provided
    /// `Scalar` value will fit within the physical type of the encoded column.
    /// `row_ids_filter` will panic if this invariant is broken.
    pub fn row_ids_filter(&self, op: &cmp::Operator, value: bool, dst: RowIDs) -> RowIDs {
        match &self {
            Self::BooleanNull(c) => c.row_ids_filter(value, op, dst),
        }
    }

    pub fn min(&self, row_ids: &[u32]) -> Value<'_> {
        match &self {
            Self::BooleanNull(c) => match c.min(row_ids) {
                Some(v) => Value::Boolean(v),
                None => Value::Null,
            },
        }
    }

    pub fn max(&self, row_ids: &[u32]) -> Value<'_> {
        match &self {
            Self::BooleanNull(c) => match c.max(row_ids) {
                Some(v) => Value::Boolean(v),
                None => Value::Null,
            },
        }
    }

    pub fn count(&self, row_ids: &[u32]) -> u32 {
        match &self {
            Self::BooleanNull(c) => c.count(row_ids),
        }
    }

    /// The name of this encoding.
    pub fn name(&self) -> &'static str {
        match &self {
            Self::BooleanNull(_) => "FIXEDN",
        }
    }
}

impl std::fmt::Display for BooleanEncoding {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::BooleanNull(enc) => enc.fmt(f),
        }
    }
}

/// Converts an Arrow `BooleanArray` into a `BooleanEncoding`.
impl From<arrow::array::BooleanArray> for BooleanEncoding {
    fn from(arr: arrow::array::BooleanArray) -> Self {
        Self::BooleanNull(Bool::from(arr))
    }
}
