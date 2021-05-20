pub mod fixed;
pub mod fixed_null;
pub mod rle;

use crate::column::{cmp, RowIDs};
use arrow::datatypes::ArrowNumericType;
use either::Either;
pub use fixed::Fixed;
pub use fixed_null::FixedNull;
pub use rle::RLE;
use std::{fmt::Debug, mem::size_of, ops::AddAssign};

#[allow(clippy::upper_case_acronyms)]
#[derive(Debug)]
pub enum ScalarEncoding<T, A>
where
    T: Debug + PartialOrd + Copy,
    A: ArrowNumericType,
{
    Fixed(Fixed<T>),
    FixedNullable(FixedNull<A>),
    RLE(RLE<T>),
}

impl<T, A> ScalarEncoding<T, A>
where
    T: Debug + PartialOrd + Copy,
    A: ArrowNumericType,
{
    pub fn name(&self) -> String {
        format!(
            "SCALAR_{}",
            match &self {
                Self::Fixed(enc) => enc.name(),
                Self::FixedNullable(enc) => enc.name(),
                Self::RLE(enc) => enc.name(),
            }
        )
    }

    /// The total size in bytes of to store columnar data in memory.
    pub fn size(&self) -> usize {
        match self {
            Self::Fixed(enc) => enc.size(),
            Self::FixedNullable(enc) => enc.size(),
            Self::RLE(enc) => enc.size(),
        }
    }

    /// The estimated total size in bytes of the underlying float values in the
    /// column if they were stored contiguously and uncompressed. `include_nulls`
    /// will effectively size each NULL value as 8b if `true`.
    pub fn size_raw(&self, include_nulls: bool) -> usize {
        match self {
            // this will be the size of a Vec<T>
            Self::Fixed(enc) => size_of::<Vec<T>>() + (enc.num_rows() as usize * size_of::<T>()),
            Self::FixedNullable(enc) => enc.size_raw(include_nulls),
            Self::RLE(enc) => enc.size_raw(include_nulls),
        }
    }

    /// The total number of rows in the column.
    pub fn num_rows(&self) -> u32 {
        match self {
            Self::Fixed(enc) => enc.num_rows(),
            Self::FixedNullable(enc) => enc.num_rows(),
            Self::RLE(enc) => enc.num_rows(),
        }
    }

    /// Determines if the column contains a NULL value.
    pub fn contains_null(&self) -> bool {
        match self {
            Self::Fixed(_) => false,
            Self::FixedNullable(enc) => enc.contains_null(),
            Self::RLE(enc) => enc.contains_null(),
        }
    }

    /// The total number of rows in the column.
    pub fn null_count(&self) -> u32 {
        match self {
            Self::Fixed(_) => 0,
            Self::FixedNullable(enc) => enc.null_count(),
            Self::RLE(enc) => enc.null_count(),
        }
    }

    /// Determines if the column contains a non-null value.
    pub fn has_any_non_null_value(&self) -> bool {
        match self {
            Self::Fixed(_) => true,
            Self::FixedNullable(enc) => enc.has_any_non_null_value(),
            Self::RLE(enc) => enc.has_any_non_null_value(),
        }
    }

    /// Determines if the column contains a non-null value at one of the
    /// provided rows.
    pub fn has_non_null_value(&self, row_ids: &[u32]) -> bool {
        match self {
            Self::Fixed(_) => !row_ids.is_empty(), // all rows will be non-null
            Self::FixedNullable(enc) => enc.has_non_null_value(row_ids),
            Self::RLE(enc) => enc.has_non_null_value(row_ids),
        }
    }

    /// Returns the logical value found at the provided row id.
    pub fn value<U>(&self, row_id: u32) -> Option<U>
    where
        U: From<T> + From<A::Native>,
    {
        match &self {
            Self::Fixed(c) => Some(c.value(row_id)),
            Self::FixedNullable(c) => c.value(row_id),
            Self::RLE(c) => c.value(row_id),
        }
    }

    /// Returns the logical values found at the provided row ids.
    ///
    /// TODO(edd): perf - pooling of destination vectors.
    pub fn values<U>(&self, row_ids: &[u32]) -> Either<Vec<U>, Vec<Option<U>>>
    where
        U: From<T> + From<A::Native>,
    {
        match &self {
            Self::Fixed(c) => Either::Left(c.values::<U>(row_ids, vec![])),
            Self::FixedNullable(c) => Either::Right(c.values(row_ids, vec![])),
            Self::RLE(c) => Either::Right(c.values(row_ids, vec![])),
        }
    }

    /// Returns all logical values in the column.
    ///
    /// TODO(edd): perf - pooling of destination vectors.
    pub fn all_values<U: Copy>(&self) -> Either<Vec<U>, Vec<Option<U>>>
    where
        U: From<T> + From<A::Native>,
    {
        match &self {
            Self::Fixed(c) => Either::Left(c.all_values::<U>(vec![])),
            Self::FixedNullable(c) => Either::Right(c.all_values(vec![])),
            Self::RLE(c) => Either::Right(c.all_values(vec![])),
        }
    }

    pub fn encoded_values<U>(&self, row_ids: &[u32]) -> Vec<U>
    where
        U: From<T> + From<A::Native>,
    {
        match &self {
            Self::Fixed(c) => c.values::<U>(row_ids, vec![]),
            _ => unreachable!("encoded values on encoding type not currently supported"),
        }
    }

    pub fn all_encoded_values<U>(&self) -> Vec<U>
    where
        U: From<T> + From<A::Native>,
    {
        match &self {
            Self::Fixed(c) => c.all_values::<U>(vec![]),
            _ => unreachable!("encoded values on encoding type not currently supported"),
        }
    }

    /// Returns the row ids that satisfy the provided predicate.
    ///
    /// Note: it is the caller's responsibility to ensure that the provided
    /// `Scalar` value will fit within the physical type of the encoded column.
    /// `row_ids_filter` will panic if this invariant is broken.
    pub fn row_ids_filter(&self, value: T, op: &cmp::Operator, dst: RowIDs) -> RowIDs
    where
        T: Into<A::Native>,
    {
        match &self {
            Self::Fixed(c) => c.row_ids_filter(value, op, dst),
            Self::FixedNullable(c) => c.row_ids_filter(value.into(), op, dst),
            Self::RLE(c) => c.row_ids_filter(value, op, dst),
        }
    }

    /// Returns the row ids that satisfy both the provided predicates.
    ///
    /// Note: it is the caller's responsibility to ensure that the provided
    /// `Scalar` value will fit within the physical type of the encoded column.
    /// `row_ids_filter` will panic if this invariant is broken.
    pub fn row_ids_filter_range(
        &self,
        low: (T, &cmp::Operator),
        high: (T, &cmp::Operator),
        dst: RowIDs,
    ) -> RowIDs {
        match &self {
            Self::Fixed(c) => c.row_ids_filter_range((low.0, &low.1), (high.0, &high.1), dst),
            Self::FixedNullable(_) => todo!(),
            Self::RLE(c) => c.row_ids_filter_range((low.0, &low.1), (high.0, &high.1), dst),
        }
    }

    pub fn min<U>(&self, row_ids: &[u32]) -> Option<U>
    where
        U: From<T> + From<A::Native> + PartialOrd,
    {
        match &self {
            Self::Fixed(c) => Some(c.min(row_ids)),
            Self::FixedNullable(c) => c.min(row_ids),
            Self::RLE(c) => c.min(row_ids),
        }
    }

    pub fn max<U>(&self, row_ids: &[u32]) -> Option<U>
    where
        U: From<T> + From<A::Native> + PartialOrd,
    {
        match &self {
            Self::Fixed(c) => Some(c.max(row_ids)),
            Self::FixedNullable(c) => c.max(row_ids),
            Self::RLE(c) => c.max(row_ids),
        }
    }

    pub fn sum<U>(&self, row_ids: &[u32]) -> Option<U>
    where
        U: AddAssign + Default + From<T> + From<A::Native> + PartialOrd + std::ops::Add<Output = U>,
    {
        match &self {
            Self::Fixed(c) => Some(c.sum(row_ids)),
            Self::FixedNullable(c) => c.sum(row_ids),
            Self::RLE(c) => c.sum(row_ids),
        }
    }

    pub fn count(&self, row_ids: &[u32]) -> u32 {
        match &self {
            Self::Fixed(c) => c.count(row_ids),
            Self::FixedNullable(c) => c.count(row_ids),
            Self::RLE(c) => c.count(row_ids),
        }
    }
}

impl<T, A> std::fmt::Display for ScalarEncoding<T, A>
where
    T: Debug + PartialOrd + Copy,
    A: ArrowNumericType + Debug,
{
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Fixed(enc) => enc.fmt(f),
            Self::FixedNullable(enc) => enc.fmt(f),
            Self::RLE(enc) => enc.fmt(f),
        }
    }
}

/// Converts a slice of `T` values into a ScalarEncoding, choosing the most
/// appropriate encoding based on the shape of the data.
impl<T, A> From<&[T]> for ScalarEncoding<T, A>
where
    T: Debug + PartialOrd + Copy,
    A: ArrowNumericType,
{
    fn from(arr: &[T]) -> Self {
        Self::from(arr.to_vec())
    }
}

/// Converts a vec of `T` values into a ScalarEncoding, choosing the most
/// appropriate encoding based on the shape of the data.
impl<T, A> From<Vec<T>> for ScalarEncoding<T, A>
where
    T: Debug + PartialOrd + Copy,
    A: ArrowNumericType,
{
    fn from(arr: Vec<T>) -> Self {
        //
        // TODO(edd): determine if we should RLE this column.
        //
        Self::Fixed(arr.into_iter().collect())
    }
}

// This macro implements the From trait for slices of various logical types.
macro_rules! encoding_from_native_opt {
    ($(($rust_type:ty, $arrow_type:ty),)*) => {
        $(
            impl From<Vec<Option<$rust_type>>> for ScalarEncoding<$rust_type, $arrow_type> {
                fn from(arr: Vec<Option<$rust_type>>) -> Self {
                    //
                    // TODO(edd): determine if we should RLE this column.
                    //
                    if arr.iter().all(|v| v.is_some()) {
                        return Self::from(arr.into_iter().map(|v| v.unwrap()).collect::<Vec<_>>());
                    }

                    // Use a nullable encoding
                    Self::FixedNullable(FixedNull::from(arr))
                }
            }

            impl From<&[Option<$rust_type>]> for ScalarEncoding<$rust_type, $arrow_type> {
                fn from(arr: &[Option<$rust_type>]) -> Self {
                    Self::from(arr.to_vec())
                }
            }
        )*
    };
}

encoding_from_native_opt! {
    (i64, arrow::datatypes::Int64Type),
    (i32, arrow::datatypes::Int32Type),
    (i16, arrow::datatypes::Int16Type),
    (i8, arrow::datatypes::Int8Type),
    (u64, arrow::datatypes::UInt64Type),
    (u32, arrow::datatypes::UInt32Type),
    (u16, arrow::datatypes::UInt16Type),
    (u8, arrow::datatypes::UInt8Type),
    (f64, arrow::datatypes::Float64Type),
}

// This macro implements the From trait for Arrow arrays.
use arrow::array::Array;
macro_rules! encoding_from_arrow_array {
    ($(($arrow_arr_type:ty, $arrow_data_type:ty, $rust_type:ty),)*) => {
        $(
            impl From<$arrow_arr_type> for ScalarEncoding<$rust_type, $arrow_data_type> {
                fn from(arr: $arrow_arr_type) -> Self {
                    // check for null count
                    if arr.null_count() == 0 {
                       return Self::from(arr.values());
                    }

                    Self::FixedNullable(FixedNull::from(arr))
                }
            }
        )*
    };
}

encoding_from_arrow_array! {
    (arrow::array::Int64Array, arrow::datatypes::Int64Type, i64),
    (arrow::array::Int32Array, arrow::datatypes::Int32Type, i32),
    (arrow::array::Int16Array, arrow::datatypes::Int16Type, i16),
    (arrow::array::Int8Array, arrow::datatypes::Int8Type, i8),
    (arrow::array::UInt64Array, arrow::datatypes::UInt64Type, u64),
    (arrow::array::UInt32Array, arrow::datatypes::UInt32Type, u32),
    (arrow::array::UInt16Array, arrow::datatypes::UInt16Type, u16),
    (arrow::array::UInt8Array, arrow::datatypes::UInt8Type, u8),
}

#[cfg(test)]
mod test_super {
    use arrow::datatypes::UInt32Type;
    use std::iter::FromIterator;

    use super::*;

    #[test]
    fn size_raw() {
        let enc: ScalarEncoding<u32, UInt32Type> =
            ScalarEncoding::Fixed(Fixed::from_iter(vec![2_u32, 22, 12, 31]));
        // (4 * 4) + 24
        assert_eq!(enc.size_raw(true), 40);
        assert_eq!(enc.size_raw(false), 40);
    }
}
