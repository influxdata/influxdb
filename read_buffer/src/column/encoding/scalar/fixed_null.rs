//! An encoding for fixed width, nullable values backed by Arrow arrays.
//!
//! This encoding stores a column of fixed-width numerical values potentially
//! using a smaller physical type in memory than the provided logical type.
//!
//! For example, if you have a column with 64-bit integers: [122, 232, 33, 0,
//! -12] then you can reduce the space needed to store them, by converting them
//! as a `Vec<i8>` instead of a `Vec<i64>`. In this case, this reduces the size
//! of the column data by 87.5% and generally should increase throughput of
//! operations on the column data.
//!
//! The encodings within this module do not concern themselves with choosing the
//! appropriate physical type for a given logical type; that is the job of the
//! consumer of these encodings.
use std::cmp::Ordering;
use std::fmt::Debug;
use std::iter::FromIterator;
use std::mem::size_of;

use arrow::{
    array::{Array, PrimitiveArray},
    datatypes::ArrowNumericType,
};

use crate::column::{cmp, RowIDs};

#[derive(Debug, PartialEq)]
pub struct FixedNull<T>
where
    T: ArrowNumericType,
{
    // backing data
    arr: PrimitiveArray<T>,
}

impl<T: ArrowNumericType> std::fmt::Display for FixedNull<T>
where
    T: ArrowNumericType + std::fmt::Debug,
    T::Native: Default
        + PartialEq
        + PartialOrd
        + Copy
        + std::fmt::Debug
        + std::ops::Add<Output = T::Native>,
{
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "[Array] rows: {:?}, nulls: {:?}, size: {}",
            self.arr.len(),
            self.arr.null_count(),
            self.size()
        )
    }
}
impl<T> FixedNull<T>
where
    T: ArrowNumericType,
{
    pub fn num_rows(&self) -> u32 {
        self.arr.len() as u32
    }

    pub fn is_empty(&self) -> bool {
        self.arr.is_empty()
    }

    pub fn contains_null(&self) -> bool {
        self.arr.null_count() > 0
    }

    pub fn null_count(&self) -> u32 {
        self.arr.null_count() as u32
    }

    /// Returns an estimation of the total size in bytes used by this column
    /// encoding.
    pub fn size(&self) -> usize {
        size_of::<PrimitiveArray<T>>() + self.arr.get_array_memory_size()
    }

    /// The estimated total size in bytes of the underlying values in the
    /// column if they were stored contiguously and uncompressed. `include_nulls`
    /// will effectively size each NULL value as 8b if `true` because the logical
    /// size of all types of `T` is 8b
    pub fn size_raw(&self, include_nulls: bool) -> usize {
        // hmmm whilst Vec<i64> is probably accurate it's not really correct if
        // T is not i64.
        let base_size = size_of::<Vec<i64>>();
        if !self.contains_null() || include_nulls {
            return base_size + (self.num_rows() as usize * 8);
        }
        base_size + ((self.num_rows() as usize - self.arr.null_count()) * 8)
    }

    //
    //
    // ---- Methods for getting row ids from values.
    //
    //

    /// Returns the first logical row that contains a value `v`.
    pub fn first_row_id_eq_value(&self, v: T::Native) -> Option<usize> {
        for i in 0..self.arr.len() {
            if self.arr.is_null(i) {
                continue;
            } else if self.arr.value(i) == v {
                return Some(i);
            }
        }
        None
    }

    //
    //
    // ---- Methods for getting decoded (materialised) values.
    //
    //

    /// Return the logical (decoded) value at the provided row ID according to
    /// the logical type of the column, which is specified by `U`. A NULL value
    /// is represented by None.
    pub fn value<U>(&self, row_id: u32) -> Option<U>
    where
        U: From<T::Native>,
    {
        if self.arr.is_null(row_id as usize) {
            return None;
        }
        Some(U::from(self.arr.value(row_id as usize)))
    }

    /// Returns the logical (decoded) values for the provided row IDs according
    /// to the logical type of the column, which is specified by `U`.
    ///
    /// NULL values are represented by None.
    ///
    /// TODO(edd): Perf - we should return a vector of values and a vector of
    /// integers representing the null validity bitmap.
    pub fn values<U>(&self, row_ids: &[u32], mut dst: Vec<Option<U>>) -> Vec<Option<U>>
    where
        U: From<T::Native>,
    {
        dst.clear();
        dst.reserve(row_ids.len());

        for &row_id in row_ids {
            if self.arr.is_null(row_id as usize) {
                dst.push(None)
            } else {
                dst.push(Some(U::from(self.arr.value(row_id as usize))))
            }
        }
        assert_eq!(dst.len(), row_ids.len());
        dst
    }

    /// Returns the logical (decoded) values for all the rows in the column
    /// according to the logical type of the column, which is specified by `U`.
    ///
    /// NULL values are represented by None.
    ///
    /// TODO(edd): Perf - we should return a vector of values and a vector of
    /// integers representing the null validity bitmap.
    pub fn all_values<U>(&self, mut dst: Vec<Option<U>>) -> Vec<Option<U>>
    where
        U: From<T::Native>,
    {
        dst.clear();
        dst.reserve(self.arr.len());

        for i in 0..self.num_rows() as usize {
            if self.arr.is_null(i) {
                dst.push(None)
            } else {
                dst.push(Some(U::from(self.arr.value(i))))
            }
        }
        assert_eq!(dst.len(), self.num_rows() as usize);
        dst
    }

    //
    //
    // ---- Methods for aggregation.
    //
    //

    /// Returns the count of the non-null values for the provided
    /// row IDs.
    pub fn count(&self, row_ids: &[u32]) -> u32 {
        if self.arr.null_count() == 0 {
            return row_ids.len() as u32;
        }

        let mut count = 0;
        for &i in row_ids {
            if self.arr.is_null(i as usize) {
                continue;
            }
            count += 1;
        }
        count
    }

    /// Returns the summation of the non-null logical (decoded) values for the
    /// provided row IDs.
    ///
    /// TODO(edd): I have experimented with using the Arrow kernels for these
    /// aggregations methods but they're currently significantly slower than
    /// this implementation (about 85% in the `sum` case). We will revisit
    /// them in the future as they do would the implementation of these
    /// aggregation functions.
    pub fn sum<U>(&self, row_ids: &[u32]) -> Option<U>
    where
        U: Default + From<T::Native> + std::ops::Add<Output = U>,
    {
        let mut result = U::default();

        if self.arr.null_count() == 0 {
            for chunks in row_ids.chunks_exact(4) {
                result = result + U::from(self.arr.value(chunks[3] as usize));
                result = result + U::from(self.arr.value(chunks[2] as usize));
                result = result + U::from(self.arr.value(chunks[1] as usize));
                result = result + U::from(self.arr.value(chunks[0] as usize));
            }

            let rem = row_ids.len() % 4;
            for &i in &row_ids[row_ids.len() - rem..row_ids.len()] {
                result = result + U::from(self.arr.value(i as usize));
            }

            return Some(result);
        }

        let mut is_none = true;
        for &i in row_ids {
            if self.arr.is_null(i as usize) {
                continue;
            }
            is_none = false;
            result = result + U::from(self.arr.value(i as usize));
        }

        if is_none {
            return None;
        }
        Some(result)
    }

    /// Returns the first logical (decoded) value from the provided
    /// row IDs.
    pub fn first<U>(&self, row_ids: &[u32]) -> Option<U>
    where
        U: From<T::Native>,
    {
        self.value(row_ids[0])
    }

    /// Returns the last logical (decoded) value from the provided
    /// row IDs.
    pub fn last<U>(&self, row_ids: &[u32]) -> Option<U>
    where
        U: From<T::Native>,
    {
        self.value(row_ids[row_ids.len() - 1])
    }

    /// Returns the minimum logical (decoded) non-null value from the provided
    /// row IDs.
    pub fn min<U>(&self, row_ids: &[u32]) -> Option<U>
    where
        U: From<T::Native> + PartialOrd,
    {
        let mut min: Option<U> = self.value(row_ids[0]);
        for &v in row_ids.iter().skip(1) {
            if self.arr.is_null(v as usize) {
                continue;
            }

            if self.value(v) < min {
                min = self.value(v);
            }
        }
        min
    }

    /// Returns the maximum logical (decoded) non-null value from the provided
    /// row IDs.
    pub fn max<U>(&self, row_ids: &[u32]) -> Option<U>
    where
        U: From<T::Native> + PartialOrd,
    {
        let mut max: Option<U> = self.value(row_ids[0]);
        for &v in row_ids.iter().skip(1) {
            if self.arr.is_null(v as usize) {
                continue;
            }

            if self.value(v) > max {
                max = self.value(v);
            }
        }
        max
    }

    //
    //
    // ---- Methods for filtering via operators.
    //
    //

    /// Returns the set of row ids that satisfy a binary operator on a logical
    /// value.
    ///
    /// Essentially, this supports `value {=, !=, >, >=, <, <=} x`.
    ///
    /// The equivalent of `IS NULL` is not currently supported via this method.
    pub fn row_ids_filter(&self, value: T::Native, op: &cmp::Operator, dst: RowIDs) -> RowIDs {
        match op {
            cmp::Operator::GT => self.row_ids_cmp_order(value, Self::ord_from_op(&op), dst),
            cmp::Operator::GTE => self.row_ids_cmp_order(value, Self::ord_from_op(&op), dst),
            cmp::Operator::LT => self.row_ids_cmp_order(value, Self::ord_from_op(&op), dst),
            cmp::Operator::LTE => self.row_ids_cmp_order(value, Self::ord_from_op(&op), dst),
            _ => self.row_ids_equal(value, op, dst),
        }
    }

    // Helper function to convert comparison operators to cmp orderings.
    fn ord_from_op(op: &cmp::Operator) -> (Ordering, Ordering) {
        match op {
            cmp::Operator::GT => (Ordering::Greater, Ordering::Greater),
            cmp::Operator::GTE => (Ordering::Greater, Ordering::Equal),
            cmp::Operator::LT => (Ordering::Less, Ordering::Less),
            cmp::Operator::LTE => (Ordering::Less, Ordering::Equal),
            _ => panic!("cannot convert operator to ordering"),
        }
    }

    // Handles finding all rows that match the provided operator on `value`.
    // For performance reasons ranges of matching values are collected up and
    // added in bulk to the bitmap.
    fn row_ids_equal(&self, value: T::Native, op: &cmp::Operator, mut dst: RowIDs) -> RowIDs {
        dst.clear();

        let desired;
        if let cmp::Operator::Equal = op {
            desired = true; // == operator
        } else {
            desired = false; // != operator
        }

        let mut found = false;
        let mut count = 0;
        for i in 0..self.num_rows() as usize {
            let cmp_result = self.arr.value(i) == value;

            if (self.arr.is_null(i) || cmp_result != desired) && found {
                let (min, max) = (i as u32 - count, i as u32);
                dst.add_range(min, max);
                found = false;
                count = 0;
                continue;
            } else if self.arr.is_null(i) || cmp_result != desired {
                continue;
            }

            if !found {
                found = true;
            }
            count += 1;
        }

        // add any remaining range.
        if found {
            let (min, max) = (self.num_rows() - count, self.num_rows());
            dst.add_range(min, max);
        }
        dst
    }

    // Handles finding all rows that match the provided operator on `value`.
    // For performance reasons ranges of matching values are collected up and
    // added in bulk to the bitmap.
    //
    // `op` is a tuple of comparisons where at least one of them must be
    // satisfied to satisfy the overall operator.
    fn row_ids_cmp_order(
        &self,
        value: T::Native,
        op: (std::cmp::Ordering, std::cmp::Ordering),
        mut dst: RowIDs,
    ) -> RowIDs {
        dst.clear();

        let mut found = false;
        let mut count = 0;
        for i in 0..self.num_rows() as usize {
            let cmp_result = self.arr.value(i).partial_cmp(&value);

            if (self.arr.is_null(i) || (cmp_result != Some(op.0) && cmp_result != Some(op.1)))
                && found
            {
                let (min, max) = (i as u32 - count, i as u32);
                dst.add_range(min, max);
                found = false;
                count = 0;
                continue;
            } else if self.arr.is_null(i) || (cmp_result != Some(op.0) && cmp_result != Some(op.1))
            {
                continue;
            }

            if !found {
                found = true;
            }
            count += 1;
        }

        // add any remaining range.
        if found {
            let (min, max) = (self.num_rows() - count, self.num_rows());
            dst.add_range(min, max);
        }
        dst
    }

    /// Returns the set of row ids that satisfy a pair of binary operators
    /// against two values of the same logical type.
    ///
    /// This method is a special case optimisation for common cases where one
    /// wishes to do the equivalent of WHERE x > y AND x <= y` for example.
    ///
    /// Essentially, this supports:
    ///     `x {>, >=, <, <=} value1 AND x {>, >=, <, <=} value2`.
    pub fn row_ids_filter_range(
        &self,
        left: (T::Native, &cmp::Operator),
        right: (T::Native, &cmp::Operator),
        dst: RowIDs,
    ) -> RowIDs {
        match (left.1, right.1) {
            (cmp::Operator::GT, cmp::Operator::LT)
            | (cmp::Operator::GT, cmp::Operator::LTE)
            | (cmp::Operator::GTE, cmp::Operator::LT)
            | (cmp::Operator::GTE, cmp::Operator::LTE)
            | (cmp::Operator::LT, cmp::Operator::GT)
            | (cmp::Operator::LT, cmp::Operator::GTE)
            | (cmp::Operator::LTE, cmp::Operator::GT)
            | (cmp::Operator::LTE, cmp::Operator::GTE) => self.row_ids_cmp_range_order(
                (left.0, Self::ord_from_op(&left.1)),
                (right.0, Self::ord_from_op(&right.1)),
                dst,
            ),

            (_, _) => panic!("unsupported operators provided"),
        }
    }

    // Special case function for finding all rows that satisfy two operators on
    // two values.
    //
    // This function exists because it is more performant than calling
    // `row_ids_cmp_order_bm` twice and predicates like `WHERE X > y and X <= x`
    // are very common, e.g., for timestamp columns.
    //
    // For performance reasons ranges of matching values are collected up and
    // added in bulk to the bitmap.
    //
    fn row_ids_cmp_range_order(
        &self,
        left: (T::Native, (std::cmp::Ordering, std::cmp::Ordering)),
        right: (T::Native, (std::cmp::Ordering, std::cmp::Ordering)),
        mut dst: RowIDs,
    ) -> RowIDs {
        dst.clear();

        let left_op = left.1;
        let right_op = right.1;

        let mut found = false;
        let mut count = 0;
        for i in 0..self.num_rows() as usize {
            let left_cmp_result = self.arr.value(i).partial_cmp(&left.0);
            let right_cmp_result = self.arr.value(i).partial_cmp(&right.0);

            let left_result_no =
                left_cmp_result != Some(left_op.0) && left_cmp_result != Some(left_op.1);
            let right_result_no =
                right_cmp_result != Some(right_op.0) && right_cmp_result != Some(right_op.1);

            if (self.arr.is_null(i) || left_result_no || right_result_no) && found {
                let (min, max) = (i as u32 - count, i as u32);
                dst.add_range(min, max);
                found = false;
                count = 0;
                continue;
            } else if self.arr.is_null(i) || left_result_no || right_result_no {
                continue;
            }

            if !found {
                found = true;
            }
            count += 1;
        }

        // add any remaining range.
        if found {
            let (min, max) = (self.num_rows() - count, self.num_rows());
            dst.add_range(min, max);
        }
        dst
    }

    /// Determines if the column contains a non-null value.
    pub fn has_any_non_null_value(&self) -> bool {
        self.arr.null_count() < self.num_rows() as usize
    }

    /// Returns true if a non-null value exists at any of the row ids.
    pub fn has_non_null_value(&self, row_ids: &[u32]) -> bool {
        if !self.contains_null() {
            return true;
        }

        row_ids.iter().any(|id| !self.arr.is_null(*id as usize))
    }
}

// This macro implements the From trait for slices of various logical types.
//
// Here are example implementations:
//
//  impl From<Vec<i64>> for FixedNull<Int64Type> {
//      fn from(v: Vec<i64>) -> Self {
//          Self{
//              arr: PrimitiveArray::from(v),
//          }
//      }
//  }
//
//  impl From<&[i64]> for FixedNull<Int64Type> {
//      fn from(v: &[i64]) -> Self {
//          Self::from(v.to_vec())
//      }
//  }
//
//  impl From<Vec<Option<i64>>> for FixedNull<Int64Type> {
//      fn from(v: Vec<Option<i64>>) -> Self {
//          Self{
//              arr: PrimitiveArray::from(v),
//          }
//      }
//  }
//
//  impl From<&[Option<i64>]> for FixedNull<Int64Type> {
//      fn from(v: &[i64]) -> Self {
//          Self::from(v.to_vec())
//      }
//  }
//
macro_rules! fixed_null_from_native_types {
    ($(($type_from:ty, $type_to:ty),)*) => {
        $(
            impl From<Vec<$type_from>> for FixedNull<$type_to> {
                fn from(v: Vec<$type_from>) -> Self {
                    Self{
                        arr: PrimitiveArray::from(v),
                    }
                }
            }

            impl From<&[$type_from]> for FixedNull<$type_to> {
                fn from(v: &[$type_from]) -> Self {
                    Self::from(v.to_vec())
                }
            }

            impl From<Vec<Option<$type_from>>> for FixedNull<$type_to> {
                fn from(v: Vec<Option<$type_from>>) -> Self {
                    Self{
                        arr: PrimitiveArray::from(v),
                    }
                }
            }

            impl From<&[Option<$type_from>]> for FixedNull<$type_to> {
                fn from(v: &[Option<$type_from>]) -> Self {
                    Self::from(v.to_vec())
                }
            }
        )*
    };
}

fixed_null_from_native_types! {
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

// This macro implements the From trait for Arrow arrays
//
// Implementation:
//
//  impl From<Int64Array> for FixedNull<Int64Type> {
//      fn from(arr: Int64Array) -> Self {
//          Self{arr}
//      }
//  }
//
macro_rules! fixed_null_from_arrow_types {
    ($(($type_from:ty, $type_to:ty),)*) => {
        $(
            impl From<$type_from> for FixedNull<$type_to> {
                fn from(arr: $type_from) -> Self {
                    Self{arr}
                }
            }
        )*
    };
}

fixed_null_from_arrow_types! {
    (arrow::array::Int64Array, arrow::datatypes::Int64Type),
    (arrow::array::UInt64Array, arrow::datatypes::UInt64Type),
    (arrow::array::Float64Array, arrow::datatypes::Float64Type),
}

// This macro implements the From trait for Arrow arrays where some down-casting
// to a smaller physical type happens. It is the caller's responsibility to
// ensure that this down-casting is safe.
//
// Example implementation:
//
//  impl From<Int64Array> for FixedNull<Int32Type> {
//    fn from(arr: Int64Array) -> Self {
//      let arr: PrimitiveArray<Int32Type> =
//          PrimitiveArray::from_iter(arr.iter().map(|v| v.map(|v| v as i32)));
//      Self { arr }
//    }
//  }
//
macro_rules! fixed_null_from_arrow_types_down_cast {
    ($(($type_from:ty, $type_to:ty, $rust_type:ty),)*) => {
        $(
            impl From<$type_from> for FixedNull<$type_to> {
                fn from(arr: $type_from) -> Self {
                    let arr: PrimitiveArray<$type_to> =
                        PrimitiveArray::from_iter(arr.iter().map(|v| v.map(|v| v as $rust_type)));
                    Self { arr }
                }
            }
        )*
    };
}

fixed_null_from_arrow_types_down_cast! {
    (arrow::array::Int64Array, arrow::datatypes::Int32Type, i32),
    (arrow::array::Int64Array, arrow::datatypes::UInt32Type, u32),
    (arrow::array::Int64Array, arrow::datatypes::Int16Type, i16),
    (arrow::array::Int64Array, arrow::datatypes::UInt16Type, u16),
    (arrow::array::Int64Array, arrow::datatypes::Int8Type, i8),
    (arrow::array::Int64Array, arrow::datatypes::UInt8Type, u8),
    (arrow::array::UInt64Array, arrow::datatypes::UInt32Type, u32),
    (arrow::array::UInt64Array, arrow::datatypes::UInt16Type, u16),
    (arrow::array::UInt64Array, arrow::datatypes::UInt8Type, u8),
}

#[cfg(test)]
mod test {
    use super::cmp::Operator;
    use super::*;
    use arrow::array::*;
    use arrow::datatypes::*;

    fn some_vec<T: Copy>(v: Vec<T>) -> Vec<Option<T>> {
        v.iter().map(|x| Some(*x)).collect()
    }

    #[test]
    fn from_arrow_downcast() {
        let arr = Int64Array::from(vec![100, u8::MAX as i64]);
        let exp_values = arr.iter().collect::<Vec<Option<i64>>>();
        let enc: FixedNull<UInt8Type> = FixedNull::from(arr);
        assert_eq!(enc.all_values(vec![]), exp_values);

        let arr = Int64Array::from(vec![100, i32::MAX as i64]);
        let exp_values = arr.iter().collect::<Vec<Option<i64>>>();
        let enc: FixedNull<UInt32Type> = FixedNull::from(arr);
        assert_eq!(enc.all_values(vec![]), exp_values);
    }

    #[test]
    fn size() {
        let v = FixedNull::<UInt64Type>::from(vec![None, None, Some(100), Some(2222)].as_slice());
        assert_eq!(v.size(), 344);
    }

    #[test]
    fn size_raw() {
        let v = FixedNull::<UInt64Type>::from(vec![None, None, Some(100), Some(2222)].as_slice());
        // values   = 4 * 8 = 32b
        // Vec<u64> = 24b
        assert_eq!(v.size_raw(true), 56);
        assert_eq!(v.size_raw(false), 40);

        let v = FixedNull::<Int64Type>::from(vec![None, None].as_slice());
        assert_eq!(v.size_raw(true), 40);
        assert_eq!(v.size_raw(false), 24);

        let v = FixedNull::<Float64Type>::from(vec![None, None, Some(22.3)].as_slice());
        assert_eq!(v.size_raw(true), 48);
        assert_eq!(v.size_raw(false), 32);
    }

    #[test]
    fn first_row_id_eq_value() {
        let v = super::FixedNull::<Int64Type>::from(vec![22, 33, 18].as_slice());

        assert_eq!(v.first_row_id_eq_value(33), Some(1));
        assert_eq!(v.first_row_id_eq_value(100), None);
    }

    #[test]
    fn value() {
        let v = super::FixedNull::<Int8Type>::from(vec![22, 33, 18].as_slice());

        assert_eq!(v.value(2), Some(18));
    }

    #[test]
    fn values() {
        let v = super::FixedNull::<Int8Type>::from((0..10).collect::<Vec<_>>().as_slice());

        assert_eq!(v.values(&[0, 1, 2, 3], vec![]), some_vec(vec![0, 1, 2, 3]));
        assert_eq!(
            v.values(&[0, 1, 2, 3, 4], vec![]),
            some_vec(vec![0, 1, 2, 3, 4])
        );
        assert_eq!(
            v.values(&(0..10).collect::<Vec<_>>(), vec![]),
            some_vec(vec![0, 1, 2, 3, 4, 5, 6, 7, 8, 9])
        );

        let mut dst = some_vec(vec![22, 33, 44, 55]);
        dst = v.values(&[0, 1], dst);
        assert_eq!(dst, some_vec(vec![0, 1]));
        assert_eq!(dst.capacity(), 4);
    }

    #[test]
    fn all_values() {
        let v = super::FixedNull::<Int8Type>::from((0..10).collect::<Vec<_>>().as_slice());

        assert_eq!(
            v.all_values(vec![]),
            (0..10).map(Some).collect::<Vec<Option<i8>>>()
        );

        let mut dst = some_vec(vec![22, 33, 44, 55]);
        dst = v.all_values(dst);
        assert_eq!(dst, (0..10).map(Some).collect::<Vec<Option<i8>>>());
        assert_eq!(dst.capacity(), 10);
    }

    #[test]
    fn count() {
        let data = vec![Some(0), None, Some(22), None, None, Some(33), Some(44)];
        let v = super::FixedNull::<Int8Type>::from(data.as_slice());

        assert_eq!(v.count(&[0, 1, 2, 3, 4, 5, 6]), 4);
        assert_eq!(v.count(&[1, 3]), 0);
        assert_eq!(v.count(&[6]), 1);
    }

    #[test]
    fn sum() {
        let v = super::FixedNull::<Int8Type>::from((0..10).collect::<Vec<_>>().as_slice());

        assert_eq!(v.sum(&[3, 5, 6, 7]), Some(21));
        assert_eq!(v.sum(&[1, 2, 4, 7, 9]), Some(23));
    }

    #[test]
    fn first() {
        let v = super::FixedNull::<Int16Type>::from((10..20).collect::<Vec<_>>().as_slice());

        assert_eq!(v.first(&[3, 5, 6, 7]), Some(13));
    }

    #[test]
    fn last() {
        let v = super::FixedNull::<Int16Type>::from((10..20).collect::<Vec<_>>().as_slice());

        assert_eq!(v.last(&[3, 5, 6, 7]), Some(17));
    }

    #[test]
    fn min() {
        let v = super::FixedNull::<Int16Type>::from(vec![100, 110, 20, 1, 110].as_slice());

        assert_eq!(v.min(&[0, 1, 2, 3, 4]), Some(1));
    }

    #[test]
    fn max() {
        let v = super::FixedNull::<Int16Type>::from(vec![100, 110, 20, 1, 109].as_slice());

        assert_eq!(v.max(&[0, 1, 2, 3, 4]), Some(110));
    }

    #[test]
    fn row_ids_filter_eq() {
        let v = super::FixedNull::<Int64Type>::from(
            vec![100, 101, 100, 102, 1000, 300, 2030, 3, 101, 4, 5, 21, 100].as_slice(),
        );

        let row_ids = v.row_ids_filter(100, &Operator::Equal, RowIDs::new_vector());
        assert_eq!(row_ids.to_vec(), vec![0, 2, 12]);

        let row_ids = v.row_ids_filter(101, &Operator::Equal, RowIDs::new_vector());
        assert_eq!(row_ids.to_vec(), vec![1, 8]);

        let row_ids = v.row_ids_filter(2030, &Operator::Equal, RowIDs::new_vector());
        assert_eq!(row_ids.to_vec(), vec![6]);

        let row_ids = v.row_ids_filter(194, &Operator::Equal, RowIDs::new_vector());
        assert_eq!(row_ids.to_vec(), Vec::<u32>::new());
    }

    #[test]
    fn row_ids_filter_neq() {
        let v = super::FixedNull::<Int64Type>::from(
            vec![100, 101, 100, 102, 1000, 300, 2030, 3, 101, 4, 5, 21, 100].as_slice(),
        );

        let row_ids = v.row_ids_filter(100, &Operator::NotEqual, RowIDs::new_vector());
        assert_eq!(row_ids.to_vec(), vec![1, 3, 4, 5, 6, 7, 8, 9, 10, 11]);

        let row_ids = v.row_ids_filter(101, &Operator::NotEqual, RowIDs::new_vector());
        assert_eq!(row_ids.to_vec(), vec![0, 2, 3, 4, 5, 6, 7, 9, 10, 11, 12]);

        let row_ids = v.row_ids_filter(2030, &Operator::NotEqual, RowIDs::new_vector());
        assert_eq!(
            row_ids.to_vec(),
            vec![0, 1, 2, 3, 4, 5, 7, 8, 9, 10, 11, 12]
        );

        let row_ids = v.row_ids_filter(194, &Operator::NotEqual, RowIDs::new_vector());
        assert_eq!(row_ids.to_vec(), (0..13).collect::<Vec<u32>>());
    }

    #[test]
    fn row_ids_filter_lt() {
        let v = super::FixedNull::<Int64Type>::from(
            vec![100, 101, 100, 102, 1000, 300, 2030, 3, 101, 4, 5, 21, 100].as_slice(),
        );

        let row_ids = v.row_ids_filter(100, &Operator::LT, RowIDs::new_vector());
        assert_eq!(row_ids.to_vec(), vec![7, 9, 10, 11]);

        let row_ids = v.row_ids_filter(3, &Operator::LT, RowIDs::new_vector());
        assert_eq!(row_ids.to_vec(), Vec::<u32>::new());
    }

    #[test]
    fn row_ids_filter_lte() {
        let v = super::FixedNull::<Int64Type>::from(
            vec![100, 101, 100, 102, 1000, 300, 2030, 3, 101, 4, 5, 21, 100].as_slice(),
        );

        let row_ids = v.row_ids_filter(100, &Operator::LTE, RowIDs::new_vector());
        assert_eq!(row_ids.to_vec(), vec![0, 2, 7, 9, 10, 11, 12]);

        let row_ids = v.row_ids_filter(2, &Operator::LTE, RowIDs::new_vector());
        assert_eq!(row_ids.to_vec(), Vec::<u32>::new());
    }

    #[test]
    fn row_ids_filter_gt() {
        let v = super::FixedNull::<Int64Type>::from(
            vec![100, 101, 100, 102, 1000, 300, 2030, 3, 101, 4, 5, 21, 100].as_slice(),
        );

        let row_ids = v.row_ids_filter(100, &Operator::GT, RowIDs::new_vector());
        assert_eq!(row_ids.to_vec(), vec![1, 3, 4, 5, 6, 8]);

        let row_ids = v.row_ids_filter(2030, &Operator::GT, RowIDs::new_vector());
        assert_eq!(row_ids.to_vec(), Vec::<u32>::new());
    }

    #[test]
    fn row_ids_filter_null() {
        let v = super::FixedNull::<Int64Type>::from(
            vec![
                Some(100),
                Some(200),
                None,
                None,
                Some(200),
                Some(22),
                Some(30),
            ]
            .as_slice(),
        );

        let row_ids = v.row_ids_filter(10, &Operator::GT, RowIDs::new_vector());
        assert_eq!(row_ids.to_vec(), vec![0, 1, 4, 5, 6]);

        let row_ids = v.row_ids_filter(30, &Operator::LTE, RowIDs::new_vector());
        assert_eq!(row_ids.to_vec(), vec![5, 6]);
    }

    #[test]
    fn row_ids_filter_gte() {
        let v = super::FixedNull::<Int64Type>::from(
            vec![100, 101, 100, 102, 1000, 300, 2030, 3, 101, 4, 5, 21, 100].as_slice(),
        );

        let row_ids = v.row_ids_filter(100, &Operator::GTE, RowIDs::new_vector());
        assert_eq!(row_ids.to_vec(), vec![0, 1, 2, 3, 4, 5, 6, 8, 12]);

        let row_ids = v.row_ids_filter(2031, &Operator::GTE, RowIDs::new_vector());
        assert_eq!(row_ids.to_vec(), Vec::<u32>::new());
    }

    #[test]
    fn row_ids_filter_range() {
        let v = FixedNull::<Int64Type>::from(
            vec![
                Some(100),
                Some(101),
                None,
                None,
                None,
                Some(100),
                Some(102),
                Some(1000),
                Some(300),
                Some(2030),
                None,
                Some(3),
                None,
                Some(101),
                Some(4),
                Some(5),
                Some(21),
                Some(100),
                None,
                None,
            ]
            .as_slice(),
        );

        let row_ids = v.row_ids_filter_range(
            (100, &Operator::GTE),
            (240, &Operator::LT),
            RowIDs::new_vector(),
        );
        assert_eq!(row_ids.to_vec(), vec![0, 1, 5, 6, 13, 17]);

        let row_ids = v.row_ids_filter_range(
            (100, &Operator::GT),
            (240, &Operator::LT),
            RowIDs::new_vector(),
        );
        assert_eq!(row_ids.to_vec(), vec![1, 6, 13]);

        let row_ids = v.row_ids_filter_range(
            (10, &Operator::LT),
            (-100, &Operator::GT),
            RowIDs::new_vector(),
        );
        assert_eq!(row_ids.to_vec(), vec![11, 14, 15]);

        let row_ids = v.row_ids_filter_range(
            (21, &Operator::GTE),
            (21, &Operator::LTE),
            RowIDs::new_vector(),
        );
        assert_eq!(row_ids.to_vec(), vec![16]);

        let row_ids = v.row_ids_filter_range(
            (10000, &Operator::LTE),
            (3999, &Operator::GT),
            RowIDs::new_vector(),
        );
        assert_eq!(row_ids.to_vec(), Vec::<u32>::new());

        let v = FixedNull::<Int64Type>::from(
            vec![
                Some(100),
                Some(200),
                Some(300),
                Some(2),
                Some(200),
                Some(22),
                Some(30),
            ]
            .as_slice(),
        );
        let row_ids = v.row_ids_filter_range(
            (200, &Operator::GTE),
            (300, &Operator::LTE),
            RowIDs::new_vector(),
        );
        assert_eq!(row_ids.to_vec(), vec![1, 2, 4]);
    }

    #[test]
    fn has_non_null_value() {
        let v = FixedNull::<UInt64Type>::from(vec![None, None].as_slice());
        assert!(!v.has_non_null_value(&[0, 1]));

        let v = FixedNull::from(vec![Some(100_u64), Some(222_u64)].as_slice());
        assert!(v.has_non_null_value(&[0, 1]));
        assert!(v.has_non_null_value(&[1]));

        let v = FixedNull::from(vec![None, Some(100_u64), Some(222_u64)].as_slice());
        assert!(v.has_non_null_value(&[0, 1, 2]));
        assert!(!v.has_non_null_value(&[0]));
        assert!(v.has_non_null_value(&[2]));
    }

    #[test]
    fn has_any_non_null_value() {
        let v = FixedNull::<UInt64Type>::from(vec![None, None].as_slice());
        assert!(!v.has_any_non_null_value());

        let v = FixedNull::from(vec![Some(100_u64), Some(222_u64)].as_slice());
        assert!(v.has_any_non_null_value());

        let v = FixedNull::from(vec![None, Some(100_u64), Some(222_u64)].as_slice());
        assert!(v.has_any_non_null_value());
    }
}
