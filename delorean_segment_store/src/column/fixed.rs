use std::cmp::Ordering;
use std::fmt::{Debug, Display};
use std::mem::size_of;
use std::ops::AddAssign;

use croaring::Bitmap;

use crate::column::cmp;

#[derive(Debug, Default)]
/// A Fixed encoding is one in which every value has a fixed width, and is
/// stored contiguous in a backing vector. Fixed encodings do not support NULL
/// values, so are suitable for columns known to not have NULL values that we
/// want to aggregate over.
///
/// For a given logical datatype `U`, `Fixed` encodings can store values with
/// a different datatype `T`, where `size_of::<T>() <= size_of::<U>()`.
///
pub struct Fixed<T>
where
    T: PartialOrd,
{
    // backing data
    values: Vec<T>,
    // TODO(edd): future optimisation to stop filtering early.
    // total_order can be used as a hint to stop scanning the column early when
    // applying a comparison predicate to the column.
    // total_order: bool,
}

impl<T> std::fmt::Display for Fixed<T>
where
    T: Display + PartialOrd + Copy,
{
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "[Plain<T>] rows: {:?}, size: {}",
            self.values.len(),
            self.size()
        )
    }
}

impl<T> Fixed<T>
where
    T: PartialOrd + Copy,
{
    pub fn num_rows(&self) -> u64 {
        self.values.len() as u64
    }

    pub fn is_empty(&self) -> bool {
        self.values.is_empty()
    }

    /// Returns the total size in bytes of the encoded data. Note, this method
    /// is really an "accurate" estimation. It doesn't include for example the
    /// size of the `Fixed` struct receiver.
    pub fn size(&self) -> usize {
        size_of::<Vec<T>>() + (size_of::<T>() * self.values.len())
    }

    //
    //
    // ---- Methods for getting row ids from values.
    //
    //

    /// Returns the first logical row that contains a value `v`.
    pub fn first_row_id_eq_value(&self, v: T) -> Option<usize> {
        self.values.iter().position(|x| *x == v)
    }

    //
    //
    // ---- Methods for getting decoded (materialised) values.
    //
    //

    /// Return the logical (decoded) value at the provided row ID.
    ///
    /// `value` materialises the returned value according to the logical type
    /// of the column, which is specified by `U`.
    pub fn value<U>(&self, row_id: usize) -> U
    where
        U: From<T>,
    {
        U::from(self.values[row_id])
    }

    /// Returns the logical (decoded) values for the provided row IDs.
    ///
    /// `values` materialises the returned values according to the logical type
    /// of the column, which is specified by the type `U`. The container for
    /// returned values must be provided by the caller, though `values` will
    /// ensure it has sufficient capacity.
    pub fn values<U>(&self, row_ids: &[usize], mut dst: Vec<U>) -> Vec<U>
    where
        U: From<T>,
    {
        dst.clear();
        dst.reserve(row_ids.len());

        // TODO(edd): There will likely be a faster unsafe way to do this.
        for chunks in row_ids.chunks_exact(4) {
            dst.push(U::from(self.values[chunks[0]]));
            dst.push(U::from(self.values[chunks[1]]));
            dst.push(U::from(self.values[chunks[2]]));
            dst.push(U::from(self.values[chunks[3]]));
        }

        let rem = row_ids.len() % 4;
        for &i in &row_ids[row_ids.len() - rem..row_ids.len()] {
            dst.push(U::from(self.values[i]));
        }

        assert_eq!(dst.len(), row_ids.len());
        dst
    }

    /// Returns the logical (decoded) values for all the rows in the column.
    ///
    /// `all_values` materialises the returned values according to the logical type
    /// of the column, which is specified by the type `U`. The container for
    /// returned values must be provided by the caller, though `values` will
    /// ensure it has sufficient capacity.
    pub fn all_values<U>(&self, mut dst: Vec<U>) -> Vec<U>
    where
        U: From<T>,
    {
        dst.clear();
        dst.reserve(self.values.len());

        for chunks in self.values.chunks_exact(4) {
            dst.push(U::from(chunks[0]));
            dst.push(U::from(chunks[1]));
            dst.push(U::from(chunks[2]));
            dst.push(U::from(chunks[3]));
        }

        for &v in &self.values[dst.len()..self.values.len()] {
            dst.push(U::from(v));
        }

        assert_eq!(dst.len(), self.values.len());
        dst
    }

    //
    //
    // ---- Methods for aggregation.
    //
    //

    /// Returns the count of the values for the provided
    /// row IDs.
    ///
    /// Since this encoding cannot have NULL values this is just the number of
    /// rows requested.
    pub fn count(&self, row_ids: &[usize]) -> u64 {
        row_ids.len() as u64
    }

    /// Returns the summation of the logical (decoded) values for the provided
    /// row IDs.
    ///
    /// The desired logical type of the output should be specified via `U`.
    pub fn sum<U>(&self, row_ids: &[usize]) -> U
    where
        U: From<T> + AddAssign + Default,
    {
        let mut result = U::default();

        // TODO(edd): There may be a faster unsafe way to do this.
        for chunks in row_ids.chunks_exact(4) {
            result += U::from(self.values[chunks[3]]);
            result += U::from(self.values[chunks[2]]);
            result += U::from(self.values[chunks[1]]);
            result += U::from(self.values[chunks[0]]);
        }

        let rem = row_ids.len() % 4;
        for &i in &row_ids[row_ids.len() - rem..row_ids.len()] {
            result += U::from(self.values[i]);
        }

        result
    }

    /// Returns the first logical (decoded) value from the provided
    /// row IDs.
    ///
    /// The desired logical type of the output should be specified via `U`.
    pub fn first<U>(&self, row_ids: &[usize]) -> U
    where
        U: From<T>,
    {
        self.value(row_ids[0])
    }

    /// Returns the last logical (decoded) value from the provided
    /// row IDs.
    ///
    /// The desired logical type of the output should be specified via `U`.
    pub fn last<U>(&self, row_ids: &[usize]) -> U
    where
        U: From<T>,
    {
        self.value(row_ids[row_ids.len() - 1])
    }

    /// Returns the minimum logical (decoded) value from the provided
    /// row IDs.
    ///
    /// The desired logical type of the output should be specified via `U`.
    pub fn min<U>(&self, row_ids: &[usize]) -> U
    where
        U: From<T>,
    {
        let mut min: T = self.value(row_ids[0]);
        for &v in row_ids.iter().skip(1) {
            if self.values[v] < min {
                min = self.values[v];
            }
        }
        U::from(min)
    }

    /// Returns the maximum logical (decoded) value from the provided
    /// row IDs.
    ///
    /// The desired logical type of the output should be specified via `U`.
    pub fn max<U>(&self, row_ids: &[usize]) -> U
    where
        U: From<T>,
    {
        let mut max: T = self.value(row_ids[0]);
        for &v in row_ids.iter().skip(1) {
            if self.values[v] > max {
                max = self.values[v];
            }
        }
        U::from(max)
    }

    //
    //
    // ---- Methods for filtering via operators.
    //
    //

    /// Returns the set of row ids that satisfy a binary operator on a logical
    /// value. Note, it is the caller's responsibility to ensure the value
    /// provided can be correctly converted from the logical to physical
    /// representation.
    ///
    /// Essentially, this supports `value {=, !=, >, >=, <, <=} x`.
    pub fn row_ids_filter<U>(&self, value: U, op: cmp::Operator, bm: Bitmap) -> Bitmap
    where
        T: From<U>,
    {
        let physical_value = T::from(value);
        match op {
            cmp::Operator::GT => {
                self.row_ids_cmp_order_bm(&physical_value, Self::ord_from_op(&op), bm)
            }
            cmp::Operator::GTE => {
                self.row_ids_cmp_order_bm(&physical_value, Self::ord_from_op(&op), bm)
            }
            cmp::Operator::LT => {
                self.row_ids_cmp_order_bm(&physical_value, Self::ord_from_op(&op), bm)
            }
            cmp::Operator::LTE => {
                self.row_ids_cmp_order_bm(&physical_value, Self::ord_from_op(&op), bm)
            }
            _ => self.row_ids_equal_bm(&physical_value, op, bm),
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
    fn row_ids_equal_bm(&self, value: &T, op: cmp::Operator, mut bm: Bitmap) -> Bitmap {
        bm.clear();

        let desired;
        if let cmp::Operator::Equal = op {
            desired = true; // == operator
        } else {
            desired = false; // != operator
        }

        let mut found = false;
        let mut count = 0;
        for (i, next) in self.values.iter().enumerate() {
            let cmp_result = next == value;

            if cmp_result != desired && found {
                let (min, max) = (i as u64 - count as u64, i as u64);
                bm.add_range(min..max);
                found = false;
                count = 0;
                continue;
            } else if cmp_result != desired {
                continue;
            }

            if !found {
                found = true;
            }
            count += 1;
        }

        // add any remaining range.
        if found {
            let (min, max) = (
                (self.values.len()) as u64 - count as u64,
                (self.values.len()) as u64,
            );
            bm.add_range(min..max);
        }
        bm
    }

    // Handles finding all rows that match the provided operator on `value`.
    // For performance reasons ranges of matching values are collected up and
    // added in bulk to the bitmap.
    //
    // `op` is a tuple of comparisons where at least one of them must be
    // satisfied to satisfy the overall operator.
    fn row_ids_cmp_order_bm(
        &self,
        value: &T,
        op: (std::cmp::Ordering, std::cmp::Ordering),
        mut bm: Bitmap,
    ) -> Bitmap {
        bm.clear();

        let mut found = false;
        let mut count = 0;
        for (i, next) in self.values.iter().enumerate() {
            let cmp_result = next.partial_cmp(value);

            if cmp_result != Some(op.0) && cmp_result != Some(op.1) && found {
                let (min, max) = (i as u64 - count as u64, i as u64);
                bm.add_range(min..max);
                found = false;
                count = 0;
                continue;
            } else if cmp_result != Some(op.0) && cmp_result != Some(op.1) {
                continue;
            }

            if !found {
                found = true;
            }
            count += 1;
        }

        // add any remaining range.
        if found {
            let (min, max) = (
                (self.values.len()) as u64 - count as u64,
                (self.values.len()) as u64,
            );
            bm.add_range(min..max);
        }
        bm
    }

    /// Returns the set of row ids that satisfy a pair of binary operators
    /// against two values of the same logical type.
    ///
    /// Note, it is the caller's responsibility to provide values that can
    /// safely be converted from the logical type `U` to the physical type `T`.
    ///
    /// This method is a special case optimisation for common cases where one
    /// wishes to do the equivalent of WHERE x > y AND x <= y` for example.
    ///
    /// Essentially, this supports:
    ///     `x {>, >=, <, <=} value1 AND x {>, >=, <, <=} value2`.
    pub fn row_ids_filter_range<U>(
        &self,
        left: (U, cmp::Operator),
        right: (U, cmp::Operator),
        bm: Bitmap,
    ) -> Bitmap
    where
        T: From<U>,
    {
        let left_physical = T::from(left.0);
        let right_physical = T::from(right.0);

        match (&left.1, &right.1) {
            (cmp::Operator::GT, cmp::Operator::LT)
            | (cmp::Operator::GT, cmp::Operator::LTE)
            | (cmp::Operator::GTE, cmp::Operator::LT)
            | (cmp::Operator::GTE, cmp::Operator::LTE)
            | (cmp::Operator::LT, cmp::Operator::GT)
            | (cmp::Operator::LT, cmp::Operator::GTE)
            | (cmp::Operator::LTE, cmp::Operator::GT)
            | (cmp::Operator::LTE, cmp::Operator::GTE) => self.row_ids_cmp_range_order_bm(
                (&left_physical, Self::ord_from_op(&left.1)),
                (&right_physical, Self::ord_from_op(&right.1)),
                bm,
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
    fn row_ids_cmp_range_order_bm(
        &self,
        left: (&T, (std::cmp::Ordering, std::cmp::Ordering)),
        right: (&T, (std::cmp::Ordering, std::cmp::Ordering)),
        mut bm: Bitmap,
    ) -> Bitmap {
        bm.clear();

        let left_op = left.1;
        let right_op = right.1;

        let mut found = false;
        let mut count = 0;
        for (i, next) in self.values.iter().enumerate() {
            let left_cmp_result = next.partial_cmp(left.0);
            let right_cmp_result = next.partial_cmp(right.0);

            let left_result_no =
                left_cmp_result != Some(left_op.0) && left_cmp_result != Some(left_op.1);
            let right_result_no =
                right_cmp_result != Some(right_op.0) && left_cmp_result != Some(right_op.1);

            if (left_result_no || right_result_no) && found {
                let (min, max) = (i as u64 - count as u64, i as u64);
                bm.add_range(min..max);
                found = false;
                count = 0;
                continue;
            } else if left_result_no || right_result_no {
                continue;
            }

            if !found {
                found = true;
            }
            count += 1;
        }

        // add any remaining range.
        if found {
            let (min, max) = (
                (self.values.len()) as u64 - count as u64,
                (self.values.len()) as u64,
            );
            bm.add_range(min..max);
        }
        bm
    }
}

// This macro implements the From trait for slices of various logical types.
//
// Here is an example implementation:
//
//    impl From<&[i64]> for Fixed<i16> {
//        fn from(v: &[i64]) -> Self {
//          Self {
//              values: v.to_vec().iter().map(|&x| x as i16).collect(),
//          }
//        }
//    }
//
macro_rules! plain_from_impls {
    ($(($type_from:ty, $type_to:ty),)*) => {
        $(
            impl From<&[$type_from]> for Fixed<$type_to> {
                fn from(v: &[$type_from]) -> Self {
                    Self { values: v.to_vec().iter().map(|&x| { x as $type_to }).collect() }
                }
            }
        )*
    };
}

// Supported logical and physical datatypes for the Fixed encoding.
plain_from_impls! {
     (i64, i64),
     (i64, i32),
     (i64, i16),
     (i64, i8),
     (i64, u32),
     (i64, u16),
     (i64, u8),
     (i32, i32),
     (i32, i16),
     (i32, i8),
     (i32, u16),
     (i32, u8),
     (i16, i16),
     (i16, i8),
     (i16, u8),
     (i8, i8),
     (u64, u64),
     (u64, u32),
     (u64, u16),
     (u64, u8),
     (u32, u32),
     (u32, u16),
     (u32, u8),
     (u16, u16),
     (u16, u8),
     (u8, u8),
     (f64, f64),
}

#[cfg(test)]
mod test {
    use super::cmp::Operator;
    use super::*;

    #[test]
    fn from_i64_to_i32() {
        let input = vec![22_i64, 33, 18, 100_000_000];
        let v = Fixed::<i32>::from(input.as_slice());
        assert_eq!(v.values, vec![22_i32, 33, 18, 100_000_000]);
    }

    #[test]
    fn first_row_id_eq_value() {
        let mut v: Fixed<i64> = Fixed::default();
        v.values = vec![22, 33, 18];

        assert_eq!(v.first_row_id_eq_value(33), Some(1));
        assert_eq!(v.first_row_id_eq_value(100), None);
    }

    #[test]
    fn value() {
        let mut v: Fixed<i8> = Fixed::default();
        v.values = vec![22, 33, 18];

        assert_eq!(v.value::<i64>(2), 18_i64);
    }

    #[test]
    fn values() {
        let mut v: Fixed<i8> = Fixed::default();
        v.values = (0..10).collect();

        assert_eq!(v.values::<i64>(&[0, 1, 2, 3], vec![]), vec![0, 1, 2, 3]);
        assert_eq!(
            v.values::<i64>(&[0, 1, 2, 3, 4], vec![]),
            vec![0, 1, 2, 3, 4]
        );
        assert_eq!(
            v.values::<i64>(&(0..10).collect::<Vec<_>>(), vec![]),
            vec![0, 1, 2, 3, 4, 5, 6, 7, 8, 9]
        );

        let mut dst = vec![22, 33, 44, 55];
        dst = v.values::<i64>(&[0, 1], dst);
        assert_eq!(dst, vec![0, 1]);
        assert_eq!(dst.capacity(), 4);
    }

    #[test]
    fn all_values() {
        let mut v: Fixed<i8> = Fixed::default();
        v.values = (0..10).collect();

        assert_eq!(v.all_values::<i64>(vec![]), (0..10).collect::<Vec<i64>>());

        let mut dst = vec![22, 33, 44, 55];
        dst = v.all_values::<i64>(dst);
        assert_eq!(dst, (0..10).collect::<Vec<i64>>());
        assert_eq!(dst.capacity(), 10);
    }

    #[test]
    fn sum() {
        let mut v: Fixed<i8> = Fixed::default();
        v.values = (0..10).collect();

        assert_eq!(v.sum::<i32>(&[3, 5, 6, 7]), 21_i32);
        assert_eq!(v.sum::<i32>(&[1, 2, 4, 7, 9]), 23_i32);
    }

    #[test]
    fn first() {
        let mut v: Fixed<i16> = Fixed::default();
        v.values = (10..20).collect();

        assert_eq!(v.first::<i64>(&[3, 5, 6, 7]), 13);
    }

    #[test]
    fn last() {
        let mut v: Fixed<i16> = Fixed::default();
        v.values = (10..20).collect();

        assert_eq!(v.last::<i64>(&[3, 5, 6, 7]), 17);
    }

    #[test]
    fn min() {
        let mut v: Fixed<i16> = Fixed::default();
        v.values = vec![100, 110, 20, 1, 110];

        assert_eq!(v.min::<i64>(&[0, 1, 2, 3, 4]), 1);
    }

    #[test]
    fn max() {
        let mut v: Fixed<i16> = Fixed::default();
        v.values = vec![100, 110, 20, 1, 109];

        assert_eq!(v.max::<i64>(&[0, 1, 2, 3, 4]), 110);
    }

    #[test]
    fn row_ids_filter_eq() {
        let mut v: Fixed<i64> = Fixed::default();
        v.values = vec![100, 101, 100, 102, 1000, 300, 2030, 3, 101, 4, 5, 21, 100];

        let bm = v.row_ids_filter(100, Operator::Equal, Bitmap::create());
        assert_eq!(bm.to_vec(), vec![0, 2, 12]);

        let bm = v.row_ids_filter(101, Operator::Equal, Bitmap::create());
        assert_eq!(bm.to_vec(), vec![1, 8]);

        let bm = v.row_ids_filter(2030, Operator::Equal, Bitmap::create());
        assert_eq!(bm.to_vec(), vec![6]);

        let bm = v.row_ids_filter(194, Operator::Equal, Bitmap::create());
        assert_eq!(bm.to_vec(), Vec::<u32>::new());
    }

    #[test]
    fn row_ids_filter_neq() {
        let mut v: Fixed<i64> = Fixed::default();
        v.values = vec![100, 101, 100, 102, 1000, 300, 2030, 3, 101, 4, 5, 21, 100];

        let bm = v.row_ids_filter(100, Operator::NotEqual, Bitmap::create());
        assert_eq!(bm.to_vec(), vec![1, 3, 4, 5, 6, 7, 8, 9, 10, 11]);

        let bm = v.row_ids_filter(101, Operator::NotEqual, Bitmap::create());
        assert_eq!(bm.to_vec(), vec![0, 2, 3, 4, 5, 6, 7, 9, 10, 11, 12]);

        let bm = v.row_ids_filter(2030, Operator::NotEqual, Bitmap::create());
        assert_eq!(bm.to_vec(), vec![0, 1, 2, 3, 4, 5, 7, 8, 9, 10, 11, 12]);

        let bm = v.row_ids_filter(194, Operator::NotEqual, Bitmap::create());
        assert_eq!(bm.to_vec(), (0..13).collect::<Vec<u32>>());
    }

    #[test]
    fn row_ids_filter_lt() {
        let mut v: Fixed<i64> = Fixed::default();
        v.values = vec![100, 101, 100, 102, 1000, 300, 2030, 3, 101, 4, 5, 21, 100];

        let bm = v.row_ids_filter(100, Operator::LT, Bitmap::create());
        assert_eq!(bm.to_vec(), vec![7, 9, 10, 11]);

        let bm = v.row_ids_filter(3, Operator::LT, Bitmap::create());
        assert_eq!(bm.to_vec(), Vec::<u32>::new());
    }

    #[test]
    fn row_ids_filter_lte() {
        let mut v: Fixed<i64> = Fixed::default();
        v.values = vec![100, 101, 100, 102, 1000, 300, 2030, 3, 101, 4, 5, 21, 100];

        let bm = v.row_ids_filter(100, Operator::LTE, Bitmap::create());
        assert_eq!(bm.to_vec(), vec![0, 2, 7, 9, 10, 11, 12]);

        let bm = v.row_ids_filter(2, Operator::LTE, Bitmap::create());
        assert_eq!(bm.to_vec(), Vec::<u32>::new());
    }

    #[test]
    fn row_ids_filter_gt() {
        let mut v: Fixed<i64> = Fixed::default();
        v.values = vec![100, 101, 100, 102, 1000, 300, 2030, 3, 101, 4, 5, 21, 100];

        let bm = v.row_ids_filter(100, Operator::GT, Bitmap::create());
        assert_eq!(bm.to_vec(), vec![1, 3, 4, 5, 6, 8]);

        let bm = v.row_ids_filter(2030, Operator::GT, Bitmap::create());
        assert_eq!(bm.to_vec(), Vec::<u32>::new());
    }

    #[test]
    fn row_ids_filter_gte() {
        let mut v: Fixed<i64> = Fixed::default();
        v.values = vec![100, 101, 100, 102, 1000, 300, 2030, 3, 101, 4, 5, 21, 100];

        let bm = v.row_ids_filter(100, Operator::GTE, Bitmap::create());
        assert_eq!(bm.to_vec(), vec![0, 1, 2, 3, 4, 5, 6, 8, 12]);

        let bm = v.row_ids_filter(2031, Operator::GTE, Bitmap::create());
        assert_eq!(bm.to_vec(), Vec::<u32>::new());
    }

    #[test]
    fn row_ids_filter_range() {
        let mut v: Fixed<i64> = Fixed::default();
        v.values = vec![100, 101, 100, 102, 1000, 300, 2030, 3, 101, 4, 5, 21, 100];

        let bm =
            v.row_ids_filter_range((100, Operator::GTE), (240, Operator::LT), Bitmap::create());
        assert_eq!(bm.to_vec(), vec![0, 1, 2, 3, 8, 12]);

        let bm = v.row_ids_filter_range((100, Operator::GT), (240, Operator::LT), Bitmap::create());
        assert_eq!(bm.to_vec(), vec![1, 3, 8]);

        let bm = v.row_ids_filter_range((10, Operator::LT), (-100, Operator::GT), Bitmap::create());
        assert_eq!(bm.to_vec(), vec![7, 9, 10]);

        let bm = v.row_ids_filter_range((21, Operator::GTE), (21, Operator::LTE), Bitmap::create());
        assert_eq!(bm.to_vec(), vec![11]);

        let bm = v.row_ids_filter_range(
            (10000, Operator::LTE),
            (3999, Operator::GT),
            Bitmap::create(),
        );
        assert_eq!(bm.to_vec(), Vec::<u32>::new());
    }
}
