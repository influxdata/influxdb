//! An encoding nullable bool, by an Arrow array.
use std::cmp::Ordering;
use std::fmt::Debug;
use std::mem::size_of;

use arrow::array::{Array, BooleanArray};
use cmp::Operator;

use crate::column::{cmp, RowIDs};

#[derive(Debug)]
pub struct Bool {
    arr: BooleanArray,
}

impl std::fmt::Display for Bool {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "[Bool] rows: {:?}, nulls: {:?}, size: {}",
            self.arr.len(),
            self.arr.null_count(),
            self.size()
        )
    }
}
impl Bool {
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
        size_of::<Self>() + self.arr.get_array_memory_size()
    }

    /// The estimated total size in bytes of the underlying bool values in the
    /// column if they were stored contiguously and uncompressed. `include_nulls`
    /// will effectively size each NULL value as 1b if `true`.
    pub fn size_raw(&self, include_nulls: bool) -> usize {
        let base_size = std::mem::size_of::<Vec<bool>>();

        if !self.contains_null() || include_nulls {
            return base_size + self.num_rows() as usize;
        }

        base_size + self.num_rows() as usize - self.arr.null_count()
    }

    //
    //
    // ---- Methods for getting row ids from values.
    //
    //

    /// Returns the first logical row that contains a value `v`.
    pub fn first_row_id_eq_value(&self, v: bool) -> Option<usize> {
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
    // ---- Methods for getting decoded values.
    //
    //

    /// Return the logical value at the provided row ID. A NULL value
    /// is represented by None.
    pub fn value(&self, row_id: u32) -> Option<bool> {
        if self.arr.is_null(row_id as usize) {
            return None;
        }
        Some(self.arr.value(row_id as usize))
    }

    /// Returns the logical values for the provided row IDs.
    ///
    /// NULL values are represented by None.
    pub fn values(&self, row_ids: &[u32], mut dst: Vec<Option<bool>>) -> Vec<Option<bool>> {
        dst.clear();
        dst.reserve(row_ids.len());

        for &row_id in row_ids {
            if self.arr.is_null(row_id as usize) {
                dst.push(None)
            } else {
                dst.push(Some(self.arr.value(row_id as usize)))
            }
        }
        assert_eq!(dst.len(), row_ids.len());
        dst
    }

    /// Returns the logical values for all the rows in the column.
    ///
    /// NULL values are represented by None.
    pub fn all_values(&self, mut dst: Vec<Option<bool>>) -> Vec<Option<bool>> {
        dst.clear();
        dst.reserve(self.arr.len());

        for i in 0..self.num_rows() as usize {
            if self.arr.is_null(i) {
                dst.push(None)
            } else {
                dst.push(Some(self.arr.value(i)))
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

    /// Returns the first logical (decoded) value from the provided
    /// row IDs.
    pub fn first(&self, row_ids: &[u32]) -> Option<bool> {
        self.value(row_ids[0])
    }

    /// Returns the last logical (decoded) value from the provided
    /// row IDs.
    pub fn last(&self, row_ids: &[u32]) -> Option<bool> {
        self.value(row_ids[row_ids.len() - 1])
    }

    /// Returns the minimum logical (decoded) non-null value from the provided
    /// row IDs.
    pub fn min(&self, row_ids: &[u32]) -> Option<bool> {
        let mut min: Option<bool> = self.value(row_ids[0]);
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
    pub fn max(&self, row_ids: &[u32]) -> Option<bool> {
        let mut max: Option<bool> = self.value(row_ids[0]);
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
    pub fn row_ids_filter(&self, value: bool, op: &cmp::Operator, dst: RowIDs) -> RowIDs {
        match op {
            cmp::Operator::GT | cmp::Operator::GTE | cmp::Operator::LT | cmp::Operator::LTE => {
                self.row_ids_cmp_order(value, Self::ord_from_op(op), dst)
            }
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
    fn row_ids_equal(&self, value: bool, op: &cmp::Operator, mut dst: RowIDs) -> RowIDs {
        // Only supports equal and not equal operations.
        assert!(matches!(op, Operator::Equal) | matches!(op, Operator::NotEqual));

        dst.clear();

        // true for == and false for !=
        let desired = matches!(op, cmp::Operator::Equal);

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
        value: bool,
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

    /// Determines if the column contains a non-null value.
    pub fn has_any_non_null_value(&self) -> bool {
        self.arr.null_count() < self.num_rows() as usize
    }

    /// Returns true if the column contains any non-null values at the rows
    /// provided.
    pub fn has_non_null_value(&self, row_ids: &[u32]) -> bool {
        !self.contains_null() || row_ids.iter().any(|id| !self.arr.is_null(*id as usize))
    }
}

impl From<&[bool]> for Bool {
    fn from(v: &[bool]) -> Self {
        Self {
            arr: BooleanArray::from(v.to_vec()),
        }
    }
}

impl From<&[Option<bool>]> for Bool {
    fn from(v: &[Option<bool>]) -> Self {
        Self {
            arr: BooleanArray::from(v.to_vec()),
        }
    }
}

impl From<BooleanArray> for Bool {
    fn from(arr: BooleanArray) -> Self {
        Self { arr }
    }
}

#[cfg(test)]
mod test {
    use super::cmp::Operator;
    use super::*;

    fn some_vec<T: Copy>(v: Vec<T>) -> Vec<Option<T>> {
        v.iter().map(|x| Some(*x)).collect()
    }

    #[test]
    fn size() {
        let v = Bool::from(vec![None, None, Some(true), Some(false)].as_slice());
        assert_eq!(v.size(), 400);
    }

    #[test]
    fn size_raw() {
        let v = Bool::from(vec![None, None, Some(true), Some(false)].as_slice());
        // 4 * 1b + 24b
        assert_eq!(v.size_raw(true), 28);
        assert_eq!(v.size_raw(false), 26);
    }

    #[test]
    fn first_row_id_eq_value() {
        let v = Bool::from(vec![true, true].as_slice());

        assert_eq!(v.first_row_id_eq_value(true), Some(0));
        assert_eq!(v.first_row_id_eq_value(false), None);
    }

    #[test]
    fn value() {
        let v = Bool::from(vec![Some(false), Some(true), Some(false), None].as_slice());
        assert_eq!(v.value(1), Some(true));
        assert_eq!(v.value(3), None);
    }

    #[test]
    fn count() {
        let v = Bool::from(&[Some(true), None, Some(true)][..]);
        assert_eq!(v.count(&[0, 1, 2]), 2);
        assert_eq!(v.count(&[0, 2]), 2);
        assert_eq!(v.count(&[0, 1]), 1);
        assert_eq!(v.count(&[1]), 0);
    }

    #[test]
    fn first() {
        let v = Bool::from(&[false, true, true][..]);
        assert_eq!(v.first(&[0, 1, 2]), Some(false));
        assert_eq!(v.first(&[1, 2]), Some(true));
    }

    #[test]
    fn last() {
        let v = Bool::from(&[false, true, false][..]);
        assert_eq!(v.last(&[0, 1, 2]), Some(false));
        assert_eq!(v.last(&[1, 2]), Some(false));
        assert_eq!(v.last(&[0, 1]), Some(true));
    }

    #[test]
    fn min() {
        let v = Bool::from(&[Some(true), Some(true), Some(false), None][..]);
        assert_eq!(v.min(&[0, 1, 2, 3]), Some(false));
        assert_eq!(v.min(&[1, 2]), Some(false));
        assert_eq!(v.min(&[0, 1]), Some(true));
        assert_eq!(v.min(&[0, 3]), Some(true));
        assert_eq!(v.min(&[3]), None);
    }

    #[test]
    fn max() {
        let v = Bool::from(&[Some(true), Some(true), Some(false), None][..]);
        assert_eq!(v.max(&[0, 1, 2, 3]), Some(true));
        assert_eq!(v.max(&[1, 2]), Some(true));
        assert_eq!(v.max(&[0, 1]), Some(true));
        assert_eq!(v.max(&[0, 3]), Some(true));
        assert_eq!(v.max(&[3]), None);
    }

    #[test]
    fn row_ids_filter() {
        let v = Bool::from(&[Some(true), Some(false), None, None, Some(true)][..]);

        // EQ
        let row_ids = v.row_ids_filter(true, &Operator::Equal, RowIDs::new_vector());
        assert_eq!(row_ids.to_vec(), vec![0, 4]);

        let row_ids = v.row_ids_filter(false, &Operator::Equal, RowIDs::new_vector());
        assert_eq!(row_ids.to_vec(), vec![1]);

        // NEQ
        let row_ids = v.row_ids_filter(true, &Operator::NotEqual, RowIDs::new_vector());
        assert_eq!(row_ids.to_vec(), vec![1]);

        let row_ids = v.row_ids_filter(false, &Operator::NotEqual, RowIDs::new_vector());
        assert_eq!(row_ids.to_vec(), vec![0, 4]);

        // GT
        let row_ids = v.row_ids_filter(true, &Operator::GT, RowIDs::new_vector());
        assert_eq!(row_ids.to_vec(), Vec::<u32>::new());

        let row_ids = v.row_ids_filter(false, &Operator::GT, RowIDs::new_vector());
        assert_eq!(row_ids.to_vec(), vec![0, 4]);

        // GTE
        let row_ids = v.row_ids_filter(true, &Operator::GTE, RowIDs::new_vector());
        assert_eq!(row_ids.to_vec(), vec![0, 4]);

        let row_ids = v.row_ids_filter(false, &Operator::GTE, RowIDs::new_vector());
        assert_eq!(row_ids.to_vec(), vec![0, 1, 4]);

        // LT
        let row_ids = v.row_ids_filter(true, &Operator::LT, RowIDs::new_vector());
        assert_eq!(row_ids.to_vec(), vec![1]);

        let row_ids = v.row_ids_filter(false, &Operator::LT, RowIDs::new_vector());
        assert_eq!(row_ids.to_vec(), Vec::<u32>::new());

        // LTE
        let row_ids = v.row_ids_filter(true, &Operator::LTE, RowIDs::new_vector());
        assert_eq!(row_ids.to_vec(), vec![0, 1, 4]);

        let row_ids = v.row_ids_filter(false, &Operator::LTE, RowIDs::new_vector());
        assert_eq!(row_ids.to_vec(), vec![1]);
    }

    #[test]
    fn has_any_non_null_value() {
        let v = Bool::from(vec![None, None].as_slice());
        assert!(!v.has_any_non_null_value());

        let v = Bool::from(vec![Some(true), Some(false)].as_slice());
        assert!(v.has_any_non_null_value());

        let v = Bool::from(vec![Some(true), None, Some(false)].as_slice());
        assert!(v.has_any_non_null_value());
    }

    #[test]
    fn has_non_null_value() {
        let v = Bool::from(vec![None, None].as_slice());
        assert!(!v.has_non_null_value(&[0, 1]));
        assert!(!v.has_non_null_value(&[0]));

        let v = Bool::from(vec![Some(true), Some(false)].as_slice());
        assert!(v.has_non_null_value(&[0, 1]));
        assert!(v.has_non_null_value(&[1]));

        let v = Bool::from(vec![Some(true), None, Some(false)].as_slice());
        assert!(v.has_non_null_value(&[0, 1, 2]));
        assert!(v.has_non_null_value(&[0]));
        assert!(v.has_non_null_value(&[1, 2]));
        assert!(!v.has_non_null_value(&[1]));
    }
}
