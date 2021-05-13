use arrow::{
    array::{Array, PrimitiveArray},
    datatypes::Float64Type,
};

use crate::column::cmp;
use crate::column::RowIDs;
use std::{
    cmp::Ordering,
    fmt::{Debug, Display},
    iter,
    mem::size_of,
};

pub const ENCODING_NAME: &str = "RLE";

#[allow(clippy::upper_case_acronyms)] // this looks weird as `Rle`
/// An RLE encoding is one where identical "runs" of values in the column are
/// stored as a tuple: `(run_length, value)`, where `run_length` indicates the
/// number of times the value is to be repeated.
#[derive(Debug, Default)]
pub struct RLE<T>
where
    T: PartialOrd + Debug,
{
    // stores tuples of run-lengths. Each value is repeats the total number of
    // times the second value should is repeated within the column.
    //
    // TODO(edd): can likely improve on 4B for run-length storage
    //
    // TODO(edd): Option<T> effectively doubles the size of storing `T` for many
    //            `T`. I am not overly concerned about this right now because of
    //            the massive savings the encoding can already provide. Further
    //            when `T` is a `NonZeroX` then we get the niche optimisation
    //            that makes `Option<T>` the same size as `T`.
    //
    //            For floats I plan to implement a check that will allow us to
    //            store them as integers if they're natural numbers, then we can
    //            make use of `Option<NonZeroX>` with no option overhead. If
    //            they're not all natural numbers then I think we could make a
    //            `NonZeroNaN` and perhaps figure out how to get `Option<NonZeroNan>`
    //            to have no overhead.
    //
    // TODO(edd): another obvious thing that might be worth doing in the future
    //            is to store all the run-lengths contiguously rather than in
    //            `Vec<(,)>`. One advantage of this is that it is possible to
    //            avoid a tuple "overhead" when there is only a single
    //            occurrence of a value by using a specific marker to represent
    //            when something is a run-length or something is just a single
    //            value.
    run_lengths: Vec<(u32, Option<T>)>,

    // number of NULL values in the column
    null_count: u32,

    // number of total logical rows (values) in the columns.
    num_rows: u32,
}

impl<T> std::fmt::Display for RLE<T>
where
    T: std::fmt::Debug + Display + PartialOrd + Copy,
{
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "[{}] size: {:?} rows: {:?} nulls: {} runs: {} ",
            self.name(),
            self.size(),
            self.num_rows(),
            self.null_count(),
            self.run_lengths.len()
        )
    }
}

impl<T: PartialOrd + Debug + Copy> RLE<T> {
    /// The name of this encoding.
    pub fn name(&self) -> &'static str {
        ENCODING_NAME
    }

    /// A reasonable estimation of the on-heap size this encoding takes up.
    pub fn size(&self) -> usize {
        size_of::<Vec<(u32, Option<T>)>>() // run length container size
            + (self.run_lengths.len() * size_of::<(u32, Option<T>)>()) // run lengths
            + size_of::<u32>()+size_of::<u32>() // null count, num rows
    }

    /// The estimated total size in bytes of the underlying values in the
    /// column if they were stored contiguously and uncompressed. `include_nulls`
    /// will effectively size each NULL value as size_of<T> if set to `true`.
    pub fn size_raw(&self, include_nulls: bool) -> usize {
        let base_size = size_of::<Self>();
        if include_nulls {
            return base_size + (self.num_rows() as usize * size_of::<T>());
        }

        // remove NULL values from calculation
        base_size
            + self
                .run_lengths
                .iter()
                .filter_map(|(rl, v)| v.is_some().then(|| *rl as usize * size_of::<T>()))
                .sum::<usize>()
    }

    /// The number of NULL values in this column.
    pub fn null_count(&self) -> u32 {
        self.null_count
    }

    /// The number of logical rows encoded in this column.
    pub fn num_rows(&self) -> u32 {
        self.num_rows
    }

    /// Determine if NULL is encoded in the column.
    pub fn contains_null(&self) -> bool {
        self.null_count() > 0
    }

    //
    //
    // ---- Helper methods for constructing an instance of the encoding
    //
    //

    /// Adds the provided string value to the encoded data. It is the caller's
    /// responsibility to ensure that the dictionary encoded remains sorted.
    pub fn push(&mut self, v: T) {
        self.push_additional(Some(v), 1);
    }

    /// Adds a NULL value to the encoded data. It is the caller's
    /// responsibility to ensure that the dictionary encoded remains sorted.
    pub fn push_none(&mut self) {
        self.push_additional(None, 1);
    }

    /// Adds additional repetitions of the provided value to the encoded data.
    /// It is the caller's responsibility to ensure that the dictionary encoded
    /// remains sorted.
    pub fn push_additional(&mut self, v: Option<T>, additional: u32) {
        match v {
            Some(v) => self.push_additional_some(v, additional),
            None => self.push_additional_none(additional),
        }
    }

    fn push_additional_some(&mut self, v: T, additional: u32) {
        if let Some((rl, rlv)) = self.run_lengths.last_mut() {
            if rlv.as_ref() == Some(&v) {
                *rl += additional; // update run-length
            } else {
                // new run-length
                self.run_lengths.push((additional, Some(v)));
            }
        } else {
            //First entry in the encoding
            self.run_lengths.push((additional, Some(v)));
        }

        self.num_rows += additional;
    }

    fn push_additional_none(&mut self, additional: u32) {
        if let Some((rl, v)) = self.run_lengths.last_mut() {
            if v.is_none() {
                *rl += additional; // update run-length
            } else {
                // new run-length
                self.run_lengths.push((additional, None));
            }
        } else {
            //First entry in the encoding
            self.run_lengths.push((additional, None));
        }

        self.null_count += additional;
        self.num_rows += additional;
    }

    //
    //
    // ---- Methods for getting row ids from values.
    //
    //

    /// Populates the provided destination container with the row ids satisfying
    /// the provided predicate.
    pub fn row_ids_filter(&self, value: T, op: &cmp::Operator, dst: RowIDs) -> RowIDs {
        match op {
            cmp::Operator::Equal | cmp::Operator::NotEqual => self.row_ids_equal(value, op, dst),
            cmp::Operator::LT | cmp::Operator::LTE | cmp::Operator::GT | cmp::Operator::GTE => {
                self.row_ids_cmp(value, op, dst)
            }
        }
    }

    /// Returns the set of row ids that satisfy a pair of binary operators
    /// against two values of the same physical type.
    ///
    /// This method is a special case optimisation for common cases where one
    /// wishes to do the equivalent of WHERE x > y AND x <= y` for example.
    ///
    /// Essentially, this supports:
    ///     `x {>, >=, <, <=} value1 AND x {>, >=, <, <=} value2`.
    pub fn row_ids_filter_range(
        &self,
        left: (T, &cmp::Operator),
        right: (T, &cmp::Operator),
        dst: RowIDs,
    ) -> RowIDs {
        match (&left.1, &right.1) {
            (cmp::Operator::GT, cmp::Operator::LT)
            | (cmp::Operator::GT, cmp::Operator::LTE)
            | (cmp::Operator::GTE, cmp::Operator::LT)
            | (cmp::Operator::GTE, cmp::Operator::LTE)
            | (cmp::Operator::LT, cmp::Operator::GT)
            | (cmp::Operator::LT, cmp::Operator::GTE)
            | (cmp::Operator::LTE, cmp::Operator::GT)
            | (cmp::Operator::LTE, cmp::Operator::GTE) => self.row_ids_cmp_range_order(
                (&left.0, Self::ord_from_op(&left.1)),
                (&right.0, Self::ord_from_op(&right.1)),
                dst,
            ),

            (_, _) => panic!("unsupported operators provided"),
        }
    }

    // Finds row ids for non-null values, based on = or != operator.
    //
    // TODO(edd): predicate filtering could be improved here because we need to
    //            effectively check all the run-lengths to find any that match
    //            the predicate. Performance will be proportional to the number
    //            of run-lengths in the column. For scalar columns where
    //            predicates are less common for out use-cases this is probably
    //            OK for a while. Improvements: (1) we could add bitsets of
    //            pre-computed rows for each distinct value, which is what we
    //            do for string RLE where equality predicates are very common,
    //            or (2) we could add a sparse index to allow us to jump to the
    //            run-lengths that contain matching logical row_ids.
    fn row_ids_equal(&self, value: T, op: &cmp::Operator, mut dst: RowIDs) -> RowIDs {
        dst.clear();

        if self.num_rows() == 0 {
            return dst;
        }

        match op {
            cmp::Operator::Equal => {
                let mut curr_logical_row_id = 0;
                for (rl, next) in &self.run_lengths {
                    if let Some(next) = next {
                        if matches!(next.partial_cmp(&value), Some(Ordering::Equal)) {
                            dst.add_range(curr_logical_row_id, curr_logical_row_id + rl);
                        }
                    }
                    curr_logical_row_id += rl;
                }
            }
            cmp::Operator::NotEqual => {
                let mut curr_logical_row_id = 0;
                for (rl, next) in &self.run_lengths {
                    if let Some(next) = next {
                        if !matches!(next.partial_cmp(&value), Some(Ordering::Equal)) {
                            dst.add_range(curr_logical_row_id, curr_logical_row_id + rl);
                        }
                    }
                    curr_logical_row_id += rl;
                }
            }
            _ => unreachable!("invalid operator"),
        }

        dst
    }

    // Finds row ids for non-null values based on <, <=, > or >= operator.
    // TODO(edd): predicate filtering could be improved here because we need to
    //            effectively check all the run-lengths to find any that match
    //            the predicate. Performance will be proportional to the number
    //            of run-lengths in the column. For scalar columns where
    //            predicates are less common for out use-cases this is probably
    //            OK for a while. Improvements: (1) we could add bitsets of
    //            pre-computed rows for each distinct value, which is what we
    //            do for string RLE where predicates are very common.
    fn row_ids_cmp(&self, value: T, op: &cmp::Operator, mut dst: RowIDs) -> RowIDs {
        dst.clear();

        if self.num_rows() == 0 {
            return dst;
        }

        // Given an operator `cmp` will be set to a predicate function for the
        // same operator
        let cmp = match op {
            cmp::Operator::GT => PartialOrd::gt,
            cmp::Operator::GTE => PartialOrd::ge,
            cmp::Operator::LT => PartialOrd::lt,
            cmp::Operator::LTE => PartialOrd::le,
            op => unreachable!("{:?} is an invalid operator", op),
        };

        let mut curr_logical_row_id = 0;
        for (rl, next) in &self.run_lengths {
            if let Some(next) = next {
                if cmp(next, &value) {
                    dst.add_range(curr_logical_row_id, curr_logical_row_id + rl);
                }
            }
            curr_logical_row_id += rl;
        }

        dst
    }

    // Special case function for finding all rows that satisfy two operators on
    // two values.
    //
    // This function exists because it is more performant than calling
    // `row_ids_cmp_order_bm` twice and predicates like `WHERE X > y and X <= x`
    // are very common, e.g., for timestamp columns.
    //
    // The method accepts two orderings for each predicate. If the predicate is
    // `x < y` then the orderings provided should be
    // `(Ordering::Less, Ordering::Less)`. This does lead to a slight overhead
    // in checking non-matching values, but it means that the predicate `x <= y`
    // can be supported by providing the ordering
    // `(Ordering::Less, Ordering::Equal)`.
    //
    // For performance reasons ranges of matching values are collected up and
    // added in bulk to the bitmap.
    //
    fn row_ids_cmp_range_order(
        &self,
        _left: (&T, (std::cmp::Ordering, std::cmp::Ordering)),
        _right: (&T, (std::cmp::Ordering, std::cmp::Ordering)),
        mut _dst: RowIDs,
    ) -> RowIDs {
        todo!()
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

    /// Populates the provided destination container with the row ids for rows
    /// that null.
    pub fn row_ids_null(&self, dst: RowIDs) -> RowIDs {
        self.row_ids_is_null(true, dst)
    }

    /// Populates the provided destination container with the row ids for rows
    /// that are not null.
    pub fn row_ids_not_null(&self, dst: RowIDs) -> RowIDs {
        self.row_ids_is_null(false, dst)
    }

    // All row ids that have either NULL or not NULL values.
    fn row_ids_is_null(&self, _is_null: bool, mut _dst: RowIDs) -> RowIDs {
        todo!()
    }

    //
    //
    // ---- Methods for getting materialised values from row IDs.
    //
    //

    /// Returns the logical value present at the provided row id.
    ///
    /// TODO(edd): a sparse index on this can help with materialisation cost by
    /// providing starting indexes into the in the run length collection.
    pub fn value(&self, row_id: u32) -> Option<T> {
        assert!(
            row_id < self.num_rows(),
            "row_id {:?} out of bounds for {:?} rows",
            row_id,
            self.num_rows()
        );

        let mut ordinal_offset = 0;
        for (rl, v) in &self.run_lengths {
            if ordinal_offset + rl > row_id {
                // this run-length overlaps desired row id
                return *v;
            }
            ordinal_offset += rl;
        }

        // we are guaranteed to find a value at the provided row_id because
        // `row_id < num_rows`
        unreachable!(
            "could not find value at row ID {:?}. num_rows = {:?}",
            row_id,
            self.num_rows()
        )
    }

    fn check_row_ids_ordered(&self, row_ids: &[u32]) -> bool {
        if row_ids.is_empty() {
            return true;
        }

        let mut last = row_ids[0];
        for &row_id in row_ids.iter().skip(1) {
            if row_id <= last {
                return false;
            }
            last = row_id;
        }
        true
    }

    /// Materialises a vector of references to the decoded values in the
    /// provided ordered set of row ids.
    ///
    /// NULL values are represented by None.
    ///
    /// # Panics
    ///
    /// The behaviour of providing row IDs that are not an ordered set is
    /// undefined. `values` may panic if the provided row IDs are are not an
    /// ordered set in ascending order.
    ///
    /// Panics if the number of row IDs requested is more than the number of
    /// rows in the column.
    ///
    /// Panics if a requested row ID is out of bounds of the ordinal offset of
    /// a logical value.
    ///
    pub fn values(&self, row_ids: &[u32], mut dst: Vec<Option<T>>) -> Vec<Option<T>> {
        assert!(!row_ids.is_empty(), "no row IDs provided");
        assert!(
            row_ids.len() <= self.num_rows() as usize,
            "more row_ids {:?} than rows {:?}",
            row_ids.len(),
            self.num_rows()
        );

        dst.clear();
        dst.reserve(row_ids.len());

        // Ensure row ids ordered
        debug_assert!(self.check_row_ids_ordered(row_ids));

        let mut curr_logical_row_id = 0;
        let (mut curr_entry_rl, mut curr_value) = self.run_lengths[0];

        let mut i = 1;
        for &row_id in row_ids {
            assert!(
                row_id < self.num_rows(),
                "row_id {:?} beyond max row {:?}",
                row_id,
                self.num_rows() - 1
            );

            while curr_logical_row_id + curr_entry_rl <= row_id {
                // this encoded entry does not cover the row we need.
                // move on to next entry
                curr_logical_row_id += curr_entry_rl;
                curr_entry_rl = self.run_lengths[i].0;
                curr_value = self.run_lengths[i].1;

                i += 1;
            }

            // this encoded entry covers the row_id we want.
            dst.push(curr_value);

            curr_logical_row_id += 1; // move forwards a logical row
            curr_entry_rl -= 1;
        }

        assert_eq!(row_ids.len(), dst.len());
        dst
    }

    /// Returns references to the logical (decoded) values for all the rows in
    /// the column.
    ///
    /// NULL values are represented by None.
    pub fn all_values(&self, mut dst: Vec<Option<T>>) -> Vec<Option<T>> {
        dst.clear();
        dst.reserve(self.num_rows as usize);

        for (rl, v) in &self.run_lengths {
            dst.extend(iter::repeat(v).take(*rl as usize));
        }
        dst
    }

    // /// Returns true if a non-null value exists at any of the row ids.
    pub fn has_non_null_value(&self, row_ids: &[u32]) -> bool {
        assert!(!row_ids.is_empty(), "no row IDs provided");
        // Ensure row ids ordered
        debug_assert!(self.check_row_ids_ordered(row_ids));

        assert!(
            row_ids.len() <= self.num_rows() as usize,
            "more row_ids {:?} than rows {:?}",
            row_ids.len(),
            self.num_rows()
        );

        let mut curr_logical_row_id = 0;
        let (mut curr_entry_rl, mut curr_value) = self.run_lengths[0];

        let mut i = 1;
        for &row_id in row_ids {
            assert!(
                row_id < self.num_rows(),
                "row_id {:?} beyond max row {:?}",
                row_id,
                self.num_rows() - 1
            );

            // find correctly logical row for `row_id`.
            while curr_logical_row_id + curr_entry_rl <= row_id {
                // this encoded entry does not cover the row we need.
                // move on to next entry
                curr_logical_row_id += curr_entry_rl;
                curr_entry_rl = self.run_lengths[i].0;
                curr_value = self.run_lengths[i].1;

                i += 1;
            }

            if curr_value.is_some() {
                return true;
            }

            curr_logical_row_id += 1; // move forwards a logical row
            curr_entry_rl -= 1;
        }

        // all provide row IDs contain NULL values
        false
    }

    /// Determines if the column contains a non-null value.
    pub fn has_any_non_null_value(&self) -> bool {
        self.null_count() < self.num_rows()
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
    pub fn count(&self, _row_ids: &[u32]) -> u32 {
        todo!()
    }

    /// Returns the summation of the logical (decoded) values for the provided
    /// row IDs.
    pub fn sum(&self, _row_ids: &[u32]) -> Option<T> {
        todo!()
    }

    /// Returns the first logical (decoded) value from the provided
    /// row IDs.
    pub fn first(&self, _row_ids: &[u32]) -> Option<T> {
        todo!()
    }

    /// Returns the last logical (decoded) value from the provided
    /// row IDs.
    pub fn last(&self, _row_ids: &[u32]) -> Option<T> {
        todo!()
    }

    /// Returns the minimum logical (decoded) value from the provided
    /// row IDs.
    pub fn min(&self, _row_ids: &[u32]) -> Option<T> {
        todo!()
    }

    /// Returns the maximum logical (decoded) value from the provided
    /// row IDs.
    pub fn max(&self, _row_ids: &[u32]) -> Option<T> {
        todo!()
    }
}

impl<T> From<&[T]> for RLE<T>
where
    T: PartialOrd + Debug + Default + Copy,
{
    fn from(arr: &[T]) -> Self {
        if arr.is_empty() {
            return Self::default();
        } else if arr.len() == 1 {
            let mut enc = Self::default();
            enc.push(arr[0]);
            return enc;
        }

        let mut enc = Self::default();
        let (mut rl, mut v) = (1, arr[0]);
        for &next in arr.iter().skip(1) {
            if next == v {
                rl += 1;
                continue;
            }

            enc.push_additional(Some(v), rl);
            rl = 1;
            v = next;
        }

        // push the final run length
        enc.push_additional(Some(v), rl);
        enc
    }
}

impl<T> From<Vec<T>> for RLE<T>
where
    T: PartialOrd + Debug + Default + Copy,
{
    fn from(arr: Vec<T>) -> Self {
        Self::from(arr.as_slice())
    }
}

impl<T> From<&[Option<T>]> for RLE<T>
where
    T: PartialOrd + Debug + Default + Copy,
{
    fn from(arr: &[Option<T>]) -> Self {
        if arr.is_empty() {
            return Self::default();
        } else if arr.len() == 1 {
            let mut enc = Self::default();
            enc.push_additional(arr[0], 1);
            return enc;
        }

        let mut enc = Self::default();
        let (mut rl, mut v) = (1, arr[0]);
        for &next in arr.iter().skip(1) {
            if next == v {
                rl += 1;
                continue;
            }

            enc.push_additional(v, rl);
            rl = 1;
            v = next;
        }

        // push the final run length
        enc.push_additional(v, rl);
        enc
    }
}

impl<T> From<Vec<Option<T>>> for RLE<T>
where
    T: PartialOrd + Debug + Default + Copy,
{
    fn from(arr: Vec<Option<T>>) -> Self {
        Self::from(arr.as_slice())
    }
}

// Build an RLE encoded column for a given Arrow Array.
impl From<PrimitiveArray<Float64Type>> for RLE<f64> {
    fn from(arr: PrimitiveArray<Float64Type>) -> Self {
        if arr.is_empty() {
            return Self::default();
        } else if arr.len() == 1 {
            let mut enc = Self::default();
            enc.push_additional((!arr.is_null(0)).then(|| arr.value(0)), 1);
            return enc;
        }

        let mut enc = Self::default();
        let (mut rl, mut v) = (1, (!arr.is_null(0)).then(|| arr.value(0)));
        for next in arr.iter().skip(1) {
            if next == v {
                rl += 1;
                continue;
            }

            enc.push_additional(v, rl);
            rl = 1;
            v = next;
        }

        // push the final run length
        enc.push_additional(v, rl);
        enc
    }
}

#[cfg(test)]
mod test {
    use cmp::Operator;

    use super::*;

    #[test]
    fn from_slice() {
        let cases = vec![
            (&[][..], vec![]),
            (&[1][..], vec![(1, Some(1))]),
            (&[100, 22][..], vec![(1, Some(100)), (1, Some(22))]),
            (
                &[100, 22, 100, 100][..],
                vec![(1, Some(100)), (1, Some(22)), (2, Some(100))],
            ),
        ];

        for (input, exp_rl) in cases {
            let enc: RLE<u8> = RLE::from(input);
            assert_eq!(enc.run_lengths, exp_rl);
        }
    }

    #[test]
    fn from_slice_opt() {
        let cases = vec![
            (&[][..], vec![]),
            (&[Some(1)][..], vec![(1, Some(1))]),
            (
                &[Some(100), Some(22)][..],
                vec![(1, Some(100)), (1, Some(22))],
            ),
            (
                &[Some(100), Some(22), Some(100), Some(100)][..],
                vec![(1, Some(100)), (1, Some(22)), (2, Some(100))],
            ),
            (&[None][..], vec![(1, None)]),
            (
                &[None, None, Some(1), None][..],
                vec![(2, None), (1, Some(1)), (1, None)],
            ),
        ];

        for (input, exp_rl) in cases {
            let enc: RLE<u8> = RLE::from(input);
            assert_eq!(enc.run_lengths, exp_rl);
        }
    }

    #[test]
    fn push() {
        let mut enc = RLE::default();
        enc.push_additional(Some(45), 3);
        enc.push_additional(Some(-1), 1);
        enc.push_additional(Some(45), 5);
        enc.push_additional(Some(45), 1);
        enc.push_additional(Some(336), 2);
        enc.push_additional(None, 2);
        enc.push_none();
        enc.push(22);

        assert_eq!(
            enc.run_lengths,
            vec![
                (3, Some(45)),
                (1, Some(-1)),
                (6, Some(45)),
                (2, Some(336)),
                (3, None),
                (1, Some(22))
            ]
        );
    }

    #[test]
    fn null_count() {
        let mut enc = RLE::default();
        enc.push_additional(Some(45), 3);
        assert_eq!(enc.null_count(), 0);

        enc.push_none();
        assert_eq!(enc.null_count(), 1);

        enc.push_none();
        assert_eq!(enc.null_count(), 2);
    }

    #[test]
    fn num_rows() {
        let mut enc = RLE::default();
        enc.push_additional(Some(45), 3);
        assert_eq!(enc.num_rows(), 3);

        enc.push_none();
        assert_eq!(enc.num_rows(), 4);
    }

    #[test]
    fn contains_null() {
        let mut enc = RLE::default();
        enc.push_additional(Some(45), 3);
        assert!(!enc.contains_null());

        enc.push_none();
        assert!(enc.contains_null());
    }

    #[test]
    fn size() {
        let mut enc: RLE<i64> = RLE::default();

        // 24b container + (0 rl * 24) + 4 + 4 = 32
        assert_eq!(enc.size(), 32);

        enc.push_none();
        // 24b container + (1 rl * 24) + 4 + 4 = 32
        assert_eq!(enc.size(), 56);

        enc.push_additional_some(1, 10);
        // 24b container + (2 rl * 24) + 4 + 4 = 32
        assert_eq!(enc.size(), 80);
    }

    #[test]
    fn value() {
        let mut enc = RLE::default();
        enc.push_none();
        enc.push_additional(Some(45), 3);
        enc.push_additional(Some(90), 2);
        enc.push(21);

        assert_eq!(enc.value(0), None);
        assert_eq!(enc.value(1), Some(45));
        assert_eq!(enc.value(3), Some(45));
        assert_eq!(enc.value(4), Some(90));
        assert_eq!(enc.value(6), Some(21));
    }

    #[test]
    fn check_row_ids_ordered() {
        let cases = vec![
            (&[0, 1, 2][..], true),
            (&[0], true),
            (&[], true),
            (&[0, 2], true),
            (&[1, 2], true),
            (&[0, 0, 2], false),
            (&[0, 1, 0], false),
            (&[2, 1, 0], false),
            (&[1, 1], false),
            (&[1, 2, 2], false),
        ];

        let enc: RLE<i16> = RLE::default();

        for (input, exp) in cases {
            assert_eq!(enc.check_row_ids_ordered(input), exp);
        }
    }

    #[test]
    fn values() {
        let mut enc = RLE::default();
        enc.push_none();
        enc.push_additional(Some(45), 3);
        enc.push_additional(Some(90), 2);
        enc.push(21);

        // ensure buffer cleared by populating it
        assert_eq!(
            enc.values(&[0, 1, 2], vec![Some(33)]),
            vec![None, Some(45), Some(45)]
        );

        assert_eq!(
            enc.values(&[0, 1, 2, 3, 4], vec![]),
            vec![None, Some(45), Some(45), Some(45), Some(90)]
        );

        assert_eq!(enc.values(&[2, 5], vec![]), vec![Some(45), Some(90)]);
    }

    #[test]
    fn all_values() {
        let mut enc = RLE::default();
        // ensure buffer cleared by populating it
        assert!(enc.all_values(vec![Some(33)]).is_empty());

        enc.push_additional(Some(45), 3);
        enc.push_additional(Some(90), 2);
        enc.push_additional(None, 2);
        enc.push(21);

        assert_eq!(
            enc.all_values(vec![None, Some(99)]),
            vec![
                Some(45),
                Some(45),
                Some(45),
                Some(90),
                Some(90),
                None,
                None,
                Some(21)
            ]
        );

        let mut enc: RLE<u8> = RLE::default();
        enc.push_none();
        assert_eq!(enc.all_values(vec![]), vec![None]);
    }

    #[test]
    fn has_non_null_value() {
        let mut enc: RLE<f64> = RLE::default();
        enc.push_none();
        assert!(!enc.has_non_null_value(&[0]));

        enc.push_additional(Some(45.2), 3);
        assert!(!enc.has_non_null_value(&[0]));
        assert!(enc.has_non_null_value(&[0, 1]));
        assert!(enc.has_non_null_value(&[2, 3]));
        assert!(enc.has_non_null_value(&[0, 1, 2, 3]));

        enc.push_additional(None, 3);
        assert!(!enc.has_non_null_value(&[0, 5]));
        assert!(!enc.has_non_null_value(&[6]));

        // NULL, 45.2, 45.2, 45.2, NULL, NULL, NULL, 19.3
        enc.push(19.3);
        assert!(enc.has_non_null_value(&[3, 7]));
        assert!(enc.has_non_null_value(&[7]));
    }

    #[test]
    fn has_any_non_null_value() {
        let mut enc: RLE<f64> = RLE::default();
        assert!(!enc.has_any_non_null_value());

        enc.push_none();
        assert!(!enc.has_any_non_null_value());

        enc.push_additional(None, 3);
        assert!(!enc.has_any_non_null_value());

        enc.push(22.34);
        assert!(enc.has_any_non_null_value());

        enc.push_additional(None, 3);
        assert!(enc.has_any_non_null_value());
    }

    #[test]
    fn row_ids_filter_eq() {
        // NULL, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, NULL, 19, 2, 2
        let mut enc: RLE<u8> = RLE::default();
        enc.push_none();
        enc.push_additional_some(2, 10);
        enc.push_none();
        enc.push_additional_some(19, 1);
        enc.push_additional_some(2, 2);

        let dst = enc.row_ids_filter(22, &Operator::Equal, RowIDs::new_vector());
        assert!(dst.unwrap_vector().is_empty());

        let dst = enc.row_ids_filter(2, &Operator::Equal, RowIDs::new_vector());
        assert_eq!(
            dst.unwrap_vector(),
            &vec![1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 13, 14]
        );

        let dst = enc.row_ids_filter(19, &Operator::Equal, RowIDs::new_vector());
        assert_eq!(dst.unwrap_vector(), &vec![12]);

        // Test all null encoding
        let mut enc: RLE<u8> = RLE::default();
        enc.push_additional_none(10);
        let dst = enc.row_ids_filter(2, &Operator::Equal, RowIDs::new_vector());
        assert!(dst.unwrap_vector().is_empty());
    }

    #[test]
    fn row_ids_filter_neq() {
        // NULL, 2.0, 2.0, 2.0, 2.0, 2.0, 2.0, 2.0, 2.0, 2.0, 2.0, NULL, 19.0, 2.000000001
        let mut enc: RLE<f64> = RLE::default();
        enc.push_none();
        enc.push_additional_some(2.0, 10);
        enc.push_none();
        enc.push_additional_some(19.0, 1);
        enc.push_additional_some(2.000000001, 1);

        let dst = enc.row_ids_filter(2.0, &Operator::NotEqual, RowIDs::new_vector());
        assert_eq!(dst.unwrap_vector(), &vec![12, 13]);

        let dst = enc.row_ids_filter(2.000000001, &Operator::NotEqual, RowIDs::new_vector());
        assert_eq!(
            dst.unwrap_vector(),
            &vec![1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 12]
        );

        // Test all null encoding
        let mut enc: RLE<i8> = RLE::default();
        enc.push_additional_none(10);
        let dst = enc.row_ids_filter(2, &Operator::NotEqual, RowIDs::new_vector());
        assert!(dst.unwrap_vector().is_empty()); // NULL values do not match !=

        // Test NaN behaviour
        let mut enc: RLE<f64> = RLE::default();
        enc.push_additional_none(3);
        enc.push(22.3);
        enc.push_additional_some(f64::NAN, 1);
        enc.push_additional_some(f64::NEG_INFINITY, 1);
        enc.push_additional_some(f64::INFINITY, 1);

        // All non-null rows match !- 2.0 including the NaN / +- ∞
        let dst = enc.row_ids_filter(2.0, &Operator::NotEqual, RowIDs::new_vector());
        assert_eq!(dst.unwrap_vector(), &vec![3, 4, 5, 6]);
    }

    #[test]
    fn row_ids_filter_lt() {
        // NULL, 2.0, 2.0, 2.0, 2.0, 2.0, 2.0, 2.0, 2.0, 2.0, 2.0, NULL, 19.0, 2.000000001
        let mut enc: RLE<f64> = RLE::default();
        enc.push_none();
        enc.push_additional_some(2.0, 10);
        enc.push_none();
        enc.push_additional_some(19.0, 1);
        enc.push_additional_some(2.000000001, 1);

        let dst = enc.row_ids_filter(2.0, &Operator::LT, RowIDs::new_vector());
        assert!(dst.unwrap_vector().is_empty());

        let dst = enc.row_ids_filter(2.0000000000001, &Operator::LT, RowIDs::new_vector());
        assert_eq!(dst.unwrap_vector(), &vec![1, 2, 3, 4, 5, 6, 7, 8, 9, 10]);

        let dst = enc.row_ids_filter(2.2, &Operator::LT, RowIDs::new_vector());
        assert_eq!(
            dst.unwrap_vector(),
            &vec![1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12]
        );

        // // Test all null encoding
        // let mut enc: RLE<i8> = RLE::default();
        // enc.push_additional_none(10);
        // let dst = enc.row_ids_filter(2, &Operator::LT, RowIDs::new_vector());
        // assert!(dst.unwrap_vector().is_empty()); // NULL values do not match !=

        // // Test NaN behaviour
        // let mut enc: RLE<f64> = RLE::default();
        // enc.push_additional_none(3);
        // enc.push(22.3);
        // enc.push_additional_some(f64::NAN, 1);
        // enc.push_additional_some(f64::NEG_INFINITY, 1);
        // enc.push_additional_some(f64::INFINITY, 1);

        // // All non-null rows match !- 2.0 including the NaN / +- ∞
        // let dst = enc.row_ids_filter(2.0, &Operator::LT, RowIDs::new_vector());
        // assert_eq!(dst.unwrap_vector(), &vec![3, 4, 5, 6]);
    }

    #[test]
    fn row_ids_filter_lte() {}

    #[test]
    fn row_ids_filter_gt() {}

    #[test]
    fn row_ids_filter_gte() {}

    #[test]
    fn row_ids_filter_range() {}
}
