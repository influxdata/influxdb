use crate::column::cmp;
use crate::column::RowIDs;
use std::{
    cmp::Ordering,
    fmt::{Debug, Display},
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
            "[{}] size: {:?} rows: {:?} cardinality: {}, nulls: {} runs: {} ",
            self.name(),
            self.size(),
            self.num_rows(),
            self.cardinality(),
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
        todo!()
    }

    /// The number of distinct logical values in this column encoding.
    pub fn cardinality(&self) -> u32 {
        todo!()
    }

    /// The number of NULL values in this column.
    pub fn null_count(&self) -> u32 {
        todo!()
    }

    /// The number of logical rows encoded in this column.
    pub fn num_rows(&self) -> u32 {
        todo!()
    }

    /// Determine if NULL is encoded in the column.
    pub fn contains_null(&self) -> bool {
        todo!()
    }

    /// Determines if the column contains a non-null value.
    pub fn has_any_non_null_value(&self) -> bool {
        todo!()
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

        self.null_count += additional;
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
    pub fn row_ids_filter(&self, _value: T, _op: &cmp::Operator, _dst: RowIDs) -> RowIDs {
        todo!()
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

    // Finds row ids based on = or != operator.
    fn row_ids_equal(&self, _value: T, _op: &cmp::Operator, mut _dst: RowIDs) -> RowIDs {
        todo!()
    }

    // Finds row ids based on <, <=, > or >= operator.
    fn row_ids_cmp(&self, _value: T, _op: &cmp::Operator, mut _dst: RowIDs) -> RowIDs {
        todo!()
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
    /// N.B right now this doesn't discern between an invalid row id and a NULL
    /// value at a valid location.
    pub fn value(&self, _row_id: u32) -> Option<T> {
        todo!()
    }

    /// Materialises a vector of references to the decoded values in the
    /// provided row ids.
    ///
    /// NULL values are represented by None. It is the caller's responsibility
    /// to ensure row ids are a monotonically increasing set.
    pub fn values<'a>(
        &'a self,
        _row_ids: &[u32],
        mut _dst: Vec<Option<&'a str>>,
    ) -> Vec<Option<T>> {
        todo!()
    }

    /// Returns references to the logical (decoded) values for all the rows in
    /// the column.
    ///
    /// NULL values are represented by None.
    pub fn all_values<'a>(&'a self, mut _dst: Vec<Option<&'a str>>) -> Vec<Option<T>> {
        todo!()
    }

    /// Returns true if a non-null value exists at any of the row ids.
    pub fn has_non_null_value(&self, _row_ids: &[u32]) -> bool {
        todo!()
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
    T: PartialOrd + Debug,
{
    fn from(_v: &[T]) -> Self {
        todo!()
    }
}

#[cfg(test)]
mod test {
    use super::*;

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
    fn size() {}

    #[test]
    fn null_count() {}

    #[test]
    fn value() {}

    #[test]
    fn values() {}

    #[test]
    fn all_values() {}

    #[test]
    fn row_ids_filter_eq() {}

    #[test]
    fn row_ids_filter_neq() {}

    #[test]
    fn row_ids_filter_lt() {}

    #[test]
    fn row_ids_filter_lte() {}

    #[test]
    fn row_ids_filter_gt() {}

    #[test]
    fn row_ids_filter_gte() {}

    #[test]
    fn row_ids_filter_range() {}
}
