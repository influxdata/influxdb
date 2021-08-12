//! An encoding for fixed width, non-nullable values.
//!
//! This encoding stores a column of fixed-width numerical values with a
//! physical type `P` in memory.
//!
//! Some of the methods for finding and materialising values within the encoding
//! allow results to be emitted as some logical type `L` via a transformation
//! `T`.
use either::Either;
use std::cmp::Ordering;
use std::fmt::Debug;
use std::marker::PhantomData;
use std::mem::size_of;
use std::ops::AddAssign;

use super::transcoders::Transcoder;
use super::ScalarEncoding;
use crate::column::{cmp, RowIDs};

pub const ENCODING_NAME: &str = "FIXED";

#[derive(Debug)]
/// A Fixed encoding is one in which every value has a fixed width, and is
/// stored contiguously in a backing vector. Fixed encodings do not support NULL
/// values, so are suitable for columns known to not have NULL values that we
/// want to aggregate over.
///
/// Types are: Physical, Logical, Transcoder
pub struct Fixed<P, L, T>
where
    P: PartialOrd + Debug,
{
    // backing data
    values: Vec<P>,

    // transcoder responsible for converting from physical type `P` to logical
    // type `L`.
    transcoder: T,
    _marker: PhantomData<L>,
    // TODO(edd): perf - consider pushing down totally ordered flag to stop
    // predicate evaluation early.
}

impl<P, L, T> std::fmt::Display for Fixed<P, L, T>
where
    P: Copy + Debug + PartialOrd + Send + Sync,
    L: AddAssign + Debug + Default + Send + Sync,
    T: Transcoder<P, L> + Send + Sync,
{
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "[{}] rows: {:?}, size: {}",
            self.name(),
            self.num_rows(),
            self.size(false)
        )
    }
}

impl<P, L, T> Fixed<P, L, T>
where
    P: Copy + Debug + PartialOrd,
    T: Transcoder<P, L>,
{
    pub fn new(values: Vec<P>, transcoder: T) -> Self {
        Self {
            values,
            transcoder,
            _marker: Default::default(),
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
    fn row_ids_equal(&self, value: &P, op: &cmp::Operator, mut dst: RowIDs) -> RowIDs {
        dst.clear();

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
                let (min, max) = (i as u32 - count as u32, i as u32);
                dst.add_range(min, max);
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
                (self.values.len()) as u32 - count as u32,
                (self.values.len()) as u32,
            );
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
    fn row_ids_cmp_order<F>(&self, value: &P, op: F, mut dst: RowIDs) -> RowIDs
    where
        F: Fn(&P, &P) -> bool,
    {
        dst.clear();

        let mut found = false;
        let mut count = 0;
        for (i, next) in self.values.iter().enumerate() {
            let cmp_result = op(next, value);

            if !cmp_result && found {
                let (min, max) = (i as u32 - count as u32, i as u32);
                dst.add_range(min, max);
                found = false;
                count = 0;
                continue;
            } else if !cmp_result {
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
                (self.values.len()) as u32 - count as u32,
                (self.values.len()) as u32,
            );
            dst.add_range(min, max);
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
        left: (&P, (std::cmp::Ordering, std::cmp::Ordering)),
        right: (&P, (std::cmp::Ordering, std::cmp::Ordering)),
        mut dst: RowIDs,
    ) -> RowIDs {
        dst.clear();

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
                right_cmp_result != Some(right_op.0) && right_cmp_result != Some(right_op.1);

            if (left_result_no || right_result_no) && found {
                let (min, max) = (i as u32 - count as u32, i as u32);
                dst.add_range(min, max);
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
                (self.values.len()) as u32 - count as u32,
                (self.values.len()) as u32,
            );
            dst.add_range(min, max);
        }
        dst
    }
}

impl<P, L, T> ScalarEncoding<L> for Fixed<P, L, T>
where
    P: Copy + Debug + PartialOrd + Send + Sync,
    L: AddAssign + Debug + Default + Send + Sync,
    T: Transcoder<P, L> + Send + Sync,
{
    fn name(&self) -> &'static str {
        ENCODING_NAME
    }

    fn num_rows(&self) -> u32 {
        self.values.len() as u32
    }

    fn size(&self, buffers: bool) -> usize {
        let values = size_of::<P>()
            * match buffers {
                true => self.values.capacity(),
                false => self.values.len(),
            };
        size_of::<Self>() + values
    }

    fn size_raw(&self, _: bool) -> usize {
        size_of::<Vec<L>>() + (size_of::<L>() * self.values.len())
    }

    fn null_count(&self) -> u32 {
        0 // this encoding never contains NULL values
    }

    fn has_any_non_null_value(&self) -> bool {
        self.num_rows() > 0 // this encoding never contains NULL values
    }

    fn has_non_null_value(&self, _: &[u32]) -> bool {
        self.num_rows() > 0 // this encoding never contains NULL values
    }

    fn value(&self, row_id: u32) -> Option<L> {
        let v = self.values[row_id as usize];
        Some(self.transcoder.decode(v))
    }

    fn values(&self, row_ids: &[u32]) -> Either<Vec<L>, Vec<Option<L>>> {
        let mut dst = Vec::with_capacity(row_ids.len());

        // TODO(edd): There will likely be a faster unsafe way to do this.
        for chunks in row_ids.chunks_exact(4) {
            dst.push(self.transcoder.decode(self.values[chunks[0] as usize]));
            dst.push(self.transcoder.decode(self.values[chunks[1] as usize]));
            dst.push(self.transcoder.decode(self.values[chunks[2] as usize]));
            dst.push(self.transcoder.decode(self.values[chunks[3] as usize]));
        }

        let rem = row_ids.len() % 4;
        for &i in &row_ids[row_ids.len() - rem..row_ids.len()] {
            dst.push(self.transcoder.decode(self.values[i as usize]));
        }

        assert_eq!(dst.len(), row_ids.len());
        Either::Left(dst)
    }

    fn all_values(&self) -> Either<Vec<L>, Vec<Option<L>>> {
        let mut dst = Vec::with_capacity(self.num_rows() as usize);

        for chunks in self.values.chunks_exact(4) {
            dst.push(self.transcoder.decode(chunks[0]));
            dst.push(self.transcoder.decode(chunks[1]));
            dst.push(self.transcoder.decode(chunks[2]));
            dst.push(self.transcoder.decode(chunks[3]));
        }

        for &v in &self.values[dst.len()..self.values.len()] {
            dst.push(self.transcoder.decode(v));
        }

        assert_eq!(dst.len(), self.values.len());
        Either::Left(dst)
    }

    fn count(&self, row_ids: &[u32]) -> u32 {
        row_ids.len() as u32
    }

    fn sum(&self, row_ids: &[u32]) -> Option<L>
    where
        L: AddAssign,
    {
        let mut result = L::default(); // Add up logical types not physical

        // TODO(edd): There may be a faster unsafe way to do this.
        for chunks in row_ids.chunks_exact(4) {
            result += self.transcoder.decode(self.values[chunks[3] as usize]);
            result += self.transcoder.decode(self.values[chunks[2] as usize]);
            result += self.transcoder.decode(self.values[chunks[1] as usize]);
            result += self.transcoder.decode(self.values[chunks[0] as usize]);
        }

        let rem = row_ids.len() % 4;
        for &i in &row_ids[row_ids.len() - rem..row_ids.len()] {
            result += self.transcoder.decode(self.values[i as usize]);
        }

        Some(result)
    }

    fn min(&self, row_ids: &[u32]) -> Option<L> {
        // find smallest physical type
        let mut min = self.values[row_ids[0] as usize];
        for &v in row_ids.iter().skip(1) {
            if self.values[v as usize] < min {
                min = self.values[v as usize];
            }
        }

        // convert to logical type
        Some(self.transcoder.decode(min))
    }

    /// Returns the maximum logical (decoded) value from the provided row IDs.
    fn max(&self, row_ids: &[u32]) -> Option<L> {
        // find smallest physical type
        let mut max = self.values[row_ids[0] as usize];
        for &v in row_ids.iter().skip(1) {
            if self.values[v as usize] > max {
                max = self.values[v as usize];
            }
        }

        // convert to logical type
        Some(self.transcoder.decode(max))
    }

    fn row_ids_filter(&self, value: L, op: &cmp::Operator, dst: RowIDs) -> RowIDs {
        let value = self.transcoder.encode(value);
        match op {
            cmp::Operator::GT => self.row_ids_cmp_order(&value, PartialOrd::gt, dst),
            cmp::Operator::GTE => self.row_ids_cmp_order(&value, PartialOrd::ge, dst),
            cmp::Operator::LT => self.row_ids_cmp_order(&value, PartialOrd::lt, dst),
            cmp::Operator::LTE => self.row_ids_cmp_order(&value, PartialOrd::le, dst),
            _ => self.row_ids_equal(&value, op, dst),
        }
    }

    fn row_ids_filter_range(
        &self,
        left: (L, &cmp::Operator),
        right: (L, &cmp::Operator),
        dst: RowIDs,
    ) -> RowIDs {
        let left = (self.transcoder.encode(left.0), left.1);
        let right = (self.transcoder.encode(right.0), right.1);

        match (&left.1, &right.1) {
            (cmp::Operator::GT, cmp::Operator::LT)
            | (cmp::Operator::GT, cmp::Operator::LTE)
            | (cmp::Operator::GTE, cmp::Operator::LT)
            | (cmp::Operator::GTE, cmp::Operator::LTE)
            | (cmp::Operator::LT, cmp::Operator::GT)
            | (cmp::Operator::LT, cmp::Operator::GTE)
            | (cmp::Operator::LTE, cmp::Operator::GT)
            | (cmp::Operator::LTE, cmp::Operator::GTE) => self.row_ids_cmp_range_order(
                (&left.0, Self::ord_from_op(left.1)),
                (&right.0, Self::ord_from_op(right.1)),
                dst,
            ),

            (a, b) => panic!("unsupported operators provided: ({:?}, {:?})", a, b),
        }
    }
}

#[cfg(test)]
mod test {
    use std::sync::Arc;

    use super::super::transcoders::{MockTranscoder, NoOpTranscoder};
    use super::cmp::Operator;
    use super::*;

    // Helper function to create a new Fixed encoding using a mock transcoder
    // that will allow tests to track calls to encode/decode.
    fn new_encoding(
        values: Vec<i64>,
    ) -> (Fixed<i64, i64, Arc<MockTranscoder>>, Arc<MockTranscoder>) {
        let mock = Arc::new(MockTranscoder::default());
        (Fixed::new(values, Arc::clone(&mock)), mock)
    }

    #[test]
    fn size() {
        let (v, _) = new_encoding(vec![22_i64, 1, 18]);
        // Self if 32 bytes and there are 3 * 8b values
        assert_eq!(v.size(false), 56);

        // check pre-allocated sizing
        let (mut v, _) = new_encoding(vec![]);
        v.values.reserve_exact(40);
        // Self if 32 bytes and there are 40 * 8b values allocated
        assert_eq!(v.size(true), 352);
    }

    #[test]
    fn value() {
        let (v, transcoder) = new_encoding(vec![22, 1, 18]);
        assert_eq!(v.value(2), Some(18_i64));
        assert_eq!(transcoder.decodings(), 1);
    }

    #[test]
    fn values() {
        let (v, transcoder) = new_encoding((0..10).collect::<Vec<i64>>());

        assert_eq!(v.values(&[0, 1, 2, 3]).unwrap_left(), vec![0, 1, 2, 3]);
        assert_eq!(transcoder.decodings(), 4);

        assert_eq!(
            v.values(&[0, 1, 2, 3, 4]).unwrap_left(),
            vec![0, 1, 2, 3, 4]
        );
        assert_eq!(
            v.values(&(0..10).collect::<Vec<_>>()).unwrap_left(),
            vec![0, 1, 2, 3, 4, 5, 6, 7, 8, 9]
        );
    }

    #[test]
    fn all_values() {
        let (v, transcoder) = new_encoding((0..10).collect::<Vec<_>>());

        assert_eq!(v.all_values(), Either::Left((0..10).collect::<Vec<_>>()));
        assert_eq!(transcoder.decodings(), 10);
    }

    #[test]
    fn sum() {
        let (v, transcoder) = new_encoding((0..10).collect::<Vec<_>>());

        assert_eq!(v.sum(&[3, 5, 6, 7]), Some(21));
        assert_eq!(transcoder.decodings(), 4);
        assert_eq!(v.sum(&[1, 2, 4, 7, 9]), Some(23));
    }

    #[test]
    fn min() {
        let (v, transcoder) = new_encoding((0..10).collect::<Vec<_>>());

        assert_eq!(v.min(&[0, 1, 2, 3, 4]), Some(0));
        assert_eq!(transcoder.decodings(), 1); // only min value decoded
    }

    #[test]
    fn max() {
        let (v, transcoder) = new_encoding((0..10).collect::<Vec<_>>());

        assert_eq!(v.max(&[0, 1, 2, 3, 4]), Some(4));
        assert_eq!(transcoder.decodings(), 1); // only max value decoded
    }

    #[test]
    fn ord_from_op() {
        assert_eq!(
            Fixed::<i64, i64, NoOpTranscoder>::ord_from_op(&cmp::Operator::LT),
            (Ordering::Less, Ordering::Less)
        );

        assert_eq!(
            Fixed::<i64, i64, NoOpTranscoder>::ord_from_op(&cmp::Operator::GT),
            (Ordering::Greater, Ordering::Greater)
        );

        assert_eq!(
            Fixed::<i64, i64, NoOpTranscoder>::ord_from_op(&cmp::Operator::LTE),
            (Ordering::Less, Ordering::Equal)
        );

        assert_eq!(
            Fixed::<i64, i64, NoOpTranscoder>::ord_from_op(&cmp::Operator::GTE),
            (Ordering::Greater, Ordering::Equal)
        );
    }

    #[test]
    fn row_ids_filter_eq() {
        let (v, transcoder) = new_encoding(vec![
            100, 101, 100, 102, 1000, 300, 2030, 3, 101, 4, 5, 21, 100,
        ]);

        let dst = v.row_ids_filter(100, &Operator::Equal, RowIDs::new_vector());
        assert_eq!(dst.unwrap_vector(), &vec![0, 2, 12]);
        assert_eq!(transcoder.encodings(), 1);

        let dst = v.row_ids_filter(101, &Operator::Equal, RowIDs::new_vector());
        assert_eq!(dst.unwrap_vector(), &vec![1, 8]);

        let dst = v.row_ids_filter(2030, &Operator::Equal, RowIDs::new_vector());
        assert_eq!(dst.unwrap_vector(), &vec![6]);

        let dst = v.row_ids_filter(194, &Operator::Equal, RowIDs::new_vector());
        assert!(dst.is_empty());
    }

    #[test]
    fn row_ids_filter_neq() {
        let (v, transcoder) = new_encoding(vec![
            100, 101, 100, 102, 1000, 300, 2030, 3, 101, 4, 5, 21, 100,
        ]);

        let dst = v.row_ids_filter(100, &Operator::NotEqual, RowIDs::new_vector());
        assert_eq!(dst.unwrap_vector(), &vec![1, 3, 4, 5, 6, 7, 8, 9, 10, 11]);
        assert_eq!(transcoder.encodings(), 1);

        let dst = v.row_ids_filter(101, &Operator::NotEqual, RowIDs::new_vector());
        assert_eq!(
            dst.unwrap_vector(),
            &vec![0, 2, 3, 4, 5, 6, 7, 9, 10, 11, 12]
        );

        let dst = v.row_ids_filter(2030, &Operator::NotEqual, RowIDs::new_vector());
        assert_eq!(
            dst.unwrap_vector(),
            &vec![0, 1, 2, 3, 4, 5, 7, 8, 9, 10, 11, 12]
        );

        let dst = v.row_ids_filter(194, &Operator::NotEqual, RowIDs::new_vector());
        assert_eq!(dst.unwrap_vector(), &(0..13).collect::<Vec<u32>>());
    }

    #[test]
    fn row_ids_filter_lt() {
        let (v, transcoder) = new_encoding(vec![
            100, 101, 100, 102, 1000, 300, 2030, 3, 101, 4, 5, 21, 100,
        ]);

        let dst = v.row_ids_filter(100, &Operator::LT, RowIDs::new_vector());
        assert_eq!(dst.unwrap_vector(), &vec![7, 9, 10, 11]);
        assert_eq!(transcoder.encodings(), 1);

        let dst = v.row_ids_filter(3, &Operator::LT, RowIDs::new_vector());
        assert_eq!(dst.unwrap_vector(), &Vec::<u32>::new());
    }

    #[test]
    fn row_ids_filter_lte() {
        let (v, transcoder) = new_encoding(vec![
            100, 101, 100, 102, 1000, 300, 2030, 3, 101, 4, 5, 21, 100,
        ]);

        let dst = v.row_ids_filter(100, &Operator::LTE, RowIDs::new_vector());
        assert_eq!(dst.unwrap_vector(), &vec![0, 2, 7, 9, 10, 11, 12]);
        assert_eq!(transcoder.encodings(), 1);

        let dst = v.row_ids_filter(2, &Operator::LTE, RowIDs::new_vector());
        assert!(dst.is_empty());
    }

    #[test]
    fn row_ids_filter_gt() {
        let (v, transcoder) = new_encoding(vec![
            100, 101, 100, 102, 1000, 300, 2030, 3, 101, 4, 5, 21, 100,
        ]);

        let dst = v.row_ids_filter(100, &Operator::GT, RowIDs::new_vector());
        assert_eq!(dst.unwrap_vector(), &vec![1, 3, 4, 5, 6, 8]);
        assert_eq!(transcoder.encodings(), 1);

        let dst = v.row_ids_filter(2030, &Operator::GT, RowIDs::new_vector());
        assert!(dst.is_empty());
    }

    #[test]
    fn row_ids_filter_gte() {
        let (v, transcoder) = new_encoding(vec![
            100, 101, 100, 102, 1000, 300, 2030, 3, 101, 4, 5, 21, 100,
        ]);

        let dst = v.row_ids_filter(100, &Operator::GTE, RowIDs::new_vector());
        assert_eq!(dst.unwrap_vector(), &vec![0, 1, 2, 3, 4, 5, 6, 8, 12]);
        assert_eq!(transcoder.encodings(), 1);

        let dst = v.row_ids_filter(2031, &Operator::GTE, RowIDs::new_vector());
        assert!(dst.is_empty());
    }

    #[test]
    fn row_ids_filter_range() {
        let (v, transcoder) = new_encoding(vec![
            100, 101, 100, 102, 1000, 300, 2030, 3, 101, 4, 5, 21, 100,
        ]);

        let dst = v.row_ids_filter_range(
            (100, &Operator::GTE),
            (240, &Operator::LT),
            RowIDs::new_vector(),
        );
        assert_eq!(dst.unwrap_vector(), &vec![0, 1, 2, 3, 8, 12]);
        assert_eq!(transcoder.encodings(), 2); // encode both operators

        let dst = v.row_ids_filter_range(
            (100, &Operator::GT),
            (240, &Operator::LT),
            RowIDs::new_vector(),
        );
        assert_eq!(dst.unwrap_vector(), &vec![1, 3, 8]);

        let dst = v.row_ids_filter_range(
            (10, &Operator::LT),
            (-100, &Operator::GT),
            RowIDs::new_vector(),
        );
        assert_eq!(dst.unwrap_vector(), &vec![7, 9, 10]);

        let dst = v.row_ids_filter_range(
            (21, &Operator::GTE),
            (21, &Operator::LTE),
            RowIDs::new_vector(),
        );
        assert_eq!(dst.unwrap_vector(), &vec![11]);

        let dst = v.row_ids_filter_range(
            (10000, &Operator::LTE),
            (3999, &Operator::GT),
            RowIDs::new_bitmap(),
        );
        assert!(dst.is_empty());

        let (v, _) = new_encoding(vec![100, 200, 300, 2, 200, 22, 30]);

        let dst = v.row_ids_filter_range(
            (200, &Operator::GTE),
            (300, &Operator::LTE),
            RowIDs::new_vector(),
        );
        assert_eq!(dst.unwrap_vector(), &vec![1, 2, 4]);
    }
}
