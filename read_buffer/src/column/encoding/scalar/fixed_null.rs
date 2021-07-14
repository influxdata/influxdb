//! An encoding for fixed width, nullable values backed by Arrow arrays.
//!
//! This encoding stores a column of fixed-width numerical values backed by an
//! an Arrow array, allowing for storage of NULL values.
use either::Either;
use std::fmt::Debug;
use std::marker::PhantomData;
use std::mem::size_of;
use std::ops::Add;
use std::{
    cmp::Ordering,
    fmt::{Display, Formatter},
};

use arrow::{
    array::{Array, PrimitiveArray},
    datatypes::ArrowNumericType,
};

use super::transcoders::Transcoder;
use super::ScalarEncoding;
use crate::column::{cmp, RowIDs};

pub const ENCODING_NAME: &str = "FIXEDN";

#[derive(Debug, PartialEq)]
/// Types are: Physical, Logical, Transcoder
pub struct FixedNull<P, L, T>
where
    P: ArrowNumericType,
    P::Native: PartialEq + PartialOrd,
{
    // backing data
    arr: PrimitiveArray<P>,

    // The transcoder is responsible for converting from physical type `P` to
    // logical type `L`.
    transcoder: T,
    _marker: PhantomData<L>,
}

impl<P, L, T> Display for FixedNull<P, L, T>
where
    P: ArrowNumericType + Debug + Send + Sync,
    L: Add<Output = L> + Debug + Default + Send + Sync,
    T: Transcoder<P::Native, L> + Send + Sync,
{
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "[{}] rows: {:?}, nulls: {:?}, size: {}",
            self.name(),
            self.arr.len(),
            self.arr.null_count(),
            self.size()
        )
    }
}

impl<P, L, T> FixedNull<P, L, T>
where
    P: ArrowNumericType + Debug + Send + Sync,
    L: Add<Output = L> + Debug + Default + Send + Sync,
    T: Transcoder<P::Native, L> + Send + Sync,
{
    /// Initialise a new FixedNull encoding from an Arrow array and a transcoder
    /// to define how to convert stored physical types to logical columns types.
    pub fn new(arr: PrimitiveArray<P>, transcoder: T) -> Self {
        Self {
            arr,
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
    fn row_ids_equal(&self, value: P::Native, op: &cmp::Operator, mut dst: RowIDs) -> RowIDs {
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
        value: P::Native,
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
        left: (P::Native, (std::cmp::Ordering, std::cmp::Ordering)),
        right: (P::Native, (std::cmp::Ordering, std::cmp::Ordering)),
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
}

impl<P, L, T> ScalarEncoding<L> for FixedNull<P, L, T>
where
    P: ArrowNumericType + Debug + Send + Sync,
    L: Add<Output = L> + Debug + Default + Send + Sync,
    T: Transcoder<P::Native, L> + Send + Sync,
{
    /// The name of this encoding.
    fn name(&self) -> &'static str {
        ENCODING_NAME
    }

    fn num_rows(&self) -> u32 {
        self.arr.len() as u32
    }

    fn has_any_non_null_value(&self) -> bool {
        self.arr.null_count() < self.num_rows() as usize
    }

    fn has_non_null_value(&self, row_ids: &[u32]) -> bool {
        if self.null_count() == 0 && self.num_rows() > 0 {
            return true;
        }

        row_ids.iter().any(|id| !self.arr.is_null(*id as usize))
    }

    fn null_count(&self) -> u32 {
        self.arr.null_count() as u32
    }

    fn size(&self) -> usize {
        size_of::<Self>() + self.arr.get_array_memory_size()
    }

    /// The estimated total size in bytes of the underlying values in the
    /// column if they were stored contiguously and uncompressed. `include_nulls`
    /// will effectively size each NULL value as the size of an `L` if `true`.
    fn size_raw(&self, include_nulls: bool) -> usize {
        let base_size = size_of::<Vec<L>>();
        if self.null_count() == 0 || include_nulls {
            return base_size + (self.num_rows() as usize * size_of::<L>());
        }
        base_size + ((self.num_rows() as usize - self.arr.null_count()) * size_of::<L>())
    }

    fn value(&self, row_id: u32) -> Option<L> {
        if self.arr.is_null(row_id as usize) {
            return None;
        }
        Some(self.transcoder.decode(self.arr.value(row_id as usize)))
    }

    /// TODO(edd): Perf - we could return a vector of values and a vector of
    /// integers representing the null validity bitmap.
    fn values(&self, row_ids: &[u32]) -> Either<Vec<L>, Vec<Option<L>>> {
        let mut dst = Vec::with_capacity(row_ids.len());

        for &row_id in row_ids {
            if self.arr.is_null(row_id as usize) {
                dst.push(None)
            } else {
                dst.push(Some(
                    self.transcoder.decode(self.arr.value(row_id as usize)),
                ))
            }
        }
        assert_eq!(dst.len(), row_ids.len());
        Either::Right(dst)
    }

    /// TODO(edd): Perf - we could return a vector of values and a vector of
    /// integers representing the null validity bitmap.
    fn all_values(&self) -> Either<Vec<L>, Vec<Option<L>>> {
        let mut dst = Vec::with_capacity(self.num_rows() as usize);

        for i in 0..self.num_rows() as usize {
            if self.arr.is_null(i) {
                dst.push(None)
            } else {
                dst.push(Some(self.transcoder.decode(self.arr.value(i))))
            }
        }
        assert_eq!(dst.len(), self.num_rows() as usize);
        Either::Right(dst)
    }

    fn count(&self, row_ids: &[u32]) -> u32 {
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

    /// TODO(edd): I have experimented with using the Arrow kernels for these
    /// aggregations methods but they're currently significantly slower than
    /// this implementation (about 85% in the `sum` case). I will revisit/ work
    /// on them them in the future.
    fn sum(&self, row_ids: &[u32]) -> Option<L> {
        let mut result = L::default();

        if self.arr.null_count() == 0 {
            for chunks in row_ids.chunks_exact(4) {
                result = result + self.transcoder.decode(self.arr.value(chunks[3] as usize));
                result = result + self.transcoder.decode(self.arr.value(chunks[2] as usize));
                result = result + self.transcoder.decode(self.arr.value(chunks[1] as usize));
                result = result + self.transcoder.decode(self.arr.value(chunks[0] as usize));
            }

            let rem = row_ids.len() % 4;
            for &i in &row_ids[row_ids.len() - rem..row_ids.len()] {
                result = result + self.transcoder.decode(self.arr.value(i as usize));
            }

            return Some(result);
        }

        let mut is_none = true;
        for &i in row_ids {
            if self.arr.is_null(i as usize) {
                continue;
            }
            is_none = false;
            result = result + self.transcoder.decode(self.arr.value(i as usize));
        }

        if is_none {
            return None;
        }
        Some(result)
    }

    fn min(&self, row_ids: &[u32]) -> Option<L> {
        // find the minimum physical value.
        let mut min: Option<P::Native> =
            (!self.arr.is_null(row_ids[0] as usize)).then(|| self.arr.value(row_ids[0] as usize));
        for &row_id in row_ids.iter().skip(1) {
            if self.arr.is_null(row_id as usize) {
                continue;
            }

            let next = Some(self.arr.value(row_id as usize));
            if next < min {
                min = next;
            }
        }

        // convert minimum physical value to logical value.
        min.map(|v| self.transcoder.decode(v))
    }

    /// Returns the maximum logical (decoded) non-null value from the provided
    /// row IDs.
    fn max(&self, row_ids: &[u32]) -> Option<L> {
        // find the maximum physical value.
        let mut max: Option<P::Native> =
            (!self.arr.is_null(row_ids[0] as usize)).then(|| self.arr.value(row_ids[0] as usize));
        for &row_id in row_ids.iter().skip(1) {
            if self.arr.is_null(row_id as usize) {
                continue;
            }

            let next = Some(self.arr.value(row_id as usize));
            if next > max {
                max = next;
            }
        }

        // convert minimum physical value to logical value.
        max.map(|v| self.transcoder.decode(v))
    }

    fn row_ids_filter(&self, value: L, op: &cmp::Operator, dst: RowIDs) -> RowIDs {
        let value = self.transcoder.encode(value);
        match op {
            cmp::Operator::GT => self.row_ids_cmp_order(value, Self::ord_from_op(&op), dst),
            cmp::Operator::GTE => self.row_ids_cmp_order(value, Self::ord_from_op(&op), dst),
            cmp::Operator::LT => self.row_ids_cmp_order(value, Self::ord_from_op(&op), dst),
            cmp::Operator::LTE => self.row_ids_cmp_order(value, Self::ord_from_op(&op), dst),
            _ => self.row_ids_equal(value, op, dst),
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
}

#[cfg(test)]
mod test {
    use std::sync::Arc;

    use arrow::datatypes::*;

    use super::super::transcoders::MockTranscoder;
    use super::cmp::Operator;
    use super::*;

    // Helper function to create a new FixedNull encoding using a mock transcoder
    // that will allow tests to track calls to encode/decode.
    fn new_encoding(
        values: Vec<Option<i64>>,
    ) -> (
        FixedNull<Int64Type, i64, Arc<MockTranscoder>>,
        Arc<MockTranscoder>,
    ) {
        let arr = PrimitiveArray::from(values);
        let mock = Arc::new(MockTranscoder::default());
        (FixedNull::new(arr, Arc::clone(&mock)), mock)
    }

    fn some_vec<T: Copy>(v: Vec<T>) -> Vec<Option<T>> {
        v.iter().map(|x| Some(*x)).collect()
    }

    #[test]
    fn size() {
        let (v, _) = new_encoding(vec![None, None, Some(100), Some(2222)]);
        assert_eq!(v.size(), 408);
    }

    #[test]
    fn size_raw() {
        let (v, _) = new_encoding(vec![None, None, Some(100), Some(2222)]);
        // values   = 4 * 8 = 32b
        // Vec<u64> = 24b
        assert_eq!(v.size_raw(true), 56);
        assert_eq!(v.size_raw(false), 40);

        let (v, _) = new_encoding(vec![None, None]);
        assert_eq!(v.size_raw(true), 40);
        assert_eq!(v.size_raw(false), 24);

        let (v, _) = new_encoding(vec![None, None, Some(22)]);
        assert_eq!(v.size_raw(true), 48);
        assert_eq!(v.size_raw(false), 32);
    }

    #[test]
    fn value() {
        let (v, transcoder) = new_encoding(vec![Some(22), Some(33), Some(18)]);
        assert_eq!(v.value(2), Some(18));
        assert_eq!(transcoder.decodings(), 1);
    }

    #[test]
    fn values() {
        let (v, transcoder) = new_encoding((0..10).map(Option::Some).collect::<Vec<_>>());

        assert_eq!(
            v.values(&[0, 1, 2, 3]).unwrap_right(),
            some_vec(vec![0, 1, 2, 3])
        );
        assert_eq!(transcoder.decodings(), 4);

        assert_eq!(
            v.values(&[0, 1, 2, 3, 4]).unwrap_right(),
            some_vec(vec![0, 1, 2, 3, 4])
        );
        assert_eq!(
            v.values(&(0..10).collect::<Vec<_>>()).unwrap_right(),
            some_vec(vec![0, 1, 2, 3, 4, 5, 6, 7, 8, 9])
        );
    }

    #[test]
    fn all_values() {
        let (v, transcoder) = new_encoding((0..10).map(Option::Some).collect::<Vec<_>>());

        assert_eq!(
            v.all_values(),
            Either::Right((0..10).map(Some).collect::<Vec<_>>())
        );
        assert_eq!(transcoder.decodings(), 10);
    }

    #[test]
    fn count() {
        let data = vec![Some(0), None, Some(22), None, None, Some(33), Some(44)];
        let (v, _) = new_encoding(data);

        assert_eq!(v.count(&[0, 1, 2, 3, 4, 5, 6]), 4);
        assert_eq!(v.count(&[1, 3]), 0);
        assert_eq!(v.count(&[6]), 1);
    }

    #[test]
    fn sum() {
        let (v, transcoder) = new_encoding((0..10).map(Option::Some).collect::<Vec<_>>());

        assert_eq!(v.sum(&[3, 5, 6, 7]), Some(21));
        assert_eq!(transcoder.decodings(), 4);
        assert_eq!(v.sum(&[1, 2, 4, 7, 9]), Some(23));
    }

    #[test]
    fn min() {
        let data = vec![Some(100), Some(110), Some(20), Some(1), Some(110)];
        let (v, transcoder) = new_encoding(data);

        assert_eq!(v.min(&[0, 1, 2, 3, 4]), Some(1));
        assert_eq!(transcoder.decodings(), 1); // only min is decoded
    }

    #[test]
    fn max() {
        let data = vec![Some(100), Some(110), Some(20), Some(1), Some(109)];
        let (v, transcoder) = new_encoding(data);
        assert_eq!(v.max(&[0, 1, 2, 3, 4]), Some(110));
        assert_eq!(transcoder.decodings(), 1); // only max is decoded
    }

    #[test]
    fn row_ids_filter_eq() {
        let (v, transcoder) = new_encoding(
            vec![100, 101, 100, 102, 1000, 300, 2030, 3, 101, 4, 5, 21, 100]
                .into_iter()
                .map(Option::Some)
                .collect::<Vec<_>>(),
        );

        let row_ids = v.row_ids_filter(100, &Operator::Equal, RowIDs::new_vector());
        assert_eq!(row_ids.to_vec(), vec![0, 2, 12]);
        assert_eq!(transcoder.encodings(), 1);

        let row_ids = v.row_ids_filter(101, &Operator::Equal, RowIDs::new_vector());
        assert_eq!(row_ids.to_vec(), vec![1, 8]);

        let row_ids = v.row_ids_filter(2030, &Operator::Equal, RowIDs::new_vector());
        assert_eq!(row_ids.to_vec(), vec![6]);

        let row_ids = v.row_ids_filter(194, &Operator::Equal, RowIDs::new_vector());
        assert_eq!(row_ids.to_vec(), Vec::<u32>::new());
    }

    #[test]
    fn row_ids_filter_neq() {
        let (v, transcoder) = new_encoding(
            vec![100, 101, 100, 102, 1000, 300, 2030, 3, 101, 4, 5, 21, 100]
                .into_iter()
                .map(Option::Some)
                .collect::<Vec<_>>(),
        );

        let row_ids = v.row_ids_filter(100, &Operator::NotEqual, RowIDs::new_vector());
        assert_eq!(row_ids.to_vec(), vec![1, 3, 4, 5, 6, 7, 8, 9, 10, 11]);
        assert_eq!(transcoder.encodings(), 1);

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
        let (v, transcoder) = new_encoding(
            vec![100, 101, 100, 102, 1000, 300, 2030, 3, 101, 4, 5, 21, 100]
                .into_iter()
                .map(Option::Some)
                .collect::<Vec<_>>(),
        );

        let row_ids = v.row_ids_filter(100, &Operator::LT, RowIDs::new_vector());
        assert_eq!(row_ids.to_vec(), vec![7, 9, 10, 11]);
        assert_eq!(transcoder.encodings(), 1);

        let row_ids = v.row_ids_filter(3, &Operator::LT, RowIDs::new_vector());
        assert_eq!(row_ids.to_vec(), Vec::<u32>::new());
    }

    #[test]
    fn row_ids_filter_lte() {
        let (v, transcoder) = new_encoding(
            vec![100, 101, 100, 102, 1000, 300, 2030, 3, 101, 4, 5, 21, 100]
                .into_iter()
                .map(Option::Some)
                .collect::<Vec<_>>(),
        );
        let row_ids = v.row_ids_filter(100, &Operator::LTE, RowIDs::new_vector());
        assert_eq!(row_ids.to_vec(), vec![0, 2, 7, 9, 10, 11, 12]);
        assert_eq!(transcoder.encodings(), 1);

        let row_ids = v.row_ids_filter(2, &Operator::LTE, RowIDs::new_vector());
        assert_eq!(row_ids.to_vec(), Vec::<u32>::new());
    }

    #[test]
    fn row_ids_filter_gt() {
        let (v, transcoder) = new_encoding(
            vec![100, 101, 100, 102, 1000, 300, 2030, 3, 101, 4, 5, 21, 100]
                .into_iter()
                .map(Option::Some)
                .collect::<Vec<_>>(),
        );

        let row_ids = v.row_ids_filter(100, &Operator::GT, RowIDs::new_vector());
        assert_eq!(row_ids.to_vec(), vec![1, 3, 4, 5, 6, 8]);
        assert_eq!(transcoder.encodings(), 1);

        let row_ids = v.row_ids_filter(2030, &Operator::GT, RowIDs::new_vector());
        assert_eq!(row_ids.to_vec(), Vec::<u32>::new());
    }

    #[test]
    fn row_ids_filter_null() {
        let (v, transcoder) = new_encoding(vec![
            Some(100),
            Some(200),
            None,
            None,
            Some(200),
            Some(22),
            Some(30),
        ]);

        let row_ids = v.row_ids_filter(10, &Operator::GT, RowIDs::new_vector());
        assert_eq!(row_ids.to_vec(), vec![0, 1, 4, 5, 6]);
        assert_eq!(transcoder.encodings(), 1);

        let row_ids = v.row_ids_filter(30, &Operator::LTE, RowIDs::new_vector());
        assert_eq!(row_ids.to_vec(), vec![5, 6]);
    }

    #[test]
    fn row_ids_filter_gte() {
        let (v, transcoder) = new_encoding(
            vec![100, 101, 100, 102, 1000, 300, 2030, 3, 101, 4, 5, 21, 100]
                .into_iter()
                .map(Option::Some)
                .collect::<Vec<_>>(),
        );

        let row_ids = v.row_ids_filter(100, &Operator::GTE, RowIDs::new_vector());
        assert_eq!(row_ids.to_vec(), vec![0, 1, 2, 3, 4, 5, 6, 8, 12]);
        assert_eq!(transcoder.encodings(), 1);

        let row_ids = v.row_ids_filter(2031, &Operator::GTE, RowIDs::new_vector());
        assert_eq!(row_ids.to_vec(), Vec::<u32>::new());
    }

    #[test]
    fn row_ids_filter_range() {
        let (v, transcoder) = new_encoding(vec![
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
        ]);

        let row_ids = v.row_ids_filter_range(
            (100, &Operator::GTE),
            (240, &Operator::LT),
            RowIDs::new_vector(),
        );
        assert_eq!(row_ids.to_vec(), vec![0, 1, 5, 6, 13, 17]);
        assert_eq!(transcoder.encodings(), 2); // both literals encoded

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

        let (v, _) = new_encoding(vec![
            Some(100),
            Some(200),
            Some(300),
            Some(2),
            Some(200),
            Some(22),
            Some(30),
        ]);
        let row_ids = v.row_ids_filter_range(
            (200, &Operator::GTE),
            (300, &Operator::LTE),
            RowIDs::new_vector(),
        );
        assert_eq!(row_ids.to_vec(), vec![1, 2, 4]);
    }

    #[test]
    fn has_non_null_value() {
        let (v, _) = new_encoding(vec![None, None]);
        assert!(!v.has_non_null_value(&[0, 1]));

        let (v, _) = new_encoding(vec![Some(100), Some(222)]);
        assert!(v.has_non_null_value(&[0, 1]));
        assert!(v.has_non_null_value(&[1]));

        let (v, _) = new_encoding(vec![None, Some(100), Some(222)]);
        assert!(v.has_non_null_value(&[0, 1, 2]));
        assert!(!v.has_non_null_value(&[0]));
        assert!(v.has_non_null_value(&[2]));
    }

    #[test]
    fn has_any_non_null_value() {
        let (v, _) = new_encoding(vec![None, None]);
        assert!(!v.has_any_non_null_value());

        let (v, _) = new_encoding(vec![Some(100), Some(222)]);
        assert!(v.has_any_non_null_value());

        let (v, _) = new_encoding(vec![None, Some(100), Some(222)]);
        assert!(v.has_any_non_null_value());
    }
}
