use either::Either;

use crate::column::cmp;
use crate::column::RowIDs;
use std::{cmp::Ordering, fmt::Debug, iter, marker::PhantomData, mem::size_of};

use super::transcoders::Transcoder;
use super::ScalarEncoding;

pub const ENCODING_NAME: &str = "RLE";

#[allow(clippy::upper_case_acronyms)] // this looks weird as `Rle`
/// An RLE encoding is one where identical "runs" of values in the column are
/// stored as a tuple: `(run_length, value)`, where `run_length` indicates the
/// number of times the value is to be repeated.
///
/// Types are: Physical, Logcial, Transcoder
#[derive(Debug)]
pub struct RLE<P, L, T>
where
    P: PartialOrd + Debug,
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
    run_lengths: Vec<(u32, Option<P>)>,

    // number of NULL values in the column
    null_count: u32,

    // number of total logical rows (values) in the columns.
    num_rows: u32,

    // The transcoder is responsible for converting from physical type `P` to
    // logical type `L`.
    transcoder: T,
    _marker: PhantomData<L>,
}

impl<P, L, T> std::fmt::Display for RLE<P, L, T>
where
    P: Copy + Debug + PartialOrd + Send + Sync,
    L: Default + Debug + Copy + PartialEq + Send + Sync,
    T: Transcoder<P, L> + Send + Sync,
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
impl<P, L, T> RLE<P, L, T>
where
    P: Copy + Debug + PartialOrd + Send + Sync,
    L: Default + Debug + Copy + PartialEq + Send + Sync,
    T: Transcoder<P, L> + Send + Sync,
{
    /// Initialise a new RLE encoding from the provided iterator. The transcoder
    /// defines how to convert stored physical types to logical columns types.
    pub fn new_from_iter(data: impl Iterator<Item = P>, transcoder: T) -> Self {
        Self::new_from_iter_opt(data.map(Option::Some), transcoder)
    }

    /// Initialise a new RLE encoding from the provided iterator. The transcoder
    /// defines how to convert stored physical types to logical columns types.
    pub fn new_from_iter_opt(mut data: impl Iterator<Item = Option<P>>, transcoder: T) -> Self {
        let mut enc = Self {
            run_lengths: vec![],
            null_count: 0,
            num_rows: 0,
            transcoder,
            _marker: Default::default(),
        };

        let first = match data.next() {
            Some(v) => v,
            None => return enc,
        };

        let (mut rl, mut v) = (1, first);
        for next in data {
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

    /// Adds the provided string value to the encoded data. It is the caller's
    /// responsibility to ensure that the dictionary encoded remains sorted.
    pub fn push(&mut self, v: P) {
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
    pub fn push_additional(&mut self, v: Option<P>, additional: u32) {
        match v {
            Some(v) => self.push_additional_some(v, additional),
            None => self.push_additional_none(additional),
        }
    }

    fn push_additional_some(&mut self, v: P, additional: u32) {
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

    // TODO(edd): predicate filtering could be improved here because we need to
    //            effectively check all the run-lengths to find any that match
    //            the predicate. Performance will be proportional to the number
    //            of run-lengths in the column. For scalar columns where
    //            predicates are less common for our use-cases this is probably
    //            OK for a while. Improvements: (1) we could add bitsets of
    //            pre-computed rows for each distinct value, which is what we
    //            do for string RLE where equality predicates are very common,
    //            or (2) we could add a sparse index to allow us to jump to the
    //            run-lengths that contain matching logical row_ids.
    fn row_ids_cmp_equal(&self, value: P, op: &cmp::Operator, mut dst: RowIDs) -> RowIDs {
        dst.clear();

        if self.num_rows() == 0 {
            return dst;
        }

        // Given an operator `cmp` is a predicate function that applies it.
        let cmp = match op {
            cmp::Operator::Equal => {
                |left: &P, right: &P| matches!(left.partial_cmp(right), Some(Ordering::Equal))
            }
            cmp::Operator::NotEqual => {
                |left: &P, right: &P| !matches!(left.partial_cmp(right), Some(Ordering::Equal))
            }
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

    // Finds row ids for non-null values based on <, <=, > or >= operator.
    // TODO(edd): predicate filtering could be improved here because we need to
    //            effectively check all the run-lengths to find any that match
    //            the predicate. Performance will be proportional to the number
    //            of run-lengths in the column. For scalar columns where
    //            predicates are less common for out use-cases this is probably
    //            OK for a while. Improvements: (1) we could add bitsets of
    //            pre-computed rows for each distinct value, which is what we
    //            do for string RLE where predicates are very common.
    fn row_ids_cmp(&self, value: P, op: &cmp::Operator, mut dst: RowIDs) -> RowIDs {
        dst.clear();

        if self.num_rows() == 0 {
            return dst;
        }

        // Given an operator `cmp` is a predicate function that applies it.
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
    // `row_ids_filter` twice on predicates like `WHERE X > y and X <= x`
    // which are very common, e.g., for timestamp columns.
    //
    // The method accepts two orderings for each predicate. If the predicate is
    // `x < y` then the orderings provided will be
    // `(Ordering::Less, Ordering::Less)`. This does lead to a slight overhead
    // in checking non-matching values, but it means that the predicate `x <= y`
    // can be supported by providing the ordering
    // `(Ordering::Less, Ordering::Equal)`.
    //.
    fn row_ids_cmp_range(
        &self,
        left: (&P, (Ordering, Ordering)),
        right: (&P, (Ordering, Ordering)),
        mut dst: RowIDs,
    ) -> RowIDs {
        dst.clear();

        let left_op = left.1;
        let right_op = right.1;

        let mut curr_logical_row_id = 0;
        for (rl, next) in &self.run_lengths {
            if let Some(next) = next {
                // compare next value against the left and right's values
                let left_cmp_result = next.partial_cmp(left.0);
                let right_cmp_result = next.partial_cmp(right.0);

                // TODO(edd): eurgh I still don't understand how I got this to
                // be correct. Need to revisit to make it simpler.
                let left_result_ok =
                    !(left_cmp_result == Some(left_op.0) || left_cmp_result == Some(left_op.1));
                let right_result_ok =
                    !(right_cmp_result == Some(right_op.0) || right_cmp_result == Some(right_op.1));

                if !(left_result_ok || right_result_ok) {
                    dst.add_range(curr_logical_row_id, curr_logical_row_id + rl);
                }
                curr_logical_row_id += rl;
            }
        }

        dst
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

    // assertion helper to ensure invariant during debug builds.
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
}

impl<P, L, T> ScalarEncoding<L> for RLE<P, L, T>
where
    P: Copy + Debug + PartialOrd + Send + Sync,
    L: Debug + Default + Copy + PartialEq + Send + Sync,
    T: Transcoder<P, L> + Send + Sync,
{
    fn name(&self) -> &'static str {
        ENCODING_NAME
    }

    fn size(&self) -> usize {
        std::mem::size_of::<Self>() + (self.run_lengths.len() * size_of::<(u32, Option<P>)>())
    }

    fn size_raw(&self, include_nulls: bool) -> usize {
        let base_size = size_of::<Vec<L>>();
        if include_nulls {
            return base_size + (self.num_rows() as usize * size_of::<Option<L>>());
        }

        // remove NULL values from calculation
        base_size
            + self
                .run_lengths
                .iter()
                .filter_map(|(rl, v)| v.is_some().then(|| *rl as usize * size_of::<Option<L>>()))
                .sum::<usize>()
    }

    fn null_count(&self) -> u32 {
        self.null_count
    }

    fn num_rows(&self) -> u32 {
        self.num_rows
    }

    fn row_ids_filter(&self, value: L, op: &cmp::Operator, dst: RowIDs) -> RowIDs {
        let value = self.transcoder.encode(value);
        match op {
            cmp::Operator::Equal | cmp::Operator::NotEqual => {
                self.row_ids_cmp_equal(value, op, dst)
            }
            cmp::Operator::LT | cmp::Operator::LTE | cmp::Operator::GT | cmp::Operator::GTE => {
                self.row_ids_cmp(value, op, dst)
            }
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
            | (cmp::Operator::LTE, cmp::Operator::GTE) => self.row_ids_cmp_range(
                (&left.0, Self::ord_from_op(&left.1)),
                (&right.0, Self::ord_from_op(&right.1)),
                dst,
            ),

            (_, _) => panic!("unsupported operators provided"),
        }
    }

    /// TODO(edd): a sparse index on this can help with materialisation cost by
    /// providing starting indexes into the in the run length collection.
    fn value(&self, row_id: u32) -> Option<L> {
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
                return v.map(|v| self.transcoder.decode(v));
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
    fn values(&self, row_ids: &[u32]) -> Either<Vec<L>, Vec<Option<L>>> {
        assert!(!row_ids.is_empty(), "no row IDs provided");
        assert!(
            row_ids.len() <= self.num_rows() as usize,
            "more row_ids {:?} than rows {:?}",
            row_ids.len(),
            self.num_rows()
        );

        let mut dst = Vec::with_capacity(row_ids.len());

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
            dst.push(curr_value.map(|v| self.transcoder.decode(v)));

            curr_logical_row_id += 1; // move forwards a logical row
            curr_entry_rl -= 1;
        }

        assert_eq!(row_ids.len(), dst.len());
        Either::Right(dst)
    }

    fn all_values(&self) -> Either<Vec<L>, Vec<Option<L>>> {
        let mut dst = Vec::with_capacity(self.num_rows() as usize);

        for (rl, v) in &self.run_lengths {
            dst.extend(iter::repeat(v.map(|v| self.transcoder.decode(v))).take(*rl as usize));
        }
        Either::Right(dst)
    }

    fn has_non_null_value(&self, row_ids: &[u32]) -> bool {
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

    fn has_any_non_null_value(&self) -> bool {
        self.num_rows() - self.null_count() > 0
    }

    fn count(&self, _row_ids: &[u32]) -> u32 {
        todo!()
    }

    fn sum(&self, _row_ids: &[u32]) -> Option<L> {
        todo!()
    }

    fn min(&self, _row_ids: &[u32]) -> Option<L> {
        todo!()
    }

    fn max(&self, _row_ids: &[u32]) -> Option<L> {
        todo!()
    }
}

// This function returns an estimated size in bytes for an input slice of `T`
// were it to be run-length encoded.
pub fn estimated_size_from<T: PartialOrd>(arr: &[T]) -> usize {
    let run_lengths = arr.len()
        - arr
            .iter()
            .zip(arr.iter().skip(1))
            .filter(|(curr, next)| matches!(curr.partial_cmp(next), Some(Ordering::Equal)))
            .count();
    run_lengths * size_of::<(u32, Option<T>)>() + size_of::<Vec<(u32, Option<T>)>>()
}

// This function returns an estimated size in bytes for an input iterator
// yielding `Option<T>`, were it to be run-length encoded.
pub fn estimated_size_from_iter<T: PartialOrd>(mut itr: impl Iterator<Item = Option<T>>) -> usize {
    let mut v = match itr.next() {
        Some(v) => v,
        None => return 0,
    };

    let mut total_rows = 0;
    for next in itr {
        if let Some(Ordering::Equal) = v.partial_cmp(&next) {
            continue;
        }

        total_rows += 1;
        v = next;
    }

    // +1 to account for original run
    (total_rows + 1) * size_of::<(u32, Option<T>)>() + size_of::<Vec<(u32, Option<T>)>>()
}

#[cfg(test)]
mod test {
    use cmp::Operator;
    use std::sync::Arc;

    use super::super::transcoders::{MockTranscoder, NoOpTranscoder};
    use super::*;

    // Helper function to create a new RLE encoding using a mock transcoder
    // that will allow tests to track calls to encode/decode.
    fn new_encoding(values: Vec<i64>) -> (RLE<i64, i64, Arc<MockTranscoder>>, Arc<MockTranscoder>) {
        let mock = Arc::new(MockTranscoder::default());
        (
            RLE::new_from_iter(values.into_iter(), Arc::clone(&mock)),
            mock,
        )
    }

    #[test]
    fn new_from_iter() {
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
            let enc = RLE::new_from_iter(input.iter().cloned(), NoOpTranscoder {});
            assert_eq!(enc.run_lengths, exp_rl);
        }
    }

    #[test]
    fn from_iter_opt() {
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
            let enc = RLE::new_from_iter_opt(input.iter().cloned(), NoOpTranscoder {});
            assert_eq!(enc.run_lengths, exp_rl);
        }
    }

    #[test]
    fn push() {
        let (mut enc, _) = new_encoding(vec![]);
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
        let (mut enc, _) = new_encoding(vec![]);
        enc.push_additional(Some(45), 3);
        assert_eq!(enc.null_count(), 0);

        enc.push_none();
        assert_eq!(enc.null_count(), 1);

        enc.push_none();
        assert_eq!(enc.null_count(), 2);
    }

    #[test]
    fn num_rows() {
        let (mut enc, _) = new_encoding(vec![]);
        enc.push_additional(Some(45), 3);
        assert_eq!(enc.num_rows(), 3);

        enc.push_none();
        assert_eq!(enc.num_rows(), 4);
    }

    #[test]
    fn size() {
        let (mut enc, _) = new_encoding(vec![]);

        // 40b Self + (0 rl * 24) = 32
        assert_eq!(enc.size(), 40);

        enc.push_none();
        // 40b Self + (1 rl * 24) = 56
        assert_eq!(enc.size(), 64);

        enc.push_additional_some(1, 10);
        // 40b Self + (2 rl * 24) = 80
        assert_eq!(enc.size(), 88);
    }

    #[test]
    fn size_raw() {
        let (mut enc, _) = new_encoding(vec![]);

        // 24b Self + (0 * 16)
        assert_eq!(enc.size_raw(true), 24);
        assert_eq!(enc.size_raw(false), 24);

        enc.push_none();
        // 24b Self + (1 * 16) = 48
        assert_eq!(enc.size_raw(true), 40);
        assert_eq!(enc.size_raw(false), 24);

        enc.push_additional_some(1, 10);
        // 24b Self + (11 * 16) = 208
        assert_eq!(enc.size_raw(true), 200);
        assert_eq!(enc.size_raw(false), 184);
    }

    #[test]
    fn value() {
        let (mut enc, transcoder) = new_encoding(vec![]);
        enc.push_none();
        enc.push_additional(Some(45), 3);
        enc.push_additional(Some(90), 2);
        enc.push(21);

        assert_eq!(enc.value(0), None);
        assert_eq!(enc.value(1), Some(45));
        assert_eq!(enc.value(3), Some(45));
        assert_eq!(enc.value(4), Some(90));
        assert_eq!(enc.value(6), Some(21));
        assert_eq!(transcoder.decodings(), 4); // don't need to decode None
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

        let (enc, _) = new_encoding(vec![]);

        for (input, exp) in cases {
            assert_eq!(enc.check_row_ids_ordered(input), exp);
        }
    }

    #[test]
    fn values() {
        let (mut enc, transcoder) = new_encoding(vec![]);
        enc.push_none();
        enc.push_additional(Some(45), 3);
        enc.push_additional(Some(90), 2);
        enc.push(21);

        // ensure buffer cleared by populating it
        assert_eq!(
            enc.values(&[0, 1, 2]).unwrap_right(),
            vec![None, Some(45), Some(45)]
        );
        assert_eq!(transcoder.decodings(), 2); // None doesn't get decoded

        assert_eq!(
            enc.values(&[0, 1, 2, 3, 4]).unwrap_right(),
            vec![None, Some(45), Some(45), Some(45), Some(90)]
        );

        assert_eq!(enc.values(&[2, 5]).unwrap_right(), vec![Some(45), Some(90)]);
    }

    #[test]
    fn all_values() {
        let (mut enc, transcoder) = new_encoding(vec![]);
        assert!(enc.all_values().unwrap_right().is_empty());
        assert_eq!(transcoder.decodings(), 0);

        enc.push_additional(Some(45), 3);
        enc.push_additional(Some(90), 2);
        enc.push_additional(None, 2);
        enc.push(21);

        assert_eq!(
            enc.all_values().unwrap_right(),
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
        // Only three distinct non-null values to decode
        assert_eq!(transcoder.decodings(), 3);

        let (mut enc, _) = new_encoding(vec![]);
        enc.push_none();
        assert_eq!(enc.all_values().unwrap_right(), vec![None]);
    }

    #[test]
    fn has_non_null_value() {
        let (mut enc, _) = new_encoding(vec![]);
        enc.push_none();
        assert!(!enc.has_non_null_value(&[0]));

        enc.push_additional(Some(45), 3);
        assert!(!enc.has_non_null_value(&[0]));
        assert!(enc.has_non_null_value(&[0, 1]));
        assert!(enc.has_non_null_value(&[2, 3]));
        assert!(enc.has_non_null_value(&[0, 1, 2, 3]));

        enc.push_additional(None, 3);
        assert!(!enc.has_non_null_value(&[0, 5]));
        assert!(!enc.has_non_null_value(&[6]));

        // NULL, 45, 45, 45, NULL, NULL, NULL, 19
        enc.push(19);
        assert!(enc.has_non_null_value(&[3, 7]));
        assert!(enc.has_non_null_value(&[7]));
    }

    #[test]
    fn has_any_non_null_value() {
        let (mut enc, _) = new_encoding(vec![]);
        assert!(!enc.has_any_non_null_value());

        enc.push_none();
        assert!(!enc.has_any_non_null_value());

        enc.push_additional(None, 3);
        assert!(!enc.has_any_non_null_value());

        enc.push(22);
        assert!(enc.has_any_non_null_value());

        enc.push_additional(None, 3);
        assert!(enc.has_any_non_null_value());
    }

    #[test]
    fn row_ids_filter() {
        // NULL, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, NULL, 19, 2, 2
        let (mut enc, transcoder) = new_encoding(vec![]);
        enc.push_none();
        enc.push_additional_some(2, 10);
        enc.push_none();
        enc.push_additional_some(19, 1);
        enc.push_additional_some(2, 2);

        let all_non_null = vec![1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 12, 13, 14];
        let cases = vec![
            (22, Operator::Equal, vec![]),
            (22, Operator::NotEqual, all_non_null.clone()),
            (22, Operator::LT, all_non_null.clone()),
            (22, Operator::LTE, all_non_null.clone()),
            (22, Operator::GT, vec![]),
            (22, Operator::GTE, vec![]),
            (0, Operator::Equal, vec![]),
            (0, Operator::NotEqual, all_non_null.clone()),
            (0, Operator::LT, vec![]),
            (0, Operator::LTE, vec![]),
            (0, Operator::GT, all_non_null.clone()),
            (0, Operator::GTE, all_non_null.clone()),
            (
                2,
                Operator::Equal,
                vec![1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 13, 14],
            ),
            (2, Operator::NotEqual, vec![12]),
            (2, Operator::LT, vec![]),
            (
                2,
                Operator::LTE,
                vec![1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 13, 14],
            ),
            (2, Operator::GT, vec![12]),
            (2, Operator::GTE, all_non_null.clone()),
            (19, Operator::Equal, vec![12]),
            (
                19,
                Operator::NotEqual,
                vec![1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 13, 14],
            ),
            (
                19,
                Operator::LT,
                vec![1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 13, 14],
            ),
            (19, Operator::LTE, all_non_null),
            (19, Operator::GT, vec![]),
            (19, Operator::GTE, vec![12]),
        ];

        let calls = cases.len();
        for (v, op, exp) in cases {
            let dst = enc.row_ids_filter(v, &op, RowIDs::new_vector());
            assert_eq!(dst.unwrap_vector(), &exp, "example '{} {:?}' failed", op, v);
        }
        assert_eq!(transcoder.encodings(), calls);
    }

    #[test]
    fn row_ids_filter_nulls() {
        // Test all null encoding
        let (mut enc, _) = new_encoding(vec![]);

        enc.push_additional_none(10);

        let cases = vec![
            (22, Operator::Equal),
            (22, Operator::NotEqual),
            (22, Operator::LT),
            (22, Operator::LTE),
            (22, Operator::GT),
            (22, Operator::GTE),
        ];

        for (v, op) in cases {
            let dst = enc.row_ids_filter(v, &op, RowIDs::new_vector());
            assert!(
                dst.unwrap_vector().is_empty(),
                "example '{} {:?}' failed",
                op,
                v,
            );
        }
    }

    #[test]
    fn row_ids_filter_range() {
        let (enc, transcoder) = new_encoding(vec![
            100, 100, 101, 101, 101, 200, 300, 2030, 3, 101, 4, 5, 21, 100,
        ]);

        let cases = vec![
            (
                (100, &Operator::GTE),
                (240, &Operator::LT),
                vec![0, 1, 2, 3, 4, 5, 9, 13],
            ),
            (
                (100, &Operator::GT),
                (240, &Operator::LT),
                vec![2, 3, 4, 5, 9],
            ),
            ((10, &Operator::LT), (-100, &Operator::GT), vec![8, 10, 11]),
            ((21, &Operator::GTE), (21, &Operator::LTE), vec![12]),
            (
                (101, &Operator::GTE),
                (2030, &Operator::LTE),
                vec![2, 3, 4, 5, 6, 7, 9],
            ),
            ((10000, &Operator::LTE), (3999, &Operator::GT), vec![]),
        ];

        let calls = cases.len();
        for (left, right, exp) in cases {
            let dst = enc.row_ids_filter_range(left, right, RowIDs::new_vector());
            assert_eq!(dst.unwrap_vector(), &exp);
        }
        assert_eq!(transcoder.encodings(), calls * 2);
    }

    #[test]
    fn estimated_size_from() {
        let cases = vec![
            (vec![1.0, 2.0, 3.0, 4.0, 5.0, 6.0, 0.0, 0.0, 0.0, 0.0], 192),
            (vec![0.0, 0.0, 1.0, 2.0, 3.0, 4.0, 5.0, 6.0, 7.0, 8.0], 240),
            (vec![0.0, 0.0], 48),
            (vec![1.0, 2.0, 1.0], 96),
            (vec![1.0, 2.0, 1.0, 1.0], 96),
            (vec![1.0], 48),
        ];

        for (input, exp) in cases {
            assert_eq!(super::estimated_size_from(input.as_slice()), exp);
        }
    }

    #[test]
    fn estimated_size_from_iter() {
        let cases = vec![
            (vec![Some(0.0), Some(2.0), Some(1.0)], 96),
            (vec![Some(0.0), Some(0.0)], 48),
        ];

        for (input, exp) in cases {
            assert_eq!(super::estimated_size_from_iter(input.into_iter()), exp);
        }
    }
}
