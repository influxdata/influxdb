//! [`NonOverlappingOrderedLexicalRanges`] represents ranges of lexically ordered values.

use arrow::array::ArrayRef;
use arrow::compute::SortOptions;
use arrow::row::{Row, RowConverter, Rows, SortField};
use datafusion::common::{Result, ScalarValue, internal_err};
use datafusion::error::DataFusionError;
use std::fmt::Display;
use std::sync::Arc;
use tracing::trace;

/// Represents a range of sort key values within a lexical space.
///
/// # Lexical Space
///
/// The "Lexical Space" is all possible values of columns in a sort order (set
/// of sort expressions).
///
/// For example, given data with a sort order of `A ASC, B ASC`
/// (`A` ascending, `B` ascending), then the lexical space is all the unique
/// combinations of `(A, B)`.
///
/// # Lexical Range
///
/// The "lexical range" is defined by two points in a lexical space (the
/// minimum and maximum sort key values for some range)
///
/// For example, for data like
///
/// |`a`| `b` |
/// |---|-----|
/// | 1 | 100 |
/// | 1 | 200 |
/// | 1 | 300 |
/// | 2 | 100 |
/// | 2 | 200 |
/// | 3 | 50  |
///
/// The lexical range is
/// * `min --> max`
/// * `(a_min, b_min) -> (a_max, b_max)`
/// * `(1,100) --> (3,50)`
#[derive(Debug, Default, Clone, PartialEq, Eq)]
pub struct LexicalRange {
    /// The minimum multi-column value in the lexical space (one `ScalarValue`
    /// for each sort key)
    min: Vec<ScalarValue>,
    /// The maximum multi-column value in the lexical space (one `ScalarValue`
    /// for each sort key)
    max: Vec<ScalarValue>,
}

impl LexicalRange {
    /// Create a [`LexicalRangeBuilder`]
    pub fn builder() -> LexicalRangeBuilder {
        LexicalRangeBuilder::new()
    }

    /// Length of values in range.
    pub fn num_values(&self) -> usize {
        assert_eq!(
            self.min.len(),
            self.max.len(),
            "mins and maxes should have the same number of values"
        );
        self.min.len()
    }
}

impl Display for LexicalRange {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "({})->({})",
            self.min
                .iter()
                .map(|v| v.to_string())
                .collect::<Vec<_>>()
                .join(","),
            self.max
                .iter()
                .map(|v| v.to_string())
                .collect::<Vec<_>>()
                .join(",")
        )
    }
}

/// Builder for [`LexicalRange`]
///
/// This builds up a multi-column min/max range one pair at a time.
#[derive(Debug, Default, Clone)]
pub struct LexicalRangeBuilder {
    inner: LexicalRange,
}

impl LexicalRangeBuilder {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn build(self) -> LexicalRange {
        self.inner
    }

    /// Adds a min and max to the end of the in-progress ranges
    pub fn push(&mut self, min: ScalarValue, max: ScalarValue) {
        self.inner.min.push(min);
        self.inner.max.push(max);
    }
}

/// Represents a set of non overlapping [`LexicalRange`]s ordered according to
/// minimum value.
///
/// # Single Column
///
/// For example given a set of ranges
/// ```text
/// (11, 20)  // partition 0
/// (1,  10)  // partition 1
/// (21, 30)  // partition 2
/// ```
///
/// Then `[1, 0, 2]` is a non-overlapping ordered set of lexical ranges. When
/// ordered by minimum value: `(1, 10)` comes first, then `(11, 20)` and then
/// `(21, 30)`.
///
/// There are no `NonOverlappingOrderedLexicalRanges` if the ranges are not
/// disjoint (they overlap). For example, the following ranges overlap between
/// 11 and 15:
///
/// ```text
/// (11, 20)  // partition 0
/// (1,  15)  // partition 1
/// ```
///
#[derive(Debug)]
pub struct NonOverlappingOrderedLexicalRanges {
    /// lexical ranges, per partition.
    ///
    /// These are typically used to represent the value ranges of each
    /// DataFusion (not IOx) partition.
    value_ranges: Vec<LexicalRange>,

    /// Indexes into `value_ranges` that define a non overlapping ordering by
    /// minimum value.
    indices: Vec<usize>,

    /// Indicates that some of the lexical ranges, when ordered by [`Self::indices`],
    /// has identical sort keys.
    has_neighbors_with_same_sortkey: bool,
}

impl Display for NonOverlappingOrderedLexicalRanges {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "LexicalRanges\n  {:?}", self.indices,)
    }
}

impl NonOverlappingOrderedLexicalRanges {
    /// Attempt to create a new [`NonOverlappingOrderedLexicalRanges`] given the
    /// specified [`SortOptions`].
    ///
    /// See struct documentation for more details
    ///
    /// Returns None if:
    /// * - There are no ranges
    /// * - The ranges are overlap in lexical space (are not disjoint)
    ///
    /// Returns Err if there is an error converting values.
    pub fn try_new(
        sort_options: &[SortOptions],
        value_ranges: Vec<LexicalRange>,
    ) -> Result<Option<Self>> {
        if value_ranges.is_empty() {
            return Ok(None);
        }

        // convert to rows, for row ordering per sort key
        let rows = ConvertedRows::try_new_from_lexical_ranges(sort_options, value_ranges.clone())?;

        // order by minimum value
        let mut indices = (0..rows.num_rows()).collect::<Vec<_>>();
        indices.sort_by_key(|&i| rows.min_row(i));

        // check that the ranges are disjoint
        if !rows.are_disjoint(&indices) {
            return Ok(None);
        };

        // determine if any neighboring value_ranges are constants
        let has_neighbors_with_same_sortkey = if indices.is_empty() {
            false
        } else {
            indices
                .iter()
                .enumerate()
                .skip(1)
                .fold(false, |acc, (indices_idx, partition_idx)| {
                    let prev_partition_idx = indices[indices_idx - 1];
                    acc || (rows.min_value(*partition_idx).unwrap()
                        == rows.max_value(prev_partition_idx).unwrap()
                        || rows.max_value(*partition_idx).unwrap()
                            == rows.min_value(prev_partition_idx).unwrap())
                })
        };

        Ok(Some(Self {
            value_ranges,
            indices,
            has_neighbors_with_same_sortkey,
        }))
    }

    /// Indices that define an ordered list of non overlapping value ranges.
    pub fn indices(&self) -> &[usize] {
        &self.indices
    }

    /// Iterator over the in lexical ranges in order
    pub fn ordered_ranges(&self) -> impl Iterator<Item = &LexicalRange> {
        self.indices.iter().map(|i| &self.value_ranges[*i])
    }

    /// Returns true if any of the neighboring [`Self::value_ranges`], when ordered by [`Self::indices`],
    /// have the same sort key.
    pub fn has_neighbors_with_same_sortkey(&self) -> bool {
        self.has_neighbors_with_same_sortkey
    }
}

/// Result of converting multiple-column ScalarValue rows to columns.
#[derive(Debug)]
struct ConvertedRows {
    /// converter for mins and maxes
    ///
    /// Must use the same conveted rows for both, otherwise they cannot be
    /// compared.
    converter: RowConverter,

    mins: Rows,
    maxes: Rows,
}

impl ConvertedRows {
    /// Create new [`ConvertedRows`] from the vector of sort keys and specified options.
    ///
    /// Keys are in the format `VecPerPartition<VecPerSortKey<value>>`
    fn try_new(
        sort_options: &[SortOptions],
        min_keys: Vec<Vec<ScalarValue>>,
        max_keys: Vec<Vec<ScalarValue>>,
    ) -> Result<Self> {
        if sort_options.len() != min_keys[0].len() {
            return internal_err!(
                "Expected number of sort options ({}) to match number of sort keys in min_keys ({})",
                sort_options.len(),
                min_keys[0].len()
            );
        }
        if sort_options.len() != max_keys[0].len() {
            return internal_err!(
                "Expected number of sort options ({}) to match number of sort keys in max_keys ({})",
                sort_options.len(),
                max_keys.len()
            );
        }

        // build converter using min keys
        let arrays = pivot_to_arrays(min_keys)?;
        let converter_fields = arrays
            .iter()
            .zip(sort_options.iter())
            .map(|(a, options)| SortField::new_with_options(a.data_type().clone(), *options))
            .collect::<Vec<_>>();
        let converter = RowConverter::new(converter_fields)?;
        let mins = converter.convert_columns(&arrays)?;

        // build maxes
        let arrays = pivot_to_arrays(max_keys)?;
        let maxes = converter.convert_columns(&arrays)?;

        Ok(Self {
            converter,
            mins,
            maxes,
        })
    }

    fn try_new_from_lexical_ranges(
        sort_options: &[SortOptions],
        lexical_ranges: Vec<LexicalRange>,
    ) -> Result<Self> {
        // convert to min/maxes, as VecPerPartition<VecPerSortKey>
        let (mins, maxes) = lexical_ranges.clone().into_iter().fold(
            (vec![], vec![]),
            |(mut mins, mut maxes), partition_range| {
                let LexicalRange {
                    min: min_per_sort_key,
                    max: max_per_sort_key,
                } = partition_range;
                mins.push(min_per_sort_key);
                maxes.push(max_per_sort_key);
                (mins, maxes)
            },
        );
        Self::try_new(sort_options, mins, maxes)
    }

    /// Return the number of partitions (rows) in the converted rows.
    fn num_rows(&self) -> usize {
        self.mins.num_rows()
    }

    /// Return the min (as [`Row`]) at the specified index
    fn min_row(&self, index: usize) -> Row<'_> {
        self.mins.row(index)
    }

    /// Return the max (as [`Row`]) at the specified index
    fn max_row(&self, index: usize) -> Row<'_> {
        self.maxes.row(index)
    }

    /// Return the min value at the specified index as a list of single
    /// row arrays
    ///
    /// Used for debugging
    fn min_value(&self, index: usize) -> Result<Vec<ArrayRef>> {
        let values = self
            .converter
            .convert_rows([self.min_row(index)])
            .map_err(|e| DataFusionError::ArrowError(Box::new(e), None))?;
        Ok(values.iter().map(Arc::clone).collect::<Vec<_>>())
    }

    /// Return the max value at the specified index as a list of single
    /// row arrays
    ///
    /// Used for debugging
    fn max_value(&self, index: usize) -> Result<Vec<ArrayRef>> {
        let values = self
            .converter
            .convert_rows([self.max_row(index)])
            .map_err(|e| DataFusionError::ArrowError(Box::new(e), None))?;
        Ok(values.iter().map(Arc::clone).collect::<Vec<_>>())
    }

    /// Return true if ranges are disjoint assuming the ranges are sorted by
    /// `ordered_by_min_partition_indices`
    ///
    /// This checks each pair of adjacent ranges in the sorted order.
    ///
    /// (0 -> 10)  (11 -> 20)  (21 -> 30) => disjoint=true
    ///      output = true
    ///
    /// (0 -> 10)  (10 -> 20)  (21 -> 30) => disjoint=true because 10==10 and does not invalidate the ordering
    ///      output = true
    ///
    /// (0 -> 13)  (12 -> 20)  (21 -> 30) => disjoint=false
    ///      output = false
    ///
    /// (1 -> 1)  (1 -> 1)  (1 -> 1) => disjoint=true for constants
    ///      output = true
    ///
    fn are_disjoint(&self, ordered_by_min_partition_indices: &[usize]) -> bool {
        for index_index in 1..ordered_by_min_partition_indices.len() {
            let index = ordered_by_min_partition_indices[index_index];
            let prev_index = ordered_by_min_partition_indices[index_index - 1];

            // Ordering is by sort key, and may be desc.
            // Therefore need to confirm that the min & max of the current range is greater than the previous range.
            let start_exclusive = self.min_row(index) > self.min_row(prev_index)
                && self.min_row(index) >= self.max_row(prev_index);
            let end_exclusive = self.max_row(index) >= self.min_row(prev_index)
                && self.max_row(index) > self.max_row(prev_index);

            // are constants
            let curr_constant = self.min_row(index) == self.max_row(index);
            let prev_constant = self.min_row(prev_index) == self.max_row(prev_index);
            let constants_are_equal =
                curr_constant && prev_constant && self.min_row(index) == self.min_row(prev_index);

            if !((start_exclusive && end_exclusive) || constants_are_equal) {
                trace!(
                    "ranges are not disjoint: {:?} <= {:?}",
                    self.min_value(index),
                    self.max_value(prev_index)
                );
                return false;
            }
        }
        true
    }
}

/// Convert a multi-column ScalarValue row to columns
fn pivot_to_arrays(keys: Vec<Vec<ScalarValue>>) -> Result<Vec<ArrayRef>> {
    let mut arrays = vec![];
    for col in 0..keys[0].len() {
        let mut column = vec![];
        for row in &keys {
            // todo avoid this clone (with take)
            column.push(row[col].clone());
        }
        arrays.push(ScalarValue::iter_to_array(column)?)
    }
    Ok(arrays)
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::compute::SortOptions;
    use datafusion::scalar::ScalarValue;
    use itertools::Itertools;

    struct TestCase {
        partitions: Vec<TestPartition>,
        num_sort_keys: usize,
        name: &'static str,
        expected_ranges_per_partition: Vec<&'static str>, // before ordering
        expect_disjoint: bool,
        expected_ordered_indices: Vec<ExpectedOrderedIndices>, // after ordering
    }

    impl TestCase {
        fn run(self) {
            println!("Beginning test : {}", self.name);
            // Test: confirm found proper lexical ranges
            let lexical_ranges = self.build_lexical_ranges();
            let expected = self
                .expected_ranges_per_partition
                .iter()
                .map(|str| str.to_string())
                .collect_vec();
            let actual = lexical_ranges
                .iter()
                .map(|range| format!("{range}"))
                .collect_vec();
            assert_eq!(
                actual, expected,
                "ERROR {}: expected ranges {:?} but found {:?}",
                self.name, expected, actual
            );

            // Test: confirm found proper non overlapping (or not) ranges per given sort ordering
            [
                TestSortOption::AscNullsFirst,
                TestSortOption::AscNullsLast,
                TestSortOption::DescNullsFirst,
                TestSortOption::DescNullsLast,
            ].into_iter().for_each(|sort_ordering| {
                if let Some(nonoverlapping) = NonOverlappingOrderedLexicalRanges::try_new(&sort_ordering.sort_options(self.num_sort_keys), lexical_ranges.clone()).expect("should not error") {
                    assert!(self.expect_disjoint,
                            "ERROR {} for {:?}: expected ranges to overlap, instead found disjoint ranges",
                            self.name, &sort_ordering);

                    let expected_ordered_indices = self.find_expected_indices(&sort_ordering);
                    assert_eq!(expected_ordered_indices, nonoverlapping.indices(),
                               "ERROR {} for {:?}: expected to find indices ordered {:?}, instead found ordering {:?}",
                               self.name, &sort_ordering, expected_ordered_indices, nonoverlapping.indices());
                } else {
                    assert!(!self.expect_disjoint,
                            "ERROR {} for {:?}: expected to find disjoint ranges, instead could either not detect ranges or found overlapping ranges",
                            self.name, &sort_ordering);
                };
            });
        }

        fn build_lexical_ranges(&self) -> Vec<LexicalRange> {
            self.partitions
                .iter()
                .map(|partition| {
                    let mut builder = LexicalRange::builder();
                    for SortKeyRange { min, max } in &partition.range_per_sort_key {
                        builder.push(min.clone(), max.clone());
                    }
                    builder.build()
                })
                .collect_vec()
        }

        fn find_expected_indices(&self, sort_ordering: &TestSortOption) -> &[usize] {
            self.expected_ordered_indices
                .iter()
                .find(|ord| ord.sort_ordering == *sort_ordering)
                .expect("should have expected outcome")
                .expected_indices
                .as_ref()
        }
    }

    struct TestPartition {
        range_per_sort_key: Vec<SortKeyRange>,
    }

    /// Range of a sort key. Note that this is not impacted by directionality of ordering (e.g. [`SortOptions`]).
    struct SortKeyRange {
        min: ScalarValue,
        max: ScalarValue,
    }

    fn build_partition_with_single_sort_key(ints: (Option<i64>, Option<i64>)) -> TestPartition {
        let range_per_sort_key = vec![SortKeyRange {
            min: ScalarValue::Int64(ints.0),
            max: ScalarValue::Int64(ints.1),
        }];
        TestPartition { range_per_sort_key }
    }

    /// Build min/maxes for a partition with three sort keys:
    /// 1. An integer sort key
    /// 2. A string sort key
    /// 3. A timestamp sort key (nanoseconds)
    fn build_partition_with_multiple_sort_keys(
        ints: (Option<i64>, Option<i64>),
        strings: (Option<&str>, Option<&str>),
        times: (Option<i64>, Option<i64>),
    ) -> TestPartition {
        let range_per_sort_key = vec![
            SortKeyRange {
                min: ScalarValue::Int64(ints.0),
                max: ScalarValue::Int64(ints.1),
            },
            SortKeyRange {
                min: ScalarValue::from(strings.0),
                max: ScalarValue::from(strings.1),
            },
            SortKeyRange {
                min: ScalarValue::TimestampNanosecond(times.0, None),
                max: ScalarValue::TimestampNanosecond(times.1, None),
            },
        ];
        TestPartition { range_per_sort_key }
    }

    #[derive(Debug, PartialEq)]
    enum TestSortOption {
        AscNullsLast,
        AscNullsFirst,
        DescNullsLast,
        DescNullsFirst,
    }

    impl TestSortOption {
        fn sort_options(&self, len: usize) -> Vec<SortOptions> {
            match self {
                Self::AscNullsLast => std::iter::repeat_n(
                    SortOptions {
                        descending: false,
                        nulls_first: false,
                    },
                    len,
                )
                .collect_vec(),
                Self::AscNullsFirst => std::iter::repeat_n(
                    SortOptions {
                        descending: false,
                        nulls_first: true,
                    },
                    len,
                )
                .collect_vec(),
                Self::DescNullsLast => std::iter::repeat_n(
                    SortOptions {
                        descending: true,
                        nulls_first: false,
                    },
                    len,
                )
                .collect_vec(),
                Self::DescNullsFirst => std::iter::repeat_n(
                    SortOptions {
                        descending: true,
                        nulls_first: true,
                    },
                    len,
                )
                .collect_vec(),
            }
        }
    }

    struct ExpectedOrderedIndices {
        /// the ordering (e.g. [`SortOptions`]) applied to all columns in the sort key.
        sort_ordering: TestSortOption,
        /// Expected outcome ordering with this sort_ordering applied.
        expected_indices: Vec<usize>,
    }

    impl From<(TestSortOption, Vec<usize>)> for ExpectedOrderedIndices {
        fn from(value: (TestSortOption, Vec<usize>)) -> Self {
            Self {
                sort_ordering: value.0,
                expected_indices: value.1,
            }
        }
    }

    #[test]
    fn test_disjointness_single_key() {
        let cases = [
            TestCase {
                partitions: vec![
                    build_partition_with_single_sort_key((Some(1), Some(10))),
                    build_partition_with_single_sort_key((Some(2), Some(10))),
                    build_partition_with_single_sort_key((Some(0), Some(0))),
                ],
                num_sort_keys: 1,
                name: "order_by_single_sort_key__overlapping",
                expected_ranges_per_partition: vec!["(1)->(10)", "(2)->(10)", "(0)->(0)"],
                expect_disjoint: false,
                expected_ordered_indices: vec![],
            },
            TestCase {
                partitions: vec![
                    build_partition_with_single_sort_key((Some(1), Some(10))),
                    build_partition_with_single_sort_key((Some(11), Some(20))),
                    build_partition_with_single_sort_key((Some(0), Some(0))),
                ],
                num_sort_keys: 1,
                name: "order_by_single_sort_key__disjoint",
                expected_ranges_per_partition: vec!["(1)->(10)", "(11)->(20)", "(0)->(0)"],
                expect_disjoint: true,
                expected_ordered_indices: vec![
                    (TestSortOption::AscNullsFirst, vec![2, 0, 1]).into(),
                    (TestSortOption::AscNullsLast, vec![2, 0, 1]).into(),
                    (TestSortOption::DescNullsFirst, vec![1, 0, 2]).into(),
                    (TestSortOption::DescNullsLast, vec![1, 0, 2]).into(),
                ],
            },
        ];

        cases.into_iter().for_each(|test_case| test_case.run());
    }

    #[test]
    fn test_disjointness_multiple_sort_keys() {
        let cases = [
            /* Using the first sort key, an integer, as the decider. */
            TestCase {
                partitions: vec![
                    build_partition_with_multiple_sort_keys(
                        (Some(1), Some(10)),
                        (Some("same"), Some("same")),
                        (Some(1), Some(1)),
                    ),
                    build_partition_with_multiple_sort_keys(
                        (Some(2), Some(10)),
                        (Some("same"), Some("same")),
                        (Some(1), Some(1)),
                    ),
                    build_partition_with_multiple_sort_keys(
                        (Some(0), Some(0)),
                        (Some("same"), Some("same")),
                        (Some(1), Some(1)),
                    ),
                ],
                num_sort_keys: 3,
                name: "order_by_first_sort_key__overlapping",
                expected_ranges_per_partition: vec![
                    "(1,same,1)->(10,same,1)",
                    "(2,same,1)->(10,same,1)",
                    "(0,same,1)->(0,same,1)",
                ],
                expect_disjoint: false,
                expected_ordered_indices: vec![],
            },
            TestCase {
                partitions: vec![
                    build_partition_with_multiple_sort_keys(
                        (Some(1), Some(10)),
                        (Some("same"), Some("same")),
                        (Some(1), Some(1)),
                    ),
                    build_partition_with_multiple_sort_keys(
                        (Some(11), Some(20)),
                        (Some("same"), Some("same")),
                        (Some(1), Some(1)),
                    ),
                    build_partition_with_multiple_sort_keys(
                        (Some(0), Some(0)),
                        (Some("same"), Some("same")),
                        (Some(1), Some(1)),
                    ),
                ],
                num_sort_keys: 3,
                name: "order_by_first_sort_key__disjoint",
                expected_ranges_per_partition: vec![
                    "(1,same,1)->(10,same,1)",
                    "(11,same,1)->(20,same,1)",
                    "(0,same,1)->(0,same,1)",
                ],
                expect_disjoint: true,
                expected_ordered_indices: vec![
                    (TestSortOption::AscNullsFirst, vec![2, 0, 1]).into(),
                    (TestSortOption::AscNullsLast, vec![2, 0, 1]).into(),
                    (TestSortOption::DescNullsFirst, vec![1, 0, 2]).into(),
                    (TestSortOption::DescNullsLast, vec![1, 0, 2]).into(),
                ],
            },
            /* Using the middle sort key, a string, as the decider. */
            TestCase {
                partitions: vec![
                    build_partition_with_multiple_sort_keys(
                        (Some(1), Some(1)),
                        (Some("a"), Some("d")),
                        (Some(1), Some(1)),
                    ),
                    build_partition_with_multiple_sort_keys(
                        (Some(1), Some(1)),
                        (Some("f"), Some("g")),
                        (Some(1), Some(1)),
                    ),
                    build_partition_with_multiple_sort_keys(
                        (Some(1), Some(1)),
                        (Some("c"), Some("e")),
                        (Some(1), Some(1)),
                    ),
                ],
                num_sort_keys: 3,
                name: "order_by_middle_sort_key__overlapping",
                expected_ranges_per_partition: vec![
                    "(1,a,1)->(1,d,1)",
                    "(1,f,1)->(1,g,1)",
                    "(1,c,1)->(1,e,1)",
                ],
                expect_disjoint: false,
                expected_ordered_indices: vec![],
            },
            TestCase {
                partitions: vec![
                    build_partition_with_multiple_sort_keys(
                        (Some(1), Some(1)),
                        (Some("a"), Some("b")),
                        (Some(1), Some(1)),
                    ),
                    build_partition_with_multiple_sort_keys(
                        (Some(1), Some(1)),
                        (Some("f"), Some("g")),
                        (Some(1), Some(1)),
                    ),
                    build_partition_with_multiple_sort_keys(
                        (Some(1), Some(1)),
                        (Some("c"), Some("e")),
                        (Some(1), Some(1)),
                    ),
                ],
                num_sort_keys: 3,
                name: "order_by_middle_sort_key__disjoint",
                expected_ranges_per_partition: vec![
                    "(1,a,1)->(1,b,1)",
                    "(1,f,1)->(1,g,1)",
                    "(1,c,1)->(1,e,1)",
                ],
                expect_disjoint: true,
                expected_ordered_indices: vec![
                    (TestSortOption::AscNullsFirst, vec![0, 2, 1]).into(),
                    (TestSortOption::AscNullsLast, vec![0, 2, 1]).into(),
                    (TestSortOption::DescNullsFirst, vec![1, 2, 0]).into(),
                    (TestSortOption::DescNullsLast, vec![1, 2, 0]).into(),
                ],
            },
            /* Using the last sort key, a nanosecond timestamp, as the decider. */
            TestCase {
                partitions: vec![
                    build_partition_with_multiple_sort_keys(
                        (Some(1), Some(1)),
                        (Some("same"), Some("same")),
                        (Some(50000000), Some(50000001)),
                    ),
                    build_partition_with_multiple_sort_keys(
                        (Some(1), Some(1)),
                        (Some("same"), Some("same")),
                        (Some(700000), Some(50000001)),
                    ),
                    build_partition_with_multiple_sort_keys(
                        (Some(1), Some(1)),
                        (Some("same"), Some("same")),
                        (Some(100000000), Some(100000001)),
                    ),
                ],
                num_sort_keys: 3,
                name: "order_by_last_sort_key__overlapping",
                expected_ranges_per_partition: vec![
                    "(1,same,50000000)->(1,same,50000001)",
                    "(1,same,700000)->(1,same,50000001)",
                    "(1,same,100000000)->(1,same,100000001)",
                ],
                expect_disjoint: false,
                expected_ordered_indices: vec![],
            },
            TestCase {
                partitions: vec![
                    build_partition_with_multiple_sort_keys(
                        (Some(1), Some(1)),
                        (Some("same"), Some("same")),
                        (Some(50000000), Some(50000001)),
                    ),
                    build_partition_with_multiple_sort_keys(
                        (Some(1), Some(1)),
                        (Some("same"), Some("same")),
                        (Some(700000), Some(7000001)),
                    ),
                    build_partition_with_multiple_sort_keys(
                        (Some(1), Some(1)),
                        (Some("same"), Some("same")),
                        (Some(100000000), Some(100000001)),
                    ),
                ],
                num_sort_keys: 3,
                name: "order_by_last_sort_key__disjoint",
                expected_ranges_per_partition: vec![
                    "(1,same,50000000)->(1,same,50000001)",
                    "(1,same,700000)->(1,same,7000001)",
                    "(1,same,100000000)->(1,same,100000001)",
                ],
                expect_disjoint: true,
                expected_ordered_indices: vec![
                    (TestSortOption::AscNullsFirst, vec![1, 0, 2]).into(),
                    (TestSortOption::AscNullsLast, vec![1, 0, 2]).into(),
                    (TestSortOption::DescNullsFirst, vec![2, 0, 1]).into(),
                    (TestSortOption::DescNullsLast, vec![2, 0, 1]).into(),
                ],
            },
        ];

        cases.into_iter().for_each(|test_case| test_case.run());
    }

    #[test]
    fn test_disjointness_with_nulls() {
        let cases = [
            TestCase {
                partitions: vec![
                    build_partition_with_single_sort_key((Some(1), Some(10))),
                    build_partition_with_single_sort_key((Some(2), Some(10))),
                    build_partition_with_single_sort_key((None, None)),
                ],
                num_sort_keys: 1,
                name: "order_by_nulls__overlapping",
                expected_ranges_per_partition: vec!["(1)->(10)", "(2)->(10)", "(NULL)->(NULL)"],
                expect_disjoint: false,
                expected_ordered_indices: vec![],
            },
            TestCase {
                partitions: vec![
                    build_partition_with_single_sort_key((Some(1), Some(10))),
                    build_partition_with_single_sort_key((Some(11), Some(20))),
                    build_partition_with_single_sort_key((None, None)),
                ],
                num_sort_keys: 1,
                name: "order_by_nulls__disjoint",
                expected_ranges_per_partition: vec!["(1)->(10)", "(11)->(20)", "(NULL)->(NULL)"],
                expect_disjoint: true,
                expected_ordered_indices: vec![
                    (TestSortOption::AscNullsFirst, vec![2, 0, 1]).into(),
                    (TestSortOption::AscNullsLast, vec![0, 1, 2]).into(),
                    (TestSortOption::DescNullsFirst, vec![2, 1, 0]).into(),
                    (TestSortOption::DescNullsLast, vec![1, 0, 2]).into(),
                ],
            },
            TestCase {
                partitions: vec![
                    build_partition_with_multiple_sort_keys(
                        (Some(1), Some(1)),
                        (None, Some("e")),
                        (Some(1), Some(1)),
                    ),
                    build_partition_with_multiple_sort_keys(
                        (Some(1), Some(1)),
                        (Some("e"), None),
                        (Some(1), Some(1)),
                    ),
                ],
                num_sort_keys: 3,
                name: "order_by_nulls__overlap",
                expected_ranges_per_partition: vec!["(1,NULL,1)->(1,e,1)", "(1,e,1)->(1,NULL,1)"],
                expect_disjoint: false,
                expected_ordered_indices: vec![],
            },
            TestCase {
                partitions: vec![
                    build_partition_with_multiple_sort_keys(
                        (Some(10), Some(20)),
                        (None, Some("c")),
                        (Some(1), Some(1)),
                    ),
                    build_partition_with_multiple_sort_keys(
                        (Some(21), Some(22)),
                        (Some("e"), Some("f")),
                        (Some(2), Some(3)),
                    ),
                ],
                num_sort_keys: 3,
                name: "order_by_nulls__disjoint2",
                expected_ranges_per_partition: vec!["(10,NULL,1)->(20,c,1)", "(21,e,2)->(22,f,3)"],
                expect_disjoint: true,
                expected_ordered_indices: vec![
                    (TestSortOption::AscNullsFirst, vec![0, 1]).into(),
                    (TestSortOption::AscNullsLast, vec![0, 1]).into(),
                    (TestSortOption::DescNullsFirst, vec![1, 0]).into(),
                    (TestSortOption::DescNullsLast, vec![1, 0]).into(),
                ],
            },
        ];

        cases.into_iter().for_each(|test_case| test_case.run());
    }

    #[test]
    fn test_disjointness_with_touching_disjoint_ranges() {
        let cases = [
            /* Using the first sort key, an integer, and has a start(current)==end(previous). */
            TestCase {
                partitions: vec![
                    build_partition_with_multiple_sort_keys(
                        (Some(1), Some(10)), // ends with 10
                        (Some("same"), Some("same")),
                        (Some(1), Some(1)),
                    ),
                    build_partition_with_multiple_sort_keys(
                        (Some(10), Some(20)), // starts with 10
                        (Some("same"), Some("same")),
                        (Some(1), Some(1)),
                    ),
                    build_partition_with_multiple_sort_keys(
                        (Some(0), Some(0)),
                        (Some("same"), Some("same")),
                        (Some(1), Some(1)),
                    ),
                ],
                num_sort_keys: 3,
                name: "order_by_first_sort_key__disjoint_touching",
                expected_ranges_per_partition: vec![
                    "(1,same,1)->(10,same,1)",
                    "(10,same,1)->(20,same,1)",
                    "(0,same,1)->(0,same,1)",
                ],
                expect_disjoint: true,
                expected_ordered_indices: vec![
                    (TestSortOption::AscNullsFirst, vec![2, 0, 1]).into(),
                    (TestSortOption::AscNullsLast, vec![2, 0, 1]).into(),
                    (TestSortOption::DescNullsFirst, vec![1, 0, 2]).into(),
                    (TestSortOption::DescNullsLast, vec![1, 0, 2]).into(),
                ],
            },
            /* Using the middle sort key, a string, as the decider, and has a start(current)==end(previous) */
            TestCase {
                partitions: vec![
                    build_partition_with_multiple_sort_keys(
                        (Some(1), Some(1)),
                        (Some("a"), Some("c")), // ends with c
                        (Some(1), Some(1)),
                    ),
                    build_partition_with_multiple_sort_keys(
                        (Some(1), Some(1)),
                        (Some("f"), Some("g")),
                        (Some(1), Some(1)),
                    ),
                    build_partition_with_multiple_sort_keys(
                        (Some(1), Some(1)),
                        (Some("c"), Some("e")), // starts with c
                        (Some(1), Some(1)),
                    ),
                ],
                num_sort_keys: 3,
                name: "order_by_middle_sort_key__disjoint_touching",
                expected_ranges_per_partition: vec![
                    "(1,a,1)->(1,c,1)",
                    "(1,f,1)->(1,g,1)",
                    "(1,c,1)->(1,e,1)",
                ],
                expect_disjoint: true,
                expected_ordered_indices: vec![
                    (TestSortOption::AscNullsFirst, vec![0, 2, 1]).into(),
                    (TestSortOption::AscNullsLast, vec![0, 2, 1]).into(),
                    (TestSortOption::DescNullsFirst, vec![1, 2, 0]).into(),
                    (TestSortOption::DescNullsLast, vec![1, 2, 0]).into(),
                ],
            },
            /* Using the last sort key, a nanosecond timestamp, as the decider, and has a start(current)==end(previous) */
            TestCase {
                partitions: vec![
                    build_partition_with_multiple_sort_keys(
                        (Some(1), Some(1)),
                        (Some("same"), Some("same")),
                        (Some(50000000), Some(50000001)), // ends with 50000001
                    ),
                    build_partition_with_multiple_sort_keys(
                        (Some(1), Some(1)),
                        (Some("same"), Some("same")),
                        (Some(700000), Some(7000001)),
                    ),
                    build_partition_with_multiple_sort_keys(
                        (Some(1), Some(1)),
                        (Some("same"), Some("same")),
                        (Some(50000001), Some(100000001)), // starts with 50000001
                    ),
                ],
                num_sort_keys: 3,
                name: "order_by_last_sort_key__disjoint_touching",
                expected_ranges_per_partition: vec![
                    "(1,same,50000000)->(1,same,50000001)",
                    "(1,same,700000)->(1,same,7000001)",
                    "(1,same,50000001)->(1,same,100000001)",
                ],
                expect_disjoint: true,
                expected_ordered_indices: vec![
                    (TestSortOption::AscNullsFirst, vec![1, 0, 2]).into(),
                    (TestSortOption::AscNullsLast, vec![1, 0, 2]).into(),
                    (TestSortOption::DescNullsFirst, vec![2, 0, 1]).into(),
                    (TestSortOption::DescNullsLast, vec![2, 0, 1]).into(),
                ],
            },
        ];

        cases.into_iter().for_each(|test_case| test_case.run());
    }

    #[test]
    fn test_disjointness_constants() {
        let cases = [
            /* All sort keys are constant */
            TestCase {
                partitions: vec![
                    build_partition_with_multiple_sort_keys(
                        (Some(10), Some(10)),
                        (Some("same"), Some("same")),
                        (Some(1), Some(1)),
                    ),
                    build_partition_with_multiple_sort_keys(
                        (Some(10), Some(10)),
                        (Some("same"), Some("same")),
                        (Some(1), Some(1)),
                    ),
                    build_partition_with_multiple_sort_keys(
                        (Some(10), Some(10)),
                        (Some("same"), Some("same")),
                        (Some(1), Some(1)),
                    ),
                ],
                num_sort_keys: 3,
                name: "all_sort_keys_constant_across_partitions",
                expected_ranges_per_partition: vec![
                    "(10,same,1)->(10,same,1)",
                    "(10,same,1)->(10,same,1)",
                    "(10,same,1)->(10,same,1)",
                ],
                expect_disjoint: true,
                expected_ordered_indices: vec![
                    (TestSortOption::AscNullsFirst, vec![0, 1, 2]).into(),
                    (TestSortOption::AscNullsLast, vec![0, 1, 2]).into(),
                    (TestSortOption::DescNullsFirst, vec![0, 1, 2]).into(),
                    (TestSortOption::DescNullsLast, vec![0, 1, 2]).into(),
                ],
            },
            /* First two sort keys are constant, 3rd is disjoint (non-overlapping) */
            TestCase {
                partitions: vec![
                    build_partition_with_multiple_sort_keys(
                        (Some(10), Some(10)),
                        (Some("same"), Some("same")),
                        (Some(2), Some(2)),
                    ),
                    build_partition_with_multiple_sort_keys(
                        (Some(10), Some(10)),
                        (Some("same"), Some("same")),
                        (Some(1), Some(1)),
                    ),
                    build_partition_with_multiple_sort_keys(
                        (Some(10), Some(10)),
                        (Some("same"), Some("same")),
                        (Some(3), Some(3)),
                    ),
                ],
                num_sort_keys: 3,
                name: "last_sort_key_is_disjoint_across_partitions",
                expected_ranges_per_partition: vec![
                    "(10,same,2)->(10,same,2)",
                    "(10,same,1)->(10,same,1)",
                    "(10,same,3)->(10,same,3)",
                ],
                expect_disjoint: true,
                expected_ordered_indices: vec![
                    (TestSortOption::AscNullsFirst, vec![1, 0, 2]).into(),
                    (TestSortOption::AscNullsLast, vec![1, 0, 2]).into(),
                    (TestSortOption::DescNullsFirst, vec![2, 0, 1]).into(),
                    (TestSortOption::DescNullsLast, vec![2, 0, 1]).into(),
                ],
            },
            /* First two sort keys are constant, 3rd is overlapping */
            TestCase {
                partitions: vec![
                    build_partition_with_multiple_sort_keys(
                        (Some(10), Some(10)),
                        (Some("same"), Some("same")),
                        (Some(2), Some(2)),
                    ),
                    build_partition_with_multiple_sort_keys(
                        (Some(10), Some(10)),
                        (Some("same"), Some("same")),
                        (Some(0), Some(1)),
                    ),
                    build_partition_with_multiple_sort_keys(
                        (Some(10), Some(10)),
                        (Some("same"), Some("same")),
                        (Some(0), Some(1)),
                    ),
                ],
                num_sort_keys: 3,
                name: "last_sort_key_is_overlapping_across_partitions",
                expected_ranges_per_partition: vec![
                    "(10,same,2)->(10,same,2)",
                    "(10,same,0)->(10,same,1)",
                    "(10,same,0)->(10,same,1)",
                ],
                expect_disjoint: false,
                expected_ordered_indices: vec![],
            },
            /* First two sort keys are constant, 3rd is touching */
            TestCase {
                partitions: vec![
                    build_partition_with_multiple_sort_keys(
                        (Some(10), Some(10)),
                        (Some("same"), Some("same")),
                        (Some(2), Some(2)),
                    ),
                    build_partition_with_multiple_sort_keys(
                        (Some(10), Some(10)),
                        (Some("same"), Some("same")),
                        (Some(1), Some(1)),
                    ),
                    build_partition_with_multiple_sort_keys(
                        (Some(10), Some(10)),
                        (Some("same"), Some("same")),
                        (Some(1), Some(1)),
                    ),
                ],
                num_sort_keys: 3,
                name: "last_sort_key_is_touching_but_nonoverlapping_across_partitions",
                expected_ranges_per_partition: vec![
                    "(10,same,2)->(10,same,2)",
                    "(10,same,1)->(10,same,1)",
                    "(10,same,1)->(10,same,1)",
                ],
                expect_disjoint: true,
                expected_ordered_indices: vec![
                    (TestSortOption::AscNullsFirst, vec![1, 2, 0]).into(),
                    (TestSortOption::AscNullsLast, vec![1, 2, 0]).into(),
                    (TestSortOption::DescNullsFirst, vec![0, 1, 2]).into(),
                    (TestSortOption::DescNullsLast, vec![0, 1, 2]).into(),
                ],
            },
            /* Last two sort keys are constant, 1st is disjoint (non-overlapping) */
            TestCase {
                partitions: vec![
                    build_partition_with_multiple_sort_keys(
                        (Some(30), Some(30)),
                        (Some("same"), Some("same")),
                        (Some(1), Some(1)),
                    ),
                    build_partition_with_multiple_sort_keys(
                        (Some(10), Some(10)),
                        (Some("same"), Some("same")),
                        (Some(1), Some(1)),
                    ),
                    build_partition_with_multiple_sort_keys(
                        (Some(20), Some(20)),
                        (Some("same"), Some("same")),
                        (Some(1), Some(1)),
                    ),
                ],
                num_sort_keys: 3,
                name: "first_sort_key_is_disjoint_across_partitions",
                expected_ranges_per_partition: vec![
                    "(30,same,1)->(30,same,1)",
                    "(10,same,1)->(10,same,1)",
                    "(20,same,1)->(20,same,1)",
                ],
                expect_disjoint: true,
                expected_ordered_indices: vec![
                    (TestSortOption::AscNullsFirst, vec![1, 2, 0]).into(),
                    (TestSortOption::AscNullsLast, vec![1, 2, 0]).into(),
                    (TestSortOption::DescNullsFirst, vec![0, 2, 1]).into(),
                    (TestSortOption::DescNullsLast, vec![0, 2, 1]).into(),
                ],
            },
            /* Last two sort keys are constant, 1st is overlapping */
            TestCase {
                partitions: vec![
                    build_partition_with_multiple_sort_keys(
                        (Some(30), Some(30)),
                        (Some("same"), Some("same")),
                        (Some(1), Some(1)),
                    ),
                    build_partition_with_multiple_sort_keys(
                        (Some(10), Some(10)),
                        (Some("same"), Some("same")),
                        (Some(1), Some(1)),
                    ),
                    build_partition_with_multiple_sort_keys(
                        (Some(20), Some(31)),
                        (Some("same"), Some("same")),
                        (Some(1), Some(1)),
                    ),
                ],
                num_sort_keys: 3,
                name: "first_sort_key_is_disjoint_across_partitions",
                expected_ranges_per_partition: vec![
                    "(30,same,1)->(30,same,1)",
                    "(10,same,1)->(10,same,1)",
                    "(20,same,1)->(31,same,1)",
                ],
                expect_disjoint: false,
                expected_ordered_indices: vec![],
            },
        ];

        cases.into_iter().for_each(|test_case| test_case.run());
    }
}
